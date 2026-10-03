"""
Spec 015 offline tests: the zip download record, the scope "all" fix, and
scan selection by scan_id (FR-002, FR-017, research R3).
"""
from __future__ import annotations

import hashlib
import json
import zipfile
from pathlib import Path

import pytest

from app.logic.download import assemble_zip
from app.logic.download_manifest import ManifestIdentity
from tests.fakes.scan_tree_fake import ScanTreeFake

PROJECT = "P015"
IDENTITY = ManifestIdentity(username="student_a", server_url="http://fake.local")


def _server() -> ScanTreeFake:
    fake = ScanTreeFake(project_name=PROJECT)
    fake.add_files("S1", "E1", "7", "SRC", [("0001-uidA.dcm", b"src-1"), ("0002-uidA.dcm", b"src-2")])
    fake.add_files("S1", "E1", "7", "DERIVED", [("mask.csv", b"0,1,1")])
    return fake


def _row(**extra):
    return [dict({"subject": "S1", "experiment": "E1", "scan_id": "7"}, **extra)]


def _strip(m):
    return {k: v for k, v in m.items() if k != "server_copy"}


def test_manifest_inside_and_beside_zip(tmp_path):  # T019
    fake = _server()
    zp = tmp_path / "cases.zip"
    out = assemble_zip(fake, PROJECT, _row(), zp, scope="all", identity=IDENTITY, uploader=fake)
    assert out.ok
    beside = tmp_path / "cases.manifest.json"
    assert out.manifest_path == beside and beside.exists()
    with zipfile.ZipFile(zp) as zf:
        inside = json.loads(zf.read("download_manifest.json"))
        members = [n for n in zf.namelist() if n != "download_manifest.json"]
        for e in inside["files"]:
            assert hashlib.sha256(zf.read(e["relative_path"])).hexdigest() == e["sha256"]
    outside = json.loads(beside.read_text(encoding="utf-8"))
    assert _strip(inside) == _strip(outside)
    assert inside["server_copy"]["status"] == "pending"
    assert outside["server_copy"]["status"] == "uploaded"
    assert sorted(members) == sorted(e["relative_path"] for e in inside["files"])
    jsonschema = pytest.importorskip("jsonschema")
    schema = json.loads((Path(__file__).resolve().parents[1] / "specs" / "015-download-manifest"
                         / "contracts" / "download-manifest.schema.json").read_text(encoding="utf-8"))
    jsonschema.validate(inside, schema)
    jsonschema.validate(outside, schema)


def test_scope_all_source_derived(tmp_path):  # T020
    labels = {}
    for scope in ("all", "source", "derived"):
        zp = tmp_path / f"{scope}.zip"
        out = assemble_zip(_server(), PROJECT, _row(), zp, scope=scope)
        assert out.ok
        with zipfile.ZipFile(zp) as zf:
            m = json.loads(zf.read("download_manifest.json"))
        assert m["scope"] == scope
        labels[scope] = {e["resource_label"] for e in m["files"]}
    assert labels == {"all": {"SRC", "DERIVED"}, "source": {"SRC"}, "derived": {"DERIVED"}}


def test_scan_id_wins_over_scan_type(tmp_path):  # T021
    zp = tmp_path / "pick.zip"
    out = assemble_zip(_server(), PROJECT, _row(scan_type="XA"), zp, scope="source")
    assert out.ok and len(out.files_written) == 2
    with zipfile.ZipFile(zp) as zf:
        m = json.loads(zf.read("download_manifest.json"))
    assert {e["scan_query_string"] for e in m["files"]} == {
        f"/projects/{PROJECT}/subjects/S1/experiments/E1/scans/7"
    }


def test_zip_without_identity_records_nulls(tmp_path):
    zp = tmp_path / "anon.zip"
    out = assemble_zip(_server(), PROJECT, _row(), zp)
    m = json.loads(out.manifest_path.read_text(encoding="utf-8"))
    assert m["username"] is None and m["server_url"] is None
    assert m["server_copy"]["status"] == "not_attempted"


def test_folder_download_keeps_same_named_files_apart(tmp_path):
    """Found live 2026-10-03: two analysis versions both ship analysis.json; the
    flat folder layout overwrote one with the other and the manifest hash no
    longer matched the server. Non-source resources get their own subfolder."""
    from app.logic.download import download_selection

    fake = _server()
    fake.add_files("S1", "E1", "7", "KNEE_FLEXION_ANGLE__v1", [("analysis.json", b"v1")])
    fake.add_files("S1", "E1", "7", "KNEE_FLEXION_ANGLE__v2", [("analysis.json", b"v2")])
    out = download_selection(fake, PROJECT, _row(), tmp_path, identity=IDENTITY)
    assert out.ok, out.friendly
    entries = json.loads(Path(out.manifest_path).read_text())["files"]
    by_label = {e["resource_label"]: e for e in entries if e["filename"] == "analysis.json"}
    assert set(by_label) == {"KNEE_FLEXION_ANGLE__v1", "KNEE_FLEXION_ANGLE__v2"}
    assert by_label["KNEE_FLEXION_ANGLE__v1"]["relative_path"] != by_label["KNEE_FLEXION_ANGLE__v2"]["relative_path"]
    for label, body in (("KNEE_FLEXION_ANGLE__v1", b"v1"), ("KNEE_FLEXION_ANGLE__v2", b"v2")):
        assert (tmp_path / by_label[label]["relative_path"]).read_bytes() == body
        assert by_label[label]["sha256"] == hashlib.sha256(body).hexdigest()
    src = [e for e in entries if e["resource_label"] == "SRC"]
    assert src and all("/" not in e["relative_path"].split("S1/E1/")[-1] for e in src)
