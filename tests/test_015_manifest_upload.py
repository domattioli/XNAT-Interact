"""
Spec 015 offline tests: the server copy of the download record (FR-018, FR-018a).

FakeXNAT stands in for the server; no network.
"""
from __future__ import annotations

import json
from pathlib import Path

from app.logic.download import download_selection
from app.logic.download_manifest import ManifestIdentity
from src.services.xnat_conventions import DOWNLOADS_RESOURCE, manifest_server_filename
from tests.fakes.scan_tree_fake import ScanTreeFake

PROJECT = "P015"
PROJ_QS = f"/project/{PROJECT}"
IDENTITY = ManifestIdentity(username="student_a", server_url="http://fake.local")
ROWS = [{"subject": "S1", "experiment": "E1"}]


def _server() -> ScanTreeFake:
    fake = ScanTreeFake(project_name=PROJECT)
    fake.add_files("S1", "E1", "1", "SRC", [("0001-uidA.dcm", b"one"), ("0002-uidA.dcm", b"two")])
    return fake


def _load(path) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def test_upload_lands_in_downloads_with_unique_name(tmp_path):
    fake = _server()
    out = download_selection(fake, PROJECT, ROWS, tmp_path, identity=IDENTITY, uploader=fake)
    m = _load(out.manifest_path)
    assert m["server_copy"]["status"] == "uploaded"
    name = m["server_copy"]["filename"]
    assert name.startswith("student_a-S1-") and name.endswith("Z.json")
    assert fake.list_files(PROJ_QS, DOWNLOADS_RESOURCE) == [name]
    assert out.notes == []


def test_uploaded_copy_is_the_pending_version(tmp_path):
    fake = _server()
    out = download_selection(fake, PROJECT, ROWS, tmp_path, identity=IDENTITY, uploader=fake)
    local = _load(out.manifest_path)
    resource = fake._flat_resource(PROJ_QS, DOWNLOADS_RESOURCE)
    server_copy = json.loads(resource.get_file_bytes(local["server_copy"]["filename"]))
    assert server_copy["server_copy"]["status"] == "pending"
    strip = lambda d: {k: v for k, v in d.items() if k != "server_copy"}  # noqa: E731
    assert strip(server_copy) == strip(local)


def test_collision_never_overwrites(tmp_path, monkeypatch):
    fake = _server()
    from app.logic import download_manifest as dm
    from datetime import datetime, timezone
    fixed = datetime(2026, 10, 3, 12, 0, 0, 123456, tzinfo=timezone.utc)
    monkeypatch.setattr(dm, "_utc_now", lambda: fixed)
    taken = manifest_server_filename("student_a", ["S1"], fixed)
    (tmp_path / "old.json").write_bytes(b"{\"old\": true}")
    fake.put_file(PROJ_QS, DOWNLOADS_RESOURCE, taken, str(tmp_path / "old.json"), overwrite=False)

    out = download_selection(fake, PROJECT, ROWS, tmp_path / "dl", identity=IDENTITY, uploader=fake)
    m = _load(out.manifest_path)
    assert m["server_copy"]["filename"] == taken.replace(".json", "-2.json")
    resource = fake._flat_resource(PROJ_QS, DOWNLOADS_RESOURCE)
    assert resource.get_file_bytes(taken) == b"{\"old\": true}"
    assert sorted(fake.list_files(PROJ_QS, DOWNLOADS_RESOURCE)) == sorted([taken, m["server_copy"]["filename"]])


def test_upload_failure_keeps_download_successful(tmp_path, monkeypatch):
    fake = _server()
    real_put = fake.put_file

    def _failing_put(qs, label, *a, **k):
        if label == DOWNLOADS_RESOURCE:
            raise ConnectionError("synthetic server refusal")
        return real_put(qs, label, *a, **k)
    monkeypatch.setattr(fake, "put_file", _failing_put)

    out = download_selection(fake, PROJECT, ROWS, tmp_path, identity=IDENTITY, uploader=fake)
    assert out.ok is True
    m = _load(out.manifest_path)
    assert m["server_copy"]["status"] == "failed"
    assert m["server_copy"]["reason"] and "Traceback" not in m["server_copy"]["reason"]
    assert any("could not be stored on the server" in n for n in out.notes)
    assert fake.list_files(PROJ_QS, DOWNLOADS_RESOURCE) == []


def test_no_uploader_means_not_attempted(tmp_path):
    out = download_selection(_server(), PROJECT, ROWS, tmp_path, identity=IDENTITY)
    assert _load(out.manifest_path)["server_copy"]["status"] == "not_attempted"


def test_server_filename_for_many_subjects():
    from datetime import datetime, timezone
    when = datetime(2026, 1, 2, 3, 4, 5, 6, tzinfo=timezone.utc)
    assert manifest_server_filename("student a", ["S1", "S2", "S1"], when) == "student_a-multi2-20260102T030405000006Z.json"
    assert manifest_server_filename(None, ["S1"], when).startswith("unknown-S1-")
