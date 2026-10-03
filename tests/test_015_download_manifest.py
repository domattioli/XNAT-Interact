"""
Spec 015 offline tests: the folder download record (download_manifest.json).

Runs against FakeXNAT with synthetic data only; no server, no network, no PHI.
"""
from __future__ import annotations

import hashlib
import io
import json
import zipfile
from pathlib import Path

import pytest

from app.logic import download_manifest as dm
from app.logic.download import download_selection
from app.logic.download_manifest import FileRecord, ManifestIdentity
from tests.fakes.scan_tree_fake import ScanTreeFake
from tests.synthetic_data import make_synthetic_dicom, make_synthetic_mp4

PROJECT = "P015"
SCHEMA = Path(__file__).resolve().parents[1] / "specs" / "015-download-manifest" / "contracts" / "download-manifest.schema.json"
IDENTITY = ManifestIdentity(username="student_a", server_url="http://fake.local")


def _two_case_server() -> ScanTreeFake:
    fake = ScanTreeFake(project_name=PROJECT)
    for subject, uid in (("S1", "uidA"), ("S2", "uidB")):
        fake.add_files(subject, f"E_{subject}", "1", "SRC",
                       [(f"000{i}-{uid}.dcm", f"{subject}-frame-{i}".encode()) for i in range(1, 4)])
    return fake


def _rows(*subjects):
    return [{"subject": s, "experiment": f"E_{s}"} for s in subjects]


def _load(path) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _validate(manifest: dict) -> None:
    jsonschema = pytest.importorskip("jsonschema")
    jsonschema.validate(manifest, json.loads(SCHEMA.read_text(encoding="utf-8")))


# --------------------------------------------------------------------------- #
# T006: builder and writer
# --------------------------------------------------------------------------- #

class TestBuilderAndWriter:
    def test_entry_hashes_and_image_identity(self, tmp_path):
        dicom = make_synthetic_dicom(tmp_path / "0001-uidZ.dcm")
        csv = tmp_path / "table.csv"
        csv.write_text("a,b\n1,2\n")
        mp4 = make_synthetic_mp4(tmp_path / "clip.mp4")

        d = dm.build_file_entry(dicom, tmp_path, "/qs", "SRC", dicom.name)
        assert d["sha256"] == hashlib.sha256(dicom.read_bytes()).hexdigest()
        assert d["image_identity_hash"] and len(d["image_identity_hash"]) == 64
        assert d["case_uid"] == "uidZ"
        for other in (csv, mp4):
            e = dm.build_file_entry(other, tmp_path, "/qs", "DERIVED", other.name)
            assert "image_identity_hash" in e and e["image_identity_hash"] is None
            assert e["case_uid"] is None

    def test_manifest_validates_against_schema_and_nulls_identity(self, tmp_path):
        f = tmp_path / "a.bin"
        f.write_bytes(b"xyz")
        m = dm.build_manifest(path_kind="folder", project=PROJECT, selection=_rows("S1"),
                              records=[FileRecord(f, "/qs", "SRC", "a.bin")], root=tmp_path, complete=True)
        assert m["username"] is None and m["server_url"] is None
        dm.finalize_server_copy(m, None, PROJECT)
        _validate(m)

    def test_unwritable_destination_gives_note_not_exception(self, tmp_path):
        m = dm.build_manifest(path_kind="folder", project=PROJECT, selection=[], records=[],
                              root=tmp_path, complete=False)
        path, notes = dm.write_manifest(m, tmp_path / "missing_dir" / "download_manifest.json")
        assert path is None
        assert notes and "could not be saved" in notes[0]


# --------------------------------------------------------------------------- #
# US1: folder download
# --------------------------------------------------------------------------- #

class TestFolderManifest:
    def test_one_entry_per_file_written(self, tmp_path):  # T009
        out = download_selection(_two_case_server(), PROJECT, _rows("S1", "S2"), tmp_path, identity=IDENTITY)
        assert out.ok and len(out.files_written) == 6
        assert out.manifest_path == tmp_path / "download_manifest.json"
        m = _load(out.manifest_path)
        assert m["complete"] is True
        written = sorted(p.relative_to(tmp_path).as_posix() for p in out.files_written)
        assert sorted(e["relative_path"] for e in m["files"]) == written
        assert m["username"] == "student_a" and m["server_copy"]["status"] == "not_attempted"
        _validate(m)

    def test_rehash_and_query_strings_and_labels(self, tmp_path):  # T010
        fake = _two_case_server()
        fake.add_files("S1", "E_S1", "1", "DERIVED", [("mask.csv", b"0,1")])
        out = download_selection(fake, PROJECT, _rows("S1"), tmp_path)
        m = _load(out.manifest_path)
        for e in m["files"]:
            assert hashlib.sha256((tmp_path / e["relative_path"]).read_bytes()).hexdigest() == e["sha256"]
            assert e["scan_query_string"] == f"/projects/{PROJECT}/subjects/S1/experiments/E_S1/scans/1"
        assert {e["resource_label"] for e in m["files"]} == {"SRC", "DERIVED"}

    def test_empty_scan_and_non_image_file(self, tmp_path):  # T011
        fake = _two_case_server()
        fake.add_files("S1", "E_S1", "2", "SRC", [])
        out = download_selection(fake, PROJECT, _rows("S1"), tmp_path)
        m = _load(out.manifest_path)
        empty_qs = f"/projects/{PROJECT}/subjects/S1/experiments/E_S1/scans/2"
        assert empty_qs in m["empty_scans"]
        assert all(e["scan_query_string"] != empty_qs for e in m["files"])
        assert all(e["image_identity_hash"] is None for e in m["files"])  # bytes are not DICOM

    def test_existing_manifest_is_moved_aside(self, tmp_path):  # T012
        fake = _two_case_server()
        first = download_selection(fake, PROJECT, _rows("S1"), tmp_path)
        first_run = _load(first.manifest_path)["run_id"]
        second = download_selection(fake, PROJECT, _rows("S1"), tmp_path)
        kept = [p for p in tmp_path.glob("download_manifest.*.json")]
        assert len(kept) == 1 and _load(kept[0])["run_id"] == first_run
        assert _load(second.manifest_path)["run_id"] != first_run
        assert any("earlier download record" in n for n in second.notes)

    def test_partial_failure_marks_incomplete(self, tmp_path):  # T013
        fake = _two_case_server()

        class _Flaky:
            """Wrap the fake so the fourth file copy fails like a dropped connection."""
            def __init__(self, inner):
                self._inner, self.count = inner, 0

            def __getattr__(self, name):
                return getattr(self._inner, name)

            def select(self, qs):
                sel = self._inner.select(qs)
                outer = self

                class _Sel:
                    def __getattr__(self, n):
                        return getattr(sel, n)

                    def resource(self, label):
                        res = sel.resource(label)

                        class _Res:
                            def __getattr__(self, n):
                                return getattr(res, n)

                            def file(self, fn):
                                outer.count += 1
                                if outer.count == 4:
                                    raise ConnectionError("synthetic drop")
                                return res.file(fn)
                        return _Res()
                return _Sel()

        out = download_selection(_Flaky(fake), PROJECT, _rows("S1", "S2"), tmp_path)
        assert out.ok is False and out.friendly is not None
        m = _load(out.manifest_path)
        assert m["complete"] is False
        assert len(m["files"]) == len(out.files_written) == 3
        assert m["server_copy"]["status"] == "skipped_incomplete"

    def test_manifest_write_failure_is_soft(self, tmp_path, monkeypatch):  # T013
        def _boom(*a, **k):
            raise OSError("disk full (synthetic)")
        monkeypatch.setattr(dm, "_atomic_write", _boom)
        out = download_selection(_two_case_server(), PROJECT, _rows("S1"), tmp_path)
        assert out.ok is True and out.manifest_path is None
        assert any("could not be saved" in n for n in out.notes)


# --------------------------------------------------------------------------- #
# US4: offline reader and PHI check
# --------------------------------------------------------------------------- #

class TestOfflineReader:
    def test_inputs_for_one_case_without_server(self, tmp_path):  # T024
        out = download_selection(_two_case_server(), PROJECT, _rows("S1", "S2"), tmp_path)
        m = _load(out.manifest_path)  # from here on, no server object is used
        mine = [e for e in m["files"] if e["case_uid"] == "uidB"]
        assert len(mine) == 3
        assert all(len(e["sha256"]) == 64 and e["scan_query_string"] for e in mine)
        assert all("image_identity_hash" in e for e in mine)

    def test_no_phi_or_credentials_in_manifest(self, tmp_path):  # T024
        fake = ScanTreeFake(project_name=PROJECT)
        dicom = make_synthetic_dicom(tmp_path / "src" / "0001-uidP.dcm")
        fake.add_files("S9", "E_S9", "1", "SRC", [("0001-uidP.dcm", dicom.read_bytes())])
        out = download_selection(fake, PROJECT, _rows("S9"), tmp_path / "dl", identity=IDENTITY)
        text = Path(out.manifest_path).read_text(encoding="utf-8")
        for phi in ("DOE^JOHN", "MRN-0001234", "SMITH^JANE", "ACC-987654", "UIOWA HOSPITAL", "super-secret-phi"):
            assert phi not in text
        assert "password" not in text.lower() and "token" not in text.lower()
        m = json.loads(text)
        assert m["files"][0]["image_identity_hash"] is not None


# --------------------------------------------------------------------------- #
# T029 / FR-020: the guided page's zip carries the record
# --------------------------------------------------------------------------- #

def test_guided_download_zip_contains_manifest():
    from app.guided.browse_view import prepare_download_zip
    fake = _two_case_server()
    outcome, zip_bytes = prepare_download_zip(fake, PROJECT, _rows("S1"), identity=IDENTITY, uploader=fake)
    assert outcome.ok and zip_bytes is not None
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        assert "download_manifest.json" in zf.namelist()
        m = json.loads(zf.read("download_manifest.json"))
    assert m["server_copy"]["status"] == "uploaded"
    assert len(fake.list_files(f"/project/{PROJECT}", "DOWNLOADS")) == 1
