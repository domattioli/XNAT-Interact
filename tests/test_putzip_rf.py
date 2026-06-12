"""
tests/test_putzip_rf.py — GAP-RF: label-addressed REST PUT for put_zip/put_file.

XNAT 1.9.3 quirk: pyxnat's resource.put_zip() / file.put() resolve the resource
to its numeric abstractresource ID and PUT to .../resources/<N>/... which returns
404 for rf-typed sessions.

Fix: PyxnatGateway.put_zip() / put_file() use _exec REST PUT against the
label-addressed URI built from the querystring converted to plural REST form.
Guard: only applies when server has _exec (real pyxnat Interface); FakeXNAT
test doubles take the existing pyxnat-chain path (no _exec).

Coverage:
1. _qs_to_rest_path — singular→plural REST path conversion
2. put_zip          — uses _exec PUT with extract=true, zip body, label URI
3. put_file         — uses _exec PUT with inbody=true, file body, label URI
4. put_zip fallback — no _exec on server → falls back to pyxnat chain
5. put_file fallback— no _exec on server → falls back to pyxnat chain
6. content/format/tags params forwarded correctly
7. overwrite param forwarded in put_file
8. FakeXNAT put_zip / put_file — existing call-log path unaffected
"""
from __future__ import annotations

import zipfile
from pathlib import Path
from unittest.mock import MagicMock, Mock

import pytest

from src.services.xnat_gateway import PyxnatGateway


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _make_gw() -> PyxnatGateway:
    """Return a PyxnatGateway with a mocked server that has _exec."""
    gw = PyxnatGateway(url="http://fake:8080", user="u", password="p")
    gw.server = Mock()
    gw.server._exec.return_value = b""
    return gw


def _make_zip(tmp_path: Path, filenames: list[str]) -> Path:
    """Write a zip archive containing *filenames* (each with placeholder bytes)."""
    zpath = tmp_path / "data.zip"
    with zipfile.ZipFile(zpath, "w") as zf:
        for fn in filenames:
            zf.writestr(fn, f"content of {fn}")
    return zpath


# ---------------------------------------------------------------------------
# 1. _qs_to_rest_path — path conversion
# ---------------------------------------------------------------------------

class TestQsToRestPath:
    def test_scan_qs_converts_to_plural(self):
        qs = "/project/MYPROJ/subject/SUBJ1/experiment/EXP1/scan/0"
        result = PyxnatGateway._qs_to_rest_path(qs)
        assert result == "/data/projects/MYPROJ/subjects/SUBJ1/experiments/EXP1/scans/0"

    def test_subject_qs(self):
        qs = "/project/P/subject/S"
        result = PyxnatGateway._qs_to_rest_path(qs)
        assert result == "/data/projects/P/subjects/S"

    def test_experiment_qs(self):
        qs = "/project/P/subject/S/experiment/E"
        result = PyxnatGateway._qs_to_rest_path(qs)
        assert result == "/data/projects/P/subjects/S/experiments/E"

    def test_unknown_segments_pass_through(self):
        qs = "/project/P/subject/S/custom/X"
        result = PyxnatGateway._qs_to_rest_path(qs)
        assert result == "/data/projects/P/subjects/S/custom/X"

    def test_leading_slash_handled(self):
        # Should not produce double /data// prefix
        qs = "/project/PUTZIP_DBG/subject/DBG1/experiment/DBG1_E/scan/0"
        result = PyxnatGateway._qs_to_rest_path(qs)
        assert result.startswith("/data/projects/")
        assert "//" not in result


# ---------------------------------------------------------------------------
# 2. put_zip — REST _exec path
# ---------------------------------------------------------------------------

class TestPutZipRestExec:
    def test_put_zip_uses_exec_with_label_uri(self, tmp_path):
        gw = _make_gw()
        zpath = _make_zip(tmp_path, ["x1.dcm", "x2.dcm"])
        qs = "/project/PZ_FIX/subject/DBG1/experiment/DBG1_E/scan/0"

        gw.put_zip(qs, "SRC", zpath)

        # _ensure_resource PUT + file PUT
        calls = gw.server._exec.call_args_list
        assert len(calls) == 2

        # First call: resource ensure
        ensure_uri = calls[0][0][0]
        assert ensure_uri == "/data/projects/PZ_FIX/subjects/DBG1/experiments/DBG1_E/scans/0/resources/SRC"
        assert calls[0][1]["method"] == "PUT"

        # Second call: zip file PUT
        zip_uri = calls[1][0][0]
        assert zip_uri == "/data/projects/PZ_FIX/subjects/DBG1/experiments/DBG1_E/scans/0/resources/SRC/files"
        assert calls[1][1]["method"] == "PUT"
        assert calls[1][1]["params"]["extract"] == "true"
        assert calls[1][1]["params"]["inbody"] == "true"
        assert calls[1][1]["headers"]["Content-Type"] == "application/zip"
        # Body is the zip bytes
        assert calls[1][1]["body"] == zpath.read_bytes()

    def test_put_zip_content_format_tags_forwarded(self, tmp_path):
        gw = _make_gw()
        zpath = _make_zip(tmp_path, ["f.dcm"])

        gw.put_zip(
            "/project/P/subject/S/experiment/E/scan/0",
            "RSC", zpath,
            content="IMAGE", format="DICOM", tags="DATA",
        )

        zip_call = gw.server._exec.call_args_list[-1]
        params = zip_call[1]["params"]
        assert params["content"] == "IMAGE"
        assert params["format"] == "DICOM"
        assert params["tags"] == "DATA"

    def test_put_zip_empty_params_not_forwarded(self, tmp_path):
        gw = _make_gw()
        zpath = _make_zip(tmp_path, ["f.dcm"])

        gw.put_zip(
            "/project/P/subject/S/experiment/E/scan/0",
            "RSC", zpath,
        )

        zip_call = gw.server._exec.call_args_list[-1]
        params = zip_call[1]["params"]
        # Empty string params should NOT appear (no junk keys)
        assert "content" not in params
        assert "format" not in params
        assert "tags" not in params


# ---------------------------------------------------------------------------
# 3. put_file — REST _exec path
# ---------------------------------------------------------------------------

class TestPutFileRestExec:
    def test_put_file_uses_exec_with_label_uri(self, tmp_path):
        gw = _make_gw()
        fpath = tmp_path / "frame.dcm"
        fpath.write_bytes(b"\xde\xad\xbe\xef")

        gw.put_file(
            "/project/PZ_FIX/subject/DBG1/experiment/DBG1_E/scan/0",
            "SRC", "frame.dcm", fpath,
        )

        calls = gw.server._exec.call_args_list
        assert len(calls) == 2  # ensure_resource + file PUT

        file_call = calls[-1]
        assert file_call[0][0] == (
            "/data/projects/PZ_FIX/subjects/DBG1/experiments/DBG1_E"
            "/scans/0/resources/SRC/files/frame.dcm"
        )
        assert file_call[1]["method"] == "PUT"
        assert file_call[1]["params"]["inbody"] == "true"
        assert file_call[1]["body"] == b"\xde\xad\xbe\xef"
        assert file_call[1]["headers"]["Content-Type"] == "application/octet-stream"

    def test_put_file_overwrite_param(self, tmp_path):
        gw = _make_gw()
        fpath = tmp_path / "f.dcm"
        fpath.write_bytes(b"x")

        gw.put_file(
            "/project/P/subject/S/experiment/E/scan/0",
            "RSC", "f.dcm", fpath, overwrite=True,
        )

        file_call = gw.server._exec.call_args_list[-1]
        assert file_call[1]["params"]["overwrite"] == "true"

    def test_put_file_content_format_tags(self, tmp_path):
        gw = _make_gw()
        fpath = tmp_path / "f.nii"
        fpath.write_bytes(b"nifti")

        gw.put_file(
            "/project/P/subject/S/experiment/E/scan/0",
            "RSC", "f.nii", fpath,
            content="DERIVED", format="NIFTI", tags="SEG",
        )

        file_call = gw.server._exec.call_args_list[-1]
        params = file_call[1]["params"]
        assert params["content"] == "DERIVED"
        assert params["format"] == "NIFTI"
        assert params["tags"] == "SEG"


# ---------------------------------------------------------------------------
# 4 & 5. Fallback — no _exec → pyxnat chain
# ---------------------------------------------------------------------------

class TestPutZipFallback:
    def test_put_zip_falls_back_when_no_exec(self, tmp_path):
        gw = PyxnatGateway(url="http://fake:8080", user="u", password="p")
        # Server mock WITHOUT _exec: spec includes 'select' but not '_exec'.
        gw.server = MagicMock(spec=["select"])
        resource_mock = MagicMock()
        gw.server.select.return_value.resource.return_value = resource_mock
        zpath = _make_zip(tmp_path, ["f.dcm"])

        gw.put_zip("/project/P/subject/S/experiment/E/scan/0", "RSC", zpath)

        gw.server.select.assert_called_once()
        resource_mock.put_zip.assert_called_once()

    def test_put_file_falls_back_when_no_exec(self, tmp_path):
        gw = PyxnatGateway(url="http://fake:8080", user="u", password="p")
        gw.server = MagicMock(spec=["select"])
        file_mock = MagicMock()
        gw.server.select.return_value.resource.return_value.file.return_value = file_mock
        fpath = tmp_path / "f.dcm"
        fpath.write_bytes(b"x")

        gw.put_file("/project/P/subject/S/experiment/E/scan/0", "RSC", "f.dcm", fpath)

        gw.server.select.assert_called_once()
        file_mock.put.assert_called_once()


# ---------------------------------------------------------------------------
# 6. FakeXNAT put_zip / put_file — call-log path unaffected
# ---------------------------------------------------------------------------

class TestFakeXnatPutZipUnaffected:
    def test_fake_put_zip_records_call_log(self, tmp_path):
        from tests.fakes.fake_xnat import FakeXNAT

        fake = FakeXNAT()
        exp_qs = "/project/FAKE_PROJECT/subject/1.2.3/experiment/SOURCE_DATA-1.2.3"
        scan_qs = f"{exp_qs}/scan/0"
        fake.create(exp_qs, xsiType="xnat:rfSessionData")

        zpath = _make_zip(tmp_path, ["a.dcm", "b.dcm"])
        fake.put_zip(scan_qs, "SRC", zpath, content="IMAGE", format="DICOM")

        put_zip_calls = [c for c in fake.calls if c["op"] == "resource.put_zip"]
        assert len(put_zip_calls) == 1
        assert put_zip_calls[0]["kwargs"]["_label"] == "SRC"
        assert put_zip_calls[0]["kwargs"]["content"] == "IMAGE"
        assert put_zip_calls[0]["kwargs"]["format"] == "DICOM"

    def test_fake_put_zip_stages_files_for_list(self, tmp_path):
        """FakeXNAT.put_zip extracts zip → staged files visible via list_files."""
        from tests.fakes.fake_xnat import FakeXNAT

        fake = FakeXNAT()
        exp_qs = "/project/FAKE_PROJECT/subject/S/experiment/E"
        scan_qs = f"{exp_qs}/scan/0"
        fake.create(exp_qs, xsiType="xnat:rfSessionData")

        zpath = _make_zip(tmp_path, ["x1.dcm", "x2.dcm"])
        fake.put_zip(scan_qs, "SRC", zpath)

        files = fake.list_files(scan_qs, "SRC")
        assert sorted(files) == ["x1.dcm", "x2.dcm"]

    def test_fake_put_file_records_call_log(self, tmp_path):
        from tests.fakes.fake_xnat import FakeXNAT

        fake = FakeXNAT()
        exp_qs = "/project/FAKE_PROJECT/subject/S/experiment/E"
        scan_qs = f"{exp_qs}/scan/0"
        fake.create(exp_qs, xsiType="xnat:rfSessionData")

        fpath = tmp_path / "frame.dcm"
        fpath.write_bytes(b"\xfe\xed")
        fake.put_file(scan_qs, "SRC", "frame.dcm", fpath)

        put_calls = [c for c in fake.calls if c["op"] == "file.put"]
        assert len(put_calls) == 1
        assert put_calls[0]["kwargs"]["_filename"] == "frame.dcm"
