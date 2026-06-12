"""
tests/test_assessor_id_addressing.py — GAP-001 fix for assessor ID addressing.

Assessor file I/O requires using the assessor's accession ID (experiment ID)
instead of the label-based path.  Tests stub the pyxnat Interface surface
(_exec, _http, select) and verify that PyxnatGateway routes correctly.

Coverage:
1. _assessor_uri_rewrite — returns /data/experiments/<id> or None
2. _assessor_id         — id() first, attrs.get fallback, None on failure
3. insert_file          — reads file bytes, calls _exec PUT with correct URI
4. list_files           — calls _exec GET, parses ResultSet.Result[].Name
5. get_file_copy        — calls _exec GET, writes bytes to dest
6. delete_file          — calls _exec DELETE with correct URI
7. Non-assessor paths   — fall through to server.select() pyxnat chain
8. FakeXNAT full workflow (create_assessor, file round-trip via call log)
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import Mock, call

import pytest

from src.services.xnat_gateway import PyxnatGateway, GatewayError
from src.services.errors import FriendlyError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_gw() -> PyxnatGateway:
    """Return a PyxnatGateway with a mocked server (not connected to network)."""
    gw = PyxnatGateway(url="http://fake:8080", user="u", password="p")
    gw.server = Mock()
    return gw


def _wire_assessor_id(gw: PyxnatGateway, exp_qs: str, label: str, aid: str) -> None:
    """Configure server.select(assessor_qs).id() to return *aid*."""
    from pathlib import PurePosixPath
    assessor_qs = str(PurePosixPath(exp_qs) / "assessor" / label)
    mock_assessor = Mock()
    mock_assessor.id.return_value = aid
    gw.server.select.side_effect = lambda qs: mock_assessor if qs == assessor_qs else Mock()


# ---------------------------------------------------------------------------
# 1. _assessor_uri_rewrite
# ---------------------------------------------------------------------------

class TestAssessorUriRewrite:
    def test_returns_none_for_non_assessor_path(self):
        gw = _make_gw()
        assert gw._assessor_uri_rewrite("/project/P/subject/S/experiment/E/scan/1") is None

    def test_returns_none_when_id_resolution_fails(self):
        gw = _make_gw()
        # assessor.id() returns empty string, attrs.get returns None → None
        mock_assessor = Mock()
        mock_assessor.id.return_value = ""
        mock_assessor.attrs.get.return_value = None
        gw.server.select.return_value = mock_assessor
        result = gw._assessor_uri_rewrite("/project/P/subject/S/experiment/E/assessor/LABEL")
        assert result is None

    def test_returns_data_experiments_uri_on_success(self):
        gw = _make_gw()
        mock_assessor = Mock()
        mock_assessor.id.return_value = "PROJ_E00042"
        gw.server.select.return_value = mock_assessor
        result = gw._assessor_uri_rewrite("/project/P/subject/S/experiment/E/assessor/LABEL")
        assert result == "/data/experiments/PROJ_E00042"


# ---------------------------------------------------------------------------
# 2. _assessor_id
# ---------------------------------------------------------------------------

class TestAssessorId:
    def test_uses_id_method_first(self):
        gw = _make_gw()
        mock_obj = Mock()
        mock_obj.id.return_value = "PROJ_E00001"
        gw.server.select.return_value = mock_obj
        aid = gw._assessor_id("/project/P/subject/S/experiment/E", "LABEL")
        assert aid == "PROJ_E00001"

    def test_falls_back_to_attrs_get(self):
        gw = _make_gw()
        mock_obj = Mock()
        mock_obj.id.side_effect = AttributeError("no id()")
        mock_obj.attrs.get.return_value = "PROJ_E00002"
        gw.server.select.return_value = mock_obj
        aid = gw._assessor_id("/project/P/subject/S/experiment/E", "LABEL")
        assert aid == "PROJ_E00002"

    def test_returns_none_when_both_fail(self):
        gw = _make_gw()
        mock_obj = Mock()
        mock_obj.id.return_value = None
        mock_obj.attrs.get.return_value = None
        gw.server.select.return_value = mock_obj
        aid = gw._assessor_id("/project/P/subject/S/experiment/E", "LABEL")
        assert aid is None


# ---------------------------------------------------------------------------
# 3. insert_file — assessor path
# ---------------------------------------------------------------------------

class TestInsertFile:
    def test_assessor_path_reads_file_and_calls_exec_put(self, tmp_path):
        gw = _make_gw()
        mock_assessor = Mock()
        mock_assessor.id.return_value = "PROJ_E00099"
        gw.server.select.return_value = mock_assessor
        gw.server._exec.return_value = b""

        src = tmp_path / "data.json"
        src.write_bytes(b'{"x":1}')

        gw.insert_file(
            "/project/P/subject/S/experiment/E/assessor/ASS",
            "MYRSC",
            "data.json",
            str(src),
            content="C",
            format="JSON",
            tags="t",
        )

        gw.server._exec.assert_called_once()
        args, kwargs = gw.server._exec.call_args
        assert args[0] == "/data/experiments/PROJ_E00099/resources/MYRSC/files/data.json"
        assert kwargs["method"] == "PUT"
        assert kwargs["body"] == b'{"x":1}'
        assert kwargs["params"]["inbody"] == "true"
        assert kwargs["params"]["format"] == "JSON"
        assert kwargs["params"]["content"] == "C"

    def test_non_assessor_path_uses_rest_exec(self, tmp_path):
        # GAP-RF: non-assessor insert_file now also uses _exec REST PUT to avoid
        # numeric-resource-ID 404 on rf-typed XNAT 1.9.3 sessions.
        gw = _make_gw()
        gw.server._exec.return_value = b""
        src = tmp_path / "f.json"
        src.write_bytes(b"data")
        gw.insert_file(
            "/project/P/subject/S/experiment/E/scan/1",
            "RSC", "f.json", str(src),
        )
        # _exec called for resource ensure + file PUT (2 calls total)
        assert gw.server._exec.call_count >= 1
        # The file PUT call goes to label-addressed URI — verify the last _exec call
        all_calls = gw.server._exec.call_args_list
        file_put_call = all_calls[-1]
        uri_arg = file_put_call[0][0]
        assert "/data/projects/P/subjects/S/experiments/E/scans/1/resources/RSC/files/f.json" == uri_arg
        assert file_put_call[1]["method"] == "PUT"
        assert file_put_call[1]["body"] == b"data"

    def test_non_assessor_path_accepts_raw_string_data(self, tmp_path):
        # insert_file non-assessor path: data may be raw string content (not a path).
        # Mirrors xnat_resource_data.push_to_xnat which passes file content string.
        gw = _make_gw()
        gw.server._exec.return_value = b""
        raw_content = '{"form": "data", "uid": "test"}'
        gw.insert_file(
            "/project/P/subject/S/experiment/E/scan/1",
            "INTAKE_FORM", "form.json", raw_content,
            content="TEXT", format="JSON",
        )
        all_calls = gw.server._exec.call_args_list
        file_put_call = all_calls[-1]
        assert file_put_call[1]["body"] == raw_content.encode("utf-8")
        assert file_put_call[1]["params"]["format"] == "JSON"
        assert file_put_call[1]["params"]["content"] == "TEXT"

    def test_assessor_path_accepts_raw_string_data(self):
        # insert_file assessor path: data may be raw string content (not a path).
        # OSError ENAMETOOLONG guard must apply symmetrically to the assessor branch.
        gw = _make_gw()
        mock_assessor = Mock()
        mock_assessor.id.return_value = "PROJ_E00200"
        gw.server.select.return_value = mock_assessor
        gw.server._exec.return_value = b""
        raw_content = '{"form": "data", "uid": "assessor-test"}'
        gw.insert_file(
            "/project/P/subject/S/experiment/E/assessor/ASS",
            "INTAKE_FORM", "form.json", raw_content,
            content="TEXT", format="JSON",
        )
        gw.server._exec.assert_called_once()
        args, kwargs = gw.server._exec.call_args
        assert args[0] == "/data/experiments/PROJ_E00200/resources/INTAKE_FORM/files/form.json"
        assert kwargs["method"] == "PUT"
        assert kwargs["body"] == raw_content.encode("utf-8")
        assert kwargs["params"]["inbody"] == "true"

    def test_assessor_path_enametoolong_string_treated_as_content(self):
        # OSError ENAMETOOLONG: >255-char string must NOT raise — treat as content bytes.
        gw = _make_gw()
        mock_assessor = Mock()
        mock_assessor.id.return_value = "PROJ_E00201"
        gw.server.select.return_value = mock_assessor
        gw.server._exec.return_value = b""
        long_json = '{"k": "' + "x" * 300 + '"}' # >255 chars, not a valid path
        gw.insert_file(
            "/project/P/subject/S/experiment/E/assessor/ASS",
            "MYRSC", "out.json", long_json,
        )
        gw.server._exec.assert_called_once()
        _, kwargs = gw.server._exec.call_args
        assert kwargs["body"] == long_json.encode("utf-8")


# ---------------------------------------------------------------------------
# 4. list_files — assessor path
# ---------------------------------------------------------------------------

class TestListFiles:
    def test_assessor_path_calls_exec_get_and_parses_name(self):
        gw = _make_gw()
        mock_assessor = Mock()
        mock_assessor.id.return_value = "PROJ_E00010"
        gw.server.select.return_value = mock_assessor

        payload = json.dumps({
            "ResultSet": {"Result": [{"Name": "a.json"}, {"Name": "b.json"}]}
        }).encode()
        gw.server._exec.return_value = payload

        result = gw.list_files("/project/P/subject/S/experiment/E/assessor/ASS", "RSC")

        assert result == ["a.json", "b.json"]
        gw.server._exec.assert_called_once()
        args, kwargs = gw.server._exec.call_args
        assert args[0] == "/data/experiments/PROJ_E00010/resources/RSC/files"
        assert kwargs["method"] == "GET"
        assert kwargs["body"] == {"format": "json"}

    def test_assessor_path_empty_result(self):
        gw = _make_gw()
        mock_assessor = Mock()
        mock_assessor.id.return_value = "PROJ_E00010"
        gw.server.select.return_value = mock_assessor
        gw.server._exec.return_value = b'{"ResultSet": {"Result": []}}'
        result = gw.list_files("/project/P/subject/S/experiment/E/assessor/ASS", "RSC")
        assert result == []

    def test_non_assessor_path_uses_pyxnat_chain(self):
        gw = _make_gw()
        gw.server.select.return_value.resource.return_value.files.return_value.get.return_value = ["x.nii"]
        result = gw.list_files("/project/P/subject/S/experiment/E/scan/1", "RSC")
        assert result == ["x.nii"]
        gw.server._exec.assert_not_called()


# ---------------------------------------------------------------------------
# 5. get_file_copy — assessor path
# ---------------------------------------------------------------------------

class TestGetFileCopy:
    def test_assessor_path_writes_exec_bytes_to_dest(self, tmp_path):
        gw = _make_gw()
        mock_assessor = Mock()
        mock_assessor.id.return_value = "PROJ_E00020"
        gw.server.select.return_value = mock_assessor
        gw.server._exec.return_value = b'{"v": 7}'

        dest = tmp_path / "out.json"
        result = gw.get_file_copy(
            "/project/P/subject/S/experiment/E/assessor/ASS",
            "RSC", "f.json", dest,
        )

        assert dest.read_bytes() == b'{"v": 7}'
        assert result == dest
        gw.server._exec.assert_called_once()
        args, kwargs = gw.server._exec.call_args
        assert args[0] == "/data/experiments/PROJ_E00020/resources/RSC/files/f.json"
        assert kwargs["method"] == "GET"

    def test_non_assessor_path_uses_pyxnat_chain(self, tmp_path):
        gw = _make_gw()
        dest = tmp_path / "out.json"
        gw.server.select.return_value.resource.return_value.file.return_value.get_copy.return_value = dest
        result = gw.get_file_copy("/project/P/subject/S/experiment/E/scan/1", "RSC", "f.json", dest)
        assert result == dest
        gw.server._exec.assert_not_called()


# ---------------------------------------------------------------------------
# 6. delete_file — assessor path
# ---------------------------------------------------------------------------

class TestDeleteFile:
    def test_assessor_path_calls_exec_delete(self):
        gw = _make_gw()
        mock_assessor = Mock()
        mock_assessor.id.return_value = "PROJ_E00030"
        gw.server.select.return_value = mock_assessor
        gw.server._exec.return_value = b""

        gw.delete_file(
            "/project/P/subject/S/experiment/E/assessor/ASS",
            "RSC", "f.json",
        )

        gw.server._exec.assert_called_once()
        args, kwargs = gw.server._exec.call_args
        assert args[0] == "/data/experiments/PROJ_E00030/resources/RSC/files/f.json"
        assert kwargs["method"] == "DELETE"

    def test_non_assessor_path_uses_pyxnat_chain(self):
        gw = _make_gw()
        gw.delete_file("/project/P/subject/S/experiment/E/scan/1", "RSC", "f.json")
        gw.server.select.assert_called_once()
        gw.server._exec.assert_not_called()


# ---------------------------------------------------------------------------
# 7. xsiType kwarg discovery (Phase 7 #27) — via FakeXNAT
# ---------------------------------------------------------------------------

class TestXsiTypeKwargDiscovery:
    def test_create_assessor_uses_xsitype_kwarg(self):
        from tests.fakes.fake_xnat import FakeXNAT
        fake = FakeXNAT(fidelity_mode=True)
        exp_qs = "/project/FAKE_PROJECT/subject/1.2.3.4/experiment/SOURCE_DATA-1.2.3.4"
        fake.create(exp_qs, xsiType="xnat:rfSessionData")
        fake.create_assessor(exp_qs, "GAP001_PROBE", xsi_type="xnat:imageAssessorData")
        assessor_qs = f"{exp_qs}/assessor/GAP001_PROBE"
        sel = fake._selectables.get(assessor_qs)
        assert sel is not None
        assert sel.attrs._datatype == "xnat:imageAssessorData"

    def test_create_assessor_default_xsi_type(self):
        from tests.fakes.fake_xnat import FakeXNAT
        fake = FakeXNAT(fidelity_mode=True)
        exp_qs = "/project/FAKE_PROJECT/subject/1.2.3.4/experiment/SOURCE_DATA-1.2.3.4"
        fake.create(exp_qs, xsiType="xnat:rfSessionData")
        fake.create_assessor(exp_qs, "DEFAULT_TYPE")
        assessor_qs = f"{exp_qs}/assessor/DEFAULT_TYPE"
        sel = fake._selectables.get(assessor_qs)
        assert sel.attrs._datatype == "xnat:assessorData"


# ---------------------------------------------------------------------------
# 8. Full FakeXNAT workflow
# ---------------------------------------------------------------------------

class TestAssessorFileWorkflowWithFake:
    def test_create_assessor_and_list_files(self):
        from tests.fakes.fake_xnat import FakeXNAT
        fake = FakeXNAT(fidelity_mode=True)
        exp_qs = "/project/FAKE_PROJECT/subject/1.2.3.4/experiment/SOURCE_DATA-1.2.3.4"
        fake.create(exp_qs, xsiType="xnat:rfSessionData")
        fake.create_assessor(exp_qs, "SEG_CONSENSUS", xsi_type="xnat:imageAssessorData")
        assessor_qs = f"{exp_qs}/assessor/SEG_CONSENSUS"
        sel = fake._selectables.get(assessor_qs)
        assert sel is not None and sel._exists
        assert sel.attrs._datatype == "xnat:imageAssessorData"

    def test_assessor_file_round_trip(self, tmp_path):
        from tests.fakes.fake_xnat import FakeXNAT
        fake = FakeXNAT()
        exp_qs = "/project/FAKE_PROJECT/subject/1.2.3.4/experiment/SOURCE_DATA-1.2.3.4"
        fake.create(exp_qs, xsiType="xnat:rfSessionData")
        seg_file = tmp_path / "seg.nii"
        seg_file.write_bytes(b"\x00segdata")
        fake.create_assessor(
            exp_qs, "SEG_CONSENSUS",
            xsi_type="xnat:imageAssessorData",
            files=[("SEGMENTATION", "seg.nii", seg_file)],
        )
        file_puts = [c for c in fake.calls if c["op"] == "assessor.file.put"]
        assert len(file_puts) == 1
        assert file_puts[0]["kwargs"]["_filename"] == "seg.nii"
        assert file_puts[0]["kwargs"]["_resource"] == "SEGMENTATION"
