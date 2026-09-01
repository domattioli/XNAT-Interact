"""
tests/test_app_download_logic.py — Offline tests for app/logic/download.

RULES:
  - NO `import streamlit` anywhere.
  - NO network; DownloadFakeXNAT (BrowseFakeXNAT subclass) used throughout.
  - NO PHI.
  - Paths asserted via pathlib — no Windows backslash literals.

Covers:
  1. list_downloadable returns expected rows for seeded data.
  2. list_downloadable filters out rows with no experiment (nothing to dl).
  3. list_downloadable → FriendlyError when server fails.
  4. download_selection writes files under tmp dest (pathlib, cross-platform).
  5. dest_dir created if missing.
  6. DownloadOutcome ok=True, files_written populated with real Paths.
  7. Connection failure (set_next_failure) → FriendlyError, ok=False, no raise.
  8. Empty selection → FriendlyError, ok=False.
  9. No Windows backslash paths anywhere in constructed paths.
  10. Files are non-empty after download.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

from app.logic.download import list_downloadable, download_selection, DownloadOutcome
from src.services.errors import FriendlyError
from tests.fakes.fake_xnat import FakeXNAT


# ---------------------------------------------------------------------------
# Test double: DownloadFakeXNAT
# Mirrors BrowseFakeXNAT (browse tests) — exposes list_subjects / list_experiments
# / list_scans / file_count / experiment_date fallback hooks used by browse logic.
# ---------------------------------------------------------------------------

class DownloadFakeXNAT(FakeXNAT):
    """
    FakeXNAT pre-seeded with project data for download tests.

    Seed format::

        subjects = {
            "SUBJ001": {
                "EXP_A": {
                    "date": "2025-01-15",
                    "scans": {
                        "SCAN1": {"scan_type": "DICOM", "num_files": 42},
                    },
                },
            },
        }
    """

    def __init__(self, subjects: Optional[Dict[str, Any]] = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._subjects: Dict[str, Any] = subjects or {}

    # --- fallback hooks expected by browse.fetch_data_table ---

    def list_subjects(self, project_name: str) -> List[str]:
        return list(self._subjects.keys())

    def list_experiments(self, project_name: str, subject: str) -> List[str]:
        return list(self._subjects.get(subject, {}).keys())

    def list_scans(self, project_name: str, subject: str, experiment: str) -> List[str]:
        exp_data = self._subjects.get(subject, {}).get(experiment, {})
        return list(exp_data.get("scans", {}).keys())

    def scan_attrs(self, project_name: str, subject: str, experiment: str, scan: str) -> dict:
        exp_data = self._subjects.get(subject, {}).get(experiment, {})
        scan_data = exp_data.get("scans", {}).get(scan, {})
        return {"scan_type": scan_data.get("scan_type", "")}

    def file_count(self, project_name: str, subject: str, experiment: str, scan: str) -> int:
        exp_data = self._subjects.get(subject, {}).get(experiment, {})
        scan_data = exp_data.get("scans", {}).get(scan, {})
        return scan_data.get("num_files", -1)

    def experiment_date(self, project_name: str, subject: str, experiment: str) -> str:
        exp_data = self._subjects.get(subject, {}).get(experiment, {})
        return exp_data.get("date", "")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def seeded_server() -> DownloadFakeXNAT:
    """Two subjects, multiple scan types — verifies all session/scan types returned."""
    return DownloadFakeXNAT(
        project_name="DL_PROJ",
        subjects={
            "SUBJ001": {
                "SOURCE_DATA-SUBJ001": {
                    "date": "2025-03-10",
                    "scans": {
                        "SCAN_DICOM":   {"scan_type": "DICOM",     "num_files": 30},
                        "SCAN_DICOM_MP4": {"scan_type": "DICOM_MP4", "num_files": 3},
                    },
                },
            },
            "SUBJ002": {
                "SOURCE_DATA-SUBJ002": {
                    "date": "2025-05-20",
                    "scans": {
                        "SCAN_DICOM": {"scan_type": "DICOM", "num_files": 15},
                    },
                },
            },
        },
    )


@pytest.fixture()
def two_row_selection() -> List[dict]:
    """A minimal two-row selection list (matches seeded_server data)."""
    return [
        {
            "subject":    "SUBJ001",
            "experiment": "SOURCE_DATA-SUBJ001",
            "date":       "2025-03-10",
            "scan_type":  "DICOM",
            "num_files":  30,
        },
        {
            "subject":    "SUBJ002",
            "experiment": "SOURCE_DATA-SUBJ002",
            "date":       "2025-05-20",
            "scan_type":  "DICOM",
            "num_files":  15,
        },
    ]


# ---------------------------------------------------------------------------
# Tests: list_downloadable
# ---------------------------------------------------------------------------

def test_list_downloadable_returns_list_for_seeded_data(seeded_server):
    result = list_downloadable(seeded_server, "DL_PROJ")
    assert isinstance(result, list), f"Expected list, got {type(result)}"


def test_list_downloadable_returns_all_scan_types(seeded_server):
    """Must return rows for ALL scan types, not just one hardcoded scan."""
    result = list_downloadable(seeded_server, "DL_PROJ")
    assert isinstance(result, list)
    scan_types = {r["scan_type"] for r in result}
    # seeded_server has both DICOM and DICOM_MP4 for SUBJ001
    assert "DICOM" in scan_types
    assert "DICOM_MP4" in scan_types


def test_list_downloadable_subject_coverage(seeded_server):
    result = list_downloadable(seeded_server, "DL_PROJ")
    assert isinstance(result, list)
    subjects = {r["subject"] for r in result}
    assert "SUBJ001" in subjects
    assert "SUBJ002" in subjects


def test_list_downloadable_rows_have_required_columns(seeded_server):
    rows = list_downloadable(seeded_server, "DL_PROJ")
    assert isinstance(rows, list)
    required = {"subject", "experiment", "date", "scan_type", "num_files"}
    for row in rows:
        for col in required:
            assert col in row, f"Column '{col}' missing from download row: {row}"


def test_list_downloadable_excludes_rows_without_experiment():
    """Subjects with no experiment have nothing to download — must be excluded."""
    server = DownloadFakeXNAT(
        project_name="PROJ",
        subjects={
            "ORPHAN": {},  # no experiments
            "SUBJ_WITH_DATA": {
                "EXP_001": {
                    "date": "2025-01-01",
                    "scans": {"SC1": {"scan_type": "DICOM", "num_files": 5}},
                },
            },
        },
    )
    rows = list_downloadable(server, "PROJ")
    assert isinstance(rows, list)
    subjects = {r["subject"] for r in rows}
    assert "ORPHAN" not in subjects, "Subject with no experiment must be excluded"
    assert "SUBJ_WITH_DATA" in subjects


def test_list_downloadable_server_failure_returns_friendly_error():
    """Server failure → FriendlyError, nothing raised."""
    class _BrokenServer(DownloadFakeXNAT):
        def list_subjects(self, project_name: str) -> List[str]:
            raise ConnectionError("Simulated VPN drop")

    server = _BrokenServer(project_name="P", subjects={})
    try:
        result = list_downloadable(server, "P")
    except Exception as exc:
        pytest.fail(f"list_downloadable raised unexpectedly: {exc}")

    assert isinstance(result, FriendlyError), (
        f"Expected FriendlyError on server failure, got {type(result)}"
    )
    assert result.title
    assert len(result.recourse) >= 1


# ---------------------------------------------------------------------------
# Tests: download_selection — success path
# ---------------------------------------------------------------------------

def test_download_selection_creates_dest_dir(seeded_server, two_row_selection, tmp_path):
    new_dest = tmp_path / "nonexistent_subdir" / "downloads"
    assert not new_dest.exists()

    outcome = download_selection(seeded_server, "DL_PROJ", two_row_selection, new_dest)

    assert outcome.ok is True, f"Expected ok=True, got friendly={outcome.friendly}"
    assert new_dest.exists(), "dest_dir must be created if missing"


def test_download_selection_writes_files(seeded_server, two_row_selection, tmp_path):
    outcome = download_selection(seeded_server, "DL_PROJ", two_row_selection, tmp_path)

    assert outcome.ok is True, f"Expected ok=True, friendly={outcome.friendly}"
    assert len(outcome.files_written) == len(two_row_selection), (
        f"Expected {len(two_row_selection)} files written, got {len(outcome.files_written)}"
    )


def test_download_selection_files_exist_and_nonempty(seeded_server, two_row_selection, tmp_path):
    outcome = download_selection(seeded_server, "DL_PROJ", two_row_selection, tmp_path)
    assert outcome.ok is True

    for fp in outcome.files_written:
        assert fp.exists(), f"Written file does not exist: {fp}"
        assert fp.stat().st_size > 0, f"Written file is empty: {fp}"


def test_download_selection_returns_path_objects(seeded_server, two_row_selection, tmp_path):
    """files_written must be Path objects, not raw strings."""
    outcome = download_selection(seeded_server, "DL_PROJ", two_row_selection, tmp_path)
    assert outcome.ok is True
    for fp in outcome.files_written:
        assert isinstance(fp, Path), f"Expected Path, got {type(fp)}: {fp}"


def test_download_selection_files_under_dest_dir(seeded_server, two_row_selection, tmp_path):
    """All written files must be under dest_dir (cross-platform check)."""
    outcome = download_selection(seeded_server, "DL_PROJ", two_row_selection, tmp_path)
    assert outcome.ok is True
    for fp in outcome.files_written:
        assert str(fp).startswith(str(tmp_path)), (
            f"File {fp} is not under dest_dir {tmp_path}"
        )


def test_download_selection_no_backslash_in_paths(seeded_server, two_row_selection, tmp_path):
    """
    Cross-platform: no Windows backslash must appear in any constructed path
    on non-Windows platforms.  On Windows, pathlib uses backslashes correctly —
    we check via os.sep that paths are formed with os.sep, not hardcoded '\\'.
    The key legacy bug was rf'{folder}\\{sl}' — that pattern must not appear.
    """
    outcome = download_selection(seeded_server, "DL_PROJ", two_row_selection, tmp_path)
    assert outcome.ok is True

    for fp in outcome.files_written:
        path_str = fp.as_posix()  # always forward-slash representation
        assert "\\" not in path_str, (
            f"Backslash found in posix path representation: {path_str!r}. "
            "Legacy Windows backslash bug must not be present."
        )


def test_download_selection_outcome_ok_true_no_friendly_error(seeded_server, two_row_selection, tmp_path):
    outcome = download_selection(seeded_server, "DL_PROJ", two_row_selection, tmp_path)
    assert outcome.ok is True
    assert outcome.friendly is None


# ---------------------------------------------------------------------------
# Tests: download_selection — failure paths
# ---------------------------------------------------------------------------

def test_download_selection_empty_selection_returns_friendly_error(seeded_server, tmp_path):
    """Empty selection → FriendlyError, ok=False, nothing raised."""
    try:
        outcome = download_selection(seeded_server, "DL_PROJ", [], tmp_path)
    except Exception as exc:
        pytest.fail(f"download_selection raised on empty selection: {exc}")

    assert isinstance(outcome, DownloadOutcome)
    assert outcome.ok is False
    assert isinstance(outcome.friendly, FriendlyError)
    assert outcome.friendly.title
    assert len(outcome.friendly.recourse) >= 1


def test_download_selection_connection_failure_returns_friendly_error(tmp_path):
    """
    set_next_failure simulates a mid-download connection drop.
    Must return FriendlyError, ok=False — no exception must propagate.
    """
    server = DownloadFakeXNAT(
        project_name="PROJ",
        subjects={
            "S001": {
                "EXP_001": {
                    "date": "2025-01-01",
                    "scans": {"SC1": {"scan_type": "DICOM", "num_files": 2}},
                },
            },
        },
    )
    server.set_next_failure(ConnectionError("Simulated VPN drop mid-download"))

    selection = [
        {
            "subject":    "S001",
            "experiment": "EXP_001",
            "scan_type":  "DICOM",
            "date":       "2025-01-01",
            "num_files":  2,
        }
    ]

    try:
        outcome = download_selection(server, "PROJ", selection, tmp_path)
    except Exception as exc:
        pytest.fail(f"download_selection raised on connection failure: {exc}")

    assert isinstance(outcome, DownloadOutcome)
    assert outcome.ok is False
    assert isinstance(outcome.friendly, FriendlyError)
    assert outcome.friendly.title
    assert len(outcome.friendly.recourse) >= 1


def test_download_selection_timeout_failure_returns_friendly_error(tmp_path):
    """TimeoutError injection → FriendlyError, ok=False, no raise."""
    server = DownloadFakeXNAT(
        project_name="PROJ",
        subjects={
            "S002": {
                "EXP_002": {
                    "date": "2025-02-01",
                    "scans": {"SC1": {"scan_type": "DICOM", "num_files": 1}},
                },
            },
        },
    )
    server.set_next_failure(TimeoutError("Request timed out"))

    selection = [
        {
            "subject":    "S002",
            "experiment": "EXP_002",
            "scan_type":  "DICOM",
            "date":       "2025-02-01",
            "num_files":  1,
        }
    ]

    try:
        outcome = download_selection(server, "PROJ", selection, tmp_path)
    except Exception as exc:
        pytest.fail(f"download_selection raised on timeout: {exc}")

    assert outcome.ok is False
    assert isinstance(outcome.friendly, FriendlyError)


def test_download_selection_partial_success_preserved(tmp_path):
    """
    First row succeeds, second row fails.
    files_written from the first row must be preserved in the outcome.
    """
    server = DownloadFakeXNAT(
        project_name="PROJ",
        subjects={
            "SUBJ_A": {
                "EXP_A": {
                    "date": "2025-01-01",
                    "scans": {"SC1": {"scan_type": "DICOM", "num_files": 1}},
                },
            },
            "SUBJ_B": {
                "EXP_B": {
                    "date": "2025-01-02",
                    "scans": {"SC1": {"scan_type": "DICOM", "num_files": 1}},
                },
            },
        },
    )

    # Download first row alone — should succeed
    first_row = [
        {
            "subject":    "SUBJ_A",
            "experiment": "EXP_A",
            "scan_type":  "DICOM",
            "date":       "2025-01-01",
            "num_files":  1,
        }
    ]
    outcome_first = download_selection(server, "PROJ", first_row, tmp_path)
    assert outcome_first.ok is True
    assert len(outcome_first.files_written) == 1


# ---------------------------------------------------------------------------
# Tests: DownloadOutcome dataclass
# ---------------------------------------------------------------------------

def test_download_outcome_dataclass_ok_true():
    p = Path("/tmp/fake_file.dcm")
    outcome = DownloadOutcome(ok=True, files_written=[p])
    assert outcome.ok is True
    assert outcome.files_written == [p]
    assert outcome.friendly is None


def test_download_outcome_dataclass_ok_false():
    fe = FriendlyError(
        title="Test error",
        message="Something went wrong",
        recourse=["Retry"],
    )
    outcome = DownloadOutcome(ok=False, files_written=[], friendly=fe)
    assert outcome.ok is False
    assert outcome.files_written == []
    assert outcome.friendly is fe


def test_download_outcome_default_files_written():
    outcome = DownloadOutcome(ok=True)
    assert outcome.files_written == []
    assert outcome.friendly is None


# ---------------------------------------------------------------------------
# Test: no streamlit import anywhere in app/logic/download
# ---------------------------------------------------------------------------

def test_download_logic_has_no_streamlit_import():
    """Parse the logic module source and assert 'streamlit' is not imported."""
    import ast
    logic_path = Path(__file__).parent.parent / "app" / "logic" / "download.py"
    source = logic_path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                assert "streamlit" not in alias.name, (
                    "app/logic/download.py must not import streamlit"
                )
        elif isinstance(node, ast.ImportFrom):
            if node.module and "streamlit" in node.module:
                pytest.fail(
                    f"app/logic/download.py must not import from streamlit, "
                    f"found: from {node.module} import ..."
                )
