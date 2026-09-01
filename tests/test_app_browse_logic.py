"""
tests/test_app_browse_logic.py — Offline tests for app/logic/browse.

RULES:
  - NO `import streamlit` anywhere.
  - NO network; FakeXNAT subclass used throughout.
  - NO PHI.

Covers:
  1. fetch_data_table returns expected rows/columns for seeded data.
  2. Subject with no experiments surfaces as a row.
  3. filter_rows narrows correctly (case-insensitive, multi-column).
  4. filter_rows empty query returns all rows.
  5. sort_rows sorts ascending by column.
  6. Server error → FriendlyError returned, nothing raised.
  7. COLUMNS constant has expected fields.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import pytest

from app.logic.browse import fetch_data_table, filter_rows, sort_rows, COLUMNS
from src.services.errors import FriendlyError
from tests.fakes.fake_xnat import FakeXNAT


# ---------------------------------------------------------------------------
# Test double: BrowseFakeXNAT
# Extends FakeXNAT with list_subjects / list_experiments / list_scans /
# file_count / experiment_date — the fallback hooks used by browse logic when
# FakeSelectable.get() is absent.
# ---------------------------------------------------------------------------

class BrowseFakeXNAT(FakeXNAT):
    """
    Fake XNAT server pre-seeded with project data.

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
        # subjects: {subj_label: {exp_label: {date, scans: {scan_label: {scan_type, num_files}}}}}
        self._subjects: Dict[str, Any] = subjects or {}

    # --- fallback hooks used by browse logic ---

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
def seeded_server() -> BrowseFakeXNAT:
    """Two subjects, one with two experiments/scans, one with one."""
    return BrowseFakeXNAT(
        project_name="TEST_PROJ",
        subjects={
            "SUBJ001": {
                "EXP_KNEE_2025": {
                    "date": "2025-01-15",
                    "scans": {
                        "SCAN_DICOM": {"scan_type": "DICOM", "num_files": 42},
                        "SCAN_MP4":   {"scan_type": "DICOM_MP4", "num_files": 5},
                    },
                },
            },
            "SUBJ002": {
                "EXP_HIP_2024": {
                    "date": "2024-06-30",
                    "scans": {
                        "SCAN_DICOM": {"scan_type": "DICOM", "num_files": 18},
                    },
                },
            },
        },
    )


# ---------------------------------------------------------------------------
# Test 1: COLUMNS constant
# ---------------------------------------------------------------------------

def test_columns_constant_has_required_fields():
    assert "subject"    in COLUMNS
    assert "experiment" in COLUMNS
    assert "date"       in COLUMNS
    assert "scan_type"  in COLUMNS
    assert "num_files"  in COLUMNS


# ---------------------------------------------------------------------------
# Test 2: fetch_data_table returns expected row count and columns
# ---------------------------------------------------------------------------

def test_fetch_data_table_returns_expected_rows(seeded_server):
    rows = fetch_data_table(seeded_server, "TEST_PROJ")

    assert isinstance(rows, list), "Expected list[dict], not FriendlyError"
    # SUBJ001 has 2 scans, SUBJ002 has 1 scan → 3 rows total
    assert len(rows) == 3

    for row in rows:
        for col in COLUMNS:
            assert col in row, f"Column '{col}' missing from row: {row}"


def test_fetch_data_table_row_values(seeded_server):
    rows = fetch_data_table(seeded_server, "TEST_PROJ")
    assert isinstance(rows, list)

    subj_labels = {r["subject"] for r in rows}
    assert "SUBJ001" in subj_labels
    assert "SUBJ002" in subj_labels

    # SUBJ002 row
    subj2_rows = [r for r in rows if r["subject"] == "SUBJ002"]
    assert len(subj2_rows) == 1
    r = subj2_rows[0]
    assert r["experiment"] == "EXP_HIP_2024"
    assert r["date"] == "2024-06-30"
    assert r["num_files"] == 18


# ---------------------------------------------------------------------------
# Test 3: Subject with no experiments surfaces as a row (stub subject)
# ---------------------------------------------------------------------------

def test_subject_with_no_experiments_surfaces_as_row():
    server = BrowseFakeXNAT(
        project_name="PROJ",
        subjects={"ORPHAN_SUBJ": {}},
    )
    rows = fetch_data_table(server, "PROJ")
    assert isinstance(rows, list)
    assert len(rows) == 1
    r = rows[0]
    assert r["subject"] == "ORPHAN_SUBJ"
    assert r["experiment"] == ""
    assert r["num_files"] == -1


# ---------------------------------------------------------------------------
# Test 4: Empty project → empty rows list (not an error)
# ---------------------------------------------------------------------------

def test_empty_project_returns_empty_list():
    server = BrowseFakeXNAT(project_name="PROJ", subjects={})
    rows = fetch_data_table(server, "PROJ")
    assert rows == []


# ---------------------------------------------------------------------------
# Test 5: Server error → FriendlyError returned, nothing raised
# ---------------------------------------------------------------------------

def test_server_error_returns_friendly_error_no_raise():
    """set_next_failure on FakeXNAT injects a failure into the first resource op.
    browse logic catches all exceptions → returns FriendlyError."""

    class _RaisingServer(BrowseFakeXNAT):
        def list_subjects(self, project_name: str) -> List[str]:
            raise ConnectionError("Simulated VPN drop")

    server = _RaisingServer(project_name="PROJ", subjects={})

    try:
        result = fetch_data_table(server, "PROJ")
    except Exception as exc:
        pytest.fail(f"fetch_data_table raised unexpectedly: {exc}")

    assert isinstance(result, FriendlyError), (
        f"Expected FriendlyError on server error, got {type(result)}"
    )


def test_server_error_friendly_error_has_recourse():
    class _RaisingServer(BrowseFakeXNAT):
        def list_subjects(self, project_name: str) -> List[str]:
            raise TimeoutError("Request timed out")

    result = fetch_data_table(_RaisingServer(project_name="P", subjects={}), "P")
    assert isinstance(result, FriendlyError)
    assert result.title
    assert len(result.recourse) >= 1


# ---------------------------------------------------------------------------
# Test 6: filter_rows — case-insensitive substring match
# ---------------------------------------------------------------------------

@pytest.fixture()
def sample_rows() -> List[dict]:
    return [
        {"subject": "SUBJ001", "experiment": "EXP_KNEE", "date": "2025-01-15",
         "scan_type": "DICOM",     "num_files": 42},
        {"subject": "SUBJ002", "experiment": "EXP_HIP",  "date": "2024-06-30",
         "scan_type": "DICOM_MP4", "num_files": 18},
        {"subject": "SUBJ003", "experiment": "EXP_SHOULDER", "date": "2023-03-01",
         "scan_type": "DERIVED",   "num_files": 7},
    ]


def test_filter_rows_empty_query_returns_all(sample_rows):
    assert filter_rows(sample_rows, "") == sample_rows
    assert filter_rows(sample_rows, "   ") == sample_rows


def test_filter_rows_case_insensitive(sample_rows):
    result = filter_rows(sample_rows, "knee")
    assert len(result) == 1
    assert result[0]["experiment"] == "EXP_KNEE"

    result2 = filter_rows(sample_rows, "KNEE")
    assert result2 == result


def test_filter_rows_matches_across_columns(sample_rows):
    # "2024" is in the date of SUBJ002
    result = filter_rows(sample_rows, "2024")
    assert len(result) == 1
    assert result[0]["subject"] == "SUBJ002"


def test_filter_rows_scan_type_match(sample_rows):
    result = filter_rows(sample_rows, "mp4")
    assert len(result) == 1
    assert result[0]["scan_type"] == "DICOM_MP4"


def test_filter_rows_no_match_returns_empty(sample_rows):
    result = filter_rows(sample_rows, "ZZZNOMATCH")
    assert result == []


def test_filter_rows_multiple_matches(sample_rows):
    # "SUBJ" matches all three subjects
    result = filter_rows(sample_rows, "SUBJ")
    assert len(result) == 3


def test_filter_rows_does_not_mutate_original(sample_rows):
    original_len = len(sample_rows)
    filter_rows(sample_rows, "knee")
    assert len(sample_rows) == original_len


# ---------------------------------------------------------------------------
# Test 7: sort_rows
# ---------------------------------------------------------------------------

def test_sort_rows_by_subject(sample_rows):
    shuffled = [sample_rows[2], sample_rows[0], sample_rows[1]]
    result = sort_rows(shuffled, "subject")
    assert [r["subject"] for r in result] == ["SUBJ001", "SUBJ002", "SUBJ003"]


def test_sort_rows_by_date(sample_rows):
    result = sort_rows(sample_rows, "date")
    dates = [r["date"] for r in result]
    assert dates == sorted(dates)


def test_sort_rows_unknown_key_does_not_raise(sample_rows):
    result = sort_rows(sample_rows, "nonexistent_column")
    assert len(result) == len(sample_rows)


# ---------------------------------------------------------------------------
# Test 8: Display frame mapping — all COLUMNS, scan_id excluded from display
# ---------------------------------------------------------------------------

def test_display_frame_maps_all_columns_correctly():
    """
    Verify that _display_frame correctly handles all 6 COLUMNS keys
    and produces exactly 5 display columns without scan_id.

    Mirrors the live ValueError bug: DataFrame with 6 COLUMNS keys,
    display shows exactly 5 renamed headers, scan_id is absent.
    """
    # Import here to avoid top-level streamlit import in test file
    from app.pages.browse import _display_frame

    # Sample row with all 6 COLUMNS keys
    filtered = [
        {
            "subject": "SUBJ001",
            "experiment": "EXP_KNEE_2025",
            "date": "2025-01-15",
            "scan_type": "DICOM",
            "num_files": 42,
            "scan_id": "SCAN_DICOM",
        },
        {
            "subject": "SUBJ002",
            "experiment": "EXP_HIP_2024",
            "date": "2024-06-30",
            "scan_type": "DICOM_MP4",
            "num_files": 18,
            "scan_id": "SCAN_MP4",
        },
    ]

    # Call _display_frame
    df = _display_frame(filtered)

    # Verify 2 rows
    assert len(df) == 2, f"Expected 2 rows, got {len(df)}"

    # Verify exactly 5 display columns
    assert len(df.columns) == 5, (
        f"Expected 5 display columns, got {len(df.columns)}: {list(df.columns)}"
    )

    # Verify column names match DISPLAY_MAP values
    expected_columns = [
        "Subject",
        "Experiment",
        "Date",
        "Scan Type",
        "# Files",
    ]
    assert list(df.columns) == expected_columns, (
        f"Expected columns {expected_columns}, got {list(df.columns)}"
    )

    # Verify scan_id is NOT in the DataFrame
    assert "scan_id" not in df.columns, "scan_id should not appear in display columns"

    # Verify values are correct
    assert df.iloc[0]["Subject"] == "SUBJ001"
    assert df.iloc[0]["Experiment"] == "EXP_KNEE_2025"
    assert df.iloc[0]["Date"] == "2025-01-15"
    assert df.iloc[0]["Scan Type"] == "DICOM"
    assert df.iloc[0]["# Files"] == 42

    assert df.iloc[1]["Subject"] == "SUBJ002"
    assert df.iloc[1]["Date"] == "2024-06-30"
    assert df.iloc[1]["Scan Type"] == "DICOM_MP4"
    assert df.iloc[1]["# Files"] == 18
