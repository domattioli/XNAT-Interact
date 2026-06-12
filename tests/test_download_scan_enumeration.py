"""
tests/test_download_scan_enumeration.py — Scan enumeration & scan_id resolution in download.

Covers the fix for confirmed-live download gap (#25/#33-C1/H7):
browse rows now include scan_id (real scan label), and download_selection uses it
to resolve which scans to download when scan_type is empty or wrong.

RULES:
  - NO streamlit import.
  - NO network. FakeXNAT + seed_resource_files used throughout.
  - NO PHI.
  - Cross-platform paths via pathlib.

Behavior contract:
  1. Row with scan_id present → only that scan downloaded.
  2. Row with empty scan_id + empty scan_type → ALL scans enumerated + downloaded.
  3. Row with scan_type but no scan_id → scan_type used (legacy back-compat).
  4. Multi-resource scan (SRC + DERIVED) → both resources' files downloaded.
  5. Nonexistent experiment → graceful skip (no crash).
  6. Count verification works per scan per resource.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

from app.logic.download import download_selection, DownloadOutcome
from src.services.errors import FriendlyError
from tests.fakes.fake_xnat import FakeXNAT, FakeResource


# ---------------------------------------------------------------------------
# Test double: EnumerationFakeXNAT
# Extends FakeXNAT with scan/resource enumeration hooks
# ---------------------------------------------------------------------------

class EnumerationFakeXNAT(FakeXNAT):
    """
    FakeXNAT for scan enumeration tests.

    Supports seeding scans, resources, and files with full pyxnat-style enumeration.
    Uses the parent FakeXNAT's _resources registry so that select().resource()
    returns the same pre-seeded FakeResource objects.
    """

    def __init__(self, subjects: Optional[Dict[str, Any]] = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._subjects: Dict[str, Any] = subjects or {}

    def get_or_create_resource(
        self,
        subject: str,
        experiment: str,
        scan: str,
        resource_label: str = "SRC",
    ) -> FakeResource:
        """Get or create a FakeResource using the standard registry."""
        key = (subject, experiment, scan, resource_label)
        if key not in self._resources:
            self._resources[key] = FakeResource(root=self, label=resource_label)
        return self._resources[key]

    # --- Enumeration hooks for browse fallback ---

    def list_subjects(self, project_name: str) -> List[str]:
        return list(self._subjects.keys())

    def list_experiments(self, project_name: str, subject: str) -> List[str]:
        return list(self._subjects.get(subject, {}).keys())

    def list_scans(self, project_name: str, subject: str, experiment: str) -> List[str]:
        exp = self._subjects.get(subject, {}).get(experiment, {})
        return list(exp.get("scans", {}).keys())

    def list_resources(self, project_name: str, subject: str, experiment: str, scan: str) -> List[str]:
        """Return resource labels for a scan."""
        exp = self._subjects.get(subject, {}).get(experiment, {})
        scan_data = exp.get("scans", {}).get(scan, {})
        return list(scan_data.get("resources", {}).keys()) if scan_data else []

    def scan_attrs(self, project_name: str, subject: str, experiment: str, scan: str) -> dict:
        exp = self._subjects.get(subject, {}).get(experiment, {})
        scan_data = exp.get("scans", {}).get(scan, {})
        return {"scan_type": scan_data.get("scan_type", "")} if scan_data else {}

    def file_count(self, project_name: str, subject: str, experiment: str, scan: str) -> int:
        exp = self._subjects.get(subject, {}).get(experiment, {})
        scan_data = exp.get("scans", {}).get(scan, {})
        return scan_data.get("num_files", -1) if scan_data else -1

    def experiment_date(self, project_name: str, subject: str, experiment: str) -> str:
        exp = self._subjects.get(subject, {}).get(experiment, {})
        return exp.get("date", "") if exp else ""


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def tmp_dest(tmp_path):
    """Temporary destination directory for downloads."""
    return tmp_path / "downloads"


@pytest.fixture()
def server_multi_scan() -> EnumerationFakeXNAT:
    """
    Server with one subject, one experiment, multiple scans (0, 1, 2).
    Scan '0' (real label) has scan_type='DICOM', multiple files, SRC resource.
    Scan '1' has scan_type='DICOM', SRC + DERIVED resources.
    """
    server = EnumerationFakeXNAT(
        project_name="ENUM_PROJ",
        subjects={
            "SUBJ_ENUM_A": {
                "EXP_ENUM_001": {
                    "date": "2025-03-10",
                    "scans": {
                        "0": {
                            "scan_type": "DICOM",
                            "num_files": 3,
                            "resources": {"SRC": {}},
                        },
                        "1": {
                            "scan_type": "DICOM",
                            "num_files": 2,
                            "resources": {"SRC": {}, "DERIVED": {}},
                        },
                        "2": {
                            "scan_type": "DERIVED",
                            "num_files": 1,
                            "resources": {"DERIVED": {}},
                        },
                    },
                },
            },
        },
    )

    # Seed scan 0, SRC with 3 files
    res_0_src = server.get_or_create_resource("SUBJ_ENUM_A", "EXP_ENUM_001", "0", "SRC")
    server.seed_resource_files(res_0_src, [
        ("img_0_001.dcm", b"fake_dcm_001"),
        ("img_0_002.dcm", b"fake_dcm_002"),
        ("img_0_003.dcm", b"fake_dcm_003"),
    ])

    # Seed scan 1, SRC with 2 files
    res_1_src = server.get_or_create_resource("SUBJ_ENUM_A", "EXP_ENUM_001", "1", "SRC")
    server.seed_resource_files(res_1_src, [
        ("img_1_001.dcm", b"fake_dcm_1_src_001"),
        ("img_1_002.dcm", b"fake_dcm_1_src_002"),
    ])

    # Seed scan 1, DERIVED with 1 file
    res_1_der = server.get_or_create_resource("SUBJ_ENUM_A", "EXP_ENUM_001", "1", "DERIVED")
    server.seed_resource_files(res_1_der, [
        ("derived_001.nii", b"fake_derived_001"),
    ])

    # Seed scan 2, DERIVED with 1 file
    res_2_der = server.get_or_create_resource("SUBJ_ENUM_A", "EXP_ENUM_001", "2", "DERIVED")
    server.seed_resource_files(res_2_der, [
        ("derived_002.nii", b"fake_derived_002"),
    ])

    return server


# ---------------------------------------------------------------------------
# Tests: Behavior contract
# ---------------------------------------------------------------------------

def test_download_with_scan_id_downloads_only_that_scan(server_multi_scan, tmp_dest):
    """
    Behavior 1: Row with scan_id present → only that scan downloaded.

    Selection row specifies scan_id='1'. Should download ONLY scan 1 files
    (both SRC and DERIVED resources), not scans 0 or 2.
    """
    selection = [
        {
            "subject": "SUBJ_ENUM_A",
            "experiment": "EXP_ENUM_001",
            "date": "2025-03-10",
            "scan_type": "DICOM",
            "num_files": 3,
            "scan_id": "1",
        }
    ]

    outcome = download_selection(server_multi_scan, "ENUM_PROJ", selection, tmp_dest)

    assert outcome.ok is True, f"Download failed: {outcome.friendly}"
    assert len(outcome.files_written) == 3, f"Expected 3 files (2 SRC + 1 DERIVED), got {len(outcome.files_written)}"

    # Verify files from scan 1 are present
    file_contents = {f.name for f in outcome.files_written}
    assert "img_1_001.dcm" in file_contents
    assert "img_1_002.dcm" in file_contents
    assert "derived_001.nii" in file_contents


def test_download_empty_scan_id_enumerates_all_scans(server_multi_scan, tmp_dest):
    """
    Behavior 2: Row with empty scan_id + empty scan_type → ALL scans enumerated + downloaded.

    Selection row has no scan_id and no scan_type. Should enumerate all scans (0, 1, 2)
    and download all files from all resources.
    """
    selection = [
        {
            "subject": "SUBJ_ENUM_A",
            "experiment": "EXP_ENUM_001",
            "date": "2025-03-10",
            "scan_type": "",
            "num_files": -1,
            "scan_id": "",
        }
    ]

    outcome = download_selection(server_multi_scan, "ENUM_PROJ", selection, tmp_dest)

    assert outcome.ok is True, f"Download failed: {outcome.friendly}"
    # Scans: 0 (3 SRC) + 1 (2 SRC + 1 DERIVED) + 2 (1 DERIVED) = 7 files
    assert len(outcome.files_written) == 7, f"Expected 7 files total, got {len(outcome.files_written)}"

    # Verify files from all scans are present
    file_names = {f.name for f in outcome.files_written}
    assert "img_0_001.dcm" in file_names, "Scan 0 SRC file missing"
    assert "img_1_001.dcm" in file_names, "Scan 1 SRC file missing"
    assert "derived_001.nii" in file_names, "Scan 1 DERIVED file missing"
    assert "derived_002.nii" in file_names, "Scan 2 DERIVED file missing"


def test_download_scan_type_without_scan_id_uses_legacy_path(tmp_dest):
    """
    Behavior 3: Row with scan_type but no scan_id → scan_type used (legacy back-compat).

    Server with a single scan labeled 'DICOM' (legacy single-scan scenario).
    Selection provides scan_type='DICOM' but no scan_id.
    Should download using scan_type as the scan ID.
    """
    server = EnumerationFakeXNAT(
        project_name="LEGACY_PROJ",
        subjects={
            "LEGACY_SUBJ": {
                "LEGACY_EXP": {
                    "date": "2025-01-01",
                    "scans": {
                        "DICOM": {
                            "scan_type": "DICOM",
                            "num_files": 2,
                            "resources": {"SRC": {}},
                        },
                    },
                },
            },
        },
    )

    # Seed the legacy scan
    res = server.get_or_create_resource("LEGACY_SUBJ", "LEGACY_EXP", "DICOM", "SRC")
    server.seed_resource_files(res, [
        ("legacy_001.dcm", b"fake_legacy_001"),
        ("legacy_002.dcm", b"fake_legacy_002"),
    ])

    selection = [
        {
            "subject": "LEGACY_SUBJ",
            "experiment": "LEGACY_EXP",
            "date": "2025-01-01",
            "scan_type": "DICOM",
            "num_files": 2,
            "scan_id": "",
        }
    ]

    outcome = download_selection(server, "LEGACY_PROJ", selection, tmp_dest)

    assert outcome.ok is True, f"Download failed: {outcome.friendly}"
    assert len(outcome.files_written) == 2, f"Expected 2 files, got {len(outcome.files_written)}"


def test_download_multi_resource_scan_downloads_all_resources(server_multi_scan, tmp_dest):
    """
    Behavior 4: Multi-resource scan (SRC + DERIVED) → both resources' files downloaded.

    Selection specifies scan 1 which has both SRC and DERIVED resources.
    Should enumerate both and download all files from both.
    """
    selection = [
        {
            "subject": "SUBJ_ENUM_A",
            "experiment": "EXP_ENUM_001",
            "date": "2025-03-10",
            "scan_type": "DICOM",
            "num_files": 3,
            "scan_id": "1",
        }
    ]

    outcome = download_selection(server_multi_scan, "ENUM_PROJ", selection, tmp_dest)

    assert outcome.ok is True, f"Download failed: {outcome.friendly}"
    # Scan 1: 2 SRC + 1 DERIVED = 3 files
    assert len(outcome.files_written) == 3, f"Expected 3 files, got {len(outcome.files_written)}"

    # Verify both SRC and DERIVED files present
    file_names = {f.name for f in outcome.files_written}
    src_files = [f for f in file_names if f.startswith("img_1_")]
    derived_files = [f for f in file_names if f.startswith("derived")]
    assert len(src_files) == 2, f"Expected 2 SRC files, got {len(src_files)}"
    assert len(derived_files) == 1, f"Expected 1 DERIVED file, got {len(derived_files)}"


def test_download_nonexistent_experiment_graceful_skip(tmp_dest):
    """
    Behavior 5: Nonexistent experiment → graceful skip (no crash).

    Selection references a nonexistent experiment.
    Should gracefully skip (not crash, not error).
    """
    server = EnumerationFakeXNAT(
        project_name="MISSING_PROJ",
        subjects={
            "SUBJ_EXISTS": {
                "EXP_EXISTS": {
                    "date": "2025-01-01",
                    "scans": {
                        "0": {
                            "scan_type": "DICOM",
                            "num_files": 1,
                            "resources": {"SRC": {}},
                        },
                    },
                },
            },
        },
    )

    # Seed only the real scan
    res = server.get_or_create_resource("SUBJ_EXISTS", "EXP_EXISTS", "0", "SRC")
    server.seed_resource_files(res, [
        ("real_file.dcm", b"real_content"),
    ])

    # Selection has one real and one fake row
    selection = [
        {
            "subject": "SUBJ_EXISTS",
            "experiment": "EXP_DOES_NOT_EXIST",
            "date": "",
            "scan_type": "",
            "num_files": -1,
            "scan_id": "",
        },
        {
            "subject": "SUBJ_EXISTS",
            "experiment": "EXP_EXISTS",
            "date": "2025-01-01",
            "scan_type": "DICOM",
            "num_files": 1,
            "scan_id": "0",
        },
    ]

    outcome = download_selection(server, "MISSING_PROJ", selection, tmp_dest)

    # Should succeed with only the real row downloaded
    assert outcome.ok is True, f"Download should succeed (graceful skip on missing exp): {outcome.friendly}"
    assert len(outcome.files_written) == 1, f"Expected 1 file (only real exp), got {len(outcome.files_written)}"


def test_download_count_verification_per_scan_and_resource(tmp_dest):
    """
    Behavior 6: Count verification works per scan per resource.

    When the FakeResource.num_files() reports a count that doesn't match
    the actual enumerated files, a FriendlyError should be raised.
    We simulate this by creating a test double that reports a mismatch.
    """
    server = EnumerationFakeXNAT(
        project_name="COUNT_PROJ",
        subjects={
            "SUBJ_COUNT": {
                "EXP_COUNT": {
                    "date": "2025-01-01",
                    "scans": {
                        "0": {
                            "scan_type": "DICOM",
                            "num_files": 5,  # Server claims 5 files
                            "resources": {"SRC": {}},
                        },
                    },
                },
            },
        },
    )

    # Seed 2 files but seed num_files to report 5 (mismatch)
    res = server.get_or_create_resource("SUBJ_COUNT", "EXP_COUNT", "0", "SRC")
    server.seed_resource_files(res, [
        ("file_001.dcm", b"content_001"),
        ("file_002.dcm", b"content_002"),
    ])

    # Monkeypatch num_files to return 5 (mismatch)
    original_num_files = res.num_files
    res.num_files = lambda: 5

    selection = [
        {
            "subject": "SUBJ_COUNT",
            "experiment": "EXP_COUNT",
            "date": "2025-01-01",
            "scan_type": "DICOM",
            "num_files": 5,
            "scan_id": "0",
        }
    ]

    outcome = download_selection(server, "COUNT_PROJ", selection, tmp_dest)

    # Should fail with FriendlyError about count mismatch
    assert outcome.ok is False, "Expected download to fail due to count mismatch"
    assert outcome.friendly is not None, "Expected FriendlyError"
    assert "count mismatch" in outcome.friendly.title.lower()


def test_download_empty_resource_graceful_skip(tmp_dest):
    """
    Empty resource (no files) should be skipped gracefully, not error.

    Scan exists but its resource has no files.
    """
    server = EnumerationFakeXNAT(
        project_name="EMPTY_PROJ",
        subjects={
            "SUBJ_EMPTY": {
                "EXP_EMPTY": {
                    "date": "2025-01-01",
                    "scans": {
                        "0": {
                            "scan_type": "DICOM",
                            "num_files": 0,  # Empty resource
                            "resources": {"SRC": {}},
                        },
                    },
                },
            },
        },
    )

    # Seed empty resource (no files)
    res = server.get_or_create_resource("SUBJ_EMPTY", "EXP_EMPTY", "0", "SRC")
    server.seed_resource_files(res, [])

    selection = [
        {
            "subject": "SUBJ_EMPTY",
            "experiment": "EXP_EMPTY",
            "date": "2025-01-01",
            "scan_type": "DICOM",
            "num_files": 0,
            "scan_id": "0",
        }
    ]

    outcome = download_selection(server, "EMPTY_PROJ", selection, tmp_dest)

    # Should succeed but with no files downloaded
    assert outcome.ok is True, "Should handle empty resource gracefully"
    assert len(outcome.files_written) == 0, "Empty resource should yield no files"


def test_download_multiple_subjects_and_experiments(tmp_dest):
    """
    Selection with multiple rows (different subjects/experiments).
    Should download from all selected rows.
    """
    server = EnumerationFakeXNAT(
        project_name="MULTI_PROJ",
        subjects={
            "SUBJ_A": {
                "EXP_A1": {
                    "date": "2025-01-01",
                    "scans": {
                        "0": {
                            "scan_type": "DICOM",
                            "num_files": 2,
                            "resources": {"SRC": {}},
                        },
                    },
                },
            },
            "SUBJ_B": {
                "EXP_B1": {
                    "date": "2025-02-01",
                    "scans": {
                        "0": {
                            "scan_type": "DICOM",
                            "num_files": 1,
                            "resources": {"SRC": {}},
                        },
                    },
                },
            },
        },
    )

    # Seed both subjects' scans
    res_a = server.get_or_create_resource("SUBJ_A", "EXP_A1", "0", "SRC")
    server.seed_resource_files(res_a, [
        ("file_a_001.dcm", b"content_a_001"),
        ("file_a_002.dcm", b"content_a_002"),
    ])

    res_b = server.get_or_create_resource("SUBJ_B", "EXP_B1", "0", "SRC")
    server.seed_resource_files(res_b, [
        ("file_b_001.dcm", b"content_b_001"),
    ])

    selection = [
        {
            "subject": "SUBJ_A",
            "experiment": "EXP_A1",
            "date": "2025-01-01",
            "scan_type": "DICOM",
            "num_files": 2,
            "scan_id": "0",
        },
        {
            "subject": "SUBJ_B",
            "experiment": "EXP_B1",
            "date": "2025-02-01",
            "scan_type": "DICOM",
            "num_files": 1,
            "scan_id": "0",
        },
    ]

    outcome = download_selection(server, "MULTI_PROJ", selection, tmp_dest)

    assert outcome.ok is True, f"Download failed: {outcome.friendly}"
    # 2 files from SUBJ_A + 1 from SUBJ_B = 3
    assert len(outcome.files_written) == 3, f"Expected 3 files, got {len(outcome.files_written)}"

    # Verify directory structure
    assert (tmp_dest / "SUBJ_A" / "EXP_A1").exists()
    assert (tmp_dest / "SUBJ_B" / "EXP_B1").exists()
