"""
tests/test_browse_labels.py — Offline tests for browse label resolution (#29).

T010 (RED FIRST): staged RF experiment →
  - Subject column shows internal _S##### ID, not the human label.

After T011/T012 patches tests must pass (GREEN).

RULES:
  - NO streamlit import.
  - NO network.  FakeXNAT subclass used throughout.
  - NO PHI.

Covers (T010→T012):
  1. Subject column resolves to human label (not internal ID) after T011.
  2. RF experiment surfaces in list_downloadable after T012.
  3. Mixed-modality project: RF and CT both appear after T012.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.logic.browse import fetch_data_table, COLUMNS
from app.logic.download import list_downloadable
from src.services.errors import FriendlyError
from tests.fakes.fake_xnat import FakeXNAT


# ---------------------------------------------------------------------------
# Test double: BrowseLabelFakeXNAT
#
# Reproduces the real pyxnat bug (#29):
#   list_subjects() returns internal IDs (e.g. PROJ_S00001) not human labels.
#   The browse path then queries experiments by internal ID instead of label.
#   After T011 fix, browse resolves label via label_for_subject() before
#   any downstream query.
# ---------------------------------------------------------------------------

class BrowseLabelFakeXNAT(FakeXNAT):
    """
    FakeXNAT that returns internal IDs from list_subjects (reproducing the real
    pyxnat wildcard-select bug) but exposes label resolution so the fixed path
    can convert internal_id → label before downstream queries.
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        # Keyed by LABEL (not internal_id) so correct queries (by label) succeed.
        self._exp_data: Dict[str, Dict[str, Any]] = {}

    def seed_experiment_data(
        self,
        subject_label: str,
        exp_label: str,
        *,
        date: str = "2026-01-01",
        scans: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Add experiment/scan data keyed by subject LABEL."""
        if subject_label not in self._exp_data:
            self._exp_data[subject_label] = {}
        self._exp_data[subject_label][exp_label] = {
            "date": date,
            "scans": scans or {},
        }

    # --- fallback hooks used by browse logic ---

    def list_subjects(self, project_name: str) -> List[str]:
        """Return internal IDs only (reproducing the real pyxnat bug)."""
        return list(self._subject_labels.keys())

    def list_subjects_with_labels(self, project_name: str) -> List[tuple]:
        """Return [(internal_id, label), ...] for label-resolution hook."""
        return list(self._subject_labels.items())

    def list_experiments(self, project_name: str, subject: str) -> List[str]:
        """
        Return experiments for *subject*.

        If *subject* is an internal ID (no label mapping), returns [].
        If *subject* is a label, returns the seeded experiments.
        This simulates the real pyxnat behaviour where queries by internal ID
        fail to find label-keyed experiments.
        """
        # If subject is a label directly → success (post-fix path queries by label)
        if subject in self._exp_data:
            return list(self._exp_data[subject].keys())
        # subject is an internal_id — real pyxnat would return [] for this
        return []

    def list_scans(self, project_name: str, subject: str, experiment: str) -> List[str]:
        data = self._exp_data.get(subject, {})
        return list(data.get(experiment, {}).get("scans", {}).keys())

    def scan_attrs(self, project_name: str, subject: str, experiment: str, scan: str) -> dict:
        scan_data = (
            self._exp_data.get(subject, {})
            .get(experiment, {})
            .get("scans", {})
            .get(scan, {})
        )
        return {"scan_type": scan_data.get("scan_type", "")}

    def file_count(self, project_name: str, subject: str, experiment: str, scan: str) -> int:
        scan_data = (
            self._exp_data.get(subject, {})
            .get(experiment, {})
            .get("scans", {})
            .get(scan, {})
        )
        return scan_data.get("num_files", -1)

    def experiment_date(self, project_name: str, subject: str, experiment: str) -> str:
        return self._exp_data.get(subject, {}).get(experiment, {}).get("date", "")


# ---------------------------------------------------------------------------
# Constants / fixtures
# ---------------------------------------------------------------------------

PROJECT = "TEST_PROJECT"
INTERNAL_ID = "TEST_S00001"
SUBJECT_LABEL = "ITEST_SUBJ_0001"
RF_EXP = "RF_EXP_20260101"
CT_EXP = "CT_EXP_20260101"


@pytest.fixture()
def rf_fake() -> BrowseLabelFakeXNAT:
    """FakeXNAT with one RF experiment seeded (internal_id != label)."""
    fake = BrowseLabelFakeXNAT(project_name=PROJECT)
    # Seed internal_id → label mapping
    fake.seed_subject_label(INTERNAL_ID, SUBJECT_LABEL)
    # Seed experiment data keyed by LABEL (correct queries by label succeed)
    fake.seed_experiment_data(
        SUBJECT_LABEL,
        RF_EXP,
        date="2026-01-01",
        scans={"0": {"scan_type": "DICOM", "num_files": 5}},
    )
    # Seed type metadata for type-agnostic enumeration (T012)
    fake.seed_rf_experiment(SUBJECT_LABEL, RF_EXP, "xnat:rfSessionData")
    return fake


@pytest.fixture()
def mixed_fake() -> BrowseLabelFakeXNAT:
    """FakeXNAT with RF + CT experiments (mixed-modality)."""
    fake = BrowseLabelFakeXNAT(project_name=PROJECT)
    fake.seed_subject_label(INTERNAL_ID, SUBJECT_LABEL)
    fake.seed_experiment_data(
        SUBJECT_LABEL, RF_EXP,
        date="2026-01-01",
        scans={"0": {"scan_type": "DICOM", "num_files": 5}},
    )
    fake.seed_experiment_data(
        SUBJECT_LABEL, CT_EXP,
        date="2026-01-02",
        scans={"0": {"scan_type": "DICOM", "num_files": 3}},
    )
    fake.seed_rf_experiment(SUBJECT_LABEL, RF_EXP, "xnat:rfSessionData")
    fake.seed_rf_experiment(SUBJECT_LABEL, CT_EXP, "xnat:ctSessionData")
    return fake


# ---------------------------------------------------------------------------
# T010 RED: current broken behaviour
# ---------------------------------------------------------------------------

class TestSubjectIDNotResolvedToday:
    """
    RED-state documentation (xfail).

    Pre-T011: subject column showed internal IDs; RF experiments were missing.
    After T011 fix: labels resolved, RF appears → assertions invert → test FAILS
    → xfail(strict=True) stays xfail.
    """

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "T010 red-state: pre-T011 _subject_names() returned internal IDs so "
            "SUBJECT_LABEL never appeared in the subject column.  After T011 fix "
            "labels are resolved → SUBJECT_LABEL appears → assert fails → xfail."
        ),
    )
    def test_internal_id_in_subject_column_today(
        self, rf_fake: BrowseLabelFakeXNAT
    ) -> None:
        """
        RED (xfail): pre-T011, subject column showed internal ID not label.
        After T011: label appears → assert 'not in' fails → xfail.
        """
        rows = list_downloadable(rf_fake, PROJECT)
        if isinstance(rows, FriendlyError):
            pytest.fail(f"list_downloadable returned FriendlyError: {rows.message}")

        subject_values = {r["subject"] for r in rows}
        assert SUBJECT_LABEL not in subject_values, (
            f"Human label '{SUBJECT_LABEL}' appeared — T011 fix already applied."
        )

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "T010 red-state: pre-T011/T012 RF experiment not found because "
            "list_experiments queried by internal_id returned [].  After fix "
            "RF_EXP appears → assert fails → xfail."
        ),
    )
    def test_rf_experiment_missing_with_internal_id_query(
        self, rf_fake: BrowseLabelFakeXNAT
    ) -> None:
        """
        RED (xfail): pre-T011/T012, RF experiment absent (internal-ID query).
        After fix: RF_EXP appears → assert 'not in' fails → xfail.
        """
        rows = list_downloadable(rf_fake, PROJECT)
        if isinstance(rows, FriendlyError):
            pytest.fail(f"list_downloadable returned FriendlyError: {rows.message}")

        experiment_labels = [r.get("experiment", "") for r in rows]
        assert RF_EXP not in experiment_labels, (
            f"RF experiment '{RF_EXP}' appeared — T011/T012 fix already applied."
        )


# ---------------------------------------------------------------------------
# T010→T012 GREEN: correct behaviour after fix
# ---------------------------------------------------------------------------

class TestLabelResolutionAfterFix:
    """
    GREEN tests: after T011/T012 patches labels are resolved and RF surfaces.
    These FAIL today (proving the fix is needed).
    """

    def test_subject_column_shows_label_not_internal_id(
        self, rf_fake: BrowseLabelFakeXNAT
    ) -> None:
        """
        After T011: subject column = SUBJECT_LABEL, not INTERNAL_ID.

        Requires browse._subject_names to call label_for_subject() (or
        equivalent) to resolve internal IDs to labels.
        """
        rows = list_downloadable(rf_fake, PROJECT)
        if isinstance(rows, FriendlyError):
            pytest.fail(f"list_downloadable returned FriendlyError: {rows.message}")

        assert rows, "No rows returned — T011/T012 fix not applied."

        for row in rows:
            assert row["subject"] == SUBJECT_LABEL, (
                f"Subject '{row['subject']}' is not the expected label "
                f"'{SUBJECT_LABEL}' — T011 label resolution not applied."
            )
            assert row["subject"] != INTERNAL_ID, (
                f"Subject column still shows internal ID '{INTERNAL_ID}'."
            )

    def test_rf_experiment_appears_after_fix(
        self, rf_fake: BrowseLabelFakeXNAT
    ) -> None:
        """
        After T011/T012: RF experiment appears in list_downloadable.

        Requires label resolution (T011) so the experiment query is by label,
        AND type-agnostic enumeration (T012) so rfSessionData surfaces.
        """
        rows = list_downloadable(rf_fake, PROJECT)
        if isinstance(rows, FriendlyError):
            pytest.fail(f"list_downloadable returned FriendlyError: {rows.message}")

        experiment_labels = [r.get("experiment") for r in rows]
        assert RF_EXP in experiment_labels, (
            f"RF experiment '{RF_EXP}' not in downloadable rows after fix. "
            f"Rows: {rows}"
        )

    def test_mixed_modality_surfaces_all_types(
        self, mixed_fake: BrowseLabelFakeXNAT
    ) -> None:
        """
        After T012: both RF and CT experiments appear (type-agnostic enumeration).
        """
        rows = list_downloadable(mixed_fake, PROJECT)
        if isinstance(rows, FriendlyError):
            pytest.fail(f"list_downloadable returned FriendlyError: {rows.message}")

        exp_labels = {r.get("experiment") for r in rows}
        assert RF_EXP in exp_labels, (
            f"RF experiment '{RF_EXP}' missing from mixed-modality project. "
            f"Rows: {rows}"
        )
        assert CT_EXP in exp_labels, (
            f"CT experiment '{CT_EXP}' missing from mixed-modality project. "
            f"Rows: {rows}"
        )
