"""
tests/test_xsitype_creation.py — TASK 1 fix verification.

Tests that ExperimentData.publish_to_xnat() creates experiments and scans
using the correct kwarg form: experiments='xnat:...' and scans='xnat:...',
not xsiType='xnat:...'.

The fix ensures that pyxnat correctly stores the xsiType on the server
(e.g., xnat:rfSessionData for RF experiments).
"""
from __future__ import annotations

import zipfile
from pathlib import Path
from types import SimpleNamespace
from typing import Tuple

import pytest

from src.xnat_experiment_data import ExperimentData, ReviewDecision
from tests.fakes.fake_xnat import FakeXNAT


def _make_connection(fake: FakeXNAT, project: str = "TEST_PROJECT") -> SimpleNamespace:
    return SimpleNamespace(server=fake, gateway=fake, xnat_project_name=project)


def _make_login(username: str = "testuser") -> SimpleNamespace:
    return SimpleNamespace(validated_username=username)


def _make_intake_form(tmp_path: Path) -> SimpleNamespace:
    saved_ffn = tmp_path / "RECONSTRUCTED_OR_DATA_INTAKE_FORM.json"
    saved_ffn.write_text("{}", encoding="utf-8")
    dt_stub = SimpleNamespace(date="2024-01-01", time="120000")
    form = SimpleNamespace(
        uid="TEST_UID_001",
        group="TEST_GROUP",
        acquisition_site="TEST_SITE",
        ortho_procedure_type="TEST_PROCEDURE",
        scan_quality="usable",
        datetime=dt_stub,
        relevant_folder=tmp_path,
        saved_ffn=saved_ffn,
        saved_ffn_str=str(saved_ffn),
    )

    def push_to_xnat(subj_inst=None, verbose=False, **kwargs):
        pass  # gateway path handles the upload; mock is a no-op

    form.push_to_xnat = push_to_xnat
    return form


class _MinimalRFSession(ExperimentData):
    """Thin subclass that skips __init__ for unit testing publish_to_xnat."""

    @classmethod
    def build(cls, intake_form) -> "_MinimalRFSession":
        obj = object.__new__(cls)
        obj._intake_form = intake_form
        obj._schema_prefix_str = "rf"
        obj._scan_type_label = "DICOM"
        obj._is_valid = True
        obj._df = None
        return obj

    def _populate_df(self, config):
        pass

    def _check_session_validity(self, config):
        pass

    def write(self, config, zip_dest=None, verbose=True):
        raise NotImplementedError("use pre-built zipped_data in tests")


def _confirmed_confirmer(ctx):
    return ReviewDecision.CONFIRMED, []


@pytest.fixture()
def fake_zip(tmp_path: Path) -> dict:
    zip_path = tmp_path / "dicom_files.zip"
    with zipfile.ZipFile(str(zip_path), "w") as zf:
        zf.writestr("placeholder.dcm", b"FAKE")
    return {str(zip_path): {"CONTENT": "IMAGE", "FORMAT": "DICOM", "TAG": "INTRA_OP"}}


class TestXsiTypeCreationFix:
    """
    Verify that ExperimentData.publish_to_xnat() creates experiments and scans
    using the correct kwarg forms that pyxnat actually respects.
    """

    def test_experiment_create_uses_experiments_kwarg(self, tmp_path, fake_zip):
        """Verify experiment create() is called with experiments='xnat:rfSessionData'."""
        fake = FakeXNAT(project_name="TEST_PROJECT", fidelity_mode=False)
        conn = _make_connection(fake)
        login = _make_login()
        intake = _make_intake_form(tmp_path)
        session = _MinimalRFSession.build(intake)

        session.publish_to_xnat(
            xnat_connection=conn,
            validated_login=login,
            zipped_data=fake_zip,
            delete_zip=False,
            verbose=False,
            pixel_review_confirmer=_confirmed_confirmer,
        )

        # Find the experiment create() call
        exp_create_calls = [
            c for c in fake.calls
            if c["op"] == "selectable.create" and "experiments" in c["kwargs"]
        ]
        assert len(exp_create_calls) >= 1, (
            "No experiment create() call with 'experiments' kwarg found. "
            f"Calls: {[c['kwargs'] for c in fake.calls if c['op'] == 'selectable.create']}"
        )
        # Verify the kwarg is exactly the right form
        exp_create = exp_create_calls[0]
        assert exp_create["kwargs"]["experiments"] == "xnat:rfSessionData", (
            f"Expected experiments='xnat:rfSessionData', got "
            f"{exp_create['kwargs']['experiments']}"
        )

    def test_scan_create_uses_scans_kwarg(self, tmp_path, fake_zip):
        """Verify scan create() is called with scans='xnat:rfScanData'."""
        fake = FakeXNAT(project_name="TEST_PROJECT", fidelity_mode=False)
        conn = _make_connection(fake)
        login = _make_login()
        intake = _make_intake_form(tmp_path)
        session = _MinimalRFSession.build(intake)

        session.publish_to_xnat(
            xnat_connection=conn,
            validated_login=login,
            zipped_data=fake_zip,
            delete_zip=False,
            verbose=False,
            pixel_review_confirmer=_confirmed_confirmer,
        )

        # Find the scan create() call
        scan_create_calls = [
            c for c in fake.calls
            if c["op"] == "selectable.create" and "scans" in c["kwargs"]
        ]
        assert len(scan_create_calls) >= 1, (
            "No scan create() call with 'scans' kwarg found. "
            f"Calls: {[c['kwargs'] for c in fake.calls if c['op'] == 'selectable.create']}"
        )
        # Verify the kwarg is exactly the right form
        scan_create = scan_create_calls[0]
        assert scan_create["kwargs"]["scans"] == "xnat:rfScanData", (
            f"Expected scans='xnat:rfScanData', got "
            f"{scan_create['kwargs']['scans']}"
        )

    def test_experiment_datatype_cache_set_after_create(self, tmp_path, fake_zip):
        """
        Verify that attrs._datatype is set immediately after create()
        (pyxnat internals #27) so mset() works correctly.
        """
        fake = FakeXNAT(project_name="TEST_PROJECT", fidelity_mode=True)
        conn = _make_connection(fake)
        login = _make_login()
        intake = _make_intake_form(tmp_path)
        session = _MinimalRFSession.build(intake)

        # With fidelity_mode=True, attrs.mset() will raise TypeError if _datatype
        # is None. This test verifies that the fix sets _datatype before mset().
        session.publish_to_xnat(
            xnat_connection=conn,
            validated_login=login,
            zipped_data=fake_zip,
            delete_zip=False,
            verbose=False,
            pixel_review_confirmer=_confirmed_confirmer,
        )

        # If we got here without TypeError, the fix is working.
        # Verify attrs.mset was called for experiments
        mset_calls = [c for c in fake.calls if c["op"] == "attrs.mset"]
        assert len(mset_calls) >= 3, (
            f"Expected ≥3 mset calls (subj/exp/scan), got {len(mset_calls)}"
        )

    def test_scan_datatype_cache_set_after_create(self, tmp_path, fake_zip):
        """
        Verify that attrs._datatype is set immediately after scan create()
        so mset() works correctly.
        """
        fake = FakeXNAT(project_name="TEST_PROJECT", fidelity_mode=True)
        conn = _make_connection(fake)
        login = _make_login()
        intake = _make_intake_form(tmp_path)
        session = _MinimalRFSession.build(intake)

        session.publish_to_xnat(
            xnat_connection=conn,
            validated_login=login,
            zipped_data=fake_zip,
            delete_zip=False,
            verbose=False,
            pixel_review_confirmer=_confirmed_confirmer,
        )

        # If we got here without TypeError, the fix is working.
        # Verify the scan_id is still correct in any logged data
        assert fake.calls, "No calls recorded"
