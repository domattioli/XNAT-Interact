"""
tests/test_publish_real_contract.py — #27 push-blocker regression contract.

Exercises publish_to_xnat against a FakeXNAT configured with fidelity_mode=True,
which reproduces pyxnat's post-create() empty datatype-cache bug:
  - Pre-fix: TypeError raised when attrs.mset() is called on a freshly create()d
    handle whose _datatype is still None.
  - Post-fix: datatype set immediately after create(); mset() succeeds;
    server state shows 1 exp + 1 subj + scan 0 + SRC.

T005b: idempotent-upsert — pre-seed an orphaned/partial subject, run publish,
final state has exactly one subject/experiment with all children (SC-001 upsert).

Offline only. No real server, no real PHI.
"""
from __future__ import annotations

import zipfile
from pathlib import Path
from types import SimpleNamespace
from typing import Tuple

import pytest

from src.xnat_experiment_data import ExperimentData, ReviewDecision
from tests.fakes.fake_xnat import FakeXNAT


# ---------------------------------------------------------------------------
# Helpers — copied from test_upload_phi_gate.py pattern
# ---------------------------------------------------------------------------

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

    def push_to_xnat(subj_inst, verbose=False):
        subj_inst.resource("INTAKE_FORM").file("form.json").insert(
            "{}", content="TEXT", format="JSON", tags="DOC"
        )

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


# ---------------------------------------------------------------------------
# T004-A — fidelity fake raises TypeError BEFORE the fix
# ---------------------------------------------------------------------------

class TestPreFixBehavior:
    """Proves the old blind spot: standard FakeXNAT masks the bug."""

    def test_standard_fake_does_not_raise(self, tmp_path, fake_zip):
        """The plain fake lets mset() pass even with no datatype — this is
        the blind spot that hid the #27 bug in the 718-test suite."""
        fake = FakeXNAT(project_name="TEST_PROJECT", fidelity_mode=False)
        conn = _make_connection(fake)
        login = _make_login()
        intake = _make_intake_form(tmp_path)
        session = _MinimalRFSession.build(intake)

        # Should NOT raise with standard (non-fidelity) fake — existing behavior.
        session.publish_to_xnat(
            xnat_connection=conn,
            validated_login=login,
            zipped_data=fake_zip,
            delete_zip=False,
            verbose=False,
            pixel_review_confirmer=_confirmed_confirmer,
        )

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "T004 red-state documentation: pre-fix code raises TypeError on "
            "first attrs.mset() after create() with fidelity_mode=True.  "
            "After T005 fix the TypeError is gone, so this test is xfail "
            "(expected to not raise = expected xpass → strict xfail passes "
            "in the green state).  Preserves the regression evidence."
        ),
    )
    def test_fidelity_fake_raises_type_error_pre_fix(self, tmp_path, fake_zip):
        """With fidelity_mode=True the fake reproduces the real pyxnat TypeError
        that today's code triggers on the first attrs.mset() after create().
        This is the RED test — it proves today's code is broken on real pyxnat."""
        fake = FakeXNAT(project_name="TEST_PROJECT", fidelity_mode=True)
        conn = _make_connection(fake)
        login = _make_login()
        intake = _make_intake_form(tmp_path)
        session = _MinimalRFSession.build(intake)

        with pytest.raises(TypeError, match="quote_from_bytes"):
            session.publish_to_xnat(
                xnat_connection=conn,
                validated_login=login,
                zipped_data=fake_zip,
                delete_zip=False,
                verbose=False,
                pixel_review_confirmer=_confirmed_confirmer,
            )


# ---------------------------------------------------------------------------
# T004-B / T005 — post-fix: fidelity fake must NOT raise; full state asserted
# ---------------------------------------------------------------------------

class TestPostFixBehavior:
    """After T005 fix these pass.  Will fail until the fix is applied."""

    def test_fidelity_fake_no_type_error_after_fix(self, tmp_path, fake_zip):
        """publish_to_xnat completes without TypeError against fidelity fake."""
        fake = FakeXNAT(project_name="TEST_PROJECT", fidelity_mode=True)
        conn = _make_connection(fake)
        login = _make_login()
        intake = _make_intake_form(tmp_path)
        session = _MinimalRFSession.build(intake)

        # Must complete without error after T005 fix.
        session.publish_to_xnat(
            xnat_connection=conn,
            validated_login=login,
            zipped_data=fake_zip,
            delete_zip=False,
            verbose=False,
            pixel_review_confirmer=_confirmed_confirmer,
        )

    def test_full_state_after_publish(self, tmp_path, fake_zip):
        """After publish: subject created, experiment created, scan created,
        SRC resource put_zip called exactly once."""
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

        ops = [c["op"] for c in fake.calls]
        assert ops.count("selectable.create") == 3, (
            "expected 3 create() calls: subject, experiment, scan"
        )
        put_zips = [c for c in fake.calls if c["op"] == "resource.put_zip"]
        assert len(put_zips) == 1, "expected exactly 1 put_zip for SRC"
        assert put_zips[0]["kwargs"]["_label"] == "SRC"

    def test_attrs_mset_called_for_all_three(self, tmp_path, fake_zip):
        """attrs.mset called at least once for subject, experiment, and scan."""
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

        mset_calls = [c for c in fake.calls if c["op"] == "attrs.mset"]
        assert len(mset_calls) >= 3, (
            f"expected ≥3 attrs.mset calls (subj/exp/scan), got {len(mset_calls)}"
        )


# ---------------------------------------------------------------------------
# T005b — idempotent upsert: pre-seeded orphan subject reused, no duplicate
# ---------------------------------------------------------------------------

class TestIdempotentUpsert:
    """A prior failed push may leave an orphaned empty subject.  Re-publish
    must reuse it (not duplicate) and fill in missing children."""

    def test_republish_over_orphaned_subject_no_duplicate(self, tmp_path, fake_zip):
        """Pre-seed an existing subject.  publish_to_xnat must reuse it and
        still create the experiment and scan under it — no AssertionError."""
        fake = FakeXNAT(project_name="TEST_PROJECT", fidelity_mode=True)
        conn = _make_connection(fake)
        login = _make_login()
        intake = _make_intake_form(tmp_path)
        session = _MinimalRFSession.build(intake)

        # Pre-seed: orphaned subject already exists (simulates prior failed push).
        subj_qs = f"/project/TEST_PROJECT/subject/{intake.uid}"
        fake.seed_existing(subj_qs)

        # publish must NOT raise AssertionError / duplicate; must complete.
        session.publish_to_xnat(
            xnat_connection=conn,
            validated_login=login,
            zipped_data=fake_zip,
            delete_zip=False,
            verbose=False,
            pixel_review_confirmer=_confirmed_confirmer,
        )

        # Experiment and scan must still be created.
        ops = [c["op"] for c in fake.calls]
        # subject create() skipped (already existed); experiment + scan created.
        create_calls = [c for c in fake.calls if c["op"] == "selectable.create"]
        assert len(create_calls) == 2, (
            f"expected 2 create() calls (experiment+scan) when subject pre-seeded, "
            f"got {len(create_calls)}: {[c['kwargs'].get('_qs') for c in create_calls]}"
        )
        put_zips = [c for c in fake.calls if c["op"] == "resource.put_zip"]
        assert len(put_zips) == 1
