"""
tests/test_publish_no_empty_shells.py — #32 cleanup-on-failure regression tests.

Exercises publish_to_xnat cleanup logic: when put_zip raises or assessor creation
raises, the function catches the exception and deletes any Subject/Experiment/Scan
objects that THIS call created (and no others).

Offline only. No real server, no real PHI.
"""
from __future__ import annotations

import zipfile
from pathlib import Path
from types import SimpleNamespace
from typing import Tuple

import pytest

from src.xnat_experiment_data import ExperimentData, ReviewDecision, UploadError
from src.services.errors import FriendlyError
from tests.fakes.fake_xnat import FakeXNAT


# ---------------------------------------------------------------------------
# Helpers — copied from test_publish_real_contract.py pattern
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


# ---------------------------------------------------------------------------
# T032 — cleanup-on-failure: put_zip raises → all created objects deleted
# ---------------------------------------------------------------------------

class TestCleanupOnFailure:
    """Verify that on upload failure, empty-shell cleanup deletes only
    objects THIS call created, not pre-existing ones."""

    def test_put_zip_raises_deletes_all_created_objects(self, tmp_path, fake_zip):
        """put_zip raises → cleanup deletes scan, experiment, subject.

        Fresh FakeXNAT, no pre-existing objects.  Inject failure on put_zip
        via set_next_failure.  Verify UploadError re-raised, all 3 creates
        recorded, all 3 deletes recorded (in reverse order: scan → exp → subj).
        Verify final state: all 3 objects non-existent.
        """
        fake = FakeXNAT(project_name="TEST_PROJECT", fidelity_mode=True)
        conn = _make_connection(fake)
        login = _make_login()
        intake = _make_intake_form(tmp_path)
        session = _MinimalRFSession.build(intake)

        # Inject failure on put_zip
        fake.set_next_failure(UploadError(
            FriendlyError(
                title="Simulated upload failure",
                message="Injected failure for testing cleanup",
                recourse=[]
            )
        ))

        # Call should raise UploadError
        with pytest.raises(UploadError):
            session.publish_to_xnat(
                xnat_connection=conn,
                validated_login=login,
                zipped_data=fake_zip,
                delete_zip=False,
                verbose=False,
                pixel_review_confirmer=_confirmed_confirmer,
            )

        # Verify creates
        create_calls = [c for c in fake.calls if c["op"] == "selectable.create"]
        assert len(create_calls) == 3, (
            f"expected 3 create() calls: subject, experiment, scan; got {len(create_calls)}"
        )

        # Verify deletes (should be in reverse order: scan, experiment, subject)
        delete_calls = [c for c in fake.calls if c["op"] == "selectable.delete"]
        assert len(delete_calls) == 3, (
            f"expected 3 delete() calls after failure; got {len(delete_calls)}"
        )

        # Verify delete order: scan first, then experiment, then subject
        delete_qss = [c["kwargs"]["_qs"] for c in delete_calls]
        assert "scan" in delete_qss[0].lower(), (
            f"first delete should be scan, got {delete_qss[0]}"
        )
        assert "experiment" in delete_qss[1].lower(), (
            f"second delete should be experiment, got {delete_qss[1]}"
        )
        assert "subject" in delete_qss[2].lower(), (
            f"third delete should be subject, got {delete_qss[2]}"
        )

        # Verify final state: all objects non-existent
        subj_qs = f"/project/TEST_PROJECT/subject/{intake.uid}"
        exp_qs = f"{subj_qs}/experiment/SOURCE_DATA-{intake.uid}"
        scan_qs = f"{exp_qs}/scan/0"
        assert not fake.exists(subj_qs), "subject should be deleted, but exists() returned True"
        assert not fake.exists(exp_qs), "experiment should be deleted, but exists() returned True"
        assert not fake.exists(scan_qs), "scan should be deleted, but exists() returned True"

    def test_success_path_unchanged(self, tmp_path, fake_zip):
        """Successful upload has no deletes; all objects + files present.

        Verify that when upload succeeds, no cleanup happens:
        - 3 creates recorded, 0 deletes recorded
        - put_zip call recorded with correct resource_label
        - All 3 objects exist in final state
        - File count in SRC resource > 0 (simulating successful put_zip unpacking)
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

        # Verify creates
        create_calls = [c for c in fake.calls if c["op"] == "selectable.create"]
        assert len(create_calls) == 3, (
            f"expected 3 create() calls on success: subject, experiment, scan; "
            f"got {len(create_calls)}"
        )

        # Verify NO deletes on success
        delete_calls = [c for c in fake.calls if c["op"] == "selectable.delete"]
        assert len(delete_calls) == 0, (
            f"expected 0 delete() calls on success; got {len(delete_calls)}"
        )

        # Verify put_zip called with correct resource_label
        put_zips = [c for c in fake.calls if c["op"] == "resource.put_zip"]
        assert len(put_zips) == 1, f"expected exactly 1 put_zip call; got {len(put_zips)}"
        assert put_zips[0]["kwargs"]["_label"] == "SRC", (
            f"expected resource_label='SRC'; got {put_zips[0]['kwargs']['_label']}"
        )

        # Verify final state: all 3 objects exist
        subj_qs = f"/project/TEST_PROJECT/subject/{intake.uid}"
        exp_qs = f"{subj_qs}/experiment/SOURCE_DATA-{intake.uid}"
        scan_qs = f"{exp_qs}/scan/0"
        assert fake.exists(subj_qs), "subject should exist after successful upload"
        assert fake.exists(exp_qs), "experiment should exist after successful upload"
        assert fake.exists(scan_qs), "scan should exist after successful upload"

        # Verify files uploaded to SRC
        src_files = fake.list_files(scan_qs, "SRC")
        assert len(src_files) > 0, "expected files in SRC resource; got none"

    def test_subject_preexisting_with_other_experiment_not_deleted(self, tmp_path, fake_zip):
        """Subject pre-exists with other experiment → on failure, only new
        exp+scan deleted, subject stays.

        Seed subject + a different experiment (use seed_existing). Call
        publish_to_xnat with new uid that creates new experiment under same
        subject.  Inject failure on put_zip.  Verify UploadError re-raised,
        subject still exists (not deleted), new experiment + scan deleted,
        old experiment still exists.
        """
        fake = FakeXNAT(project_name="TEST_PROJECT", fidelity_mode=True)
        conn = _make_connection(fake)
        login = _make_login()
        intake = _make_intake_form(tmp_path)
        session = _MinimalRFSession.build(intake)

        # Pre-seed subject + old experiment (different from the new one we'll create)
        old_subj_qs = f"/project/TEST_PROJECT/subject/{intake.uid}"
        old_exp_qs = f"{old_subj_qs}/experiment/OLD_EXP"
        fake.seed_existing(old_subj_qs)
        fake.seed_existing(old_exp_qs)

        # Inject failure on put_zip
        fake.set_next_failure(UploadError(
            FriendlyError(
                title="Simulated upload failure",
                message="Injected failure for testing cleanup",
                recourse=[]
            )
        ))

        # Call should raise UploadError
        with pytest.raises(UploadError):
            session.publish_to_xnat(
                xnat_connection=conn,
                validated_login=login,
                zipped_data=fake_zip,
                delete_zip=False,
                verbose=False,
                pixel_review_confirmer=_confirmed_confirmer,
            )

        # Verify creates: subject NOT created (already exists), exp + scan created
        create_calls = [c for c in fake.calls if c["op"] == "selectable.create"]
        assert len(create_calls) == 2, (
            f"expected 2 create() calls (exp+scan) when subject pre-seeded; "
            f"got {len(create_calls)}: {[c['kwargs'].get('_qs') for c in create_calls]}"
        )

        # Verify deletes: only the new exp + scan deleted
        delete_calls = [c for c in fake.calls if c["op"] == "selectable.delete"]
        assert len(delete_calls) == 2, (
            f"expected 2 delete() calls (new exp+scan) after failure; got {len(delete_calls)}"
        )

        # Verify final state
        new_exp_qs = f"{old_subj_qs}/experiment/SOURCE_DATA-{intake.uid}"
        new_scan_qs = f"{new_exp_qs}/scan/0"

        # Subject should still exist (not deleted)
        assert fake.exists(old_subj_qs), "pre-existing subject should NOT be deleted"

        # Old experiment should still exist
        assert fake.exists(old_exp_qs), "pre-existing experiment should NOT be deleted"

        # New experiment + scan should be deleted
        assert not fake.exists(new_exp_qs), "new experiment should be deleted on failure"
        assert not fake.exists(new_scan_qs), "new scan should be deleted on failure"

    def test_assessor_failure_cleans_subject_only(self, tmp_path, fake_zip):
        """Assessor create raises after put_zip succeeds → cleanup safeguard:
        subject deleted, but scan/exp retained (fail-safe: files are present).

        Fresh FakeXNAT. Let put_zip succeed (files staged in resource).
        Inject failure on assessor creation via monkey-patch. Call
        publish_to_xnat with assessor=Path(...).  Verify UploadError re-raised,
        subject deleted (THIS call created it), but scan+experiment NOT deleted
        (fail-safe: list_files reports files present → cleanup assumes non-empty
        and skips deletion to avoid data loss). Only subject is safe to delete
        since we can't enumerate its children via pyxnat.
        """
        fake = FakeXNAT(project_name="TEST_PROJECT", fidelity_mode=True)
        conn = _make_connection(fake)
        login = _make_login()
        intake = _make_intake_form(tmp_path)
        session = _MinimalRFSession.build(intake)

        # Create a fake assessor file
        assessor_file = tmp_path / "assessor_output.nii.gz"
        assessor_file.write_bytes(b"FAKE_ASSESSOR")

        # Monkey-patch create_assessor to raise exception
        def failing_create_assessor(*args, **kwargs):
            raise UploadError(
                FriendlyError(
                    title="Assessor creation failed",
                    message="Simulated assessor failure",
                    recourse=[]
                )
            )

        fake.create_assessor = failing_create_assessor

        # Call should raise UploadError
        with pytest.raises(UploadError):
            session.publish_to_xnat(
                xnat_connection=conn,
                validated_login=login,
                zipped_data=fake_zip,
                delete_zip=False,
                verbose=False,
                pixel_review_confirmer=_confirmed_confirmer,
                assessor=assessor_file,
                assessor_label="TEST_ASSESSOR",
            )

        # Verify creates: subject, experiment, scan all created
        create_calls = [c for c in fake.calls if c["op"] == "selectable.create"]
        assert len(create_calls) == 3, (
            f"expected 3 create() calls; got {len(create_calls)}"
        )

        # Verify deletes: only subject deleted (fail-safe: scan/exp kept due to files present)
        delete_calls = [c for c in fake.calls if c["op"] == "selectable.delete"]
        assert len(delete_calls) == 1, (
            f"expected 1 delete() (subject only, fail-safe for files) after assessor failure; "
            f"got {len(delete_calls)}"
        )
        assert "subject" in delete_calls[0]["kwargs"]["_qs"].lower(), (
            f"expected subject deletion; got {delete_calls[0]['kwargs']['_qs']}"
        )

        # Verify final state: subject deleted (fail-safe allows scan/exp to remain)
        subj_qs = f"/project/TEST_PROJECT/subject/{intake.uid}"
        exp_qs = f"{subj_qs}/experiment/SOURCE_DATA-{intake.uid}"
        scan_qs = f"{exp_qs}/scan/0"
        assert not fake.exists(subj_qs), "subject should be deleted after assessor failure"
        # Note: scan and experiment remain (fail-safe: don't delete when files detected)
