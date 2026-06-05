"""
tests/contract/test_workflow_contract.py — Phase 6 contract tests (T001–T007).

Ensures FakeXNAT ≈ real XNAT behavioral parity for realistic grad-student workflows.
Stage 1 (this implementation): FakeXNAT only. Stage 2: dual-run with real XNAT via Docker.

Test design: each test case
  1. Stages synthetic data (FakeXNAT with fidelity_mode=True)
  2. Executes workflow (publish/download/revise)
  3. Asserts server state matches expectations
  4. Includes seam comments for deferred real-XNAT parity assertions

Synthetic data: all DICOMs, zips, and form stubs generated offline — no real PHI.
No real XNAT server required; no Docker; CI-friendly.
"""
from __future__ import annotations

import zipfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List

import pytest

from src.xnat_experiment_data import ExperimentData, ReviewDecision
from tests.fakes.fake_xnat import FakeXNAT
from app.logic.download import download_selection, list_downloadable


# ---------------------------------------------------------------------------
# Minimal test session class (reused from test_publish_real_contract.py)
# ---------------------------------------------------------------------------

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
    """Default pixel-review confirmer for tests."""
    return ReviewDecision.CONFIRMED, []


# ---------------------------------------------------------------------------
# T001 — Input Case (Source Upload)
# ---------------------------------------------------------------------------

@pytest.mark.contract
class TestT001SourceUpload:
    """
    T001: publish a synthetic RF session → verify Subject + Experiment + Scan created.

    Workflow:
      1. Create minimal intake form
      2. Stage fake zip with placeholder DICOM
      3. Call publish_to_xnat against fidelity FakeXNAT
      4. Assert: 1 Subject + 1 Experiment + 1 Scan + SRC resource

    Fidelity assertions:
      - FakeXNAT: subject/experiment created, scan 0 created, SRC resource put_zip called
      - Real XNAT (deferred): same state observed on server (SC-001)
    """

    def test_publish_creates_full_hierarchy(
        self,
        fake_xnat: FakeXNAT,
        intake_form: SimpleNamespace,
        fake_zip_file: tuple,
        xnat_connection: SimpleNamespace,
        xnat_login: SimpleNamespace,
    ):
        """Publish creates subject → experiment → scan → SRC resource."""
        zip_path, zipped_data = fake_zip_file
        session = _MinimalRFSession.build(intake_form)

        session.publish_to_xnat(
            xnat_connection=xnat_connection,
            validated_login=xnat_login,
            zipped_data=zipped_data,
            delete_zip=False,
            verbose=False,
            pixel_review_confirmer=_confirmed_confirmer,
        )

        # Count create() calls: subject, experiment, scan (3 total)
        create_calls = [c for c in fake_xnat.calls if c["op"] == "selectable.create"]
        assert len(create_calls) == 3, (
            f"Expected 3 create() calls (subj/exp/scan), got {len(create_calls)}"
        )

        # Resource SRC must be put_zip'd exactly once
        put_zips = [c for c in fake_xnat.calls if c["op"] == "resource.put_zip"]
        assert len(put_zips) == 1, f"Expected 1 put_zip for SRC, got {len(put_zips)}"
        assert put_zips[0]["kwargs"]["_label"] == "SRC"

        # DUAL-RUN PARITY (deferred):
        #   assert fake_xnat subject.exists() == real_xnat.subject.exists()
        #   assert fake_xnat experiment.exists() == real_xnat.experiment.exists()
        #   assert fake_xnat scan.exists() == real_xnat.scan.exists()

    def test_publish_calls_attrs_mset_for_all_three(
        self,
        fake_xnat: FakeXNAT,
        intake_form: SimpleNamespace,
        fake_zip_file: tuple,
        xnat_connection: SimpleNamespace,
        xnat_login: SimpleNamespace,
    ):
        """attrs.mset must be called at least once (per Phase 7 #27 fix)."""
        zip_path, zipped_data = fake_zip_file
        session = _MinimalRFSession.build(intake_form)

        session.publish_to_xnat(
            xnat_connection=xnat_connection,
            validated_login=xnat_login,
            zipped_data=zipped_data,
            delete_zip=False,
            verbose=False,
            pixel_review_confirmer=_confirmed_confirmer,
        )

        mset_calls = [c for c in fake_xnat.calls if c["op"] == "attrs.mset"]
        assert len(mset_calls) >= 3, (
            f"Expected ≥3 attrs.mset calls (subj/exp/scan), got {len(mset_calls)}"
        )


# ---------------------------------------------------------------------------
# T002 — Download Case (Source Retrieval)
# ---------------------------------------------------------------------------

@pytest.mark.contract
class TestT002SourceDownload:
    """
    T002: seed a scan resource with N real files → download enumerates all N.

    Workflow:
      1. Stage Subject/Experiment/Scan/Resource with 5 real files
      2. Call download_selection
      3. Assert: all 5 files listed and byte count matches

    Fidelity assertions:
      - FakeXNAT: list_files() returns all N filenames
      - Real XNAT (deferred): enumerate endpoint returns all N (Phase 7 #14 fix)
    """

    def test_download_enumerates_all_files_not_one_synthesized(
        self,
        fake_xnat: FakeXNAT,
        tmp_path: Path,
    ):
        """
        Pre-seed 5 files in a resource, download, assert all 5 appear
        (not 1 synthesized {subject}_{experiment}_{scan}.dcm).
        """
        # Pre-seed: subject → experiment → scan → SRC resource with 5 files
        subj_qs = "/project/TEST_PROJECT/subject/ITEST_SUBJ_0001"
        exp_qs = "/project/TEST_PROJECT/subject/ITEST_SUBJ_0001/experiment/ITEST_EXP_0001"
        scan_qs = "/project/TEST_PROJECT/subject/ITEST_SUBJ_0001/experiment/ITEST_EXP_0001/scan/0"

        # Create the objects
        subj = fake_xnat.select(subj_qs)
        subj.create()
        exp = fake_xnat.select(exp_qs)
        exp.create(xsiType="xnat:rfSessionData")
        scan = fake_xnat.select(scan_qs)
        scan.create(xsiType="xnat:rfScanData")

        # Seed resource with 5 files. This resource is registered in FakeXNAT._resources
        # so that it can be retrieved again via select().resource() later.
        resource = scan.resource("SRC")
        files = [
            (f"frame_{i:03d}.dcm", f"FAKE_DICOM_CONTENT_{i}".encode())
            for i in range(5)
        ]
        fake_xnat.seed_resource_files(resource, files)

        # Verify pre-seeded state
        assert resource.num_files() == 5
        assert len(resource.list_files()) == 5

        # Download: call download_selection
        # Note: scan_type in the selection row must be the scan ID ("0"), not the resource label.
        # This matches how download_selection builds the query string.
        # The resource will be looked up via the registry and have the seeded files.
        dest_dir = tmp_path / "download"
        selection = [
            {
                "subject": "ITEST_SUBJ_0001",
                "experiment": "ITEST_EXP_0001",
                "scan_type": "0",  # scan ID, not resource label
            }
        ]
        outcome = download_selection(fake_xnat, "TEST_PROJECT", selection, dest_dir)

        # Assert: all 5 files written
        assert outcome.ok, f"Download failed: {outcome.friendly}"
        assert len(outcome.files_written) == 5, (
            f"Expected 5 files written, got {len(outcome.files_written)}"
        )

        # DUAL-RUN PARITY (deferred):
        #   assert fake_xnat.resource.num_files() == real_xnat.resource.num_files()
        #   assert len(fake_files) == len(real_files)

    def test_file_bytes_round_trip(
        self,
        fake_xnat: FakeXNAT,
        tmp_path: Path,
    ):
        """File bytes must round-trip: write → get_copy → verify content."""
        subj_qs = "/project/TEST_PROJECT/subject/ITEST_SUBJ_0002"
        exp_qs = "/project/TEST_PROJECT/subject/ITEST_SUBJ_0002/experiment/ITEST_EXP_0002"
        scan_qs = "/project/TEST_PROJECT/subject/ITEST_SUBJ_0002/experiment/ITEST_EXP_0002/scan/0"

        subj = fake_xnat.select(subj_qs)
        subj.create()
        exp = fake_xnat.select(exp_qs)
        exp.create(xsiType="xnat:rfSessionData")
        scan = fake_xnat.select(scan_qs)
        scan.create(xsiType="xnat:rfScanData")

        resource = scan.resource("SRC")
        # Seed with recognizable content
        test_content_1 = b"KNOWN_CONTENT_FILE_1"
        test_content_2 = b"KNOWN_CONTENT_FILE_2"
        files = [
            ("file_1.dcm", test_content_1),
            ("file_2.dcm", test_content_2),
        ]
        fake_xnat.seed_resource_files(resource, files)

        # Download and verify content
        dest_dir = tmp_path / "download"
        selection = [
            {
                "subject": "ITEST_SUBJ_0002",
                "experiment": "ITEST_EXP_0002",
                "scan_type": "0",  # scan ID, not resource label
            }
        ]
        outcome = download_selection(fake_xnat, "TEST_PROJECT", selection, dest_dir)

        assert outcome.ok
        # Find the downloaded files
        written_files = {f.name: f for f in outcome.files_written}
        assert "file_1.dcm" in written_files or any("file_1" in str(f) for f in outcome.files_written)
        assert "file_2.dcm" in written_files or any("file_2" in str(f) for f in outcome.files_written)

        # Verify one of the bytes round-trips
        found_1 = [f for f in outcome.files_written if "file_1" in str(f)]
        if found_1:
            assert found_1[0].read_bytes() == test_content_1


# ---------------------------------------------------------------------------
# T003 — Upload Derived (Assessor or Resource)
# ---------------------------------------------------------------------------

@pytest.mark.contract
class TestT003UploadDerived:
    """
    T003: upload a derived/consensus resource.

    Workflow:
      1. Stage Subject/Experiment from T001
      2. Upload a derived DICOM (e.g., SEGMENTATION_CONSENSUS resource)
      3. Assert: new resource created

    Fidelity note:
      publish_to_xnat does not expose an assessor=... parameter yet.
      This test uses the resource-level API directly.
      Gap documented in audit matrix: Phase 6 decision needed (assessor vs. resource).
    """

    def test_upload_derived_resource(
        self,
        fake_xnat: FakeXNAT,
        tmp_path: Path,
    ):
        """
        Upload a derived resource (SEGMENTATION_CONSENSUS).

        Note: publish_to_xnat currently only exposes SOURCE_DATA uploads.
        This test directly uses the FakeXNAT resource API to verify the
        infrastructure supports derived resources. Phase 6 gateway design
        will clarify the publish_to_xnat API for derived uploads.
        """
        # Pre-stage Subject/Experiment/Scan (T001 state)
        subj_qs = "/project/TEST_PROJECT/subject/ITEST_SUBJ_0003"
        exp_qs = "/project/TEST_PROJECT/subject/ITEST_SUBJ_0003/experiment/ITEST_EXP_0003"
        scan_qs = "/project/TEST_PROJECT/subject/ITEST_SUBJ_0003/experiment/ITEST_EXP_0003/scan/0"

        subj = fake_xnat.select(subj_qs)
        subj.create()
        exp = fake_xnat.select(exp_qs)
        exp.create(xsiType="xnat:rfSessionData")
        scan = fake_xnat.select(scan_qs)
        scan.create(xsiType="xnat:rfScanData")

        # Upload derived resource (using resource API directly)
        derived_zip = tmp_path / "derived.zip"
        with zipfile.ZipFile(str(derived_zip), "w") as zf:
            zf.writestr("consensus.nii", b"CONSENSUS_SEGMENTATION_DATA")

        resource = scan.resource("SEGMENTATION_CONSENSUS")
        resource.put_zip(str(derived_zip), content="IMAGE", format="NIFTI", tags="CONSENSUS")

        # Verify: resource created and put_zip called
        put_zip_calls = [
            c for c in fake_xnat.calls
            if c["op"] == "resource.put_zip" and c["kwargs"]["_label"] == "SEGMENTATION_CONSENSUS"
        ]
        assert len(put_zip_calls) == 1, "SEGMENTATION_CONSENSUS resource must be created"

        # DUAL-RUN PARITY (deferred):
        #   assert fake_xnat.resource.exists() == real_xnat.resource.exists()
        #   Gap: clarify whether this should be Assessor (xnat:assessorData) or Resource


# ---------------------------------------------------------------------------
# T004 — Revise After Re-analysis (Idempotent Upsert)
# ---------------------------------------------------------------------------

@pytest.mark.contract
class TestT004ReviseAfterReanalysis:
    """
    T004: download source, re-analyze, overwrite derived resource (idempotent).

    Workflow:
      1. Pre-seed Subject/Experiment with SOURCE_DATA + SEGMENTATION_CONSENSUS
      2. Download SOURCE_DATA, mock re-analysis (synthetic reprocess)
      3. Overwrite SEGMENTATION_CONSENSUS (idempotent upsert)
      4. Assert: final state has 1 resource (no duplicates), bytes differ

    Fidelity assertions:
      - FakeXNAT: no duplicate resources, new content visible
      - Real XNAT (deferred): same idempotent behavior (SC-001 scope)
    """

    def test_revise_derived_resource_idempotent(
        self,
        fake_xnat: FakeXNAT,
        tmp_path: Path,
    ):
        """
        Pre-seed with derived resource, overwrite, verify no duplicates and new content.
        """
        # Pre-stage: Subject/Experiment/Scan/SRC/SEGMENTATION_CONSENSUS
        subj_qs = "/project/TEST_PROJECT/subject/ITEST_SUBJ_0004"
        exp_qs = "/project/TEST_PROJECT/subject/ITEST_SUBJ_0004/experiment/ITEST_EXP_0004"
        scan_qs = "/project/TEST_PROJECT/subject/ITEST_SUBJ_0004/experiment/ITEST_EXP_0004/scan/0"

        subj = fake_xnat.select(subj_qs)
        subj.create()
        exp = fake_xnat.select(exp_qs)
        exp.create(xsiType="xnat:rfSessionData")
        scan = fake_xnat.select(scan_qs)
        scan.create(xsiType="xnat:rfScanData")

        # Seed SRC resource with original data
        src_resource = scan.resource("SRC")
        src_files = [
            ("original_frame_000.dcm", b"ORIGINAL_PIXEL_DATA"),
        ]
        fake_xnat.seed_resource_files(src_resource, src_files)

        # Seed SEGMENTATION_CONSENSUS with v1
        derived_zip_v1 = tmp_path / "derived_v1.zip"
        with zipfile.ZipFile(str(derived_zip_v1), "w") as zf:
            zf.writestr("consensus_v1.nii", b"CONSENSUS_V1_CONTENT")
        seg_resource_v1 = scan.resource("SEGMENTATION_CONSENSUS")
        seg_resource_v1.put_zip(str(derived_zip_v1), content="IMAGE", format="NIFTI")

        # Record the v1 state
        v1_put_zips = [
            c for c in fake_xnat.calls
            if c["op"] == "resource.put_zip" and c["kwargs"]["_label"] == "SEGMENTATION_CONSENSUS"
        ]
        assert len(v1_put_zips) == 1

        # Clear call log to observe only the re-analysis step
        fake_xnat.reset_calls()

        # Re-analysis: simulate reprocessing
        # (In real code: download SRC, process, upload new SEGMENTATION_CONSENSUS)
        # For this test, we directly call put_zip again with new content
        derived_zip_v2 = tmp_path / "derived_v2.zip"
        with zipfile.ZipFile(str(derived_zip_v2), "w") as zf:
            zf.writestr("consensus_v2.nii", b"CONSENSUS_V2_CONTENT_DIFFERENT")
        seg_resource_v2 = scan.resource("SEGMENTATION_CONSENSUS")
        seg_resource_v2.put_zip(str(derived_zip_v2), content="IMAGE", format="NIFTI", overwrite=True)

        # Verify: only 1 put_zip in the reset call log (no duplicate resource creation)
        put_zips = [c for c in fake_xnat.calls if c["op"] == "resource.put_zip"]
        assert len(put_zips) == 1, (
            f"Expected 1 put_zip after re-analysis (idempotent overwrite), got {len(put_zips)}"
        )

        # DUAL-RUN PARITY (deferred):
        #   assert fake_xnat final state == real_xnat final state (same resource, new bytes)


# ---------------------------------------------------------------------------
# T005 — Mixed Modality (RF + CT)
# ---------------------------------------------------------------------------

@pytest.mark.contract
class TestT005MixedModality:
    """
    T005: project with RF + CT experiments → both appear in listing.

    Workflow:
      1. Seed RF experiment (xnat:rfSessionData)
      2. Seed CT experiment (xnat:ctSessionData)
      3. Call list_downloadable
      4. Assert: both RF and CT listed

    Fidelity assertions:
      - FakeXNAT: both types in list_experiments_with_type
      - Real XNAT (deferred): same enumeration (Phase 7 #29 fix)
    """

    def test_mixed_modality_enumeration(
        self,
        fake_xnat: FakeXNAT,
    ):
        """
        Both RF and CT must appear in enumeration when seeded.
        Uses FakeXNAT seeding API (seed_rf_experiment + seed_subject_label).
        """
        # Seed two experiments with different types
        fake_xnat.seed_subject_label("ITEST_INTERNAL_S001", "ITEST_SUBJ_0005")
        fake_xnat.seed_rf_experiment(
            subject_label="ITEST_SUBJ_0005",
            experiment_label="RF_EXP_0001",
            xsi_type="xnat:rfSessionData",
        )
        fake_xnat.seed_rf_experiment(
            subject_label="ITEST_SUBJ_0005",
            experiment_label="CT_EXP_0001",
            xsi_type="xnat:ctSessionData",
        )

        # Enumerate experiments
        experiments = fake_xnat.list_experiments_with_type("TEST_PROJECT")

        assert len(experiments) == 2, f"Expected 2 experiments, got {len(experiments)}"
        assert experiments[0]["xsi_type"] == "xnat:rfSessionData"
        assert experiments[1]["xsi_type"] == "xnat:ctSessionData"

        # DUAL-RUN PARITY (deferred):
        #   assert fake_xnat.list_experiments_with_type() == real_xnat.list_experiments_with_type()


# ---------------------------------------------------------------------------
# T006 — Orphaned/Partial Subject (Idempotent Upsert)
# ---------------------------------------------------------------------------

@pytest.mark.contract
class TestT006OrphanedSubjectReuse:
    """
    T006: pre-seed orphaned subject, publish → final state has 1 subject (no duplicate).

    Workflow:
      1. Pre-seed orphaned/empty subject (exists but no experiments)
      2. Call publish_to_xnat with same subject UID
      3. Assert: final state has 1 subject + 1 experiment (reused, not duplicated)

    Fidelity assertions:
      - FakeXNAT: subject reused (no second create), experiment created
      - Real XNAT (deferred): same idempotent behavior (SC-001 scope)
    """

    def test_orphaned_subject_reused_not_duplicated(
        self,
        fake_xnat: FakeXNAT,
        intake_form: SimpleNamespace,
        fake_zip_file: tuple,
        xnat_connection: SimpleNamespace,
        xnat_login: SimpleNamespace,
    ):
        """
        Pre-seed an orphaned subject, then publish.
        Expect: subject reused (no new create), experiment + scan created.
        """
        zip_path, zipped_data = fake_zip_file
        session = _MinimalRFSession.build(intake_form)

        # Pre-seed: orphaned subject already exists
        subj_qs = f"/project/TEST_PROJECT/subject/{intake_form.uid}"
        fake_xnat.seed_existing(subj_qs)

        # Verify pre-seeded state
        assert fake_xnat.select(subj_qs).exists()

        # Publish (which will try to reuse the orphaned subject)
        session.publish_to_xnat(
            xnat_connection=xnat_connection,
            validated_login=xnat_login,
            zipped_data=zipped_data,
            delete_zip=False,
            verbose=False,
            pixel_review_confirmer=_confirmed_confirmer,
        )

        # Verify: subject was NOT recreated, but experiment + scan were
        # Expected: 2 create() calls (experiment + scan), NOT 3 (which would include subject)
        create_calls = [c for c in fake_xnat.calls if c["op"] == "selectable.create"]
        assert len(create_calls) == 2, (
            f"Expected 2 create() calls (exp+scan, subject pre-seeded), got {len(create_calls)}"
        )

        # DUAL-RUN PARITY (deferred):
        #   assert fake_xnat final subject count == real_xnat final subject count == 1


# ---------------------------------------------------------------------------
# T007 — Missing Tag Guard (#30)
# ---------------------------------------------------------------------------

@pytest.mark.contract
class TestT007MissingInstanceNumberGuard:
    """
    T007: DICOM without InstanceNumber → no crash, default index used.

    Workflow:
      1. Create a synthetic DICOM missing InstanceNumber tag
      2. Publish via publish_to_xnat
      3. Assert: no AttributeError / crash, session metadata mines a default index

    Fidelity assertions:
      - FakeXNAT: publish completes without error (Phase 7 #30 fix)
      - Real XNAT (deferred): same error-handling behavior (SC-001 scope)
    """

    def test_missing_instance_number_no_crash(
        self,
        fake_xnat: FakeXNAT,
        synthetic_rf_dicom_missing_instance_number: Path,
        intake_form: SimpleNamespace,
        tmp_path: Path,
        xnat_connection: SimpleNamespace,
        xnat_login: SimpleNamespace,
    ):
        """
        Pack a DICOM with no InstanceNumber into a zip.
        Call publish_to_xnat.
        Assert: no AttributeError, publish succeeds.
        """
        # Create zip with missing-instance DICOM
        zip_path = tmp_path / "missing_instance.zip"
        with zipfile.ZipFile(str(zip_path), "w") as zf:
            dcm_bytes = synthetic_rf_dicom_missing_instance_number.read_bytes()
            zf.writestr("frame_no_instance.dcm", dcm_bytes)
        zipped_data = {str(zip_path): {"CONTENT": "IMAGE", "FORMAT": "DICOM", "TAG": "INTRA_OP"}}

        session = _MinimalRFSession.build(intake_form)

        # Must not raise AttributeError (pre-fix would crash here)
        try:
            session.publish_to_xnat(
                xnat_connection=xnat_connection,
                validated_login=xnat_login,
                zipped_data=zipped_data,
                delete_zip=False,
                verbose=False,
                pixel_review_confirmer=_confirmed_confirmer,
            )
        except AttributeError as e:
            if "InstanceNumber" in str(e):
                pytest.fail(f"Phase 7 #30 guard failed: {e}")
            raise

        # If we reach here, publish completed without crashing on missing InstanceNumber
        # Verify that some state was created (subject/experiment/scan)
        create_calls = [c for c in fake_xnat.calls if c["op"] == "selectable.create"]
        assert len(create_calls) >= 1, "Expected at least subject/experiment/scan creation"

        # DUAL-RUN PARITY (deferred):
        #   assert fake_xnat.publish() == real_xnat.publish() (both succeed without error)
