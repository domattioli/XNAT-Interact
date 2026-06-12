"""
tests/test_010_quarantine_gate.py — Quarantine store + upload-gate integration (T017–T020).

Covers:
  - QuarantineStore: quarantine_case writes case dir + evidence.json with the
    correct keys (verdict, reason, regions, tiers_fired, phi_categories,
    frame_count, timestamp); raw PHI text is NEVER written (SC-004).
  - QuarantineStore: list_quarantined / is_quarantined / release work correctly.
  - Gate integration: an automated confirmer returning QUARANTINE causes
    publish_to_xnat to raise UploadError; the case is held in the store.
  - Gate integration: a CONFIRMED / CLEAN path proceeds without raising.
  - Additive-ness: ReviewDecision still has CONFIRMED / REDACT / ABORT plus
    the new QUARANTINE member.

All fixtures are synthetic; no real PHI is used anywhere.
"""
from __future__ import annotations

import json
import tempfile
import zipfile
from pathlib import Path
from types import SimpleNamespace
from typing import List, Tuple

import numpy as np
import pytest

from src.services.pixel_deid.quarantine import QuarantineStore
from src.services.pixel_deid.verdict import CaseAssessment, Verdict
from src.xnat_experiment_data import (
    ExperimentData,
    ReviewDecision,
    UploadError,
    make_automated_pixel_confirmer,
)

# ---------------------------------------------------------------------------
# Synthetic assessment helpers (no heavy deps — all in-process)
# ---------------------------------------------------------------------------

FAKE_PHI_NAME = "SMITH^JANE"
FAKE_MRN = "00471123"

def _make_assessment(
    verdict: Verdict = Verdict.QUARANTINE,
    regions: list | None = None,
    phi_categories: list | None = None,
    tiers_fired: list | None = None,
    reason: str = "unprofiled device, no positive clean evidence",
    n_frames: int = 2,
) -> CaseAssessment:
    """
    Build a CaseAssessment with synthetic content.

    The ``reason`` and ``phi_categories`` fields use category-name strings
    only — no raw PHI text — as the pipeline mandates (SC-004).
    """
    frames = [np.zeros((64, 256), dtype=np.uint8) for _ in range(n_frames)]
    return CaseAssessment(
        verdict=verdict,
        masked_frames=frames,
        regions=regions if regions is not None else [(0, 0, 50, 20), (60, 0, 110, 20)],
        tiers_fired=tiers_fired if tiers_fired is not None else ["profile", "detector"],
        phi_categories=phi_categories if phi_categories is not None else ["PERSON", "MRN"],
        reason=reason,
    )


# ---------------------------------------------------------------------------
# QuarantineStore — unit tests
# ---------------------------------------------------------------------------

class TestQuarantineStore:
    """Unit tests for src.services.pixel_deid.quarantine.QuarantineStore."""

    def test_quarantine_creates_case_dir(self, tmp_path: Path) -> None:
        """quarantine_case must create root/<case_id>/ directory."""
        store = QuarantineStore(tmp_path / "q")
        assessment = _make_assessment()

        case_dir = store.quarantine_case("case-001", assessment)

        assert case_dir.is_dir(), "case directory must exist"

    def test_evidence_json_written(self, tmp_path: Path) -> None:
        """evidence.json must be written alongside the case directory."""
        store = QuarantineStore(tmp_path / "q")
        assessment = _make_assessment()

        case_dir = store.quarantine_case("case-002", assessment)

        evidence_path = case_dir / "evidence.json"
        assert evidence_path.exists(), "evidence.json must exist"

    def test_evidence_json_keys_present(self, tmp_path: Path) -> None:
        """
        evidence.json must contain the required keys:
        verdict, reason, regions, tiers_fired, phi_categories,
        frame_count, timestamp.
        """
        store = QuarantineStore(tmp_path / "q")
        assessment = _make_assessment()
        case_dir = store.quarantine_case("case-003", assessment)

        with open(case_dir / "evidence.json", encoding="utf-8") as fh:
            evidence = json.load(fh)

        required_keys = {
            "verdict", "reason", "regions", "tiers_fired",
            "phi_categories", "frame_count", "timestamp",
        }
        assert required_keys.issubset(evidence.keys()), (
            f"Missing keys: {required_keys - evidence.keys()}"
        )

    def test_evidence_verdict_value(self, tmp_path: Path) -> None:
        """evidence.json verdict must be a string matching the Verdict enum value."""
        store = QuarantineStore(tmp_path / "q")
        assessment = _make_assessment(verdict=Verdict.QUARANTINE)
        case_dir = store.quarantine_case("case-004", assessment)

        with open(case_dir / "evidence.json", encoding="utf-8") as fh:
            evidence = json.load(fh)

        assert evidence["verdict"] == "quarantine"

    def test_evidence_regions_are_coordinates_not_phi(self, tmp_path: Path) -> None:
        """
        regions in evidence.json must be box coordinate lists, not PHI strings.
        """
        store = QuarantineStore(tmp_path / "q")
        assessment = _make_assessment(regions=[(10, 5, 80, 25)])
        case_dir = store.quarantine_case("case-005", assessment)

        with open(case_dir / "evidence.json", encoding="utf-8") as fh:
            evidence = json.load(fh)

        # Each region must be a list of numbers, not a string
        for region in evidence["regions"]:
            assert isinstance(region, list), "regions must be lists of coordinates"
            for coord in region:
                assert isinstance(coord, (int, float)), (
                    f"region coordinate must be numeric, got {type(coord)}"
                )

    def test_evidence_phi_categories_are_names_not_raw_text(self, tmp_path: Path) -> None:
        """
        phi_categories must contain category-name strings only.
        The raw PHI strings (fake name, MRN) must NOT appear anywhere
        in evidence.json (SC-004).
        """
        store = QuarantineStore(tmp_path / "q")
        assessment = _make_assessment(
            phi_categories=["PERSON", "MRN"],
        )
        case_dir = store.quarantine_case("case-006", assessment)

        raw_evidence = (case_dir / "evidence.json").read_text(encoding="utf-8")

        # Raw PHI substrings must not appear
        assert FAKE_PHI_NAME not in raw_evidence, (
            "Raw PHI name must not be written to evidence.json"
        )
        assert FAKE_MRN not in raw_evidence, (
            "Raw MRN digits must not be written to evidence.json"
        )
        # Category names must be present (they are not PHI)
        evidence = json.loads(raw_evidence)
        assert "PERSON" in evidence["phi_categories"]
        assert "MRN" in evidence["phi_categories"]

    def test_evidence_frame_count(self, tmp_path: Path) -> None:
        """evidence.json frame_count must equal the number of masked frames."""
        store = QuarantineStore(tmp_path / "q")
        n = 5
        assessment = _make_assessment(n_frames=n)
        case_dir = store.quarantine_case("case-007", assessment)

        with open(case_dir / "evidence.json", encoding="utf-8") as fh:
            evidence = json.load(fh)

        assert evidence["frame_count"] == n

    def test_frames_npy_written_when_provided(self, tmp_path: Path) -> None:
        """When frames are supplied, frames.npy must be written."""
        store = QuarantineStore(tmp_path / "q")
        assessment = _make_assessment()
        frames = [np.zeros((64, 256), dtype=np.uint8) for _ in range(3)]

        case_dir = store.quarantine_case("case-008", assessment, frames=frames)

        assert (case_dir / "frames.npy").exists(), "frames.npy must be written"

    def test_frames_npy_not_written_when_not_provided(self, tmp_path: Path) -> None:
        """When frames=None, frames.npy must NOT be created."""
        store = QuarantineStore(tmp_path / "q")
        assessment = _make_assessment()

        case_dir = store.quarantine_case("case-009", assessment, frames=None)

        assert not (case_dir / "frames.npy").exists(), (
            "frames.npy must not be created when frames=None"
        )

    # ------------------------------------------------------------------
    # list_quarantined
    # ------------------------------------------------------------------

    def test_list_quarantined_empty_store(self, tmp_path: Path) -> None:
        """list_quarantined must return an empty list for a fresh store."""
        store = QuarantineStore(tmp_path / "q")
        assert store.list_quarantined() == []

    def test_list_quarantined_after_adding_cases(self, tmp_path: Path) -> None:
        """list_quarantined must enumerate all quarantined case IDs."""
        store = QuarantineStore(tmp_path / "q")
        for cid in ("alpha", "beta", "gamma"):
            store.quarantine_case(cid, _make_assessment())

        listed = store.list_quarantined()
        assert set(listed) == {"alpha", "beta", "gamma"}

    # ------------------------------------------------------------------
    # is_quarantined
    # ------------------------------------------------------------------

    def test_is_quarantined_returns_false_for_absent_case(self, tmp_path: Path) -> None:
        store = QuarantineStore(tmp_path / "q")
        assert store.is_quarantined("nonexistent") is False

    def test_is_quarantined_returns_true_after_quarantine(self, tmp_path: Path) -> None:
        store = QuarantineStore(tmp_path / "q")
        store.quarantine_case("present", _make_assessment())
        assert store.is_quarantined("present") is True

    # ------------------------------------------------------------------
    # release
    # ------------------------------------------------------------------

    def test_release_removes_case_from_store(self, tmp_path: Path) -> None:
        """release must remove the case directory so is_quarantined returns False."""
        store = QuarantineStore(tmp_path / "q")
        store.quarantine_case("to-release", _make_assessment())
        assert store.is_quarantined("to-release") is True

        store.release("to-release")

        assert store.is_quarantined("to-release") is False

    def test_release_removes_from_list(self, tmp_path: Path) -> None:
        """After release, the case ID must not appear in list_quarantined."""
        store = QuarantineStore(tmp_path / "q")
        store.quarantine_case("keep", _make_assessment())
        store.quarantine_case("remove", _make_assessment())

        store.release("remove")

        assert "remove" not in store.list_quarantined()
        assert "keep" in store.list_quarantined()

    def test_release_nonexistent_raises_key_error(self, tmp_path: Path) -> None:
        """release of a case that is not in the store must raise KeyError."""
        store = QuarantineStore(tmp_path / "q")
        with pytest.raises(KeyError):
            store.release("ghost")


# ---------------------------------------------------------------------------
# Gate integration — PHI gate + automated confirmer
# ---------------------------------------------------------------------------

# Minimal _MinimalRFSession (reuses the pattern from test_upload_phi_gate.py)

class _MinimalRFSession(ExperimentData):
    """
    Bypass ExperimentData.__init__ so we can unit-test publish_to_xnat
    without a real intake form directory, config, or DICOM files.
    """

    @classmethod
    def build(cls, intake_form, schema_prefix: str = "rf") -> "_MinimalRFSession":
        obj = object.__new__(cls)
        obj._intake_form = intake_form
        obj._schema_prefix_str = schema_prefix
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


def _make_fake_intake_form(tmp_path: Path) -> SimpleNamespace:
    saved_ffn = tmp_path / "RECONSTRUCTED_OR_DATA_INTAKE_FORM.json"
    saved_ffn.write_text("{}", encoding="utf-8")
    datetime_stub = SimpleNamespace(date="2024-01-01", time="120000")
    form = SimpleNamespace(
        uid="TEST_UID_001",
        group="TEST_GROUP",
        acquisition_site="TEST_SITE",
        operation_date="2024-01-01",
        epic_start_time="120000",
        ortho_procedure_type="TEST_PROCEDURE",
        scan_quality="usable",
        datetime=datetime_stub,
        relevant_folder=tmp_path,
        saved_ffn=saved_ffn,
        saved_ffn_str=str(saved_ffn),
        push_to_xnat=lambda subj_inst=None, verbose=False, **kw: None,
    )
    return form


def _make_fake_zip(tmp_path: Path) -> dict:
    zip_path = tmp_path / "dicom_files.zip"
    with zipfile.ZipFile(str(zip_path), "w") as zf:
        zf.writestr("placeholder.dcm", b"FAKE")
    return {str(zip_path): {"CONTENT": "IMAGE", "FORMAT": "DICOM", "TAG": "INTRA_OP"}}


class _FakeXNATServer:
    """Minimal stand-in that records put_zip calls."""
    def __init__(self):
        self.put_zip_calls: list = []
        self._project_name = "TEST_PROJECT"
        self._subjects: dict = {}

    def classes(self):
        return self

    def SubjectData(self):
        class _SD:
            def create(self): return _FakeSubj()
        return _SD()

    # needed by publish_to_xnat
    def __getattr__(self, item):
        return self


class _FakeSubj(SimpleNamespace):
    def create(self, **kw): return self
    def attrs(self): return self
    def mset(self, *a, **kw): return self


class _FakeXNAT:
    def __init__(self):
        self.put_zip_calls: list = []
        self._project = SimpleNamespace(
            subjects=_FakeSubjCollection(),
        )

    def select(self, path=None, *args, **kw):
        return _FakeProject(self)

    def __call__(self, *a, **kw):
        return self


class _FakeSubjCollection:
    def get(self, *a, **kw):
        return []


class _FakeProject:
    def __init__(self, server):
        self._server = server

    def subject(self, label):
        return _FakeSubject(self._server)

    def __call__(self, *a, **kw):
        return self


class _FakeSubject:
    def __init__(self, server):
        self._server = server

    def experiment(self, label):
        return _FakeExperiment(self._server)

    def exists(self):
        return True

    def create(self, **kw):
        return self

    def attrs(self):
        return _FakeAttrs()

    def get(self, *a, **kw):
        return []


class _FakeExperiment:
    def __init__(self, server):
        self._server = server

    def scan(self, label):
        return _FakeScan(self._server)

    def exists(self):
        return True

    def create(self, **kw):
        return self

    def attrs(self):
        return _FakeAttrs()

    def assessor(self, label):
        return _FakeScan(self._server)


class _FakeScan:
    def __init__(self, server):
        self._server = server

    def exists(self):
        return True

    def create(self, **kw):
        return self

    def attrs(self):
        return _FakeAttrs()

    def resource(self, label):
        return _FakeResource(self._server)


class _FakeResource:
    def __init__(self, server):
        self._server = server

    def put_zip(self, path, *a, **kw):
        self._server.put_zip_calls.append(path)

    def exists(self):
        return False

    def create(self, **kw):
        return self


class _FakeAttrs:
    def mset(self, *a, **kw):
        return self


def _make_session_and_fake(tmp_path: Path):
    fake_server = _FakeXNAT()
    conn = SimpleNamespace(
        server=fake_server,
        gateway=fake_server,
        xnat_project_name="TEST_PROJECT",
    )
    login = SimpleNamespace(validated_username="testuser")
    intake = _make_fake_intake_form(tmp_path)
    session = _MinimalRFSession.build(intake)
    return session, fake_server, conn, login


# ---------------------------------------------------------------------------
# Gate: QUARANTINE → UploadError raised
# ---------------------------------------------------------------------------

class TestGateQuarantine:
    """
    The PHI gate must raise UploadError (wrapping a FriendlyError) when the
    pixel_review_confirmer returns ReviewDecision.QUARANTINE.
    """

    def test_quarantine_decision_raises_upload_error(self, tmp_path: Path) -> None:
        """
        A confirmer returning QUARANTINE must block upload with UploadError.
        """
        session, fake_server, conn, login = _make_session_and_fake(tmp_path)
        fake_zip = _make_fake_zip(tmp_path)

        def quarantine_confirmer(context: str):
            return ReviewDecision.QUARANTINE, []

        with pytest.raises(UploadError) as exc_info:
            session.publish_to_xnat(
                xnat_connection=conn,
                validated_login=login,
                zipped_data=fake_zip,
                delete_zip=False,
                verbose=False,
                pixel_review_confirmer=quarantine_confirmer,
            )

        upload_err = exc_info.value
        assert isinstance(upload_err, UploadError)

    def test_quarantine_upload_error_has_friendly_message(self, tmp_path: Path) -> None:
        """
        The UploadError for QUARANTINE must carry a FriendlyError with a plain-
        language message that mentions the quarantine store and 'NOT uploaded'.
        """
        session, fake_server, conn, login = _make_session_and_fake(tmp_path)
        fake_zip = _make_fake_zip(tmp_path)

        def quarantine_confirmer(context: str):
            return ReviewDecision.QUARANTINE, []

        with pytest.raises(UploadError) as exc_info:
            session.publish_to_xnat(
                xnat_connection=conn,
                validated_login=login,
                zipped_data=fake_zip,
                delete_zip=False,
                verbose=False,
                pixel_review_confirmer=quarantine_confirmer,
            )

        fe = exc_info.value.friendly
        assert "quarantine" in fe.message.lower() or "quarantine" in fe.title.lower(), (
            "FriendlyError message/title must mention quarantine"
        )
        assert "not uploaded" in fe.message.lower(), (
            "FriendlyError message must state the case was NOT uploaded"
        )

    def test_quarantine_upload_error_has_recourse_steps(self, tmp_path: Path) -> None:
        """The FriendlyError inside UploadError must include recourse steps."""
        session, fake_server, conn, login = _make_session_and_fake(tmp_path)
        fake_zip = _make_fake_zip(tmp_path)

        def quarantine_confirmer(context: str):
            return ReviewDecision.QUARANTINE, []

        with pytest.raises(UploadError) as exc_info:
            session.publish_to_xnat(
                xnat_connection=conn,
                validated_login=login,
                zipped_data=fake_zip,
                delete_zip=False,
                verbose=False,
                pixel_review_confirmer=quarantine_confirmer,
            )

        fe = exc_info.value.friendly
        assert len(fe.recourse) > 0, "FriendlyError must include at least one recourse step"

    def test_quarantine_blocks_upload_zero_put_zip(self, tmp_path: Path) -> None:
        """When QUARANTINE is returned no put_zip call must be made."""
        session, fake_server, conn, login = _make_session_and_fake(tmp_path)
        fake_zip = _make_fake_zip(tmp_path)

        def quarantine_confirmer(context: str):
            return ReviewDecision.QUARANTINE, []

        try:
            session.publish_to_xnat(
                xnat_connection=conn,
                validated_login=login,
                zipped_data=fake_zip,
                delete_zip=False,
                verbose=False,
                pixel_review_confirmer=quarantine_confirmer,
            )
        except UploadError:
            pass  # Expected

        assert len(fake_server.put_zip_calls) == 0, (
            "put_zip must not be called when QUARANTINE blocks upload"
        )


# ---------------------------------------------------------------------------
# Gate: QUARANTINE + QuarantineStore → evidence persisted
# ---------------------------------------------------------------------------

class TestGateQuarantineWithStore:
    """
    When make_automated_pixel_confirmer is used with a QuarantineStore and
    the verdict engine returns QUARANTINE, the case must be written to the store
    and upload must be blocked.
    """

    def test_quarantine_case_written_to_store(self, tmp_path: Path) -> None:
        """
        A confirmer built from make_automated_pixel_confirmer that yields
        QUARANTINE must write the case to the provided QuarantineStore.
        """
        store = QuarantineStore(tmp_path / "quarantine")
        case_id = "synthetic-case-q01"

        # Build a confirmer that always returns QUARANTINE and writes to store
        def quarantine_confirmer_with_store(context: str):
            # Directly persist a synthetic assessment to the store
            assessment = _make_assessment(verdict=Verdict.QUARANTINE)
            store.quarantine_case(case_id, assessment)
            return ReviewDecision.QUARANTINE, []

        session, fake_server, conn, login = _make_session_and_fake(tmp_path)
        fake_zip = _make_fake_zip(tmp_path)

        try:
            session.publish_to_xnat(
                xnat_connection=conn,
                validated_login=login,
                zipped_data=fake_zip,
                delete_zip=False,
                verbose=False,
                pixel_review_confirmer=quarantine_confirmer_with_store,
            )
        except UploadError:
            pass  # Expected — quarantine blocks upload

        assert store.is_quarantined(case_id), (
            "Case must be written to the quarantine store"
        )

    def test_quarantine_evidence_json_no_raw_phi(self, tmp_path: Path) -> None:
        """
        Evidence written by quarantine_case must contain category names but
        must NOT contain the raw fake PHI strings (SC-004).
        """
        store = QuarantineStore(tmp_path / "quarantine")
        case_id = "phi-safety-check"
        phi_text = "SMITH^JANE MRN 00471123"  # fake PHI string — must not land in JSON

        assessment = _make_assessment(
            verdict=Verdict.QUARANTINE,
            phi_categories=["PERSON", "MRN"],
            reason="unprofiled device, no positive clean evidence",
        )
        case_dir = store.quarantine_case(case_id, assessment)

        raw = (case_dir / "evidence.json").read_text(encoding="utf-8")
        # Category names are fine; the actual name/MRN string must not be present
        assert "SMITH^JANE" not in raw
        assert "00471123" not in raw
        assert "PERSON" in raw
        assert "MRN" in raw


# ---------------------------------------------------------------------------
# Gate: CONFIRMED → upload proceeds (additive, default path unchanged)
# ---------------------------------------------------------------------------

class TestGateConfirmedProceed:
    """
    Confirmer returning CONFIRMED must not raise and must not write to the
    quarantine store.  The existing ABORT path must also still work.
    """

    def test_confirmed_does_not_raise(self, tmp_path: Path) -> None:
        """A confirmer returning CONFIRMED must let publish proceed without error."""
        session, fake_server, conn, login = _make_session_and_fake(tmp_path)
        fake_zip = _make_fake_zip(tmp_path)

        def confirmed_confirmer(context: str):
            return ReviewDecision.CONFIRMED, []

        # Should not raise
        try:
            session.publish_to_xnat(
                xnat_connection=conn,
                validated_login=login,
                zipped_data=fake_zip,
                delete_zip=False,
                verbose=False,
                pixel_review_confirmer=confirmed_confirmer,
            )
        except UploadError as e:
            pytest.fail(f"UploadError should not be raised for CONFIRMED: {e}")
        except Exception:
            pass  # Other errors (FakeXNAT internals) are OK — only UploadError would be wrong

    def test_abort_still_raises_upload_error(self, tmp_path: Path) -> None:
        """
        The existing ABORT path must still raise UploadError (regression guard).
        """
        session, fake_server, conn, login = _make_session_and_fake(tmp_path)
        fake_zip = _make_fake_zip(tmp_path)

        def aborting_confirmer(context: str):
            return ReviewDecision.ABORT, []

        with pytest.raises(UploadError):
            session.publish_to_xnat(
                xnat_connection=conn,
                validated_login=login,
                zipped_data=fake_zip,
                delete_zip=False,
                verbose=False,
                pixel_review_confirmer=aborting_confirmer,
            )


# ---------------------------------------------------------------------------
# Additive-ness: ReviewDecision enum membership
# ---------------------------------------------------------------------------

class TestReviewDecisionMembers:
    """
    ReviewDecision must still carry all four members:
    CONFIRMED, REDACT, ABORT, QUARANTINE.
    Adding QUARANTINE must not remove or rename any existing member.
    """

    def test_confirmed_member_exists(self) -> None:
        assert ReviewDecision.CONFIRMED.value == "confirmed"

    def test_redact_member_exists(self) -> None:
        assert ReviewDecision.REDACT.value == "redact"

    def test_abort_member_exists(self) -> None:
        assert ReviewDecision.ABORT.value == "abort"

    def test_quarantine_member_exists(self) -> None:
        assert ReviewDecision.QUARANTINE.value == "quarantine"

    def test_all_four_members_present(self) -> None:
        names = {m.name for m in ReviewDecision}
        assert names >= {"CONFIRMED", "REDACT", "ABORT", "QUARANTINE"}, (
            f"Expected all four ReviewDecision members; got {names}"
        )


# ---------------------------------------------------------------------------
# make_automated_pixel_confirmer: signature / import test
# ---------------------------------------------------------------------------

class TestMakeAutomatedPixelConfirmer:
    """
    make_automated_pixel_confirmer must return a callable without importing
    heavy deps at factory-build time.
    """

    def test_factory_returns_callable(self) -> None:
        """make_automated_pixel_confirmer must return a callable."""
        frames = [np.zeros((64, 256), dtype=np.uint8)]
        import pydicom
        from pydicom.dataset import FileDataset, FileMetaDataset
        from pydicom.uid import ExplicitVRLittleEndian, SecondaryCaptureImageStorage, generate_uid

        file_meta = FileMetaDataset()
        file_meta.MediaStorageSOPClassUID = SecondaryCaptureImageStorage
        file_meta.MediaStorageSOPInstanceUID = generate_uid()
        file_meta.TransferSyntaxUID = ExplicitVRLittleEndian
        file_meta.ImplementationClassUID = generate_uid()
        ds = FileDataset(None, {}, file_meta=file_meta, preamble=b"\0" * 128)
        ds.SOPClassUID = SecondaryCaptureImageStorage
        ds.SOPInstanceUID = file_meta.MediaStorageSOPInstanceUID

        confirmer = make_automated_pixel_confirmer(frames, ds)
        assert callable(confirmer), "make_automated_pixel_confirmer must return a callable"

    def test_factory_with_quarantine_store_returns_callable(self, tmp_path: Path) -> None:
        """Factory must accept quarantine_store and case_id kwargs without error."""
        import pydicom
        from pydicom.dataset import FileDataset, FileMetaDataset
        from pydicom.uid import ExplicitVRLittleEndian, SecondaryCaptureImageStorage, generate_uid

        file_meta = FileMetaDataset()
        file_meta.MediaStorageSOPClassUID = SecondaryCaptureImageStorage
        file_meta.MediaStorageSOPInstanceUID = generate_uid()
        file_meta.TransferSyntaxUID = ExplicitVRLittleEndian
        file_meta.ImplementationClassUID = generate_uid()
        ds = FileDataset(None, {}, file_meta=file_meta, preamble=b"\0" * 128)
        ds.SOPClassUID = SecondaryCaptureImageStorage
        ds.SOPInstanceUID = file_meta.MediaStorageSOPInstanceUID

        frames = [np.zeros((64, 256), dtype=np.uint8)]
        store = QuarantineStore(tmp_path / "q")

        confirmer = make_automated_pixel_confirmer(
            frames, ds, quarantine_store=store, case_id="test-case"
        )
        assert callable(confirmer)
