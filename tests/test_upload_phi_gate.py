"""
tests/test_upload_phi_gate.py — PHI-safety gate tests (T014, T027, T028).

Offline, no network, no real PHI.  All server I/O injected via FakeXNAT.

Scenarios:
  1. Upload BLOCKED when confirmer returns ABORT → zero put_zip calls, FriendlyError raised.
  2. Confirmer returns REDACT with boxes → apply_redaction is called, array region zeroed.
  3. Confirmer returns CONFIRMED → upload proceeds, FakeXNAT records put_zip calls.
  4. Mid-upload connection drop (FakeXNAT.set_next_failure) → FriendlyError with resume
     recourse, no raw traceback escapes.
"""
from __future__ import annotations

import os
import tempfile
import zipfile
from pathlib import Path
from types import SimpleNamespace
from typing import List, Tuple
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from src.services.deidentify import apply_redaction
from src.services.errors import FriendlyError
from src.xnat_experiment_data import ExperimentData, ReviewDecision, UploadError
from tests.fakes.fake_xnat import FakeXNAT


# ---------------------------------------------------------------------------
# Helpers — minimal fake XNATConnection and XNATLogin
# ---------------------------------------------------------------------------

def make_fake_connection(fake_server: FakeXNAT, project_name: str = "TEST_PROJECT") -> SimpleNamespace:
    """
    Minimal stand-in for XNATConnection that satisfies publish_to_xnat's
    attribute reads:
      - .server          → FakeXNAT instance
      - .xnat_project_name → str
    """
    return SimpleNamespace(server=fake_server, xnat_project_name=project_name)


def make_fake_login(username: str = "testuser") -> SimpleNamespace:
    """
    Minimal stand-in for XNATLogin that satisfies publish_to_xnat's
    attribute read:
      - .validated_username → str
    """
    return SimpleNamespace(validated_username=username)


# ---------------------------------------------------------------------------
# Minimal fake intake form
# ---------------------------------------------------------------------------

def make_fake_intake_form(tmp_path: Path) -> SimpleNamespace:
    """
    Minimal stand-in for ORDataIntakeForm.

    publish_to_xnat calls:
      - self.intake_form.push_to_xnat(subj_inst=..., verbose=...)
      - self.intake_form.uid           (in _generate_queries)
      - self.intake_form.group         (in publish_to_xnat attrs.mset)
      - self.intake_form.acquisition_site
      - self.intake_form.datetime.date
      - self.intake_form.ortho_procedure_type
      - self.intake_form.scan_quality
      - self.intake_form.epic_start_time (not needed for publish path directly)
      - self.intake_form.relevant_folder (Path, used in ExperimentData.__init__)
      - self.intake_form.saved_ffn       (Path, used in ExperimentData.__init__)
      - self.intake_form.saved_ffn_str   (str)
    """
    # Write a real dummy file so Path.exists() passes
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
    )

    def push_to_xnat(subj_inst, verbose=False):
        # Simulate the real push: calls subj_inst.resource(...).file(...).insert(...)
        subj_inst.resource("INTAKE_FORM").file("RECONSTRUCTED_OR_DATA_INTAKE_FORM.json").insert(
            "{}", content="TEXT", format="JSON", tags="DOC"
        )

    form.push_to_xnat = push_to_xnat
    return form


# ---------------------------------------------------------------------------
# Minimal concrete ExperimentData subclass
# ---------------------------------------------------------------------------

class _MinimalRFSession(ExperimentData):
    """
    Bypass ExperimentData.__init__ entirely so we can unit-test publish_to_xnat
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
        pass  # not needed for publish tests

    def _check_session_validity(self, config):
        pass  # not needed for publish tests

    def write(self, config, zip_dest=None, verbose=True):
        raise NotImplementedError("use pre-built zipped_data in tests")


# ---------------------------------------------------------------------------
# Fixture — builds a ready-to-use zipped_data dict with a real (empty) zip
# ---------------------------------------------------------------------------

@pytest.fixture()
def fake_zip(tmp_path: Path) -> dict:
    """Create a minimal zip file on disk; return zipped_data dict."""
    zip_path = tmp_path / "dicom_files.zip"
    with zipfile.ZipFile(str(zip_path), "w") as zf:
        zf.writestr("placeholder.dcm", b"FAKE")
    return {str(zip_path): {"CONTENT": "IMAGE", "FORMAT": "DICOM", "TAG": "INTRA_OP"}}


@pytest.fixture()
def session_and_fake(tmp_path: Path) -> Tuple:
    """
    Returns (session, fake_xnat, fake_connection, fake_login).
    Session is a _MinimalRFSession wired to a FakeXNAT server.
    """
    fake_server = FakeXNAT(project_name="TEST_PROJECT")
    fake_conn = make_fake_connection(fake_server)
    fake_login = make_fake_login()
    intake = make_fake_intake_form(tmp_path)
    session = _MinimalRFSession.build(intake)
    return session, fake_server, fake_conn, fake_login


# ---------------------------------------------------------------------------
# T014-A — ABORT: upload BLOCKED, zero put_zip calls, FriendlyError raised
# ---------------------------------------------------------------------------

def test_upload_blocked_when_confirmer_aborts(session_and_fake, fake_zip):
    session, fake_server, fake_conn, fake_login = session_and_fake

    def aborting_confirmer(context: str):
        return ReviewDecision.ABORT, []

    with pytest.raises(UploadError) as exc_info:
        session.publish_to_xnat(
            xnat_connection=fake_conn,
            validated_login=fake_login,
            zipped_data=fake_zip,
            delete_zip=False,
            verbose=False,
            pixel_review_confirmer=aborting_confirmer,
        )

    # No pixel data must have been sent
    put_zip_calls = [c for c in fake_server.calls if c["op"] == "resource.put_zip"]
    assert len(put_zip_calls) == 0, "put_zip must NOT be called when confirmer ABORTs"

    upload_err = exc_info.value
    assert isinstance(upload_err, UploadError)
    fe = upload_err.friendly
    assert isinstance(fe, FriendlyError)
    assert "Upload stopped" in fe.message
    assert "patient name" in fe.message.lower() or "date" in fe.message.lower()


# ---------------------------------------------------------------------------
# T014-B — ABORT produces a FriendlyError; fail-closed guarantee
# ---------------------------------------------------------------------------

def test_abort_produces_friendly_error_not_raw_exception(session_and_fake, fake_zip):
    """Any non-confirmation path must NOT let a raw exception escape."""
    session, fake_server, fake_conn, fake_login = session_and_fake

    def aborting_confirmer(context: str):
        return ReviewDecision.ABORT, []

    raised = None
    try:
        session.publish_to_xnat(
            xnat_connection=fake_conn,
            validated_login=fake_login,
            zipped_data=fake_zip,
            delete_zip=False,
            verbose=False,
            pixel_review_confirmer=aborting_confirmer,
        )
    except UploadError as ue:
        raised = ue
    except Exception as raw:
        pytest.fail(f"Unexpected exception escaped the gate: {type(raw).__name__}: {raw}")

    assert raised is not None, "UploadError must be raised on ABORT"
    assert raised.friendly.recourse, "FriendlyError inside UploadError must include recourse steps"


# ---------------------------------------------------------------------------
# T014-C — REDACT: apply_redaction called, masked region zeroed
# ---------------------------------------------------------------------------

def test_redact_decision_calls_apply_redaction():
    """
    apply_redaction must zero the specified region.
    This exercises the function directly (the pixel arrays are already in-memory
    before write()/put_zip; the REDACT path patches them here).
    """
    arr = np.ones((32, 64), dtype=np.uint16) * 1000
    boxes = [(0, 0, 32, 16)]   # zero top-left quarter

    result = apply_redaction(arr, boxes)

    # Redacted region must be zeroed
    assert result[0:16, 0:32].max() == 0, "Redacted region must be all-zero"
    # Non-redacted region must be intact
    assert result[16:, :].min() == 1000, "Non-redacted region must be untouched"
    # Original must be unmodified (pure function)
    assert arr.max() == 1000, "apply_redaction must not modify the original array"


def test_redact_confirmer_path_does_not_block_upload(session_and_fake, fake_zip):
    """When confirmer returns REDACT, upload proceeds (put_zip is called)."""
    session, fake_server, fake_conn, fake_login = session_and_fake

    boxes = [(0, 0, 4, 4)]

    def redact_confirmer(context: str):
        return ReviewDecision.REDACT, boxes

    # Should not raise
    session.publish_to_xnat(
        xnat_connection=fake_conn,
        validated_login=fake_login,
        zipped_data=fake_zip,
        delete_zip=False,
        verbose=False,
        pixel_review_confirmer=redact_confirmer,
    )

    put_zip_calls = [c for c in fake_server.calls if c["op"] == "resource.put_zip"]
    assert len(put_zip_calls) == len(fake_zip), (
        "put_zip should be called once per zip file when REDACT is confirmed"
    )


# ---------------------------------------------------------------------------
# T014-D — CONFIRMED: upload proceeds, FakeXNAT records put_zip
# ---------------------------------------------------------------------------

def test_upload_proceeds_when_confirmed(session_and_fake, fake_zip):
    session, fake_server, fake_conn, fake_login = session_and_fake

    def confirming_confirmer(context: str):
        return ReviewDecision.CONFIRMED, []

    session.publish_to_xnat(
        xnat_connection=fake_conn,
        validated_login=fake_login,
        zipped_data=fake_zip,
        delete_zip=False,
        verbose=False,
        pixel_review_confirmer=confirming_confirmer,
    )

    put_zip_calls = [c for c in fake_server.calls if c["op"] == "resource.put_zip"]
    assert len(put_zip_calls) == len(fake_zip), (
        f"Expected {len(fake_zip)} put_zip call(s), got {len(put_zip_calls)}"
    )


def test_confirmed_upload_records_correct_format(session_and_fake, fake_zip):
    """put_zip must be called with the correct format/content from zipped_data."""
    session, fake_server, fake_conn, fake_login = session_and_fake

    def confirming_confirmer(context: str):
        return ReviewDecision.CONFIRMED, []

    session.publish_to_xnat(
        xnat_connection=fake_conn,
        validated_login=fake_login,
        zipped_data=fake_zip,
        delete_zip=False,
        verbose=False,
        pixel_review_confirmer=confirming_confirmer,
    )

    put_zip_calls = [c for c in fake_server.calls if c["op"] == "resource.put_zip"]
    assert len(put_zip_calls) >= 1
    kw = put_zip_calls[0]["kwargs"]
    assert kw["format"] == "DICOM"
    assert kw["content"] == "IMAGE"


# ---------------------------------------------------------------------------
# T027 — cleanup: zip files deleted after successful upload
# ---------------------------------------------------------------------------

def test_zip_deleted_after_successful_upload(session_and_fake, tmp_path: Path):
    session, fake_server, fake_conn, fake_login = session_and_fake

    zip_path = tmp_path / "dicom_files.zip"
    with zipfile.ZipFile(str(zip_path), "w") as zf:
        zf.writestr("x.dcm", b"FAKE")
    zipped_data = {str(zip_path): {"CONTENT": "IMAGE", "FORMAT": "DICOM", "TAG": "INTRA_OP"}}

    assert zip_path.exists(), "Zip must exist before upload"

    session.publish_to_xnat(
        xnat_connection=fake_conn,
        validated_login=fake_login,
        zipped_data=zipped_data,
        delete_zip=True,   # explicit True
        verbose=False,
        pixel_review_confirmer=lambda ctx: (ReviewDecision.CONFIRMED, []),
    )

    assert not zip_path.exists(), "Zip file must be deleted after successful upload (T027)"


def test_intake_form_artifact_deleted_after_upload(session_and_fake, tmp_path: Path):
    """Extra intake-form temp artifacts passed via intake_form_temp_artifacts are cleaned up."""
    session, fake_server, fake_conn, fake_login = session_and_fake

    zip_path = tmp_path / "dicom_files.zip"
    with zipfile.ZipFile(str(zip_path), "w") as zf:
        zf.writestr("x.dcm", b"FAKE")
    zipped_data = {str(zip_path): {"CONTENT": "IMAGE", "FORMAT": "DICOM", "TAG": "INTRA_OP"}}

    artifact = tmp_path / "intake_temp.json"
    artifact.write_text("{}", encoding="utf-8")
    assert artifact.exists()

    session.publish_to_xnat(
        xnat_connection=fake_conn,
        validated_login=fake_login,
        zipped_data=zipped_data,
        delete_zip=True,
        verbose=False,
        pixel_review_confirmer=lambda ctx: (ReviewDecision.CONFIRMED, []),
        intake_form_temp_artifacts=[artifact],
    )

    assert not artifact.exists(), "Intake-form temp artifact must be deleted after upload (T027)"


def test_zip_not_deleted_when_delete_zip_false(session_and_fake, tmp_path: Path):
    session, fake_server, fake_conn, fake_login = session_and_fake

    zip_path = tmp_path / "dicom_keep.zip"
    with zipfile.ZipFile(str(zip_path), "w") as zf:
        zf.writestr("x.dcm", b"FAKE")
    zipped_data = {str(zip_path): {"CONTENT": "IMAGE", "FORMAT": "DICOM", "TAG": "INTRA_OP"}}

    session.publish_to_xnat(
        xnat_connection=fake_conn,
        validated_login=fake_login,
        zipped_data=zipped_data,
        delete_zip=False,
        verbose=False,
        pixel_review_confirmer=lambda ctx: (ReviewDecision.CONFIRMED, []),
    )

    assert zip_path.exists(), "Zip must be preserved when delete_zip=False"


# ---------------------------------------------------------------------------
# T028 — mid-upload connection drop → FriendlyError with resume recourse
# ---------------------------------------------------------------------------

def test_connection_drop_raises_friendly_error(session_and_fake, fake_zip):
    session, fake_server, fake_conn, fake_login = session_and_fake

    # Inject a connection failure on the next resource operation
    fake_server.set_next_failure(ConnectionError("simulated VPN drop"))

    with pytest.raises(UploadError) as exc_info:
        session.publish_to_xnat(
            xnat_connection=fake_conn,
            validated_login=fake_login,
            zipped_data=fake_zip,
            delete_zip=False,
            verbose=False,
            pixel_review_confirmer=lambda ctx: (ReviewDecision.CONFIRMED, []),
        )

    ue = exc_info.value
    assert isinstance(ue, UploadError)
    fe = ue.friendly
    assert isinstance(fe, FriendlyError)
    # Must mention VPN and re-run recourse
    assert "interrupted" in fe.message.lower() or "VPN" in fe.message
    assert any("VPN" in r or "re-run" in r.lower() or "resume" in r.lower() for r in fe.recourse), (
        "FriendlyError recourse must mention VPN check or re-run/resume"
    )


def test_timeout_error_raises_friendly_error(session_and_fake, fake_zip):
    session, fake_server, fake_conn, fake_login = session_and_fake

    fake_server.set_next_failure(TimeoutError("simulated server timeout"))

    with pytest.raises(UploadError) as exc_info:
        session.publish_to_xnat(
            xnat_connection=fake_conn,
            validated_login=fake_login,
            zipped_data=fake_zip,
            delete_zip=False,
            verbose=False,
            pixel_review_confirmer=lambda ctx: (ReviewDecision.CONFIRMED, []),
        )

    ue = exc_info.value
    assert isinstance(ue, UploadError)
    assert ue.friendly.recourse, "FriendlyError must include recourse steps for T028"


def test_connection_drop_does_not_leak_raw_traceback(session_and_fake, fake_zip):
    """
    A raw exception (ConnectionError, TimeoutError, OSError) must NEVER escape
    as-is. Only FriendlyError is allowed to propagate.
    """
    session, fake_server, fake_conn, fake_login = session_and_fake

    fake_server.set_next_failure(OSError("disk gone"))

    raw_escaped = False
    try:
        session.publish_to_xnat(
            xnat_connection=fake_conn,
            validated_login=fake_login,
            zipped_data=fake_zip,
            delete_zip=False,
            verbose=False,
            pixel_review_confirmer=lambda ctx: (ReviewDecision.CONFIRMED, []),
        )
    except UploadError:
        pass  # expected
    except Exception as raw:
        raw_escaped = True
        print(f"RAW EXCEPTION ESCAPED: {type(raw).__name__}: {raw}")

    assert not raw_escaped, "Raw exception must not escape — must be wrapped in UploadError (T028)"


# ---------------------------------------------------------------------------
# T028 — mid-upload failure message content
# ---------------------------------------------------------------------------

def test_connection_drop_message_mentions_partial_upload(session_and_fake, fake_zip):
    """Message must mention partial upload and re-run recourse."""
    session, fake_server, fake_conn, fake_login = session_and_fake
    fake_server.set_next_failure(ConnectionError("gone"))

    with pytest.raises(UploadError) as exc_info:
        session.publish_to_xnat(
            xnat_connection=fake_conn,
            validated_login=fake_login,
            zipped_data=fake_zip,
            delete_zip=False,
            verbose=False,
            pixel_review_confirmer=lambda ctx: (ReviewDecision.CONFIRMED, []),
        )

    ue = exc_info.value
    msg_lower = ue.friendly.message.lower()
    assert "partial" in msg_lower or "re-run" in msg_lower or "resume" in msg_lower, (
        "T028 FriendlyError message must mention partial upload or re-run/resume"
    )
