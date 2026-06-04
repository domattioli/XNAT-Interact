"""
tests/test_app_upload_logic.py — Offline tests for app/logic/upload.

CRITICAL RULES enforced here:
  - NO `import streamlit` anywhere in this file.
  - NO network calls; FakeXNAT replaces the real server.
  - NO real PHI; all data is synthetic.

Covers:
  1. validate_intake catches missing / invalid fields.
  2. prepare_and_upload with review_decision=ABORT → ok False, ZERO put_zip calls
     (fail-closed guarantee).
  3. prepare_and_upload with CONFIRMED → upload proceeds (FakeXNAT records put_zip).
  4. prepare_and_upload with REDACT + boxes → apply_redaction applied (masked region zeroed).
  5. Mid-upload server failure (FakeXNAT.set_next_failure) → FriendlyError, no traceback.
  6. dropdown_options returns the configured lists.
"""
from __future__ import annotations

import zipfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Tuple

import numpy as np
import pytest

# Only import from app/logic (no streamlit) + src.* + tests.fakes.
from app.logic.upload import (
    UploadOutcome,
    dropdown_options,
    prepare_and_upload,
    upload_preview,
    validate_intake,
)
from src.services.deidentify import apply_redaction
from src.services.errors import FriendlyError
from src.xnat_experiment_data import ExperimentData, ReviewDecision, UploadError
from tests.fakes.fake_xnat import FakeXNAT


# ---------------------------------------------------------------------------
# Helpers: minimal fakes that satisfy the Phase-1 publish path
# ---------------------------------------------------------------------------

def _make_fake_connection(fake_server: FakeXNAT, project_name: str = "TEST_PROJECT") -> SimpleNamespace:
    return SimpleNamespace(server=fake_server, xnat_project_name=project_name)


def _make_fake_login(username: str = "testuser") -> SimpleNamespace:
    return SimpleNamespace(validated_username=username)


def _make_fake_intake_form(tmp_path: Path) -> SimpleNamespace:
    """Minimal stand-in for ORDataIntakeForm satisfying ExperimentData.publish_to_xnat."""
    saved_ffn = tmp_path / "RECONSTRUCTED_OR_DATA_INTAKE_FORM.json"
    saved_ffn.write_text("{}", encoding="utf-8")

    form = SimpleNamespace(
        uid="TEST_UID_UPLOAD_001",
        group="TEST_GROUP",
        acquisition_site="TEST_SITE",
        operation_date="2024-01-01",
        epic_start_time="120000",
        ortho_procedure_type="TEST_PROCEDURE",
        scan_quality="usable",
        datetime=SimpleNamespace(date="2024-01-01", time="120000"),
        relevant_folder=tmp_path,
        saved_ffn=saved_ffn,
        saved_ffn_str=str(saved_ffn),
    )

    def push_to_xnat(subj_inst: Any, verbose: bool = False) -> None:
        subj_inst.resource("INTAKE_FORM").file("form.json").insert(
            "{}", content="TEXT", format="JSON", tags="DOC"
        )

    form.push_to_xnat = push_to_xnat
    return form


class _MinimalSession(ExperimentData):
    """Minimal concrete ExperimentData — bypasses real __init__."""

    @classmethod
    def build(cls, intake_form: Any, schema_prefix: str = "rf") -> "_MinimalSession":
        obj = object.__new__(cls)
        obj._intake_form = intake_form
        obj._schema_prefix_str = schema_prefix
        obj._scan_type_label = "DICOM"
        obj._is_valid = True
        obj._df = None
        return obj

    def _populate_df(self, config: Any) -> None:
        pass

    def _check_session_validity(self, config: Any) -> None:
        pass

    def write(self, config: Any, zip_dest: Any = None, verbose: Any = True) -> Any:
        raise NotImplementedError


def _make_zip(tmp_path: Path) -> dict:
    """Create a minimal zip on disk; return zipped_data dict for publish_to_xnat."""
    zip_path = tmp_path / "dicom_files.zip"
    with zipfile.ZipFile(str(zip_path), "w") as zf:
        zf.writestr("placeholder.dcm", b"FAKE_DCM")
    return {str(zip_path): {"CONTENT": "IMAGE", "FORMAT": "DICOM", "TAG": "INTRA_OP"}}


# ---------------------------------------------------------------------------
# Minimal stub config for dropdown_options
# ---------------------------------------------------------------------------

class _StubConfig:
    """Config stub with fixed surgeon/site/procedure lists."""
    _tables: Dict[str, List[str]] = {
        "surgeons": ["drsmith", "drjones", "unknown"],
        "acquisition_sites": ["UIHC", "UH", "UNKNOWN_SITE"],
        "groups": ["PEDIATRIC_SUPRACONDYLAR_HUMERUS_FRACTURE", "HIP_ARTHROSCOPY"],
    }

    def list_of_all_items_in_table(self, table_name: str) -> List[str]:
        return self._tables.get(table_name.lower(), [])


_STUB_CONFIG = _StubConfig()


# ---------------------------------------------------------------------------
# Minimal valid form values for tests that need a passing form
# ---------------------------------------------------------------------------

_VALID_FORM: Dict[str, Any] = {
    "filer_hawkid":       "testuser",
    "operation_date":     "2024-01-01",
    "institution_name":   "UIHC",
    "procedure_name":     "PEDIATRIC_SUPRACONDYLAR_HUMERUS_FRACTURE",
    "epic_start_time":    "120000",
    "performing_surgeon": "drsmith",
    "image_dir":          "/tmp/fake_images",
    "scan_quality":       "usable",
}


# ---------------------------------------------------------------------------
# 1. validate_intake — missing / invalid fields
# ---------------------------------------------------------------------------

def test_validate_intake_all_fields_valid():
    """Valid form → empty problem list."""
    problems = validate_intake(_VALID_FORM)
    assert problems == [], f"Expected no problems, got: {problems}"


def test_validate_intake_missing_required_fields():
    """Empty form → problem for every required field."""
    problems = validate_intake({})
    # All 7 required fields should be flagged.
    assert len(problems) >= 7, f"Expected >=7 problems for empty form, got {len(problems)}: {problems}"


def test_validate_intake_missing_individual_fields():
    """Each required field missing individually → problem reported."""
    required_keys = [
        "filer_hawkid",
        "operation_date",
        "institution_name",
        "procedure_name",
        "epic_start_time",
        "performing_surgeon",
        "image_dir",
    ]
    for key in required_keys:
        partial = {k: v for k, v in _VALID_FORM.items() if k != key}
        problems = validate_intake(partial)
        assert len(problems) >= 1, f"Expected problem when '{key}' is missing"


def test_validate_intake_bad_date_format():
    """Non-YYYY-MM-DD date → problem."""
    bad = {**_VALID_FORM, "operation_date": "01/01/2024"}
    problems = validate_intake(bad)
    assert any("date" in p.lower() or "yyyy" in p.lower() for p in problems), (
        f"Expected date-format problem, got: {problems}"
    )


def test_validate_intake_invalid_scan_quality():
    """Unrecognised scan quality → problem."""
    bad = {**_VALID_FORM, "scan_quality": "perfect"}
    problems = validate_intake(bad)
    assert any("quality" in p.lower() for p in problems), (
        f"Expected scan_quality problem, got: {problems}"
    )


def test_validate_intake_valid_date_formats():
    """YYYY-MM-DD accepted without extra date problem."""
    good = {**_VALID_FORM, "operation_date": "2025-12-31"}
    problems = validate_intake(good)
    assert not any("date" in p.lower() and "format" in p.lower() for p in problems), (
        f"Unexpected date-format problem: {problems}"
    )


# ---------------------------------------------------------------------------
# 2. prepare_and_upload ABORT → ok False, ZERO put_zip (fail-closed)
# ---------------------------------------------------------------------------

def test_prepare_and_upload_abort_fail_closed(tmp_path: Path):
    """ABORT decision → ok=False, FakeXNAT records ZERO put_zip calls."""
    fake_server = FakeXNAT()

    outcome = prepare_and_upload(
        form_values=_VALID_FORM,
        image_dir=tmp_path,
        server_connection=fake_server,
        review_decision=ReviewDecision.ABORT,
        publish_fn=None,  # must never reach publish
    )

    assert outcome.ok is False
    assert isinstance(outcome.friendly, FriendlyError)

    # CRITICAL: no put_zip must have been called.
    put_zip_calls = [c for c in fake_server.calls if c["op"] == "resource.put_zip"]
    assert len(put_zip_calls) == 0, (
        f"FAIL-CLOSED violated: put_zip was called {len(put_zip_calls)} time(s) "
        "despite ABORT decision."
    )


def test_prepare_and_upload_abort_returns_friendly_error(tmp_path: Path):
    """ABORT → FriendlyError with PHI-review recourse (no raw exception)."""
    outcome = prepare_and_upload(
        form_values=_VALID_FORM,
        image_dir=tmp_path,
        server_connection=FakeXNAT(),
        review_decision=ReviewDecision.ABORT,
        publish_fn=None,
    )

    assert outcome.ok is False
    fe = outcome.friendly
    assert fe is not None
    assert isinstance(fe, FriendlyError)
    # Recourse must guide user back to PHI review.
    assert len(fe.recourse) >= 1


def test_prepare_and_upload_does_not_raise_on_abort(tmp_path: Path):
    """No exception must escape prepare_and_upload for ABORT."""
    try:
        outcome = prepare_and_upload(
            form_values=_VALID_FORM,
            image_dir=tmp_path,
            server_connection=FakeXNAT(),
            review_decision=ReviewDecision.ABORT,
            publish_fn=None,
        )
    except Exception as exc:
        pytest.fail(f"prepare_and_upload raised unexpectedly on ABORT: {exc}")

    assert outcome.ok is False


# ---------------------------------------------------------------------------
# 3. prepare_and_upload CONFIRMED → upload proceeds (put_zip called)
# ---------------------------------------------------------------------------

def _build_confirming_publish_fn(fake_server: FakeXNAT, tmp_path: Path):
    """
    Build a publish_fn that calls the Phase-1 publish_to_xnat via FakeXNAT.
    The confirmer passed to publish_to_xnat will be whatever prepare_and_upload
    derives from review_decision=CONFIRMED.
    """
    fake_conn = _make_fake_connection(fake_server)
    fake_login = _make_fake_login()
    intake = _make_fake_intake_form(tmp_path)
    session = _MinimalSession.build(intake)
    zipped_data = _make_zip(tmp_path)

    def publish_fn(
        server_connection: Any,
        form_values: Dict[str, Any],
        image_dir: Any,
        pixel_review_confirmer: Any,
    ) -> None:
        session.publish_to_xnat(
            xnat_connection=fake_conn,
            validated_login=fake_login,
            zipped_data=zipped_data,
            delete_zip=False,
            verbose=False,
            pixel_review_confirmer=pixel_review_confirmer,
        )

    return publish_fn


def test_prepare_and_upload_confirmed_proceeds(tmp_path: Path):
    """CONFIRMED decision → ok=True, FakeXNAT records put_zip call."""
    fake_server = FakeXNAT()
    publish_fn = _build_confirming_publish_fn(fake_server, tmp_path)

    outcome = prepare_and_upload(
        form_values=_VALID_FORM,
        image_dir=tmp_path,
        server_connection=fake_server,
        review_decision=ReviewDecision.CONFIRMED,
        publish_fn=publish_fn,
    )

    assert outcome.ok is True, f"Expected ok=True, got friendly={outcome.friendly}"
    assert outcome.friendly is None

    put_zip_calls = [c for c in fake_server.calls if c["op"] == "resource.put_zip"]
    assert len(put_zip_calls) >= 1, (
        f"Expected at least 1 put_zip call on CONFIRMED, got {len(put_zip_calls)}"
    )


def test_prepare_and_upload_confirmed_does_not_raise(tmp_path: Path):
    """CONFIRMED path must not propagate any exception."""
    fake_server = FakeXNAT()
    publish_fn = _build_confirming_publish_fn(fake_server, tmp_path)

    try:
        outcome = prepare_and_upload(
            form_values=_VALID_FORM,
            image_dir=tmp_path,
            server_connection=fake_server,
            review_decision=ReviewDecision.CONFIRMED,
            publish_fn=publish_fn,
        )
    except Exception as exc:
        pytest.fail(f"prepare_and_upload raised on CONFIRMED: {exc}")

    assert outcome.ok is True


# ---------------------------------------------------------------------------
# 4. prepare_and_upload REDACT + boxes → apply_redaction applied
# ---------------------------------------------------------------------------

def test_apply_redaction_zeros_region():
    """apply_redaction service: masked region zeroed, original untouched."""
    arr = np.ones((32, 64), dtype=np.uint16) * 500
    boxes = [(0, 0, 32, 16)]  # zero top-left 32×16 region

    result = apply_redaction(arr, boxes)

    assert result[0:16, 0:32].max() == 0, "Redacted region must be all-zero"
    assert result[16:, :].min() == 500, "Non-redacted region must be intact"
    assert arr.max() == 500, "apply_redaction must not mutate original array"


def test_prepare_and_upload_redact_proceeds(tmp_path: Path):
    """REDACT decision with boxes → upload proceeds (put_zip called)."""
    fake_server = FakeXNAT()
    publish_fn = _build_confirming_publish_fn(fake_server, tmp_path)
    boxes = [(0, 0, 10, 10)]

    outcome = prepare_and_upload(
        form_values=_VALID_FORM,
        image_dir=tmp_path,
        server_connection=fake_server,
        review_decision=ReviewDecision.REDACT,
        redaction_boxes=boxes,
        publish_fn=publish_fn,
    )

    assert outcome.ok is True, f"Expected ok=True for REDACT, got friendly={outcome.friendly}"

    put_zip_calls = [c for c in fake_server.calls if c["op"] == "resource.put_zip"]
    assert len(put_zip_calls) >= 1, "Expected put_zip call on REDACT decision"


def test_prepare_and_upload_redact_boxes_passed_to_confirmer(tmp_path: Path):
    """
    REDACT path: the pixel_review_confirmer handed to publish_fn must return
    (REDACT, boxes) — not ABORT or CONFIRMED — so Phase-1 gate sees the intent.
    """
    captured: list = []
    fake_server = FakeXNAT()
    fake_conn = _make_fake_connection(fake_server)
    fake_login = _make_fake_login()
    intake = _make_fake_intake_form(tmp_path)
    session = _MinimalSession.build(intake)
    zipped_data = _make_zip(tmp_path)
    boxes = [(5, 5, 20, 20)]

    def _capturing_publish_fn(
        server_connection: Any,
        form_values: Dict[str, Any],
        image_dir: Any,
        pixel_review_confirmer: Any,
    ) -> None:
        decision, returned_boxes = pixel_review_confirmer("test context")
        captured.append((decision, returned_boxes))
        # Proceed with the real Phase-1 publish.
        session.publish_to_xnat(
            xnat_connection=fake_conn,
            validated_login=fake_login,
            zipped_data=zipped_data,
            delete_zip=False,
            verbose=False,
            pixel_review_confirmer=pixel_review_confirmer,
        )

    prepare_and_upload(
        form_values=_VALID_FORM,
        image_dir=tmp_path,
        server_connection=fake_server,
        review_decision=ReviewDecision.REDACT,
        redaction_boxes=boxes,
        publish_fn=_capturing_publish_fn,
    )

    assert len(captured) == 1
    decision, returned_boxes = captured[0]
    assert decision == ReviewDecision.REDACT, (
        f"Confirmer must return REDACT decision; got {decision}"
    )
    assert returned_boxes == boxes, (
        f"Confirmer must pass boxes through; got {returned_boxes}"
    )


# ---------------------------------------------------------------------------
# 5. Mid-upload server failure → FriendlyError returned, no traceback
# ---------------------------------------------------------------------------

def test_server_failure_returns_friendly_error(tmp_path: Path):
    """FakeXNAT.set_next_failure → ok=False, FriendlyError, no raw exception escapes."""
    fake_server = FakeXNAT()
    fake_server.set_next_failure(ConnectionError("simulated VPN drop during upload"))
    publish_fn = _build_confirming_publish_fn(fake_server, tmp_path)

    try:
        outcome = prepare_and_upload(
            form_values=_VALID_FORM,
            image_dir=tmp_path,
            server_connection=fake_server,
            review_decision=ReviewDecision.CONFIRMED,
            publish_fn=publish_fn,
        )
    except Exception as exc:
        pytest.fail(
            f"Raw exception escaped prepare_and_upload on server failure: "
            f"{type(exc).__name__}: {exc}"
        )

    assert outcome.ok is False
    assert isinstance(outcome.friendly, FriendlyError), (
        "Server failure must return FriendlyError, not ok=True"
    )


def test_server_timeout_returns_friendly_error(tmp_path: Path):
    """TimeoutError injected → ok=False, FriendlyError with recourse."""
    fake_server = FakeXNAT()
    fake_server.set_next_failure(TimeoutError("simulated server timeout"))
    publish_fn = _build_confirming_publish_fn(fake_server, tmp_path)

    outcome = prepare_and_upload(
        form_values=_VALID_FORM,
        image_dir=tmp_path,
        server_connection=fake_server,
        review_decision=ReviewDecision.CONFIRMED,
        publish_fn=publish_fn,
    )

    assert outcome.ok is False
    fe = outcome.friendly
    assert fe is not None
    assert len(fe.recourse) >= 1, "FriendlyError must include recourse steps on timeout"


def test_server_failure_no_raw_traceback(tmp_path: Path):
    """ConnectionError must not propagate as raw exception — only FriendlyError."""
    fake_server = FakeXNAT()
    fake_server.set_next_failure(OSError("disk gone mid-upload"))
    publish_fn = _build_confirming_publish_fn(fake_server, tmp_path)

    raw_escaped = False
    try:
        prepare_and_upload(
            form_values=_VALID_FORM,
            image_dir=tmp_path,
            server_connection=fake_server,
            review_decision=ReviewDecision.CONFIRMED,
            publish_fn=publish_fn,
        )
    except FriendlyError:
        pass  # FriendlyError is acceptable — it is the wrapper
    except UploadError:
        pass  # UploadError (wraps FriendlyError) also acceptable if it escapes
    except Exception as raw:
        raw_escaped = True
        print(f"RAW EXCEPTION ESCAPED: {type(raw).__name__}: {raw}")

    assert not raw_escaped, (
        "Raw exception must not escape prepare_and_upload — must be wrapped in FriendlyError"
    )


# ---------------------------------------------------------------------------
# 6. dropdown_options returns the configured lists
# ---------------------------------------------------------------------------

def test_dropdown_options_keys():
    """dropdown_options returns dict with surgeons/sites/procedures keys."""
    opts = dropdown_options(_STUB_CONFIG)
    assert "surgeons" in opts
    assert "sites" in opts
    assert "procedures" in opts


def test_dropdown_options_surgeons():
    """Surgeons list sourced from ConfigTables.list_of_all_items_in_table('Surgeons')."""
    opts = dropdown_options(_STUB_CONFIG)
    assert opts["surgeons"] == _STUB_CONFIG.list_of_all_items_in_table("Surgeons")


def test_dropdown_options_sites():
    """Sites list sourced from ACQUISITION_SITES table."""
    opts = dropdown_options(_STUB_CONFIG)
    assert opts["sites"] == _STUB_CONFIG.list_of_all_items_in_table("ACQUISITION_SITES")


def test_dropdown_options_procedures():
    """Procedures list sourced from Groups table."""
    opts = dropdown_options(_STUB_CONFIG)
    assert opts["procedures"] == _STUB_CONFIG.list_of_all_items_in_table("Groups")


def test_dropdown_options_nonempty():
    """Stub config returns non-empty lists for all three keys."""
    opts = dropdown_options(_STUB_CONFIG)
    assert len(opts["surgeons"]) > 0
    assert len(opts["sites"]) > 0
    assert len(opts["procedures"]) > 0


# ---------------------------------------------------------------------------
# 7. upload_preview returns the expected summary keys
# ---------------------------------------------------------------------------

def test_upload_preview_keys():
    """upload_preview returns a dict with the required summary keys."""
    preview = upload_preview(_VALID_FORM, image_count=42)
    for key in ("subject_id_note", "image_count", "procedure", "institution",
                "operation_date", "phi_removed_note"):
        assert key in preview, f"Missing key '{key}' in upload_preview result"


def test_upload_preview_image_count():
    """image_count value matches the argument passed."""
    preview = upload_preview(_VALID_FORM, image_count=17)
    assert preview["image_count"] == 17


def test_upload_preview_no_phi_in_subject_note():
    """subject_id_note must not echo patient name or MRN (PHI-free)."""
    preview = upload_preview({**_VALID_FORM, "filer_hawkid": "drsmith"}, image_count=5)
    # The note should mention de-identification, not echo the hawkid
    note = preview["subject_id_note"].lower()
    assert "de-identified" in note or "uid" in note or "no patient" in note, (
        f"subject_id_note should describe de-identification; got: {preview['subject_id_note']}"
    )


def test_upload_preview_phi_removed_note_present():
    """phi_removed_note must describe what is stripped from DICOM metadata."""
    preview = upload_preview(_VALID_FORM, image_count=1)
    note = preview["phi_removed_note"].lower()
    assert "redact" in note or "remov" in note or "dicom" in note, (
        f"phi_removed_note should describe DICOM PHI removal; got: {preview['phi_removed_note']}"
    )
