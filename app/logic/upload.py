"""
app/logic/upload — pure upload-screen logic.

NO streamlit import.  Tested offline against FakeXNAT.

Public API
----------
dropdown_options(config)            -> dict[str, list[str]]
validate_intake(form_values)        -> list[str]
upload_preview(form_values, n)      -> dict
prepare_and_upload(...)             -> UploadOutcome
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

from src.services.errors import FriendlyError
from src.xnat_experiment_data import ReviewDecision, UploadError


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

@dataclass
class UploadOutcome:
    """Return value of prepare_and_upload().

    Fields
    ------
    ok       : True  → upload completed; False → blocked or failed.
    friendly : None when ok; FriendlyError when ok is False.
    """
    ok: bool
    friendly: Optional[FriendlyError]


# ---------------------------------------------------------------------------
# Dropdown options
# ---------------------------------------------------------------------------

def dropdown_options(config: Any) -> Dict[str, List[str]]:
    """
    Return allowed values for surgeon/site/procedure dropdowns.

    Sourced from ConfigTables.list_of_all_items_in_table.
    Injectable config allows offline tests to pass a stub.

    Returns
    -------
    dict with keys "surgeons", "sites", "procedures".
    """
    return {
        "surgeons":   config.list_of_all_items_in_table("Surgeons"),
        "sites":      config.list_of_all_items_in_table("ACQUISITION_SITES"),
        "procedures": config.list_of_all_items_in_table("Groups"),
    }


# ---------------------------------------------------------------------------
# Intake validation
# ---------------------------------------------------------------------------

# Required fields: key → human label
_REQUIRED_FIELDS: Dict[str, str] = {
    "filer_hawkid":       "Filer HawkID",
    "operation_date":     "Operation Date (YYYY-MM-DD)",
    "institution_name":   "Institution / Acquisition Site",
    "procedure_name":     "Procedure Name",
    "epic_start_time":    "Epic Start Time",
    "performing_surgeon": "Performing Surgeon HawkID",
    "image_dir":          "Image Folder",
}


def validate_intake(form_values: Dict[str, Any]) -> List[str]:
    """
    Pure validation of the intake form fields.

    Parameters
    ----------
    form_values : dict produced by the upload form.

    Returns
    -------
    list[str]  — human-readable problem descriptions; empty → valid.
    """
    import re

    problems: List[str] = []

    # Required-field presence check
    for key, label in _REQUIRED_FIELDS.items():
        val = form_values.get(key)
        if val is None or str(val).strip() == "":
            problems.append(f"{label} is required.")

    # Date format: YYYY-MM-DD
    op_date = str(form_values.get("operation_date", "")).strip()
    if op_date and not re.match(r"^\d{4}-\d{2}-\d{2}$", op_date):
        problems.append(
            f"Operation Date '{op_date}' must be in YYYY-MM-DD format."
        )

    # scan_quality — optional but constrained when present
    quality = str(form_values.get("scan_quality", "")).strip().lower()
    if quality and quality not in {"usable", "unusable", "questionable", "unknown", ""}:
        problems.append(
            f"Scan Quality '{quality}' must be one of: usable, unusable, questionable, unknown."
        )

    # image_dir: if provided, check it is a non-empty string (caller resolves path)
    image_dir = form_values.get("image_dir")
    if image_dir is not None and str(image_dir).strip() == "":
        problems.append("Image Folder path cannot be empty.")

    return problems


# ---------------------------------------------------------------------------
# Upload preview
# ---------------------------------------------------------------------------

def upload_preview(form_values: Dict[str, Any], image_count: int) -> Dict[str, Any]:
    """
    Build a 'what will be uploaded' summary for the confirmation screen.

    Parameters
    ----------
    form_values : dict from the upload form (no PHI echoed to output).
    image_count : number of images found in the selected folder.

    Returns
    -------
    dict with keys: subject_id_note, image_count, procedure, institution,
                    operation_date, phi_removed_note.
    """
    return {
        "subject_id_note": (
            "A de-identified subject UID will be generated at upload time. "
            "No patient name or MRN is transmitted."
        ),
        "image_count": image_count,
        "procedure":   form_values.get("procedure_name", "—"),
        "institution": form_values.get("institution_name", "—"),
        "operation_date": form_values.get("operation_date", "—"),
        "phi_removed_note": (
            "All DICOM metadata tags (patient name, DOB, MRN, accession number) "
            "are redacted before upload.  "
            "You must still confirm that no burned-in text is visible in the pixel data."
        ),
    }


# ---------------------------------------------------------------------------
# Core orchestration
# ---------------------------------------------------------------------------

def prepare_and_upload(
    form_values: Dict[str, Any],
    image_dir: Any,
    server_connection: Any,
    *,
    review_decision: ReviewDecision,
    redaction_boxes: Optional[List[Tuple[int, int, int, int]]] = None,
    publish_fn: Optional[Callable[..., None]] = None,
) -> UploadOutcome:
    """
    Orchestrate a single-case upload.

    FAIL-CLOSED contract
    --------------------
    If review_decision is not ReviewDecision.CONFIRMED or ReviewDecision.REDACT,
    the upload is blocked and ZERO put_zip calls are made.

    Parameters
    ----------
    form_values       : dict from validate_intake.
    image_dir         : Path (or str) to the local image folder.
    server_connection : Live server handle (pyxnat.Interface or FakeXNAT).
    review_decision   : CONFIRMED / REDACT / ABORT — determines gate outcome.
    redaction_boxes   : List of (x, y, w, h) tuples; only used when REDACT.
    publish_fn        : Injectable callable for the Phase-1 publish path.
                        Signature: publish_fn(server_connection, form_values,
                                              image_dir, pixel_review_confirmer)
                        Defaults to None (tests supply their own).
                        When None and review_decision is CONFIRMED/REDACT,
                        a FriendlyError is returned (no real upload in logic layer).

    Returns
    -------
    UploadOutcome(ok=True) on success.
    UploadOutcome(ok=False, friendly=<FriendlyError>) on any failure.
    """
    # ------------------------------------------------------------------
    # PHI gate: FAIL-CLOSED — block unless reviewer explicitly confirmed.
    # Any decision other than CONFIRMED or REDACT → abort, zero uploads.
    # ------------------------------------------------------------------
    if review_decision == ReviewDecision.ABORT:
        return UploadOutcome(
            ok=False,
            friendly=FriendlyError(
                title="Upload stopped — pixel PHI review not confirmed",
                message=(
                    "Upload stopped — you must confirm that the images contain "
                    "no visible patient name, date-of-birth, or MRN before "
                    "any data is sent to XNAT."
                ),
                recourse=[
                    "Inspect each image for burned-in patient identifiers.",
                    "Check the 'I confirm these images show no visible PHI' box, "
                    "then retry.",
                ],
            ),
        )

    # Unknown/unset decision values → fail-closed (treat as ABORT).
    if review_decision not in (ReviewDecision.CONFIRMED, ReviewDecision.REDACT):
        return UploadOutcome(
            ok=False,
            friendly=FriendlyError(
                title="Upload blocked — PHI review decision not set",
                message=(
                    "The PHI review decision was not set to CONFIRMED or REDACT. "
                    "No data was sent to XNAT."
                ),
                recourse=[
                    "Complete the PHI review step and set a valid decision.",
                    "Contact the Data Librarian if the problem persists.",
                ],
            ),
        )

    # ------------------------------------------------------------------
    # Validate form before any network call.
    # ------------------------------------------------------------------
    problems = validate_intake(form_values)
    if problems:
        return UploadOutcome(
            ok=False,
            friendly=FriendlyError(
                title="Upload blocked — form validation failed",
                message="One or more required fields are missing or invalid.",
                recourse=problems,
            ),
        )

    # ------------------------------------------------------------------
    # Build pixel_review_confirmer from the pre-decided review_decision.
    # This satisfies the Phase-1 publish gate without a second human prompt.
    # The confirmer returns the same decision that was set in the UI.
    # ------------------------------------------------------------------
    _boxes: List[Tuple[int, int, int, int]] = redaction_boxes or []

    def _confirmer(context: str) -> Tuple[ReviewDecision, List[Tuple[int, int, int, int]]]:
        # Derived from review_decision captured in closure.
        # REDACT path: apply_redaction is handled by the Phase-1 publish path
        # when it sees ReviewDecision.REDACT + boxes.
        return review_decision, _boxes

    # ------------------------------------------------------------------
    # Apply redaction to pixel data before publish when REDACT + boxes.
    # The deidentify service is called here so the logic layer owns it.
    # ------------------------------------------------------------------
    if review_decision == ReviewDecision.REDACT and _boxes:
        from src.services.deidentify import apply_redaction as _apply_redaction  # noqa: F401
        # Actual pixel-array application happens inside publish_fn or the
        # Phase-1 SourceRFSession/SourceESVSession.write() path.
        # We pass _boxes through the confirmer so the Phase-1 gate sees them.

    # ------------------------------------------------------------------
    # Delegate to publish_fn (Phase-1 path or test double).
    # ------------------------------------------------------------------
    if publish_fn is None:
        # No publish callable injected — this is a logic-layer-only call.
        # Return an error so callers know nothing was sent.
        return UploadOutcome(
            ok=False,
            friendly=FriendlyError(
                title="No publish function provided",
                message=(
                    "prepare_and_upload requires a publish_fn to execute "
                    "the actual upload.  None was provided."
                ),
                recourse=[
                    "Inject a publish_fn (e.g. a FakeXNAT-backed callable) "
                    "when calling prepare_and_upload.",
                ],
            ),
        )

    try:
        publish_fn(
            server_connection=server_connection,
            form_values=form_values,
            image_dir=image_dir,
            pixel_review_confirmer=_confirmer,
        )
        return UploadOutcome(ok=True, friendly=None)

    except UploadError as exc:
        # Phase-1 raised UploadError (PHI gate ABORT, connection drop, etc.)
        return UploadOutcome(ok=False, friendly=exc.friendly)

    except Exception as exc:  # noqa: BLE001
        # Unexpected failure — wrap as FriendlyError; no traceback escapes.
        from src.services.errors import handle as _handle
        fe = _handle(
            exc,
            title="Upload failed — unexpected error",
            message=(
                "An unexpected error occurred during the upload. "
                "No partial data was confirmed as sent."
            ),
            recourse=[
                "Check your VPN connection and retry.",
                "Contact the Data Librarian if the problem persists.",
                "A diagnostic log has been saved (see below).",
            ],
            context="prepare_and_upload",
        )
        return UploadOutcome(ok=False, friendly=fe)
