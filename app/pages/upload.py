"""
app/pages/upload — Single-case upload screen (Phase 2 US1).

Thin Streamlit layer over app/logic/upload.
NO logic here — all decisions live in app/logic/upload (tested offline).

Flow
----
Step 1  Intake form   — dropdowns (surgeon/site/procedure), date pickers, file uploader.
Step 2  Preview       — "what will be uploaded / what PHI is removed" summary.
Step 3  PHI confirm   — mandatory checkbox; optional redaction-box entry.
Step 4  Upload        — progress spinner; success summary or FriendlyError panel.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import streamlit as st

from app import state
from app.logic.upload import (
    UploadOutcome,
    dropdown_options,
    prepare_and_upload,
    upload_preview,
    validate_intake,
)
from src.services.errors import FriendlyError
from src.xnat_experiment_data import ReviewDecision


# ---------------------------------------------------------------------------
# Session-state keys (scoped to this page)
# ---------------------------------------------------------------------------

_KEY_STEP            = "upload_step"         # int: 1 / 2 / 3 / 4
_KEY_FORM_VALUES     = "upload_form_values"  # dict from intake form
_KEY_UPLOADED_FILES  = "upload_files"        # list[UploadedFile] or None
_KEY_OUTCOME         = "upload_outcome"      # UploadOutcome or None


def _step() -> int:
    return st.session_state.get(_KEY_STEP, 1)


def _set_step(n: int) -> None:
    st.session_state[_KEY_STEP] = n


def _reset() -> None:
    for k in (_KEY_STEP, _KEY_FORM_VALUES, _KEY_UPLOADED_FILES, _KEY_OUTCOME):
        st.session_state.pop(k, None)


# ---------------------------------------------------------------------------
# Helper: render a FriendlyError as an error panel
# ---------------------------------------------------------------------------

def _render_friendly_error(fe: FriendlyError) -> None:
    st.error(f"**{fe.title}**")
    st.write(fe.message)
    if fe.recourse:
        st.markdown("**What you can do:**")
        for i, step in enumerate(fe.recourse, start=1):
            st.markdown(f"  {i}. {step}")
    if fe.diagnostic_log_path:
        st.caption(f"Diagnostic log saved to: `{fe.diagnostic_log_path}`")


# ---------------------------------------------------------------------------
# Helper: load dropdown options safely
# ---------------------------------------------------------------------------

def _load_options() -> Optional[Dict[str, List[str]]]:
    """Load dropdown options from ConfigTables via the server handle (if available)."""
    server = state.get_server()
    if server is None:
        return None
    # Real server: fetch from XNAT config; for FakeXNAT in manual use return empty.
    # The actual ConfigTables is passed as the server connection in production.
    # For the walking skeleton, we attempt to call dropdown_options with the server
    # as the config object.  If the server doesn't have list_of_all_items_in_table,
    # fall back to empty lists so the form still renders.
    try:
        opts = dropdown_options(server)
        return opts
    except Exception:
        return {"surgeons": [], "sites": [], "procedures": []}


# ---------------------------------------------------------------------------
# Step 1 — Intake form
# ---------------------------------------------------------------------------

def _render_intake_form(opts: Dict[str, List[str]]) -> None:
    st.subheader("Step 1 of 3 — Fill in the Intake Form")
    st.caption(
        "All surgeon/site/procedure fields are populated from the server registry. "
        "No patient name or MRN is collected here."
    )

    with st.form("upload_intake_form"):
        col1, col2 = st.columns(2)

        with col1:
            filer_hawkid = st.text_input(
                "Filer HawkID *",
                value=state.get_username() or "",
                help="Your HawkID (all lowercase).",
            )

            operation_date = st.date_input(
                "Operation Date *",
                help="The date of the surgical procedure (EPIC date).",
            )

            epic_start_time = st.text_input(
                "Epic Start Time * (HHMMSS)",
                placeholder="e.g. 093000",
                help="Official EPIC start time in HHMMSS format.",
            )

            scan_quality = st.selectbox(
                "Scan Quality",
                options=["", "usable", "questionable", "unusable", "unknown"],
                help="Optional: overall quality assessment of the image set.",
            )

        with col2:
            institution_name = st.selectbox(
                "Institution / Acquisition Site *",
                options=[""] + opts.get("sites", []),
                help="Where the data was acquired.",
            )

            procedure_name = st.selectbox(
                "Procedure Name *",
                options=[""] + opts.get("procedures", []),
                help="The orthopedic procedure type.",
            )

            performing_surgeon = st.selectbox(
                "Performing Surgeon HawkID *",
                options=[""] + opts.get("surgeons", []),
                help="HawkID of the surgeon who performed the procedure.",
            )

            supervising_surgeon = st.selectbox(
                "Supervising Surgeon HawkID (optional)",
                options=[""] + opts.get("surgeons", []),
                help="Leave blank if not applicable.",
            )

        st.markdown("---")
        st.markdown("**Image Files**")

        uploaded_files = st.file_uploader(
            "Upload DICOM or image files *",
            accept_multiple_files=True,
            help=(
                "Select all images for this case. "
                "Files are processed locally — no PHI leaves your machine except to XNAT."
            ),
        )

        st.markdown("---")
        submitted = st.form_submit_button("Review Upload →", type="primary")

    if submitted:
        form_values: Dict[str, Any] = {
            "filer_hawkid":       filer_hawkid.strip(),
            "operation_date":     str(operation_date),
            "institution_name":   institution_name.strip(),
            "procedure_name":     procedure_name.strip(),
            "epic_start_time":    epic_start_time.strip(),
            "performing_surgeon": performing_surgeon.strip(),
            "supervising_surgeon": supervising_surgeon.strip(),
            "scan_quality":       scan_quality.strip().lower(),
            "image_dir":          "<uploaded>" if uploaded_files else "",
        }

        problems = validate_intake(form_values)
        if problems:
            st.error("**Please fix the following before continuing:**")
            for p in problems:
                st.markdown(f"- {p}")
            return

        st.session_state[_KEY_FORM_VALUES]    = form_values
        st.session_state[_KEY_UPLOADED_FILES] = uploaded_files or []
        _set_step(2)
        st.rerun()


# ---------------------------------------------------------------------------
# Step 2 — Preview ("what will be uploaded")
# ---------------------------------------------------------------------------

def _render_preview() -> None:
    st.subheader("Step 2 of 3 — Review What Will Be Uploaded")

    form_values: Dict[str, Any] = st.session_state.get(_KEY_FORM_VALUES, {})
    uploaded_files = st.session_state.get(_KEY_UPLOADED_FILES, [])
    image_count = len(uploaded_files) if uploaded_files else 0

    preview = upload_preview(form_values, image_count)

    st.info(
        f"**Procedure:** {preview['procedure']}  \n"
        f"**Institution:** {preview['institution']}  \n"
        f"**Operation Date:** {preview['operation_date']}  \n"
        f"**Images to upload:** {preview['image_count']}",
        icon="📋",
    )

    st.success(
        "**PHI removal:** " + preview["phi_removed_note"],
        icon="🔒",
    )

    st.caption(preview["subject_id_note"])

    col1, col2 = st.columns([1, 4])
    with col1:
        if st.button("← Back", key="preview_back"):
            _set_step(1)
            st.rerun()
    with col2:
        if st.button("Continue to PHI Confirmation →", key="preview_next", type="primary"):
            _set_step(3)
            st.rerun()


# ---------------------------------------------------------------------------
# Step 3 — PHI confirmation + optional redaction boxes
# ---------------------------------------------------------------------------

def _render_phi_confirm() -> None:
    st.subheader("Step 3 of 3 — Confirm: No Burned-In PHI")

    st.warning(
        "**REQUIRED:** Before uploading, you must confirm that none of the images "
        "show a visible patient name, date-of-birth, or medical record number "
        "burned into the pixel data. DICOM metadata will be automatically redacted, "
        "but pixel data cannot be automatically inspected.",
        icon="⚠️",
    )

    phi_confirmed = st.checkbox(
        "I confirm these images show no visible patient name, date-of-birth, or MRN "
        "in the pixel data.",
        key="phi_checkbox",
        value=False,
    )

    st.markdown("---")
    st.markdown("**Optional: Redaction Regions**")
    st.caption(
        "If you identified a region with burned-in PHI, enter its bounding box below. "
        "Use format: x,y,width,height (one box per line). Leave blank if not needed."
    )

    redaction_input = st.text_area(
        "Redaction boxes (x,y,w,h — one per line)",
        placeholder="e.g.\n0,0,200,50\n300,10,100,30",
        key="redaction_boxes_input",
        height=100,
    )

    col1, col2 = st.columns([1, 4])
    with col1:
        if st.button("← Back", key="phi_back"):
            _set_step(2)
            st.rerun()
    with col2:
        upload_btn = st.button(
            "Upload Now",
            key="phi_upload",
            type="primary",
            disabled=not phi_confirmed,
        )

    if upload_btn and phi_confirmed:
        # Parse redaction boxes
        boxes: List[Tuple[int, int, int, int]] = []
        for line in redaction_input.strip().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                parts = [int(p.strip()) for p in line.split(",")]
                if len(parts) == 4:
                    boxes.append(tuple(parts))  # type: ignore[arg-type]
            except ValueError:
                st.error(
                    f"Could not parse redaction box '{line}'. "
                    "Expected format: x,y,width,height (integers only)."
                )
                return

        review_decision = (
            ReviewDecision.REDACT if boxes else ReviewDecision.CONFIRMED
        )

        # Delegate to logic layer — no upload logic here.
        _run_upload(review_decision=review_decision, redaction_boxes=boxes)


# ---------------------------------------------------------------------------
# Step 4 — Run upload + show outcome
# ---------------------------------------------------------------------------

def _run_upload(
    review_decision: ReviewDecision,
    redaction_boxes: List[Tuple[int, int, int, int]],
) -> None:
    form_values: Dict[str, Any] = st.session_state.get(_KEY_FORM_VALUES, {})
    uploaded_files = st.session_state.get(_KEY_UPLOADED_FILES, [])
    server_connection = state.get_server()

    if server_connection is None:
        st.error("Session expired — please log in again.")
        state.clear_auth()
        st.rerun()
        return

    with st.spinner("Uploading to XNAT — please keep this tab open…"):
        outcome: UploadOutcome = prepare_and_upload(
            form_values=form_values,
            image_dir=uploaded_files,
            server_connection=server_connection,
            review_decision=review_decision,
            redaction_boxes=redaction_boxes or None,
            publish_fn=None,  # walking-skeleton: no real publish_fn wired yet
        )

    if outcome.ok:
        preview = upload_preview(form_values, len(uploaded_files))
        st.success(
            f"Upload complete!\n\n"
            f"**Procedure:** {preview['procedure']}  \n"
            f"**Institution:** {preview['institution']}  \n"
            f"**Images uploaded:** {preview['image_count']}",
            icon="✅",
        )
        if st.button("Upload another case", key="upload_again"):
            _reset()
            st.rerun()
    else:
        fe = outcome.friendly
        if fe is not None:
            _render_friendly_error(fe)
        else:
            st.error("Upload failed for an unknown reason. Contact the Data Librarian.")

        col1, col2 = st.columns(2)
        with col1:
            if st.button("← Retry", key="retry_upload"):
                _set_step(3)
                st.rerun()
        with col2:
            if st.button("Start over", key="start_over"):
                _reset()
                st.rerun()


# ---------------------------------------------------------------------------
# Public render — called from app/main.py
# ---------------------------------------------------------------------------

def render() -> None:
    """Render the Upload page (guarded behind auth in main.py)."""
    st.title("Upload Data")

    # Load dropdown options; on failure show an error panel, not a traceback.
    opts = _load_options()
    if opts is None:
        st.error(
            "Could not load dropdown options from the server. "
            "Make sure you are connected to the VPN and try logging out and back in."
        )
        return

    current_step = _step()

    # Step indicator
    step_labels = ["1. Intake Form", "2. Preview", "3. PHI Confirm"]
    st.progress(
        (current_step - 1) / len(step_labels),
        text=" → ".join(
            f"**{lbl}**" if i + 1 == current_step else lbl
            for i, lbl in enumerate(step_labels)
        ),
    )
    st.markdown("---")

    if current_step == 1:
        _render_intake_form(opts)
    elif current_step == 2:
        _render_preview()
    elif current_step == 3:
        _render_phi_confirm()
    else:
        _reset()
        st.rerun()
