"""
app/guided/wizard_upload — Upload wizard (6-step guided flow).

STEPS = ["Identify", "Details", "Images", "Privacy check", "Review", "Confirm"]

Core logic: step_blockers(step_index, form_values, privacy_affirmed) -> list[str]
Returns plain-language reasons why a step cannot advance.
"""
from __future__ import annotations

import os
import tempfile
from typing import Any, Dict, List, Optional, Tuple

import streamlit as st

from app.guided import components, wizard_state
from app.guided.publish_impl import make_publish_fn
from app.logic.upload import (
    UploadOutcome,
    dropdown_options,
    prepare_and_upload,
    upload_preview,
    validate_intake,
)
from src.xnat_experiment_data import ReviewDecision


# ---------------------------------------------------------------------------
# Step definitions and field mapping
# ---------------------------------------------------------------------------

STEPS = ["Identify", "Details", "Images", "Privacy check", "Review", "Confirm"]

# Map step index to required form keys
_STEP_REQUIRED_FIELDS = {
    0: ["filer_hawkid", "operation_date"],
    1: ["institution_name", "procedure_name", "performing_surgeon", "epic_start_time"],
    2: ["image_dir"],
    3: [],  # Privacy step has its own gate
    4: [],  # Review step is read-only
    5: [],  # Confirm step is final
}


# ---------------------------------------------------------------------------
# Pure validation logic (testable without streamlit)
# ---------------------------------------------------------------------------

def step_blockers(
    step_index: int,
    form_values: Dict[str, Any],
    privacy_affirmed: bool,
    image_count: int = 0,
) -> List[str]:
    """
    Return plain-language blockers preventing advance from this step.

    Parameters
    ----------
    step_index   : int, the current step (0-indexed).
    form_values  : dict, accumulated form values.
    privacy_affirmed : bool, whether privacy step's checkbox is set.
    image_count  : int, number of images (for step 2).

    Returns
    -------
    list[str] — blockers; empty = can advance.
    """
    blockers = []

    # Step 0: Identify
    if step_index == 0:
        if not form_values.get("filer_hawkid", "").strip():
            blockers.append("Filer HawkID is required.")
        if not form_values.get("operation_date", "").strip():
            blockers.append("Operation Date is required.")

    # Step 1: Details
    elif step_index == 1:
        if not form_values.get("institution_name", "").strip():
            blockers.append("Institution / Acquisition Site is required.")
        if not form_values.get("procedure_name", "").strip():
            blockers.append("Procedure Name is required.")
        if not form_values.get("performing_surgeon", "").strip():
            blockers.append("Performing Surgeon is required.")
        if not form_values.get("epic_start_time", "").strip():
            blockers.append("Epic Start Time is required.")

    # Step 2: Images
    elif step_index == 2:
        has_dir = bool(form_values.get("image_dir", "").strip())
        has_uploads = int(form_values.get("image_count", 0)) > 0
        if not has_dir and not has_uploads:
            blockers.append("Image files are required: upload files or provide a folder path.")

    # Step 3: Privacy check
    elif step_index == 3:
        if not privacy_affirmed:
            blockers.append("You must confirm that the images contain no visible PHI.")

    # Step 4: Review (no blockers, read-only)
    elif step_index == 4:
        pass

    # Step 5: Confirm (final, no blockers)
    elif step_index == 5:
        pass

    return blockers


# ---------------------------------------------------------------------------
# UI renderers for each step
# ---------------------------------------------------------------------------

def _render_identify_step(config: Any) -> None:
    """Step 0: Identify — filer_hawkid, operation_date."""
    st.subheader("Who is filing this data?")

    form = wizard_state.get_form()

    filer_hawkid = st.text_input(
        "Your HawkID *",
        value=form.get("filer_hawkid", ""),
        help="All lowercase (e.g., jsmith).",
        key="input_filer_hawkid",
    )

    operation_date = st.date_input(
        "Operation Date (YYYY-MM-DD) *",
        help="The date the surgery took place.",
        key="input_operation_date",
    )

    # Update form
    wizard_state.update_form({
        "filer_hawkid": filer_hawkid.strip(),
        "operation_date": str(operation_date) if operation_date else "",
    })

    # Technical detail
    with st.expander("🔧 Show technical detail", expanded=False):
        st.json({
            "filer_hawkid": filer_hawkid.strip(),
            "operation_date": str(operation_date) if operation_date else "",
        })


def _render_details_step(config: Any) -> None:
    """Step 1: Details — institution, procedure, surgeon, epic_start_time."""
    st.subheader("Tell us about the surgery")

    form = wizard_state.get_form()

    # Load dropdown options
    try:
        opts = dropdown_options(config)
    except Exception:
        opts = {"surgeons": [], "sites": [], "procedures": []}

    col1, col2 = st.columns(2)

    with col1:
        institution_name = st.selectbox(
            "Institution / Acquisition Site *",
            options=[""] + opts.get("sites", []),
            index=0,
            help="Where the data was acquired.",
            key="select_institution",
        )

        performing_surgeon = st.selectbox(
            "Performing Surgeon *",
            options=[""] + opts.get("surgeons", []),
            index=0,
            help="Surgeon who performed the procedure (HawkID).",
            key="select_surgeon",
        )

    with col2:
        procedure_name = st.selectbox(
            "Procedure Name *",
            options=[""] + opts.get("procedures", []),
            index=0,
            help="Type of orthopedic procedure.",
            key="select_procedure",
        )

        epic_start_time = st.text_input(
            "Epic Start Time (HH:MM:SS) *",
            value=form.get("epic_start_time", ""),
            placeholder="e.g., 09:30:00",
            help="Official EPIC start time.",
            key="input_epic_start_time",
        )

    supervising_surgeon = st.selectbox(
        "Supervising Surgeon (optional)",
        options=[""] + opts.get("surgeons", []),
        index=0,
        help="Leave blank if not applicable.",
        key="select_supervising_surgeon",
    )

    scan_quality = st.selectbox(
        "Scan Quality (optional)",
        options=["", "usable", "questionable", "unusable", "unknown"],
        index=0,
        help="Overall quality of the image set.",
        key="select_scan_quality",
    )

    # Update form
    wizard_state.update_form({
        "institution_name": institution_name.strip(),
        "procedure_name": procedure_name.strip(),
        "performing_surgeon": performing_surgeon.strip(),
        "supervising_surgeon": supervising_surgeon.strip(),
        "epic_start_time": epic_start_time.strip(),
        "scan_quality": scan_quality.strip().lower(),
    })

    # Technical detail
    with st.expander("🔧 Show technical detail", expanded=False):
        st.json(wizard_state.get_form())


def _render_images_step(config: Any) -> None:
    """Step 2: Images — file uploader (primary) + folder path fallback."""
    st.subheader("Where are your images?")

    form = wizard_state.get_form()

    # Primary: browser file uploader
    uploaded_files = st.file_uploader(
        "Select your case's image files",
        accept_multiple_files=True,
        type=["dcm", "dicom", "png", "jpg", "jpeg"],
        key="file_uploader_images",
        help="Select all DICOM/image files for this case.",
    )

    image_dir = form.get("image_dir", "")
    image_count = 0

    if uploaded_files:
        # Write uploaded files to a temp directory and persist the path
        existing_tmpdir = form.get("_upload_tmpdir", "")
        if not existing_tmpdir or not os.path.isdir(existing_tmpdir):
            existing_tmpdir = tempfile.mkdtemp(prefix="guided_upload_")
        # Write each file
        for uf in uploaded_files:
            dest = os.path.join(existing_tmpdir, uf.name)
            with open(dest, "wb") as fh:
                fh.write(uf.read())
        image_dir = existing_tmpdir
        image_count = len(uploaded_files)
        wizard_state.update_form({
            "image_dir": image_dir,
            "image_count": image_count,
            "_upload_tmpdir": existing_tmpdir,
        })
        st.success(f"✅ {image_count} file(s) ready for upload.")
    elif image_dir and image_dir == form.get("_upload_tmpdir", ""):
        # Persist previously uploaded files across re-runs
        existing_files = [f for f in os.listdir(image_dir) if os.path.isfile(os.path.join(image_dir, f))] if os.path.isdir(image_dir) else []
        image_count = len(existing_files)
        wizard_state.update_form({"image_count": image_count})
        if image_count > 0:
            st.success(f"✅ {image_count} file(s) ready for upload (from previous selection).")

    # Fallback: folder path on this computer
    with st.expander("Advanced: use a folder path on this computer", expanded=False):
        folder_path = st.text_input(
            "Image Folder Path",
            value=form.get("_manual_folder_path", ""),
            placeholder="e.g., /path/to/images",
            help="Local path to the folder containing DICOM/image files.",
            key="input_image_dir_manual",
        )
        if folder_path.strip():
            if os.path.isdir(folder_path.strip()):
                files_in_dir = [f for f in os.listdir(folder_path.strip()) if os.path.isfile(os.path.join(folder_path.strip(), f))]
                cnt = len(files_in_dir)
                wizard_state.update_form({
                    "image_dir": folder_path.strip(),
                    "image_count": cnt,
                    "_manual_folder_path": folder_path.strip(),
                })
                image_dir = folder_path.strip()
                image_count = cnt
                st.caption(f"Found {cnt} file(s) in folder.")
            else:
                st.warning("Path does not exist or is not a folder.")

    st.caption(
        "Files will be processed locally. No patient identifiers will leave your machine."
    )

    # Technical detail
    with st.expander("🔧 Show technical detail", expanded=False):
        st.json({
            "image_dir": image_dir,
            "image_count": image_count,
        })


def _render_privacy_check_step(config: Any) -> None:
    """Step 3: Privacy check — affirmation checkbox."""
    st.subheader("Confirm: No Visible Patient Information")

    st.warning(
        "**REQUIRED:** Before uploading, you must confirm that NONE of the images "
        "show a visible patient name, date of birth, or medical record number "
        "burned into the pixel data.  \n\n"
        "DICOM metadata tags (name, DOB, MRN) will be automatically redacted, "
        "but you must still verify the pixel data.",
        icon="⚠️",
    )

    st.markdown("**Verification checklist:**")
    st.markdown("- [ ] I have reviewed all images for visible PHI")
    st.markdown("- [ ] No patient name or identifier appears in any image")
    st.markdown("- [ ] No date of birth or MRN is visible")

    privacy_affirmed = st.checkbox(
        "I confirm these images contain no visible patient information.",
        value=wizard_state.is_privacy_affirmed(),
        key="checkbox_privacy",
    )

    wizard_state.set_privacy_affirmed(privacy_affirmed)


def _render_review_step(config: Any) -> None:
    """Step 4: Review — show exactly what will be uploaded."""
    st.subheader("Review What Will Be Uploaded")

    form = wizard_state.get_form()
    image_count = form.get("image_count", 0)

    preview = upload_preview(form, image_count)

    st.info(
        f"**Procedure:** {preview['procedure']}  \n"
        f"**Institution:** {preview['institution']}  \n"
        f"**Operation Date:** {preview['operation_date']}  \n"
        f"**Images:** {preview['image_count']}",
        icon="📋",
    )

    st.success(
        "**PHI Handling:** " + preview["phi_removed_note"],
        icon="🔒",
    )

    st.caption(preview["subject_id_note"])

    with st.expander("🔧 Show technical detail", expanded=False):
        st.json(form)


def _render_confirm_step(config: Any) -> None:
    """Step 5: Confirm — final upload button."""
    st.subheader("Ready to Upload")

    st.info(
        "Click the button below to upload this case to XNAT.  \n"
        "Once confirmed, the data will be published and cannot be modified.",
        icon="✅",
    )


# ---------------------------------------------------------------------------
# Main wizard renderer
# ---------------------------------------------------------------------------

def render_upload_wizard(config: Any, server: Any) -> None:
    """
    Render the upload wizard (6 steps).

    Parameters
    ----------
    config : ConfigTables-like object (for dropdown_options).
    server : XNAT server handle (for publish_fn).
    """
    current_step = wizard_state.get_step()

    # Render step rail
    components.render_step_rail(STEPS, current_step)

    # Render step
    if current_step == 0:
        _render_identify_step(config)
    elif current_step == 1:
        _render_details_step(config)
    elif current_step == 2:
        _render_images_step(config)
    elif current_step == 3:
        _render_privacy_check_step(config)
    elif current_step == 4:
        _render_review_step(config)
    elif current_step == 5:
        _render_confirm_step(config)
    else:
        st.error(f"Unknown step {current_step}.")
        return

    st.markdown("---")

    # Navigation
    col1, col2 = st.columns([1, 4])

    form = wizard_state.get_form()
    privacy_affirmed = wizard_state.is_privacy_affirmed()
    image_count = form.get("image_count", 0)

    blockers = step_blockers(current_step, form, privacy_affirmed, image_count)

    with col1:
        if current_step > 0:
            if st.button("← Back", key="btn_back", use_container_width=True):
                wizard_state.set_step(current_step - 1)
                st.rerun()
        else:
            if st.button("Cancel", key="btn_cancel", use_container_width=True):
                wizard_state.reset_wizard()
                st.rerun()

    with col2:
        if current_step < len(STEPS) - 1:
            # "Next" button
            next_disabled = bool(blockers)
            if st.button(
                "Next →",
                key="btn_next",
                type="primary",
                disabled=next_disabled,
                use_container_width=True,
            ):
                wizard_state.set_step(current_step + 1)
                st.rerun()

            # Show blockers if present
            if blockers:
                st.warning("**Cannot advance to next step:**")
                for blocker in blockers:
                    st.markdown(f"- {blocker}")
        else:
            # Step 5: Final "Upload" button
            if st.button(
                "✅ Upload Now",
                key="btn_upload_final",
                type="primary",
                use_container_width=True,
            ):
                _execute_upload(server)


def _execute_upload(server: Any) -> None:
    """Execute the upload and show outcome."""
    form = wizard_state.get_form()
    image_dir = form.get("image_dir", "")

    # Validate form one last time
    problems = validate_intake(form)
    if problems:
        st.error("**Form validation failed:**")
        for problem in problems:
            st.markdown(f"- {problem}")
        return

    # Get review decision (privacy affirmed earlier, so this is CONFIRMED)
    review_decision = ReviewDecision.CONFIRMED

    with st.spinner("Uploading to XNAT — please keep this tab open…"):
        outcome: UploadOutcome = prepare_and_upload(
            form_values=form,
            image_dir=image_dir,
            server_connection=server,
            review_decision=review_decision,
            redaction_boxes=None,
            publish_fn=make_publish_fn(server),
        )

    if outcome.ok:
        st.success(
            f"✅ **Surgery uploaded successfully!**  \n"
            f"Procedure: {form.get('procedure_name')}  \n"
            f"Institution: {form.get('institution_name')}  \n\n"
            f"To view it, go to **Find & view past cases**.",
            icon=None,
        )
        wizard_state.set_uploaded(True)

        if st.button("Done", key="btn_done_upload"):
            wizard_state.reset_wizard()
            st.rerun()
    else:
        if outcome.friendly:
            components.render_friendly_error(outcome.friendly)
        else:
            st.error("Upload failed. Contact the Data Librarian.")

        if st.button("← Retry", key="btn_retry_upload"):
            wizard_state.set_step(4)  # Back to review
            st.rerun()
