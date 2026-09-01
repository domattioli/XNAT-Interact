"""
app/guided/browse_view — Browse interface for viewing surgeries in archive.

Provides render_browse() and helper _browse_display_frame() for display-column
transformation and rendering.
"""
from __future__ import annotations

import io
import tempfile
import zipfile as _zipfile
from pathlib import Path

import pandas as pd
import streamlit as st

from app.guided import components, wizard_state
from app.logic.browse import fetch_data_table
from app.logic.download import download_selection
from src.services.errors import FriendlyError


def _browse_display_frame(rows: list[dict]) -> pd.DataFrame:
    """
    Convert rows to DataFrame with only display columns (no scan_id).

    Display columns: Subject, Experiment, Date, Type, # Files
    (internal order: subject, experiment, date, scan_type, num_files)

    Parameters
    ----------
    rows : list[dict]
        Rows from fetch_data_table() with keys: subject, experiment, date,
        scan_type, num_files, scan_id.

    Returns
    -------
    pd.DataFrame
        DataFrame with 5 display columns, renamed per DISPLAY_MAP.
    """
    df = pd.DataFrame(rows)
    # Select & rename to display order
    display_cols = ["subject", "experiment", "date", "scan_type", "num_files"]
    df = df[display_cols]
    df = df.rename(columns={
        "subject": "Subject",
        "experiment": "Experiment",
        "date": "Date",
        "scan_type": "Type",
        "num_files": "# Files"
    })
    return df


def render_browse(server, project_name: str) -> None:
    """
    Render browse interface: subheader, fetch, display, download, home button.

    Fetches the data table for the project, handles errors gracefully,
    and displays results in a filtered/sortable DataFrame view.
    Includes a per-row download button that streams files as a zip to the browser.

    Parameters
    ----------
    server : pyxnat.Interface or FakeXNAT
        XNAT server connection (real or test double).
    project_name : str
        XNAT project name to browse.
    """
    st.subheader("🔎 Surgeries in the archive")

    with st.spinner("Loading surgeries…"):
        result = fetch_data_table(server, project_name)

    # Handle error
    if isinstance(result, FriendlyError):
        components.render_friendly_error(result)
        if st.button("← Home"):
            wizard_state.set_task(None)
            st.rerun()
        return

    rows = result

    # Handle empty result
    if not rows:
        st.info("No surgeries found yet.")
        if st.button("← Home"):
            wizard_state.set_task(None)
            st.rerun()
        return

    # Display
    df = _browse_display_frame(rows)
    n = len(rows)
    st.caption(f"{n} surgery" if n == 1 else f"{n} surgeries")
    st.dataframe(df, use_container_width=True, hide_index=True)

    # --- Per-case download ---
    st.markdown("---")
    st.markdown("**Download a case**")

    # Build subject options for selectbox
    subjects = [r["subject"] for r in rows]
    unique_subjects = list(dict.fromkeys(subjects))  # preserve order, deduplicate

    selected_subject = st.selectbox(
        "Select a subject to download",
        options=[""] + unique_subjects,
        key="browse_download_subject",
    )

    if selected_subject:
        # Filter rows for this subject
        subject_rows = [r for r in rows if r["subject"] == selected_subject]

        if st.button(f"📥 Download {selected_subject}", key="btn_download_case", type="primary"):
            with st.spinner(f"Downloading {selected_subject}…"):
                # Zip must be built INSIDE the tempdir context — the files are
                # deleted the moment it exits (live-found bug: empty/crashed zip).
                with tempfile.TemporaryDirectory() as tmpdir:
                    outcome = download_selection(server, project_name, subject_rows, tmpdir)

                    if outcome.ok and outcome.files_written:
                        zip_buffer = io.BytesIO()
                        with _zipfile.ZipFile(zip_buffer, "w", _zipfile.ZIP_DEFLATED) as zf:
                            base = Path(tmpdir)
                            for fp in outcome.files_written:
                                try:
                                    arcname = fp.relative_to(base)
                                except ValueError:
                                    arcname = fp.name
                                zf.write(fp, arcname)
                        # Persist across Streamlit reruns so the save button
                        # survives the click-rerun cycle.
                        st.session_state["browse_zip_bytes"] = zip_buffer.getvalue()
                        st.session_state["browse_zip_subject"] = selected_subject
                        st.session_state["browse_zip_count"] = len(outcome.files_written)
                    elif outcome.ok and not outcome.files_written:
                        st.warning("No files found for this subject.")
                    else:
                        if outcome.friendly:
                            components.render_friendly_error(outcome.friendly)
                        else:
                            st.error("Download failed. Contact the Data Librarian.")

        if st.session_state.get("browse_zip_bytes") and st.session_state.get("browse_zip_subject") == selected_subject:
            st.success(f"✅ Ready: {st.session_state['browse_zip_count']} file(s).")
            st.download_button(
                label=f"💾 Save {selected_subject}.zip",
                data=st.session_state["browse_zip_bytes"],
                file_name=f"{selected_subject}.zip",
                mime="application/zip",
                key="btn_save_zip",
            )

    # Annotations panel for selected case
    if selected_subject:
        subject_rows_for_ann = [r for r in rows if r["subject"] == selected_subject]
        if subject_rows_for_ann:
            _render_annotations_panel(server, project_name, subject_rows_for_ann[0])

    st.markdown("---")

    # Home button
    if st.button("← Home"):
        wizard_state.set_task(None)
        st.rerun()


def _render_annotations_panel(server, project_name: str, row: dict) -> None:
    """
    Minimal annotations panel for the selected case in guided browse.

    Shows annotation versions if any exist for the case's scan resource.
    Seeded demo annotations are stored under the ANNOTATIONS resource.
    """
    subject = row.get("subject", "")
    experiment = row.get("experiment", "")
    scan_id = str(row.get("scan_id", "0"))

    image_ref = (
        f"/projects/{project_name}/subjects/{subject}"
        f"/experiments/{experiment}/scans/{scan_id}"
    )

    with st.expander("🏷️ View Annotations", expanded=False):
        try:
            from app.logic.annotations import list_image_annotations
            ann_set = list_image_annotations(server, image_ref)

            if isinstance(ann_set, FriendlyError):
                st.info("No annotations found for this case.")
                return

            if not ann_set or not ann_set.annotations:
                st.info("No annotations found for this case.")
                return

            st.markdown(f"**{len(ann_set.annotations)} annotation(s) found**")
            for i, ann in enumerate(ann_set.annotations):
                ann_type = getattr(ann, "annotation_type", "unknown")
                ann_ver = getattr(ann, "version", "?")
                ann_id = getattr(ann, "annotator_id", "unknown")
                st.markdown(f"- Version {ann_ver} — type: `{ann_type}` — annotator: `{ann_id}`")

        except Exception as e:
            st.info(f"Annotations not available: {e}")
