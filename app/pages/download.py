"""
app/pages/download — Download screen (Phase 2 US5).

Thin Streamlit page.  All logic lives in app/logic/download (no streamlit there).

Layout
------
  - Table of downloadable items (from list_downloadable)
  - Multiselect or checkbox selection of rows
  - Destination folder text input
  - Download button → download_selection with a progress indicator
  - Success: shows files written and destination
  - Failure: render_friendly_error panel

State contract (from app/state):
  - get_server()   → live server handle (pyxnat.Interface or FakeXNAT in tests)
  - get_username() → authenticated username (display only)
  - project_name   → from AppConfig (FR-015)
"""
from __future__ import annotations

import streamlit as st

from app.components.error_panel import render_friendly_error
from app import state
from app.logic.download import list_downloadable, download_selection
from src.services.config import AppConfig
from src.services.errors import FriendlyError


def render() -> None:
    """Render the Download Data page."""
    st.title("Download Data")

    # --- Auth guard ---
    if not state.is_authenticated():
        st.warning("Not logged in — please log in first.")
        return

    server = state.get_server()
    username = state.get_username() or ""

    # --- Load config for project name ---
    try:
        cfg = AppConfig.load()
        project_name = cfg.project_name
    except Exception as exc:
        from src.services.errors import handle as _handle
        fe = _handle(
            exc,
            title="Could not load app configuration",
            message=(
                "The application configuration could not be loaded. "
                "Check that your config file or environment variables are set correctly."
            ),
            recourse=[
                "Check your XNAT-Interact configuration file.",
                "Set the required environment variables (XNAT_PROJECT_NAME, XNAT_SERVER_URL).",
                "Contact the Data Librarian if the problem persists.",
            ],
            context="download.render, AppConfig.load",
        )
        render_friendly_error(fe)
        return

    # --- Header ---
    col_title, col_user = st.columns([3, 1])
    with col_title:
        st.subheader(f"Project: {project_name}")
    with col_user:
        st.caption(f"Logged in as: {username}")

    # --- Load downloadable items (cached per session to avoid re-fetching) ---
    _CACHE_KEY = "_download_rows_cache"
    if _CACHE_KEY not in st.session_state:
        with st.spinner("Loading downloadable items from XNAT server…"):
            result = list_downloadable(server, project_name)
        if isinstance(result, FriendlyError):
            render_friendly_error(result)
            return
        st.session_state[_CACHE_KEY] = result

    rows = st.session_state[_CACHE_KEY]

    # Refresh button clears the cache
    if st.button("Refresh", help="Re-fetch data from the XNAT server"):
        del st.session_state[_CACHE_KEY]
        st.rerun()

    if not rows:
        st.info(f"No downloadable items found in project '{project_name}'.")
        return

    # --- Selectable table ---
    import pandas as pd

    df = pd.DataFrame(rows, columns=["subject", "experiment", "date", "scan_type", "num_files"])
    df.columns = ["Subject", "Experiment", "Date", "Scan Type", "# Files"]

    st.write("**Select rows to download:**")

    # Build a list of display labels for multiselect
    row_labels = [
        f"{r['subject']} / {r['experiment']} ({r.get('scan_type', '') or 'SRC'})"
        for r in rows
    ]

    selected_labels = st.multiselect(
        "Cases to download",
        options=row_labels,
        help="Select one or more cases. Use the search box inside the selector to filter.",
    )

    # Show table for context (non-interactive display)
    st.dataframe(df, use_container_width=True, hide_index=True)

    # Map selected labels back to rows
    label_to_row = {label: row for label, row in zip(row_labels, rows)}
    selected_rows = [label_to_row[lbl] for lbl in selected_labels if lbl in label_to_row]

    # --- Destination folder ---
    dest_dir = st.text_input(
        "Destination folder",
        value="",
        placeholder="/path/to/download/folder",
        help=(
            "Enter the full path to the folder where files will be saved. "
            "The folder will be created if it does not exist."
        ),
    )

    # --- Download button ---
    if st.button("Download", type="primary", disabled=(not selected_rows or not dest_dir.strip())):
        if not selected_rows:
            st.warning("Select at least one case before downloading.")
            return
        if not dest_dir.strip():
            st.warning("Enter a destination folder path before downloading.")
            return

        progress_bar = st.progress(0, text="Starting download…")
        status_placeholder = st.empty()

        # Show incremental progress — update bar per row
        total = len(selected_rows)
        all_files = []
        failed = False
        friendly_error = None

        for idx, row in enumerate(selected_rows, start=1):
            subj = row.get("subject", "?")
            progress_bar.progress(
                int((idx - 1) / total * 100),
                text=f"Downloading {subj} ({idx}/{total})…",
            )
            outcome = download_selection(server, project_name, [row], dest_dir.strip())
            if not outcome.ok:
                failed = True
                friendly_error = outcome.friendly
                break
            all_files.extend(outcome.files_written)

        if not failed:
            progress_bar.progress(100, text="Download complete.")
            status_placeholder.success(
                f"Downloaded {len(all_files)} file(s) to `{dest_dir.strip()}`."
            )
            if all_files:
                with st.expander("Files written", expanded=False):
                    for fp in all_files:
                        st.code(str(fp))
        else:
            progress_bar.empty()
            status_placeholder.empty()
            if friendly_error is not None:
                render_friendly_error(friendly_error)
            else:
                st.error("Download failed for an unknown reason. Check your VPN connection.")
