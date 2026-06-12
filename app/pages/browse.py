"""
app/pages/browse — Browse screen (Phase 2 US3).

Thin Streamlit page.  All logic lives in app/logic/browse (no streamlit there).

Layout
------
  - Search bar (st.text_input) → filter_rows
  - st.dataframe: filtered/sorted rows
  - On error: render_friendly_error, never a traceback

Thumbnail/image preview: TODO (later slice — requires per-case image download
from XNAT resource; documented here as a known gap, not a blocker for the table).

State contract (from app/state):
  - get_server()   → live server handle (pyxnat.Interface or FakeXNAT in tests)
  - get_username() → authenticated username (display only)
  - project_name   → from AppConfig (FR-015)
"""
from __future__ import annotations

import streamlit as st

from app.components.error_panel import render_friendly_error
from app import state
from app.logic.browse import fetch_data_table, filter_rows, COLUMNS
from src.services.config import AppConfig
from src.services.errors import FriendlyError


# --- Display mapping: maps COLUMNS keys to user-facing display names ---
# scan_id is excluded (internal addressing field, not displayed)
DISPLAY_MAP = {
    "subject": "Subject",
    "experiment": "Experiment",
    "date": "Date",
    "scan_type": "Scan Type",
    "num_files": "# Files",
}


def _display_frame(filtered: list[dict]) -> object:
    """
    Convert filtered rows to a DataFrame with only display columns, properly named.

    Selects the 5 user-facing columns in order (excludes scan_id),
    then renames using DISPLAY_MAP.

    Parameters
    ----------
    filtered : list[dict]
        Rows from filter_rows, each with all COLUMNS keys.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns in display order, renamed for readability.
        scan_id excluded.
    """
    import pandas as pd

    # Build DataFrame from filtered rows
    df = pd.DataFrame(filtered, columns=COLUMNS)

    # Select only display columns in order
    display_cols = ["subject", "experiment", "date", "scan_type", "num_files"]
    df = df[display_cols]

    # Rename to display names
    df = df.rename(columns=DISPLAY_MAP)

    return df


def render() -> None:
    """Render the Browse XNAT Data page."""
    st.title("Browse XNAT Data")

    # --- Auth guard (belt-and-suspenders; main.py already guards routing) ---
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
            context="browse.render, AppConfig.load",
        )
        render_friendly_error(fe)
        return

    # --- Header row ---
    col_title, col_user = st.columns([3, 1])
    with col_title:
        st.subheader(f"Project: {project_name}")
    with col_user:
        st.caption(f"Logged in as: {username}")

    # --- Fetch data (cached in session state to avoid re-fetching on every widget interaction) ---
    _CACHE_KEY = "_browse_rows_cache"
    if _CACHE_KEY not in st.session_state:
        with st.spinner("Loading data from XNAT server…"):
            result = fetch_data_table(server, project_name)
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
        st.info(f"No subjects/experiments found in project '{project_name}'.")
        return

    # --- Search / filter ---
    search_query = st.text_input(
        "Search",
        placeholder="Filter by subject, experiment, date, scan type…",
        help="Case-insensitive substring match across all columns.",
    )

    filtered = filter_rows(rows, search_query)

    # --- Row count badge ---
    total = len(rows)
    shown = len(filtered)
    if search_query.strip():
        st.caption(f"Showing {shown} of {total} rows")
    else:
        st.caption(f"{total} row(s)")

    # --- Table ---
    if filtered:
        df = _display_frame(filtered)
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("No rows match the current search filter.")

    # --- Thumbnail / image preview placeholder ---
    # TODO (later slice): selecting a row and showing a thumbnail requires
    # downloading the first DICOM frame from the XNAT resource.  The table
    # above gives the subject/experiment/scan coordinates needed; wire in
    # a per-case image fetch once the download service layer is ready (US5).
    with st.expander("Image Preview (coming soon)", expanded=False):
        st.caption(
            "Select a row above, then a thumbnail preview will appear here. "
            "This feature is planned for a later slice (US5 / download)."
        )
