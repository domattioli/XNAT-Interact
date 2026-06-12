"""
app/guided/browse_view — Browse interface for viewing surgeries in archive.

Provides render_browse() and helper _browse_display_frame() for display-column
transformation and rendering.
"""
from __future__ import annotations

import pandas as pd
import streamlit as st

from app.guided import components, wizard_state
from app.logic.browse import fetch_data_table
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
    Render browse interface: subheader, fetch, display, home button.

    Fetches the data table for the project, handles errors gracefully,
    and displays results in a filtered/sortable DataFrame view.

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

    # Home button
    if st.button("← Home"):
        wizard_state.set_task(None)
        st.rerun()
