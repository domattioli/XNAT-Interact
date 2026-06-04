"""
app/pages/browse — STUB (Phase 2 US3).

Browse: searchable/filterable table of server data + thumbnail preview.
Another agent fills this in.  Guarded behind auth in main.py.
"""
from __future__ import annotations

import streamlit as st


def render() -> None:
    """Stub Browse page — populated by a later agent in Phase 2."""
    st.title("Browse XNAT Data")
    st.info(
        "Browse screen coming soon.  "
        "This stub confirms routing works — fill it in when US3 is implemented.",
        icon="ℹ️",
    )
