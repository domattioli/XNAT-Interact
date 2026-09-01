"""
app/guided/components — Reusable UI components for the guided app.

Includes header, step rail, error rendering.
"""
from __future__ import annotations

from typing import List

import streamlit as st

from src.services.errors import FriendlyError


def render_header(username: str, project: str, connected: bool) -> None:
    """
    Render a persistent top-bar header.

    Shows: username, project, connection status.
    Uses st.columns for horizontal layout.
    """
    col1, col2, col3 = st.columns([1, 2, 1])

    with col1:
        st.caption(f"👤 {username}")

    with col2:
        st.caption(f"📁 {project}")

    with col3:
        if connected:
            st.success("● Connected", icon=None)
        else:
            st.error("● Disconnected", icon=None)

    st.markdown("---")


def render_step_rail(steps: List[str], current: int) -> None:
    """
    Render a step-rail progress indicator.

    Shows: "Step {current+1} of {len(steps)}: {steps[current]}"
    Plus a visual indicator of all steps with current highlighted.
    """
    if not steps or current < 0 or current >= len(steps):
        return

    # Main step display
    st.markdown(
        f"**Step {current + 1} of {len(steps)}: {steps[current]}**"
    )

    # Visual step indicator using columns (compact)
    cols = st.columns(len(steps))
    for i, (col, step_name) in enumerate(zip(cols, steps)):
        with col:
            if i == current:
                st.markdown(f"**{i + 1}. {step_name[:12]}**")
            elif i < current:
                st.markdown(f"✓ {i + 1}")
            else:
                st.markdown(f"{i + 1}. {step_name[:12]}")

    st.markdown("---")


def render_friendly_error(fe: FriendlyError) -> None:
    """
    Render a FriendlyError as a styled error panel.

    Shows: title, message, numbered recourse, diagnostic log path.
    Reuses the component from app/components/error_panel if available,
    else inline rendering.
    """
    try:
        from app.components.error_panel import render_friendly_error as _render
        _render(fe)
    except ImportError:
        # Fallback inline rendering
        with st.container():
            st.error(f"**{fe.title}**")
            st.write(fe.message)
            if fe.recourse:
                st.write("**What you can do:**")
                for i, step in enumerate(fe.recourse, start=1):
                    st.write(f"{i}. {step}")
            if fe.diagnostic_log_path:
                st.caption(f"Diagnostic details: `{fe.diagnostic_log_path}`")
