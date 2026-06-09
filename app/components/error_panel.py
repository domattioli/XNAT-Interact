"""
app/components/error_panel — renders a Phase 1 FriendlyError as a Streamlit panel.

Never shows a raw traceback.  Always surfaces title + message + recourse.
"""
from __future__ import annotations

from typing import Optional

import streamlit as st

from src.services.errors import FriendlyError


def render_friendly_error(fe: FriendlyError) -> None:
    """
    Render *fe* as a styled Streamlit error panel.

    Shows: bold title, message, numbered recourse list,
    and (if available) the diagnostic log path.
    Does NOT surface a Python traceback.
    """
    with st.container():
        st.error(f"**{fe.title}**")
        st.write(fe.message)
        if fe.recourse:
            st.write("**What you can do:**")
            for i, step in enumerate(fe.recourse, start=1):
                st.write(f"{i}. {step}")
        if fe.diagnostic_log_path:
            st.caption(f"Diagnostic details saved to: `{fe.diagnostic_log_path}`")
