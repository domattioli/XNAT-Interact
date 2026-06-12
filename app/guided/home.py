"""
app/guided/home — Task-first home screen (landing page).

Three plain-language task cards: Upload, Download, Annotations.
"""
from __future__ import annotations

import streamlit as st

from app.guided import wizard_state


def render_home() -> None:
    """Render the task-first home screen."""
    st.title("XNAT-Interact — Guided")

    st.markdown(
        "**What would you like to do?**  \n"
        "Pick a task and we'll guide you through it step by step."
    )

    st.markdown("")

    # Task cards as columns
    col1, col2, col3 = st.columns(3)

    with col1:
        with st.container(border=True):
            st.markdown("### ➕ Add a Surgery")
            st.markdown(
                "Upload de-identified images from one surgery, step by step."
            )
            if st.button("Start", key="btn_upload", type="primary", use_container_width=True):
                wizard_state.set_task("upload")
                wizard_state.set_step(0)
                st.rerun()

    with col2:
        with st.container(border=True):
            st.markdown("### 🔎 Find & Download")
            st.markdown(
                "Browse what's in the archive and get images back out."
            )
            if st.button("Start", key="btn_download", use_container_width=True):
                wizard_state.set_task("download")
                wizard_state.set_step(0)
                st.rerun()

    with col3:
        with st.container(border=True):
            st.markdown("### 🏷️ Work with Annotations")
            st.markdown(
                "Add or review segmentation labels on existing images."
            )
            if st.button("Start", key="btn_annotations", use_container_width=True):
                wizard_state.set_task("annotations")
                wizard_state.set_step(0)
                st.rerun()

    st.markdown("---")

    with st.expander("🔧 Advanced Tools", expanded=False):
        st.markdown(
            "The classic XNAT-Interact app has all tools available in one place.  \n"
            "Launch it with: `streamlit run streamlit_app.py`"
        )
