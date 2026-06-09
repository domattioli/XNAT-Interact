"""
app/pages/annotations — Annotations screen (Phase 5 T017-T020).

Thin Streamlit page.  All logic lives in app/logic/annotations (no streamlit there).

Layout
------
  1. Image reference input (text field).
  2. Load button → fetch AnnotationSet via list_image_annotations.
  3. Annotations table: annotator_id, tool, annotation_type, version, derived.
  4. Consensus controls: annotation type select + aggregator select → Run Consensus.
  5. Consensus result display.
  6. Upload button: upload the current AnnotationSet back to XNAT.
  7. Download button: re-fetch the AnnotationSet from XNAT.

Errors: rendered via app/components/error_panel.render_friendly_error.
Auth guard: belt-and-suspenders (main.py also guards routing).

render() is the single entry point called by app/main.py.
"""
from __future__ import annotations

import streamlit as st

from app import state
from app.components.error_panel import render_friendly_error
from app.logic.annotations import (
    list_image_annotations,
    run_consensus,
    upload_annotations,
    list_aggregator_names,
)
from src.annotations.model import AnnotationSet
from src.annotations.registry import list_types
from src.services.errors import FriendlyError


# Session-state cache keys
_KEY_ASET    = "_ann_annotation_set"
_KEY_IMGREF  = "_ann_image_ref"
_KEY_RESULT  = "_ann_consensus_result"


def render() -> None:
    """Render the Annotations page."""
    st.title("Annotations")

    # Auth guard
    if not state.is_authenticated():
        st.warning("Not logged in — please log in first.")
        return

    server = state.get_server()

    # ------------------------------------------------------------------
    # 1. Image reference input
    # ------------------------------------------------------------------
    image_ref = st.text_input(
        "Image reference",
        placeholder="e.g. subjects/S001/experiments/E01/scans/scan1",
        help="Opaque XNAT scan reference — not a patient identifier.",
        key="ann_image_ref_input",
    )

    col_load, col_reset = st.columns([1, 1])
    with col_load:
        load_clicked = st.button("Load annotations", key="ann_load_btn")
    with col_reset:
        if st.button("Clear", key="ann_clear_btn"):
            for k in (_KEY_ASET, _KEY_IMGREF, _KEY_RESULT):
                st.session_state.pop(k, None)
            st.rerun()

    # ------------------------------------------------------------------
    # 2. Fetch AnnotationSet
    # ------------------------------------------------------------------
    if load_clicked and image_ref.strip():
        with st.spinner("Fetching annotations from XNAT…"):
            result = list_image_annotations(server, image_ref.strip())
        if isinstance(result, FriendlyError):
            render_friendly_error(result)
        else:
            st.session_state[_KEY_ASET] = result
            st.session_state[_KEY_IMGREF] = image_ref.strip()
            st.session_state.pop(_KEY_RESULT, None)
            st.success(f"Loaded {len(result.annotations)} annotation(s).")

    aset: AnnotationSet | None = st.session_state.get(_KEY_ASET)

    if aset is None:
        st.info("Enter an image reference and click 'Load annotations'.")
        return

    # ------------------------------------------------------------------
    # 3. Annotations table
    # ------------------------------------------------------------------
    st.subheader(f"Annotations for: `{aset.image_ref}`")
    st.caption(f"{len(aset.annotations)} annotation(s) loaded.")

    if aset.annotations:
        import pandas as pd
        rows = [
            {
                "annotator_id":    ann.annotator_id,
                "tool":            ann.tool,
                "annotation_type": ann.annotation_type,
                "version":         ann.version,
                "derived":         ann.derived,
            }
            for ann in aset.annotations
        ]
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("No annotations in this set.")
        return

    st.markdown("---")

    # ------------------------------------------------------------------
    # 4. Consensus controls
    # ------------------------------------------------------------------
    st.subheader("Run Consensus")

    available_types = list_types()
    present_types = list({ann.annotation_type for ann in aset.annotations})
    type_choices = [t for t in available_types if t in present_types] or available_types

    annotation_type = st.selectbox(
        "Annotation type",
        options=type_choices,
        help="Type to aggregate over (only types present in this set are shown first).",
        key="ann_type_select",
    )

    aggregators = list_aggregator_names()
    aggregator_name = st.selectbox(
        "Aggregator",
        options=aggregators,
        help="Select the aggregation method.",
        key="ann_aggregator_select",
    )

    if st.button("Run consensus", key="ann_consensus_btn"):
        with st.spinner("Running consensus…"):
            consensus = run_consensus(aset, annotation_type, aggregator_name)
        if isinstance(consensus, FriendlyError):
            render_friendly_error(consensus)
        else:
            st.session_state[_KEY_RESULT] = consensus
            st.success(f"Consensus complete — method: `{consensus.method}`.")

    consensus_result = st.session_state.get(_KEY_RESULT)
    if consensus_result is not None:
        with st.expander("Consensus result", expanded=True):
            st.write(f"**Method:** `{consensus_result.method}`")
            st.write(f"**Inputs used:** {len(consensus_result.per_annotator)} annotator(s)")
            import numpy as np
            if isinstance(consensus_result.payload, np.ndarray):
                st.write(f"**Output shape:** {consensus_result.payload.shape}")
                st.write(f"**Unique values:** {np.unique(consensus_result.payload).tolist()}")
            elif isinstance(consensus_result.payload, dict):
                st.json(consensus_result.payload)
            else:
                st.write(consensus_result.payload)

    st.markdown("---")

    # ------------------------------------------------------------------
    # 5. Upload / Download buttons
    # ------------------------------------------------------------------
    st.subheader("Storage")

    col_up, col_dl = st.columns(2)

    with col_up:
        if st.button("Upload annotations to XNAT", key="ann_upload_btn"):
            with st.spinner("Uploading…"):
                up_result = upload_annotations(server, aset.image_ref, aset)
            if isinstance(up_result, FriendlyError):
                render_friendly_error(up_result)
            elif not up_result.ok:
                render_friendly_error(up_result.friendly)
            else:
                st.success(
                    f"Uploaded {len(up_result.files_written)} file(s): "
                    + ", ".join(up_result.files_written[:5])
                    + ("…" if len(up_result.files_written) > 5 else "")
                )

    with col_dl:
        if st.button("Re-download annotations", key="ann_download_btn"):
            with st.spinner("Downloading…"):
                dl_result = list_image_annotations(server, aset.image_ref)
            if isinstance(dl_result, FriendlyError):
                render_friendly_error(dl_result)
            else:
                st.session_state[_KEY_ASET] = dl_result
                st.session_state.pop(_KEY_RESULT, None)
                st.success(f"Re-downloaded {len(dl_result.annotations)} annotation(s).")
                st.rerun()
