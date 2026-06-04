"""
app/pages/batch — Batch upload screen (Phase 2 US4).

Thin Streamlit layer over app/logic/batch.
NO logic here — all decisions live in app/logic/batch (tested offline).

Flow
----
Step 1  File upload    — st.file_uploader for the .xlsx.
Step 2  Validation     — per-row validation table shown BEFORE upload.
Step 3  Upload         — progress per row; end-of-run summary.
Step 4  Re-run         — "Re-run failed rows" button → rerun_failed_rows.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, List, Optional

import streamlit as st

from app import state
from app.logic.batch import (
    BatchRunResult,
    RowValidation,
    load_batch,
    rerun_failed_rows,
    run_batch,
    validate_batch,
)
from src.services.errors import FriendlyError


# ---------------------------------------------------------------------------
# Session-state keys
# ---------------------------------------------------------------------------

_KEY_STEP       = "batch_step"        # int: 1 / 2 / 3
_KEY_ROWS_DF    = "batch_rows_df"     # pd.DataFrame from load_batch
_KEY_VALIDATIONS = "batch_validations"  # list[RowValidation]
_KEY_RESULT     = "batch_result"      # BatchRunResult after upload
_KEY_RERUN_RESULT = "batch_rerun_result"  # BatchRunResult after re-run


def _step() -> int:
    return st.session_state.get(_KEY_STEP, 1)


def _set_step(n: int) -> None:
    st.session_state[_KEY_STEP] = n


def _reset() -> None:
    for k in (_KEY_STEP, _KEY_ROWS_DF, _KEY_VALIDATIONS, _KEY_RESULT, _KEY_RERUN_RESULT):
        st.session_state.pop(k, None)


# ---------------------------------------------------------------------------
# Helper: render a FriendlyError as an error panel
# ---------------------------------------------------------------------------

def _render_friendly_error(fe: FriendlyError) -> None:
    st.error(f"**{fe.title}**")
    st.write(fe.message)
    if fe.recourse:
        st.markdown("**What you can do:**")
        for i, step in enumerate(fe.recourse, start=1):
            st.markdown(f"  {i}. {step}")
    if fe.diagnostic_log_path:
        st.caption(f"Diagnostic log saved to: `{fe.diagnostic_log_path}`")


# ---------------------------------------------------------------------------
# Step 1 — File upload
# ---------------------------------------------------------------------------

def _render_file_upload() -> None:
    st.subheader("Step 1 of 3 — Upload Batch Spreadsheet")
    st.caption(
        "Drag and drop your batch-upload `.xlsx` file. "
        "A per-row validation table will appear before any data is sent to XNAT."
    )

    uploaded = st.file_uploader(
        "Batch spreadsheet (.xlsx)",
        type=["xlsx"],
        key="batch_xlsx_uploader",
        help="Use the batch-upload template provided by the Data Librarian.",
    )

    if uploaded is not None:
        with st.spinner("Reading spreadsheet…"):
            df, err = load_batch(uploaded)

        if err is not None:
            _render_friendly_error(err)
            return

        validations = validate_batch(df)
        st.session_state[_KEY_ROWS_DF] = df
        st.session_state[_KEY_VALIDATIONS] = validations
        _set_step(2)
        st.rerun()


# ---------------------------------------------------------------------------
# Step 2 — Validation table
# ---------------------------------------------------------------------------

def _render_validation() -> None:
    st.subheader("Step 2 of 3 — Review Row Validation")
    st.caption(
        "Rows marked **OK** will be uploaded. "
        "Rows with problems will be skipped (fix the spreadsheet and re-upload to retry them)."
    )

    validations: List[RowValidation] = st.session_state.get(_KEY_VALIDATIONS, [])
    df = st.session_state.get(_KEY_ROWS_DF)

    if not validations or df is None:
        st.warning("No validation data found. Please re-upload the spreadsheet.")
        _set_step(1)
        st.rerun()
        return

    n_ok   = sum(1 for v in validations if v.ok)
    n_bad  = sum(1 for v in validations if not v.ok)
    n_total = len(validations)

    col1, col2, col3 = st.columns(3)
    col1.metric("Total rows",   n_total)
    col2.metric("Valid rows",   n_ok,  delta=None)
    col3.metric("Problem rows", n_bad, delta=None, delta_color="off" if n_bad == 0 else "inverse")

    if n_bad > 0:
        st.warning(
            f"{n_bad} row(s) have validation errors and will be skipped during upload. "
            "Fix the spreadsheet and re-upload to retry those rows."
        )

    # Build a display DataFrame
    import pandas as pd
    table_rows = []
    for v in validations:
        status = "OK" if v.ok else "PROBLEM"
        problems_str = "; ".join(v.problems) if v.problems else ""
        table_rows.append({
            "Row": v.row_index + 2,   # +2: 1-based + header row
            "Status": status,
            "Problems": problems_str,
        })
    table_df = pd.DataFrame(table_rows)

    st.dataframe(
        table_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Row":      st.column_config.NumberColumn("Row #", width="small"),
            "Status":   st.column_config.TextColumn("Status",   width="small"),
            "Problems": st.column_config.TextColumn("Problems", width="large"),
        },
    )

    st.markdown("---")

    col_back, col_upload = st.columns([1, 4])
    with col_back:
        if st.button("← Re-upload file", key="batch_back_to_upload"):
            _reset()
            st.rerun()
    with col_upload:
        if n_ok == 0:
            st.button(
                "Upload valid rows →",
                key="batch_start_upload",
                type="primary",
                disabled=True,
                help="No valid rows to upload.",
            )
        else:
            if st.button(
                f"Upload {n_ok} valid row(s) →",
                key="batch_start_upload",
                type="primary",
            ):
                _set_step(3)
                st.rerun()


# ---------------------------------------------------------------------------
# Step 3 — Upload + summary
# ---------------------------------------------------------------------------

def _render_upload() -> None:
    st.subheader("Step 3 of 3 — Batch Upload")

    df      = st.session_state.get(_KEY_ROWS_DF)
    validations: List[RowValidation] = st.session_state.get(_KEY_VALIDATIONS, [])
    server  = state.get_server()

    if df is None or not validations:
        st.error("Session state lost. Please start over.")
        _reset()
        st.rerun()
        return

    if server is None:
        st.error("Session expired — please log in again.")
        state.clear_auth()
        st.rerun()
        return

    # Only attempt valid rows
    only_rows = [v.row_index for v in validations if v.ok]

    # Run once; store result in session state to survive reruns
    if _KEY_RESULT not in st.session_state:
        with st.spinner(f"Uploading {len(only_rows)} row(s) — please wait…"):
            result: BatchRunResult = run_batch(
                rows=df,
                server_connection=server,
                only_rows=only_rows,
                publish_fn=None,  # wired when real Phase-1 publish path is connected
            )
        st.session_state[_KEY_RESULT] = result

    result: BatchRunResult = st.session_state[_KEY_RESULT]
    rerun_result: Optional[BatchRunResult] = st.session_state.get(_KEY_RERUN_RESULT)

    _render_run_summary("Upload", result)

    if rerun_result is not None:
        st.markdown("---")
        st.markdown("#### Re-run Results")
        _render_run_summary("Re-run", rerun_result)

    st.markdown("---")

    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("← Back to validation", key="batch_back_to_validation"):
            # Keep df/validations; clear result so user can re-run
            st.session_state.pop(_KEY_RESULT, None)
            st.session_state.pop(_KEY_RERUN_RESULT, None)
            _set_step(2)
            st.rerun()

    # Re-run failed rows button
    active_result = rerun_result if rerun_result is not None else result
    if active_result.failed_count > 0:
        with col2:
            if st.button(
                f"Re-run {active_result.failed_count} failed row(s)",
                key="batch_rerun_failed",
                type="secondary",
            ):
                with st.spinner("Re-running failed rows…"):
                    new_rerun = rerun_failed_rows(
                        prev_result=active_result,
                        rows=df,
                        server_connection=server,
                        publish_fn=None,
                    )
                st.session_state[_KEY_RERUN_RESULT] = new_rerun
                st.rerun()

    with col3:
        if st.button("Upload another batch", key="batch_start_over"):
            _reset()
            st.rerun()


def _render_run_summary(label: str, result: BatchRunResult) -> None:
    """Render a succeeded / skipped / failed summary for one run."""
    col1, col2, col3 = st.columns(3)
    col1.metric(f"{label} — Succeeded", result.succeeded_count)
    col2.metric(f"{label} — Skipped",   result.skipped_count)
    col3.metric(f"{label} — Failed",    result.failed_count)

    if result.failures:
        with st.expander(f"Show {len(result.failures)} failure(s)"):
            for f in result.failures:
                field_hint = f" [{f.offending_field}]" if f.offending_field else ""
                st.markdown(f"- **Row {f.row_index + 2}**{field_hint}: {f.reason}")


# ---------------------------------------------------------------------------
# Public render — called from app/main.py
# ---------------------------------------------------------------------------

def render() -> None:
    """Render the Batch Upload page (guarded behind auth in main.py)."""
    st.title("Batch Upload")

    current_step = _step()

    # Step indicator
    step_labels = ["1. Upload File", "2. Validate", "3. Upload"]
    st.progress(
        (current_step - 1) / len(step_labels),
        text=" → ".join(
            f"**{lbl}**" if i + 1 == current_step else lbl
            for i, lbl in enumerate(step_labels)
        ),
    )
    st.markdown("---")

    if current_step == 1:
        _render_file_upload()
    elif current_step == 2:
        _render_validation()
    elif current_step == 3:
        _render_upload()
    else:
        _reset()
        st.rerun()
