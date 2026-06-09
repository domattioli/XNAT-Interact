"""
app/logic/batch — pure batch-upload logic for the Streamlit batch screen.

NO streamlit import.  Tested offline.

Public API
----------
load_batch(file_or_path)            -> (DataFrame | None, FriendlyError | None)
validate_batch(rows)                -> list[RowValidation]
run_batch(rows, server_connection, *, publish_fn, only_rows) -> BatchRunResult
rerun_failed_rows(prev_result, rows, server_connection, *, publish_fn) -> BatchRunResult
"""
from __future__ import annotations

import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, List, Optional, Union

import pandas as pd

from src.batch_upload import BatchRunResult, BatchUploadRepresentation, RowOutcome, rerun_failed
from src.services.errors import FriendlyError


# ---------------------------------------------------------------------------
# RowValidation — per-row pre-upload validation result
# ---------------------------------------------------------------------------

@dataclass
class RowValidation:
    """Pre-upload validation result for one spreadsheet row."""
    row_index: int          # 0-based index into the DataFrame
    ok: bool                # True = no blocking errors
    problems: List[str] = field(default_factory=list)   # human-readable error strings


# ---------------------------------------------------------------------------
# Columns required for the batch xlsx (minimal — must match BatchUploadRepresentation)
# ---------------------------------------------------------------------------

_REQUIRED_COLUMNS = [
    "Filer HawkID",
    "Operation Date",
    "Epic Start Time",
    "Institution Name",
    "Procedure Name",
    "Full Path to Data",
]


# ---------------------------------------------------------------------------
# load_batch
# ---------------------------------------------------------------------------

def load_batch(
    file_or_path: Any,
) -> tuple[Optional[pd.DataFrame], Optional[FriendlyError]]:
    """
    Read an xlsx batch spreadsheet into a DataFrame.

    Parameters
    ----------
    file_or_path
        Path (str / pathlib.Path) or file-like object (e.g. Streamlit UploadedFile).

    Returns
    -------
    (DataFrame, None)   on success — DataFrame has original column names.
    (None, FriendlyError) on any failure (bad file, locked, empty, wrong format).
    """
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=UserWarning)
            df = pd.read_excel(file_or_path, header=0)

        # Normalise column names (strip whitespace / newlines)
        df.columns = [str(c).replace("\n", " ").strip() for c in df.columns]

        # Drop unnamed / helper columns (mirrors BatchUploadRepresentation)
        df = df.loc[:, ~df.columns.str.contains(r"^Unnamed")]
        df = df.loc[:, ~df.columns.str.contains(r"^Case Name \[Optional\]")]

        # Fill NaN → empty string (consistent with Phase-1 processing)
        df = df.fillna("")

        if df.empty:
            return None, FriendlyError(
                title="Spreadsheet is empty",
                message="The uploaded file contains no data rows.",
                recourse=[
                    "Open the spreadsheet and confirm it has at least one data row.",
                    "Re-export from the batch-upload template and try again.",
                ],
            )

        return df, None

    except Exception as exc:  # noqa: BLE001
        return None, FriendlyError(
            title="Could not read the spreadsheet",
            message=(
                f"The file could not be opened or parsed as an xlsx spreadsheet "
                f"({type(exc).__name__}: {exc})."
            ),
            recourse=[
                "Make sure the file is a valid .xlsx spreadsheet (not .xls or .csv).",
                "Close the file in Excel / LibreOffice if it is open, then try again.",
                "Re-export from the batch-upload template and try again.",
            ],
        )


# ---------------------------------------------------------------------------
# validate_batch
# ---------------------------------------------------------------------------

def validate_batch(rows: pd.DataFrame) -> List[RowValidation]:
    """
    Run per-row pre-upload validation without hitting the network.

    Reuses the same required-field checks that BatchUploadRepresentation applies
    internally, but surfaces them as RowValidation objects so the UI can display
    a validation table BEFORE the user starts the upload.

    Parameters
    ----------
    rows
        DataFrame returned by load_batch (normalised column names, NaN → "").

    Returns
    -------
    list[RowValidation]  — one entry per row, in row order.
    """
    results: List[RowValidation] = []

    for idx, row in rows.iterrows():
        problems: List[str] = []

        # Required-column presence checks (mirrors _check_required_columns)
        proc_name = str(row.get("Procedure Name", "")).strip()
        if not proc_name:
            problems.append("'Procedure Name' is blank or missing.")

        filer = str(row.get("Filer HawkID", "")).strip()
        if not filer:
            problems.append("'Filer HawkID' is blank or missing.")

        epic_start = str(row.get("Epic Start Time", "")).strip()
        if not epic_start:
            problems.append("'Epic Start Time' is blank or missing.")

        institution = str(row.get("Institution Name", "")).strip()
        if not institution:
            problems.append("'Institution Name' is blank or missing.")

        data_path = str(row.get("Full Path to Data", "")).strip()
        if not data_path:
            problems.append("'Full Path to Data' is blank or missing.")
        elif not Path(data_path).exists():
            problems.append(
                f"'Full Path to Data' '{data_path}' does not exist on this machine."
            )

        results.append(RowValidation(
            row_index=int(idx),
            ok=len(problems) == 0,
            problems=problems,
        ))

    return results


# ---------------------------------------------------------------------------
# _build_batch_repr  (internal helper)
# ---------------------------------------------------------------------------

def _build_batch_repr(
    rows: pd.DataFrame,
    source_path: Optional[Path] = None,
) -> BatchUploadRepresentation:
    """
    Construct a BatchUploadRepresentation from an already-loaded DataFrame,
    bypassing the xlsx-reading __init__ (mirrors the test helper pattern).

    The _errors DataFrame is pre-populated from validate_batch results so that
    upload_sessions can apply its continue-on-error logic correctly.
    """
    inst = BatchUploadRepresentation.__new__(BatchUploadRepresentation)

    # source path for result-file naming
    inst._ffn = source_path or Path("batch-upload.xlsx")

    inst._df = rows.copy()

    # Build _errors / _warnings frames with same shape (empty string = no issue)
    cols = list(rows.columns)
    n = len(rows)
    inst._errors = pd.DataFrame(
        data=[[""] * len(cols) for _ in range(n)],
        columns=cols,
    )
    inst._warnings = inst._errors.copy()

    # Populate _errors from validate_batch so upload_sessions sees them
    validations = validate_batch(rows)
    for vr in validations:
        if not vr.ok:
            # Record all problems into the first column that has a problem
            # Use "Procedure Name" as the canonical error anchor when that's
            # the blank field; otherwise "Filer HawkID"; fallback = first col.
            for problem in vr.problems:
                # Find which column the problem is about
                col_hint = _problem_to_column(problem, cols)
                existing = inst._errors.at[vr.row_index, col_hint]
                if not existing:
                    inst._errors.at[vr.row_index, col_hint] = [problem]
                elif isinstance(existing, list):
                    existing.append(problem)

    # Stub generate_summary (upload_sessions calls it for side-effects it then
    # rebuilds itself from _errors — a no-op tuple is fine)
    inst.generate_summary = lambda write_to_file=False: (True, "", "", "", "")  # type: ignore[method-assign]

    return inst


def _problem_to_column(problem: str, cols: List[str]) -> str:
    """Map a problem string to the most relevant column name."""
    # Simple heuristic: look for a quoted column name in the problem string
    for col in cols:
        if col in problem:
            return col
    # Fallback: first column
    return cols[0] if cols else "Filer HawkID"


# ---------------------------------------------------------------------------
# run_batch
# ---------------------------------------------------------------------------

def run_batch(
    rows: pd.DataFrame,
    server_connection: Any,
    *,
    publish_fn: Optional[Callable[..., None]] = None,
    only_rows: Optional[List[int]] = None,
    source_path: Optional[Path] = None,
) -> BatchRunResult:
    """
    Run a batch upload with continue-on-error semantics.

    Thin wrapper over BatchUploadRepresentation.upload_sessions.
    Never raises on foreseeable problems; surfaces them in BatchRunResult.

    Parameters
    ----------
    rows
        DataFrame from load_batch.
    server_connection
        Live XNAT connection (XNATConnection) or FakeXNAT for tests.
        Must have .is_verified attribute.
    publish_fn
        Injectable callable for tests (same contract as upload_sessions _publish_fn).
        Signature: publish_fn(row, row_index) -> None.  Raise to simulate failure.
    only_rows
        Optional list of 0-based row indices to attempt; all others → skipped.
    source_path
        Optional path to the source xlsx (for result-file naming).

    Returns
    -------
    BatchRunResult
        Per-row outcomes + summary counts.
    """
    try:
        inst = _build_batch_repr(rows, source_path=source_path)
        result = inst.upload_sessions(
            config=None,           # not used when _publish_fn is injected
            validated_login=None,  # not used when _publish_fn is injected
            xnat_connection=server_connection,
            verbose=False,
            only_rows=only_rows,
            _publish_fn=publish_fn,
        )
        return result
    except ConnectionError as exc:
        # Server not verified — wrap as a single failed outcome
        outcome = RowOutcome(
            row_index=-1,
            status="failed",
            reason=str(exc),
        )
        return BatchRunResult(outcomes=[outcome])
    except Exception as exc:  # noqa: BLE001
        outcome = RowOutcome(
            row_index=-1,
            status="failed",
            reason=f"Unexpected error during batch upload: {type(exc).__name__}: {exc}",
        )
        return BatchRunResult(outcomes=[outcome])


# ---------------------------------------------------------------------------
# rerun_failed_rows
# ---------------------------------------------------------------------------

def rerun_failed_rows(
    prev_result: BatchRunResult,
    rows: pd.DataFrame,
    server_connection: Any,
    *,
    publish_fn: Optional[Callable[..., None]] = None,
    source_path: Optional[Path] = None,
) -> BatchRunResult:
    """
    Re-attempt only the rows that failed in a previous run.

    Wraps Phase-1 rerun_failed.  Never raises.

    Parameters
    ----------
    prev_result
        BatchRunResult from a previous run_batch call.
    rows
        Same DataFrame that was passed to run_batch.
    server_connection
        Live XNAT connection or FakeXNAT.
    publish_fn
        Injectable callable for tests.
    source_path
        Optional source path for naming.

    Returns
    -------
    BatchRunResult covering only the previously-failed rows.
    """
    try:
        if not prev_result.failed_row_indices:
            return BatchRunResult(source_path=prev_result.source_path)

        inst = _build_batch_repr(rows, source_path=source_path)
        result = rerun_failed(
            prev_result,
            inst,
            config=None,
            validated_login=None,
            xnat_connection=server_connection,
            verbose=False,
            _publish_fn=publish_fn,
        )
        return result
    except Exception as exc:  # noqa: BLE001
        outcome = RowOutcome(
            row_index=-1,
            status="failed",
            reason=f"Unexpected error during re-run: {type(exc).__name__}: {exc}",
        )
        return BatchRunResult(outcomes=[outcome])
