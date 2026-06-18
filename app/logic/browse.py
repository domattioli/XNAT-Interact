"""
app/logic/browse — pure browse logic, no streamlit import.

Tested offline against FakeXNAT.  All server I/O is injected via the
``server`` parameter (real pyxnat.Interface in production; FakeXNAT in
tests).

Public API
----------
fetch_data_table(server, project_name) -> list[dict] | FriendlyError
    Query project for subjects/experiments/scans; return rows with columns:
    subject, experiment, date, scan_type, num_files.

filter_rows(rows, query) -> list[dict]
    Case-insensitive substring match across all column values.

sort_rows(rows, key) -> list[dict]
    Sort rows by the given column key (ascending, stable).

COLUMNS surfaced (mirrors the CLI preview in src/delete_contents_of_server.py
and the XNAT data model in src/xnat_experiment_data.py):
    subject       — subject/case label (UID or name on XNAT)
    experiment    — experiment label (SOURCE_DATA-<uid>)
    date          — acquisition date from experiment attrs (may be "" if unset)
    scan_type     — scan type label (e.g., DICOM, DICOM_MP4, DERIVED)
    num_files     — count of files in the scan resource (int; -1 if unavailable)
"""
from __future__ import annotations

from typing import Any, List, Union

from src.services.errors import FriendlyError


# ---------------------------------------------------------------------------
# Column names (single source of truth — referenced by pages/browse.py too)
# ---------------------------------------------------------------------------

COLUMNS = ["subject", "experiment", "date", "scan_type", "num_files"]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _safe_int(value: Any, default: int = -1) -> int:
    """Convert *value* to int; return *default* on failure."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _subject_names(server: Any, project_name: str) -> List[str]:
    """
    Return list of subject **labels** in *project_name*.

    T011 fix (#29): real pyxnat's wildcard-select returns internal IDs
    (e.g. PROJ_S00001) rather than human-readable labels.  After a subject
    list is obtained we resolve each entry to its label via
    label_for_subject() when the server supports it.  Downstream queries
    then use labels — not internal IDs — so experiment lookups succeed.

    Resolution priority:
      1. server.list_subjects_with_labels(project_name) → [(internal_id, label), ...]
      2. Internal IDs from select().get() or list_subjects(), then resolved
         individually via server.label_for_subject(id).
      3. Raw IDs unchanged (backward-compatible fallback).
    """
    # T011: prefer the dedicated label-aware listing hook when available.
    if hasattr(server, "list_subjects_with_labels") and callable(server.list_subjects_with_labels):
        pairs = server.list_subjects_with_labels(project_name)
        return [label for _, label in pairs] if pairs else []

    # Primary path — real pyxnat wildcard select
    raw_ids: List[str] = []
    if hasattr(server, "select") and callable(server.select):
        qs = f"/projects/{project_name}/subjects/*"
        selected = server.select(qs)
        if hasattr(selected, "get") and callable(selected.get):
            raw = selected.get()
            raw_ids = list(raw) if raw else []

    # Fallback for test doubles that expose list_subjects directly
    if not raw_ids and hasattr(server, "list_subjects") and callable(server.list_subjects):
        raw_ids = list(server.list_subjects(project_name))

    if not raw_ids:
        return []

    # T011: resolve internal IDs → labels when the server supports it.
    if hasattr(server, "label_for_subject") and callable(server.label_for_subject):
        return [server.label_for_subject(iid) for iid in raw_ids]

    return raw_ids


def _experiment_labels(server: Any, project_name: str, subject: str) -> List[str]:
    """Return experiment labels for a subject."""
    qs = f"/projects/{project_name}/subjects/{subject}/experiments/*"
    selected = server.select(qs)
    if hasattr(selected, "get") and callable(selected.get):
        raw = selected.get()
        return list(raw) if raw else []
    if hasattr(server, "list_experiments") and callable(server.list_experiments):
        return list(server.list_experiments(project_name, subject))
    return []


def _scan_labels(server: Any, project_name: str, subject: str, experiment: str) -> List[str]:
    """Return scan labels for an experiment."""
    qs = f"/projects/{project_name}/subjects/{subject}/experiments/{experiment}/scans/*"
    selected = server.select(qs)
    if hasattr(selected, "get") and callable(selected.get):
        raw = selected.get()
        return list(raw) if raw else []
    if hasattr(server, "list_scans") and callable(server.list_scans):
        return list(server.list_scans(project_name, subject, experiment))
    return []


def _scan_attrs(server: Any, project_name: str, subject: str, experiment: str, scan: str) -> dict:
    """Return a dict of scan attributes (best-effort; {} on any error)."""
    try:
        qs = f"/projects/{project_name}/subjects/{subject}/experiments/{experiment}/scans/{scan}"
        sel = server.select(qs)
        if hasattr(sel, "attrs") and hasattr(sel.attrs, "get"):
            return dict(sel.attrs.get()) or {}
    except Exception:
        pass
    if hasattr(server, "scan_attrs") and callable(server.scan_attrs):
        return server.scan_attrs(project_name, subject, experiment, scan) or {}
    return {}


def _file_count(server: Any, project_name: str, subject: str, experiment: str, scan: str) -> int:
    """Return file count for a scan resource (best-effort; -1 on failure)."""
    try:
        # L6 (#33): list files across ALL of the scan's resources, not just SRC.
        # A scan whose files live under a non-SRC resource (e.g. DERIVED) was
        # previously undercounted because the query pinned /resources/SRC.
        qs = (
            f"/projects/{project_name}/subjects/{subject}"
            f"/experiments/{experiment}/scans/{scan}/files"
        )
        sel = server.select(qs)
        if hasattr(sel, "get") and callable(sel.get):
            files = sel.get()
            return len(list(files)) if files is not None else -1
    except Exception:
        pass
    if hasattr(server, "file_count") and callable(server.file_count):
        return _safe_int(server.file_count(project_name, subject, experiment, scan))
    return -1


def _experiment_date(server: Any, project_name: str, subject: str, experiment: str) -> str:
    """Return acquisition date string for experiment (best-effort; '' on failure)."""
    try:
        qs = f"/projects/{project_name}/subjects/{subject}/experiments/{experiment}"
        sel = server.select(qs)
        if hasattr(sel, "attrs") and hasattr(sel.attrs, "get"):
            attrs = dict(sel.attrs.get() or {})
            return str(attrs.get("xnat:experimentData/DATE", ""))
    except Exception:
        pass
    if hasattr(server, "experiment_date") and callable(server.experiment_date):
        return str(server.experiment_date(project_name, subject, experiment) or "")
    return ""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_data_table(
    server: Any,
    project_name: str,
) -> Union[List[dict], FriendlyError]:
    """
    Query *server* for all subjects/experiments/scans in *project_name*.

    Returns a list[dict] with keys matching COLUMNS, or a FriendlyError on
    any foreseeable server/query error.  Never raises for expected failures.

    Parameters
    ----------
    server       : pyxnat.Interface (production) or FakeXNAT / test double.
    project_name : XNAT project name string.

    Returns
    -------
    list[dict]  — zero or more rows, each with keys: subject, experiment,
                  date, scan_type, num_files.
    FriendlyError — on connection / query failure.
    """
    rows: List[dict] = []
    try:
        subjects = _subject_names(server, project_name)
        for subj in subjects:
            experiments = _experiment_labels(server, project_name, subj)
            if not experiments:
                # Subject with no experiments — still surface it
                rows.append({
                    "subject": subj,
                    "experiment": "",
                    "date": "",
                    "scan_type": "",
                    "num_files": -1,
                })
                continue
            for exp in experiments:
                date = _experiment_date(server, project_name, subj, exp)
                scans = _scan_labels(server, project_name, subj, exp)
                if not scans:
                    rows.append({
                        "subject": subj,
                        "experiment": exp,
                        "date": date,
                        "scan_type": "",
                        "num_files": -1,
                    })
                    continue
                for scan in scans:
                    attrs = _scan_attrs(server, project_name, subj, exp, scan)
                    scan_type = str(
                        attrs.get("xnat:mrScanData/TYPE", "")
                        or attrs.get("scan_type", "")
                    )
                    n_files = _file_count(server, project_name, subj, exp, scan)
                    rows.append({
                        "subject": subj,
                        "experiment": exp,
                        "date": date,
                        "scan_type": scan_type,
                        "num_files": n_files,
                    })
    except Exception as exc:
        from src.services.errors import handle  # local import, keep module importable
        return handle(
            exc,
            title="Could not load project data",
            message=(
                f"An error occurred while fetching data for project "
                f"'{project_name}'. Check your VPN connection and that "
                f"the project name is correct."
            ),
            recourse=[
                "Make sure you are connected to the UIowa VPN.",
                "Verify the project name in your configuration.",
                "Retry the operation.",
                "Contact the Data Librarian if the problem persists.",
            ],
            context=f"fetch_data_table, project={project_name}",
        )
    return rows


def filter_rows(rows: List[dict], query: str) -> List[dict]:
    """
    Case-insensitive substring filter across all column values.

    Parameters
    ----------
    rows  : list[dict] from fetch_data_table.
    query : Search string.  Empty / whitespace-only → return all rows.

    Returns
    -------
    Filtered list[dict] (original order preserved, no mutation).
    """
    q = query.strip().lower()
    if not q:
        return list(rows)
    return [
        row for row in rows
        if any(q in str(v).lower() for v in row.values())
    ]


def sort_rows(rows: List[dict], key: str) -> List[dict]:
    """
    Stable ascending sort of *rows* by column *key*.

    Unknown keys sort by str representation (no KeyError).
    """
    return sorted(rows, key=lambda r: str(r.get(key, "")))
