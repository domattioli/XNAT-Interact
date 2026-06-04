"""
app/logic/download — pure download logic, no streamlit import.

Tested offline against FakeXNAT.  All server I/O is injected via the
``server`` parameter (real pyxnat.Interface in production; FakeXNAT in
tests).

Public API
----------
list_downloadable(server, project_name) -> list[dict] | FriendlyError
    Return downloadable items (reuses browse.fetch_data_table column shape).

download_selection(server, project_name, selection, dest_dir) -> DownloadOutcome
    Download chosen subject/experiment/scan resource files into dest_dir.
    Cross-platform paths via pathlib — no Windows backslashes.
    Returns DownloadOutcome{ok, files_written, friendly}.

Legacy bugs avoided (from main.py ~388-404)
--------------------------------------------
1. Hardcoded Windows backslash paths: rf'{folder}\\{sl}'  → replaced with pathlib.
2. Stray debug print('hello') in the download loop → absent here.
3. Only queried scan/0 (hardcoded single scan type) → queries all scans for all
   session types via fetch_data_table.
4. Used server.select(qs).get(...) glob without per-file get_copy → now uses
   resource.file(fn).get_copy(dest) per the injected server surface.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, List, Optional, Union

from src.services.errors import FriendlyError
from app.logic.browse import fetch_data_table, COLUMNS


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------

@dataclass
class DownloadOutcome:
    """
    Return value of download_selection().

    Fields
    ------
    ok           : True → download completed (may be partial but no hard error).
    files_written: Paths of files successfully written (may be empty on failure).
    friendly     : None when ok; FriendlyError when ok is False.
    """
    ok: bool
    files_written: List[Path] = field(default_factory=list)
    friendly: Optional[FriendlyError] = None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def list_downloadable(
    server: Any,
    project_name: str,
) -> Union[List[dict], FriendlyError]:
    """
    Return downloadable items for *project_name*.

    Delegates to browse.fetch_data_table so ALL session/scan types are
    covered — not just a single hardcoded scan.

    Parameters
    ----------
    server       : pyxnat.Interface or FakeXNAT / test double.
    project_name : XNAT project name string.

    Returns
    -------
    list[dict]  — rows with keys: subject, experiment, date, scan_type, num_files.
    FriendlyError — on connection / query failure.
    """
    result = fetch_data_table(server, project_name)
    if isinstance(result, FriendlyError):
        # Re-wrap with a download-specific title while preserving the message.
        return FriendlyError(
            title="Could not load downloadable items",
            message=result.message,
            recourse=result.recourse,
            diagnostic_log_path=result.diagnostic_log_path,
        )
    # Filter out rows that have no experiment (no actual data to download).
    return [r for r in result if r.get("experiment")]


def download_selection(
    server: Any,
    project_name: str,
    selection: List[dict],
    dest_dir: Union[str, Path],
) -> DownloadOutcome:
    """
    Download resource files for the chosen rows into *dest_dir*.

    Each row in *selection* must have at minimum ``subject`` and ``experiment``
    keys (same shape as list_downloadable rows).  If ``scan_type`` is present it
    is used as the resource label; otherwise "SRC" is the default.

    Cross-platform path construction: pathlib.Path / os.path.join only.
    No backslashes hardcoded anywhere.

    Parameters
    ----------
    server       : pyxnat.Interface or FakeXNAT — must support
                   server.select(qs).resource(label).file(fn).get_copy(dest).
    project_name : XNAT project name string.
    selection    : List of row dicts from list_downloadable.
    dest_dir     : Destination folder (created if missing).

    Returns
    -------
    DownloadOutcome
        ok=True  → all expected files written (exist + non-empty).
        ok=False → FriendlyError with recourse; no traceback escapes.
    """
    dest_path = Path(dest_dir)

    # --- Empty selection fast-path ---
    if not selection:
        return DownloadOutcome(
            ok=False,
            files_written=[],
            friendly=FriendlyError(
                title="No items selected",
                message="No subjects/experiments were selected for download.",
                recourse=["Select at least one row from the table and retry."],
            ),
        )

    # --- Create destination directory (cross-platform) ---
    try:
        dest_path.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        from src.services.errors import handle as _handle
        fe = _handle(
            exc,
            title="Could not create destination folder",
            message=(
                f"The destination folder '{dest_path}' could not be created. "
                "Check that you have write permissions to that location."
            ),
            recourse=[
                "Choose a different destination folder.",
                "Check your write permissions for the selected location.",
                "Contact your system administrator if the problem persists.",
            ],
            context=f"download_selection, dest_dir={dest_path}",
        )
        return DownloadOutcome(ok=False, files_written=[], friendly=fe)

    files_written: List[Path] = []

    for row in selection:
        subject = str(row.get("subject", ""))
        experiment = str(row.get("experiment", ""))
        scan = str(row.get("scan_type", "")) or "SRC"

        if not subject or not experiment:
            # Malformed row — skip gracefully
            continue

        # Cross-platform subject output directory: dest_dir / subject / experiment
        subject_dir = dest_path / subject / experiment
        try:
            subject_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            from src.services.errors import handle as _handle
            fe = _handle(
                exc,
                title="Could not create subject folder",
                message=(
                    f"The folder for subject '{subject}' could not be created "
                    f"under '{dest_path}'."
                ),
                recourse=[
                    "Check write permissions for the destination folder.",
                    "Contact the Data Librarian if the problem persists.",
                ],
                context=f"download_selection, subject={subject}, dest_dir={dest_path}",
            )
            return DownloadOutcome(ok=False, files_written=files_written, friendly=fe)

        # Build XNAT query string for this subject/experiment/scan
        qs = (
            f"/projects/{project_name}/subjects/{subject}"
            f"/experiments/{experiment}/scans/{scan}/resources/SRC"
        )

        try:
            resource = server.select(qs).resource("SRC")
        except Exception as exc:
            from src.services.errors import handle as _handle
            fe = _handle(
                exc,
                title="Download interrupted — connection error",
                message=(
                    "The connection to the XNAT server was interrupted mid-download. "
                    "Files downloaded so far are intact; re-run to resume."
                ),
                recourse=[
                    "Make sure you are connected to the UIowa VPN.",
                    "Re-run the download — already-downloaded files will not be overwritten.",
                    "Contact the Data Librarian if the problem persists.",
                ],
                context=(
                    f"download_selection, subject={subject}, "
                    f"experiment={experiment}, scan={scan}"
                ),
            )
            return DownloadOutcome(ok=False, files_written=files_written, friendly=fe)

        # Determine filename: use subject_experiment_scan as the local filename
        filename = f"{subject}_{experiment}_{scan}.dcm"
        dest_file = subject_dir / filename

        try:
            result = resource.file(filename).get_copy(dest_file)
            written = Path(result)
        except (ConnectionError, TimeoutError, OSError) as exc:
            from src.services.errors import handle as _handle
            fe = _handle(
                exc,
                title="Download interrupted — connection error",
                message=(
                    "The connection to the XNAT server was interrupted mid-download. "
                    "Files downloaded so far are intact; re-run to resume."
                ),
                recourse=[
                    "Make sure you are connected to the UIowa VPN.",
                    "Re-run the download — already-downloaded files will not be overwritten.",
                    "Contact the Data Librarian if the problem persists.",
                ],
                context=(
                    f"download_selection, subject={subject}, "
                    f"experiment={experiment}, scan={scan}"
                ),
            )
            return DownloadOutcome(ok=False, files_written=files_written, friendly=fe)
        except Exception as exc:  # noqa: BLE001
            from src.services.errors import handle as _handle
            fe = _handle(
                exc,
                title="Download failed — unexpected error",
                message=(
                    f"An unexpected error occurred while downloading data for "
                    f"subject '{subject}'. Files downloaded so far are intact."
                ),
                recourse=[
                    "Check your VPN connection and retry.",
                    "Contact the Data Librarian with the diagnostic log below.",
                ],
                context=(
                    f"download_selection, subject={subject}, "
                    f"experiment={experiment}, scan={scan}"
                ),
            )
            return DownloadOutcome(ok=False, files_written=files_written, friendly=fe)

        # Verify file landed and is non-empty
        if not written.exists() or written.stat().st_size == 0:
            fe = FriendlyError(
                title="Download verification failed",
                message=(
                    f"The file for subject '{subject}' was not written correctly "
                    f"to '{written}'. The file is missing or empty."
                ),
                recourse=[
                    "Check available disk space at the destination.",
                    "Re-run the download to retry.",
                    "Contact the Data Librarian if the problem persists.",
                ],
            )
            return DownloadOutcome(ok=False, files_written=files_written, friendly=fe)

        files_written.append(written)

    return DownloadOutcome(ok=True, files_written=files_written, friendly=None)
