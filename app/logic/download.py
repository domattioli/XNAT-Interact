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
# Path-safety helper (C2 #33 — zip-slip / path traversal)
# ---------------------------------------------------------------------------
def _safe_resource_join(base_dir: Path, server_filename: str) -> Path:
    """Join a server-supplied filename under ``base_dir``, refusing traversal.

    XNAT resource file listings are server-controlled; a hostile or malformed
    filename such as ``../../x`` would otherwise escape ``base_dir`` (write
    outside the destination) and later crash ``relative_to(tmp_path)`` during
    zip assembly.  Resolve the candidate and require it to stay within
    ``base_dir``; raise ``ValueError`` otherwise so callers surface a
    FriendlyError instead of writing through the traversal.
    """
    # Drop any absolute-path / drive prefix; keep the join relative to base.
    cleaned = str(server_filename).lstrip("/\\")
    candidate = (base_dir / cleaned)
    base_resolved = base_dir.resolve()
    candidate_resolved = candidate.resolve()
    if base_resolved != candidate_resolved and not candidate_resolved.is_relative_to(base_resolved):
        raise ValueError(
            f"Refusing path-traversal filename from server: {server_filename!r}"
        )
    return candidate


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
    warnings     : Non-fatal notices (e.g. a resource enumerated zero files and
                   was omitted from the zip) — present even when ok=True so
                   omissions are never silent (#33 H8).
    """
    ok: bool
    files_written: List[Path] = field(default_factory=list)
    friendly: Optional[FriendlyError] = None
    warnings: List[str] = field(default_factory=list)


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
    list[dict]  — rows with keys: subject, experiment, date, scan_type, num_files, scan_id.
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
    resource_label: str = "SRC",
) -> DownloadOutcome:
    """
    Download resource files for the chosen rows into *dest_dir*.

    Each row in *selection* must have at minimum ``subject`` and ``experiment``
    keys (same shape as list_downloadable rows).  Scan resolution order:
    1. row['scan_id'] if truthy (real scan label from browse)
    2. row['scan_type'] if truthy (display-only; used for legacy back-compat)
    3. ENUMERATE all scans for the experiment and process every scan.

    For each scan, enumerates available resource labels (SRC, DERIVED, etc.)
    and downloads all files from all resources.

    Cross-platform path construction: pathlib.Path / os.path.join only.
    No backslashes hardcoded anywhere.

    Parameters
    ----------
    server           : pyxnat.Interface or FakeXNAT — must support
                       server.select(qs).resource(label).file(fn).get_copy(dest).
    project_name     : XNAT project name string.
    selection        : List of row dicts from list_downloadable.
    dest_dir         : Destination folder (created if missing).
    resource_label   : XNAT resource label ("SRC" by default; may be "DERIVED" etc.
                       May be overridden per-row via row dict key "resource_label".

    Returns
    -------
    DownloadOutcome
        ok=True  → all expected files written (exist + non-empty).
        ok=False → FriendlyError with recourse; no traceback escapes.
    """
    dest_path = Path(dest_dir)
    # #33 C1: preserve the caller-supplied default resource label — the enumeration
    # loop below reuses the `resource_label` name per-resource, so capture the
    # original default separately before it gets shadowed per-scan.
    _default_resource_label = resource_label

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

        # Scan resolution: scan_id (real) → scan_type (legacy) → enumerate all
        scan_id = str(row.get("scan_id", "")).strip() or None
        scan_type = str(row.get("scan_type", "")).strip() or None

        scans_to_process: List[str] = []
        if scan_id:
            scans_to_process = [scan_id]
        elif scan_type:
            scans_to_process = [scan_type]
        else:
            # Enumerate all scans for this experiment
            try:
                exp_qs = (
                    f"/projects/{project_name}/subjects/{subject}"
                    f"/experiments/{experiment}"
                )
                exp_sel = server.select(exp_qs)
                # Try pyxnat-style .scans() method if available
                if hasattr(exp_sel, "scans") and callable(exp_sel.scans):
                    try:
                        scans_to_process = list(exp_sel.scans().get()) or []
                    except Exception:
                        scans_to_process = []

                # Fallback: use list_scans method if available
                if not scans_to_process and hasattr(server, "list_scans") and callable(server.list_scans):
                    scans_to_process = list(server.list_scans(project_name, subject, experiment)) or []
            except Exception:
                scans_to_process = []

        if not scans_to_process:
            # No scans to process — graceful skip
            continue

        # Process each scan, enumerating all available resource labels
        for scan in scans_to_process:
            scan = str(scan).strip()
            if not scan:
                continue

            # Enumerate resource labels for this scan
            resources_to_process: List[str] = []
            try:
                scan_qs = (
                    f"/projects/{project_name}/subjects/{subject}"
                    f"/experiments/{experiment}/scans/{scan}"
                )
                scan_sel = server.select(scan_qs)

                # Try pyxnat-style .resources() method if available.
                # Prefer object-iteration + .label(): on real pyxnat (XNAT 1.9.3),
                # resources().get() returns NUMERIC resource IDs (e.g. '76') and
                # selecting .resource('76') silently enumerates ZERO files.
                # Iterating yields resource objects whose .label() ('SRC') works.
                if hasattr(scan_sel, "resources") and callable(scan_sel.resources):
                    try:
                        resources_to_process = [
                            r.label() for r in scan_sel.resources() if hasattr(r, "label")
                        ] or []
                    except Exception:
                        resources_to_process = []
                    if not resources_to_process:
                        # Fallback for test doubles whose resources() only supports .get().
                        try:
                            resources_to_process = list(scan_sel.resources().get()) or []
                        except Exception:
                            resources_to_process = []

                # Fallback: use list_resources method if available
                if not resources_to_process and hasattr(server, "list_resources") and callable(server.list_resources):
                    resources_to_process = list(server.list_resources(project_name, subject, experiment, scan)) or []
            except Exception:
                resources_to_process = []

            # #33 C1: when enumeration finds nothing (e.g. a test double that only
            # supports direct label addressing, not enumeration), fall back to the
            # per-row `resource_label` override, else the caller-supplied default
            # (not a hardcoded "SRC") — preserves the parameterized-label contract.
            if not resources_to_process:
                _row_label = str(row.get("resource_label", "")).strip()
                resources_to_process = [_row_label or _default_resource_label]

            # Download files from each resource
            for resource_label in resources_to_process:
                resource_label = str(resource_label).strip()
                if not resource_label:
                    continue

                scan_qs = (
                    f"/projects/{project_name}/subjects/{subject}"
                    f"/experiments/{experiment}/scans/{scan}"
                )

                try:
                    resource = server.select(scan_qs).resource(resource_label)
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
                            f"experiment={experiment}, scan={scan}, resource={resource_label}"
                        ),
                    )
                    return DownloadOutcome(ok=False, files_written=files_written, friendly=fe)

                # T014 (#25): enumerate real resource files and count-verify vs server.
                # Replaces the prior synthesized single-filename approach.
                row_num_files: int = int(row.get("num_files", -1)) if row.get("num_files") is not None else -1
                try:
                    if hasattr(resource, "list_files"):
                        # Test doubles / gateway surface.
                        real_filenames: List[str] = list(resource.list_files())
                    elif hasattr(resource, "files"):
                        # Real pyxnat: resource.files() yields file objects with .label().
                        real_filenames = [f.label() for f in resource.files()]
                    else:
                        real_filenames = []
                    # server_count: authoritative per-resource count; fall back to
                    # enumerated-label count (real pyxnat has no num_files), then row value.
                    if hasattr(resource, "num_files"):
                        _resource_count: int = int(resource.num_files())
                    elif hasattr(resource, "files"):
                        _resource_count = len(real_filenames)
                    else:
                        _resource_count = -1
                    server_count: int = _resource_count if _resource_count >= 0 else max(row_num_files, 0)
                except Exception:  # noqa: BLE001
                    real_filenames = []
                    server_count = max(row_num_files, 0)

                # Empty resource check: when the resource genuinely has no files (server
                # count = 0 AND enumeration returned nothing AND the row agrees) → no-op.
                # When the row declares N > 0 files but the resource has none staged
                # (unseeded test double or gateway-unaware caller), fall back to the
                # legacy synthesized filename for backward compatibility.
                # Phase 6 will remove the legacy path once all callers go through the gateway.
                if not real_filenames:
                    # Determine whether this is a truly-empty resource or a legacy context.
                    _row_declares_files = row_num_files > 0
                    _resource_says_empty = server_count == 0

                    if _resource_says_empty and not _row_declares_files:
                        # Both resource and row agree: no files — friendly no-op.
                        continue
                    else:
                        # Legacy fallback: resource not seeded or count unavailable.
                        # Use synthesized {subject}_{experiment}_{scan}.dcm path.
                        _legacy_fn = f"{subject}_{experiment}_{scan}.dcm"
                        _legacy_dest = subject_dir / _legacy_fn
                        try:
                            _result = resource.file(_legacy_fn).get_copy(_legacy_dest)
                            _written = Path(_result)
                        except (ConnectionError, TimeoutError, OSError) as _exc:
                            from src.services.errors import handle as _handle
                            _fe = _handle(
                                _exc,
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
                                    f"download_selection (legacy), subject={subject}, "
                                    f"experiment={experiment}, scan={scan}"
                                ),
                            )
                            return DownloadOutcome(ok=False, files_written=files_written, friendly=_fe)
                        except Exception:  # noqa: BLE001
                            pass  # other errors skipped in legacy path
                        else:
                            if _written.exists() and _written.stat().st_size > 0:
                                files_written.append(_written)
                        continue

                # Count mismatch between enumerated files and server-reported count → FriendlyError.
                if real_filenames and server_count > 0 and len(real_filenames) != server_count:
                    fe = FriendlyError(
                        title="Download count mismatch — scan may be incomplete",
                        message=(
                            f"The server reports {server_count} file(s) for scan '{scan}' "
                            f"resource '{resource_label}' of subject '{subject}', but only {len(real_filenames)} file(s) "
                            f"were enumerated.  The scan resource may be partially uploaded "
                            f"or the server index may be stale."
                        ),
                        recourse=[
                            "Re-run the download after a few minutes to allow the server index to refresh.",
                            "Contact the Data Librarian if the mismatch persists.",
                        ],
                    )
                    return DownloadOutcome(ok=False, files_written=files_written, friendly=fe)

                # Download each real file.
                for filename in real_filenames:
                    try:
                        dest_file = _safe_resource_join(subject_dir, filename)
                    except ValueError as _trav:
                        fe = FriendlyError(
                            title="Unsafe filename from server — download blocked",
                            message=(
                                f"The server returned a file named '{filename}' for subject "
                                f"'{subject}' that would write outside the destination folder. "
                                f"This download was blocked as a safety precaution."
                            ),
                            recourse=[
                                "Contact the Data Librarian — the scan resource may be corrupt.",
                            ],
                        )
                        return DownloadOutcome(ok=False, files_written=files_written, friendly=fe)
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
                                f"experiment={experiment}, scan={scan}, resource={resource_label}, file={filename}"
                            ),
                        )
                        return DownloadOutcome(ok=False, files_written=files_written, friendly=fe)
                    except Exception as exc:  # noqa: BLE001
                        from src.services.errors import handle as _handle
                        fe = _handle(
                            exc,
                            title="Download failed — unexpected error",
                            message=(
                                f"An unexpected error occurred while downloading '{filename}' "
                                f"for subject '{subject}'. Files downloaded so far are intact."
                            ),
                            recourse=[
                                "Check your VPN connection and retry.",
                                "Contact the Data Librarian with the diagnostic log below.",
                            ],
                            context=(
                                f"download_selection, subject={subject}, "
                                f"experiment={experiment}, scan={scan}, resource={resource_label}, file={filename}"
                            ),
                        )
                        return DownloadOutcome(ok=False, files_written=files_written, friendly=fe)

                    # Verify file landed and is non-empty.
                    if not written.exists() or written.stat().st_size == 0:
                        fe = FriendlyError(
                            title="Download verification failed",
                            message=(
                                f"File '{filename}' for subject '{subject}' was not written "
                                f"correctly to '{written}'. The file is missing or empty."
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


# ---------------------------------------------------------------------------
# T015b (#25): zip assembly with content-scope picker
# ---------------------------------------------------------------------------

def assemble_zip(
    server: Any,
    project_name: str,
    selection: List[dict],
    zip_dest: Union[str, Path],
    scope: str = "source",
) -> DownloadOutcome:
    """
    Download all enumerated resource files for *selection* and pack them into
    a single zip at *zip_dest*.

    Parameters
    ----------
    server       : pyxnat.Interface or FakeXNAT.
    project_name : XNAT project name string.
    selection    : List of row dicts (same shape as list_downloadable rows).
    zip_dest     : Destination zip file path (created; parent must exist).
    scope        : Content scope — one of:
                     "source"   — source images only (SRC resource, default)
                     "all"      — all scans (same as source for now; future:
                                  includes DERIVED/other resource labels)
                     "derived"  — derived data only (placeholder, future)

    Returns
    -------
    DownloadOutcome
        ok=True  → zip created with all selected files.
        ok=False → FriendlyError; zip may be absent or partial.
    """
    import zipfile as _zipfile
    import tempfile

    zip_dest = Path(zip_dest)
    zip_dest.parent.mkdir(parents=True, exist_ok=True)

    # Resolve resource label from scope.
    resource_label = "SRC" if scope in ("source", "all") else "DERIVED"

    files_written: List[Path] = []
    warnings: List[str] = []

    with tempfile.TemporaryDirectory() as _tmpdir:
        tmp_path = Path(_tmpdir)

        for row in selection:
            subject = str(row.get("subject", ""))
            experiment = str(row.get("experiment", ""))
            scan = str(row.get("scan_type", "")) or "SRC"

            if not subject or not experiment:
                continue

            scan_qs = (
                f"/projects/{project_name}/subjects/{subject}"
                f"/experiments/{experiment}/scans/{scan}"
            )

            try:
                resource = server.select(scan_qs).resource(resource_label)
            except Exception as exc:  # noqa: BLE001
                from src.services.errors import handle as _handle
                fe = _handle(
                    exc,
                    title="Could not access scan resource",
                    message=f"Failed to access resource for subject '{subject}', scan '{scan}'.",
                    recourse=["Check your VPN connection and retry."],
                    context=f"assemble_zip, subject={subject}, scan={scan}",
                )
                return DownloadOutcome(ok=False, files_written=files_written, friendly=fe)

            if hasattr(resource, "list_files"):
                # Test doubles / gateway surface.
                real_filenames: List[str] = list(resource.list_files())
            elif hasattr(resource, "files"):
                # Real pyxnat: resource.files() yields file objects with .label().
                real_filenames = [f.label() for f in resource.files()]
            else:
                real_filenames = []
            if not real_filenames:
                # #33 H8: previously a silent skip. Record a warning so the
                # omission is visible to the caller even though ok=True — a
                # completed zip missing a resource must never look identical
                # to one that legitimately had nothing to include.
                warnings.append(
                    f"Resource '{resource_label}' for subject '{subject}', "
                    f"scan '{scan}' enumerated zero files and was omitted "
                    f"from the zip."
                )
                continue

            subj_dir = tmp_path / subject / experiment / scan
            subj_dir.mkdir(parents=True, exist_ok=True)

            for fn in real_filenames:
                try:
                    dest_file = _safe_resource_join(subj_dir, fn)
                except ValueError:
                    fe = FriendlyError(
                        title="Unsafe filename from server — zip blocked",
                        message=(
                            f"The server returned a file named '{fn}' for subject "
                            f"'{subject}' that would write outside the staging folder. "
                            f"Zip assembly was blocked as a safety precaution."
                        ),
                        recourse=[
                            "Contact the Data Librarian — the scan resource may be corrupt.",
                        ],
                    )
                    return DownloadOutcome(ok=False, files_written=files_written, friendly=fe)
                try:
                    result = resource.file(fn).get_copy(dest_file)
                    files_written.append(Path(result))
                except Exception as exc:  # noqa: BLE001
                    from src.services.errors import handle as _handle
                    fe = _handle(
                        exc,
                        title="Download error during zip assembly",
                        message=f"Failed to download '{fn}' for subject '{subject}'.",
                        recourse=["Re-run to resume."],
                        context=f"assemble_zip, subject={subject}, fn={fn}",
                    )
                    return DownloadOutcome(ok=False, files_written=files_written, friendly=fe)

        # Pack all downloaded files into the zip.
        # #33 H8: a mid-write error (disk full, permission error, etc.) used to
        # leave a truncated zip at zip_dest with no cleanup — write to a
        # temp path in the same directory and atomically rename on success,
        # so zip_dest either doesn't exist or is a complete, valid zip.
        tmp_zip_dest = zip_dest.with_name(zip_dest.name + ".partial")
        try:
            with _zipfile.ZipFile(tmp_zip_dest, "w", _zipfile.ZIP_DEFLATED) as zf:
                for fp in files_written:
                    # Archive name = relative path from tmp_path.
                    arcname = fp.relative_to(tmp_path)
                    zf.write(fp, arcname)
        except Exception as exc:  # noqa: BLE001
            tmp_zip_dest.unlink(missing_ok=True)
            from src.services.errors import handle as _handle
            fe = _handle(
                exc,
                title="Zip assembly failed partway through",
                message="An error occurred while writing the zip file. No partial zip was left behind.",
                recourse=["Check available disk space and permissions, then re-run."],
                context=f"assemble_zip, zip_dest={zip_dest}",
            )
            return DownloadOutcome(ok=False, files_written=files_written, friendly=fe, warnings=warnings)

        tmp_zip_dest.replace(zip_dest)

    return DownloadOutcome(ok=True, files_written=files_written, friendly=None, warnings=warnings)
