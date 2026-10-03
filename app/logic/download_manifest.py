"""
app/logic/download_manifest — the record written beside every download (spec 015).

What this module does, in plain words
-------------------------------------
When a student downloads cases, this module writes a small JSON file named
``download_manifest.json``.  It lists every file that was downloaded, where on
the XNAT server it came from (the scan query string and resource label), how
big it is, and a SHA-256 "fingerprint" of its bytes.  For DICOM images it also
stores the spec 009 image identity hash (a fingerprint of the pixels only).
Later, an analysis can name exactly which inputs it used by reading this file,
with no network connection.

A complete manifest is also uploaded, as a new uniquely named file, to the
project resource ``DOWNLOADS`` on the server.  That upload is "best effort":
if it fails, the download still counts as successful, the local file says the
server copy is missing, and the student sees a short note.

The file format is documented in
``specs/015-download-manifest/contracts/download-manifest.schema.json``.

Nothing here ever stores a password or session token.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

MANIFEST_VERSION = "1.0"
MANIFEST_FILENAME = "download_manifest.json"

# server_copy.status values (see data-model.md, "ServerCopy").
STATUS_PENDING = "pending"
STATUS_NOT_ATTEMPTED = "not_attempted"
STATUS_UPLOADED = "uploaded"
STATUS_FAILED = "failed"
STATUS_SKIPPED_INCOMPLETE = "skipped_incomplete"

# File names made by generate_source_image_file_name: four digits, a dash, then the case uid.
_CASE_UID_RE = re.compile(r"^\d{4}-(?P<uid>[^.]+)")

_CHUNK = 1024 * 1024


@dataclass(frozen=True)
class ManifestIdentity:
    """
    Who downloaded, and from which server.

    Both values are read at run time from the logged-in session and the user's
    configuration; they are never written into source code.  There is
    deliberately no password or token field.
    """
    username: Optional[str] = None
    server_url: Optional[str] = None


@dataclass(frozen=True)
class FileRecord:
    """One file the download wrote, with where it came from on the server."""
    path: Path
    scan_query_string: str
    resource_label: str
    filename: str


# ---------------------------------------------------------------------------
# Building entries
# ---------------------------------------------------------------------------

def sha256_of_file(path: Path) -> str:
    """Return the SHA-256 of the file's bytes, read in chunks so big files are fine."""
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(_CHUNK), b""):
            digest.update(chunk)
    return digest.hexdigest()


def image_identity_of_file(path: Path) -> Optional[str]:
    """
    Return the spec 009 image identity hash of a DICOM file, or None.

    None means "this file has no pixel identity": it is not DICOM, or its pixel
    data cannot be read.  Any problem reading the file gives None, never an error.
    """
    try:
        import pydicom
        from src.services.identity import image_content_hash
        ds = pydicom.dcmread(str(path), stop_before_pixels=False)
        return image_content_hash(ds)
    except Exception:  # noqa: BLE001 — any failure simply means "no image identity"
        return None


def case_uid_from_filename(filename: str) -> Optional[str]:
    """Return the case uid from a ``NNNN-<uid>`` file name, or None when the name does not follow it."""
    match = _CASE_UID_RE.match(Path(str(filename)).name)
    return match.group("uid") if match else None


def build_file_entry(path: Path, root: Path, scan_qs: str, resource_label: str, filename: str) -> Dict[str, Any]:
    """Describe one downloaded file as a manifest entry (see data-model.md, "FileEntry")."""
    path = Path(path)
    try:
        relative = path.resolve().relative_to(Path(root).resolve()).as_posix()
    except ValueError:
        relative = path.name
    return {
        "relative_path": relative,
        "scan_query_string": scan_qs,
        "resource_label": resource_label,
        "filename": filename,
        "size_bytes": path.stat().st_size,
        "sha256": sha256_of_file(path),
        "image_identity_hash": image_identity_of_file(path),
        "case_uid": case_uid_from_filename(filename),
    }


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(when: datetime) -> str:
    return when.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _selected_row(row: Dict[str, Any]) -> Dict[str, Any]:
    scan = str(row.get("scan_id") or "").strip() or str(row.get("scan_type") or "").strip() or None
    return {
        "subject": str(row.get("subject", "")),
        "experiment": str(row.get("experiment", "")),
        "scan": scan,
    }


def build_manifest(
    *,
    path_kind: str,
    project: str,
    selection: Iterable[Dict[str, Any]],
    records: Iterable[FileRecord],
    root: Path,
    complete: bool,
    identity: Optional[ManifestIdentity] = None,
    scope: Optional[str] = None,
    empty_scans: Iterable[str] = (),
    finished_at: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    Build the manifest as a plain dictionary (see data-model.md, "DownloadManifest").

    ``path_kind`` is ``"folder"`` or ``"zip"``.  ``root`` is the folder the
    relative paths are measured from.  When no identity is given, the username
    and server address are written as explicit nulls (FR-010).
    """
    identity = identity or ManifestIdentity()
    when = finished_at or _utc_now()
    files = [build_file_entry(r.path, root, r.scan_query_string, r.resource_label, r.filename) for r in records]
    listed_scans = {f["scan_query_string"] for f in files}
    empties = [s for s in dict.fromkeys(empty_scans) if s not in listed_scans]
    return {
        "manifest_version": MANIFEST_VERSION,
        "run_id": uuid.uuid4().hex,
        "finished_at": _iso(when),
        "path": path_kind,
        "scope": scope,
        "project": project,
        "username": identity.username or None,
        "server_url": identity.server_url or None,
        "complete": bool(complete),
        "selection": [_selected_row(r) for r in selection],
        "files": files,
        "empty_scans": empties,
        "server_copy": {
            "status": STATUS_NOT_ATTEMPTED,
            "resource_label": None,
            "filename": None,
            "reason": None,
        },
    }


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------

def manifest_bytes(manifest: Dict[str, Any]) -> bytes:
    """Return the manifest as UTF-8 JSON text, sorted and indented so it is easy to read."""
    return (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _atomic_write(dest: Path, data: bytes) -> None:
    tmp = dest.with_name(f".{dest.name}.{uuid.uuid4().hex}.tmp")
    try:
        tmp.write_bytes(data)
        os.replace(tmp, dest)
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass


def _move_aside(dest: Path) -> Optional[str]:
    """Rename an older manifest at *dest* so it is kept, and return a note for the user."""
    if not dest.exists():
        return None
    stamp = None
    try:
        stamp = json.loads(dest.read_text(encoding="utf-8")).get("finished_at")
    except Exception:  # noqa: BLE001 — an unreadable old file still gets kept
        stamp = None
    safe = re.sub(r"[^0-9A-Za-z]", "", str(stamp or "")) or uuid.uuid4().hex[:12]
    stem = dest.name[: -len(".json")] if dest.name.endswith(".json") else dest.name
    target = dest.with_name(f"{stem}.{safe}.json")
    counter = 2
    while target.exists():
        target = dest.with_name(f"{stem}.{safe}-{counter}.json")
        counter += 1
    os.replace(dest, target)
    return (
        f"An earlier download record was found in this folder. It was kept as "
        f"'{target.name}', and a new '{dest.name}' was written for this download."
    )


def write_manifest(
    manifest: Dict[str, Any], dest: Path, *, move_existing: bool = True
) -> Tuple[Optional[Path], List[str]]:
    """
    Write *manifest* to *dest* safely and return ``(path or None, notes)``.

    The file is written under a temporary name and then renamed, so a half
    written file never looks complete.  An older manifest at the same place is
    kept under a dated name (FR-009).  If writing fails, the function returns
    None and a plain-language note instead of raising (FR-011).
    """
    dest = Path(dest)
    notes: List[str] = []
    try:
        if move_existing:
            note = _move_aside(dest)
            if note:
                notes.append(note)
        _atomic_write(dest, manifest_bytes(manifest))
        return dest, notes
    except OSError as exc:
        from src.services.errors import handle as _handle
        _handle(
            exc,
            title="Could not save the download record",
            message="The files were downloaded, but the download record could not be saved.",
            recourse=["Check free disk space and write permission, then download again."],
            context=f"write_manifest, dest={dest.name}",
        )
        notes.append(
            "Your files were downloaded, but the download record (download_manifest.json) "
            "could not be saved. Check free disk space and write permission, then download "
            "again if you need the record."
        )
        return None, notes


# ---------------------------------------------------------------------------
# Server copy
# ---------------------------------------------------------------------------

def _server_copy(status: str, filename: Optional[str] = None, reason: Optional[str] = None) -> Dict[str, Any]:
    from src.services.xnat_conventions import DOWNLOADS_RESOURCE
    return {
        "status": status,
        "resource_label": DOWNLOADS_RESOURCE if status in (STATUS_PENDING, STATUS_UPLOADED, STATUS_FAILED) else None,
        "filename": filename,
        "reason": reason,
    }


def mark_pending(manifest: Dict[str, Any], filename: str) -> None:
    """Set ``server_copy`` to ``pending`` with the chosen server file name (FR-018 ordering rule)."""
    manifest["server_copy"] = _server_copy(STATUS_PENDING, filename)


def mark_status(manifest: Dict[str, Any], status: str, filename: Optional[str] = None, reason: Optional[str] = None) -> None:
    """Set the final ``server_copy`` block."""
    manifest["server_copy"] = _server_copy(status, filename, reason)


def choose_server_filename(manifest: Dict[str, Any], uploader: Any, project: str) -> str:
    """
    Pick a server file name that is not already taken in ``DOWNLOADS``.

    The base name comes from ``manifest_server_filename``; if it is taken, a
    ``-2``, ``-3`` and so on is added before ``.json``.
    """
    from src.services.xnat_conventions import DOWNLOADS_RESOURCE, manifest_server_filename, project_qs
    when = datetime.strptime(manifest["finished_at"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    base = manifest_server_filename(
        manifest.get("username"), [r["subject"] for r in manifest.get("selection", [])], when
    )
    try:
        existing = set(uploader.list_files(project_qs(project), DOWNLOADS_RESOURCE) or [])
    except Exception:  # noqa: BLE001 — the upload step reports problems itself
        existing = set()
    name, counter = base, 2
    while name in existing:
        name = f"{base[: -len('.json')]}-{counter}.json"
        counter += 1
    return name


def upload_manifest(data: bytes, filename: str, uploader: Any, project: str) -> Tuple[str, Optional[str]]:
    """
    Upload manifest bytes to ``DOWNLOADS`` under *filename* and confirm it arrived.

    Returns ``(status, reason)`` where status is ``uploaded`` or ``failed``.
    Never raises: every problem becomes a plain-language reason (Principle II).
    Uploads never overwrite (``overwrite=False``).
    """
    import tempfile
    from src.services.xnat_conventions import DOWNLOADS_RESOURCE, project_qs
    qs = project_qs(project)
    try:
        with tempfile.TemporaryDirectory() as td:
            local = Path(td) / filename
            local.write_bytes(data)
            uploader.put_file(qs, DOWNLOADS_RESOURCE, filename, str(local),
                              content="DOWNLOAD_MANIFEST", format="JSON", overwrite=False)
        listed = uploader.list_files(qs, DOWNLOADS_RESOURCE) or []
        if filename not in listed:
            return STATUS_FAILED, "The server did not list the download record after the upload."
        return STATUS_UPLOADED, None
    except Exception as exc:  # noqa: BLE001 — upload is best effort; the local record is authoritative
        from src.services.errors import handle as _handle
        _handle(
            exc,
            title="Could not store the download record on the server",
            message="The download record could not be stored on the server.",
            recourse=["Try the download again later.", "Contact the Data Librarian if this keeps happening."],
            context=f"upload_manifest, resource={DOWNLOADS_RESOURCE}",
        )
        return STATUS_FAILED, "The server copy of the download record could not be stored."


def upload_failed_note() -> str:
    return (
        "Your files were downloaded and the download record was saved on your computer, "
        "but a copy could not be stored on the server. Nothing needs to be redone; "
        "try again later or contact the Data Librarian if this keeps happening."
    )


def finalize_server_copy(
    manifest: Dict[str, Any],
    uploader: Any,
    project: str,
) -> Tuple[Optional[bytes], List[str]]:
    """
    Apply the FR-018 ordering rule to *manifest* before it is first written.

    - Incomplete download: mark ``skipped_incomplete``; nothing is uploaded.
    - No uploader: mark ``not_attempted``.
    - Otherwise: choose a server name and mark ``pending``.  The caller writes
      (and for a zip, packs) this pending version, then calls
      :func:`complete_server_copy` with the returned bytes.

    Returns ``(pending_bytes or None, notes)``.
    """
    if not manifest.get("complete"):
        mark_status(manifest, STATUS_SKIPPED_INCOMPLETE)
        return None, []
    if uploader is None:
        mark_status(manifest, STATUS_NOT_ATTEMPTED)
        return None, []
    name = choose_server_filename(manifest, uploader, project)
    mark_pending(manifest, name)
    return manifest_bytes(manifest), []


def complete_server_copy(
    manifest: Dict[str, Any],
    pending_bytes: bytes,
    uploader: Any,
    project: str,
    local_path: Optional[Path],
) -> List[str]:
    """
    Upload the pending manifest, then rewrite only the local copy with the final status.

    Returns notes for the user (empty when everything worked).
    """
    name = manifest["server_copy"]["filename"]
    status, reason = upload_manifest(pending_bytes, name, uploader, project)
    mark_status(manifest, status, name, reason)
    notes: List[str] = []
    if status != STATUS_UPLOADED:
        notes.append(upload_failed_note())
    if local_path is not None:
        _, write_notes = write_manifest(manifest, local_path, move_existing=False)
        notes.extend(write_notes)
    return notes
