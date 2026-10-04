"""
Provenance: which inputs a result was computed from (spec 016, FR-007 to FR-009).

The preferred source is the spec 015 ``download_manifest.json`` written beside
every download.  This step never touches the network.
"""
from __future__ import annotations

import copy
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from app.logic.download_manifest import case_uid_from_filename, image_identity_of_file, sha256_of_file
from src.services import xnat_conventions as conventions
from src.services.analysis_intake.errors import refuse

MANIFEST_FILENAME = "download_manifest.json"
TOOL_VERSION = "xnat-interact-016"
_SKIP_NAMES = {"analysis.json", "analysis.yaml", MANIFEST_FILENAME, ".DS_Store"}


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


_PLURAL_TO_SINGULAR = {
    "projects": "project",
    "subjects": "subject",
    "experiments": "experiment",
    "scans": "scan",
    "resources": "resource",
}


def to_pyxnat_qs(qs: str) -> str:
    """
    Return *qs* in the singular form pyxnat selects on.

    The download manifest (spec 015) records REST-style plural paths such as
    ``/projects/P/subjects/S/experiments/E/scans/0``; pyxnat and the rest of
    this project address the same object as
    ``/project/P/subject/S/experiment/E/scan/0``. Found on the first live run:
    the plural form made the gateway report the parent experiment as missing.
    """
    parts = [p for p in str(qs).split("/") if p]
    return "/" + "/".join(_PLURAL_TO_SINGULAR.get(seg, seg) for seg in parts)


def experiment_qs_from_scan_qs(scan_qs: str) -> Optional[str]:
    """Return the experiment part of a scan query string (pyxnat form), or None if it has no scan part."""
    parts = [p for p in to_pyxnat_qs(scan_qs).split("/") if p]
    if "scan" in parts:
        idx = parts.index("scan")
        if idx >= 2:
            return "/" + "/".join(parts[:idx])
    return None


def _no_manifest_help() -> List[str]:
    return ["Download the case again with XNAT-Interact; it writes download_manifest.json beside the files.",
            "Or name the folder of input frames with --inputs so the hashes can be rebuilt."]


def _from_manifest(manifest_path: Path, case_uid: str) -> Tuple[List[Dict[str, Any]], List[str], Optional[str], Optional[str]]:
    try:
        data = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise refuse("Download record not found", f"The download record '{Path(manifest_path).name}' does not exist.",
                     _no_manifest_help())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        raise refuse("Download record could not be read",
                     f"'{Path(manifest_path).name}' is not a readable download record.", _no_manifest_help())
    if not isinstance(data, dict) or not isinstance(data.get("files"), list):
        raise refuse("Download record is not valid",
                     f"'{Path(manifest_path).name}' does not look like a download record from XNAT-Interact.",
                     _no_manifest_help())
    if data.get("complete") is not True:
        raise refuse("Download was not complete",
                     f"'{Path(manifest_path).name}' says the download did not finish, so its list of inputs cannot be trusted.",
                     ["Download the case again and let it finish, then run this command with the new record."])
    entries = [e for e in data["files"] if isinstance(e, dict) and e.get("case_uid") == case_uid]
    if not entries:
        raise refuse("Case not in the download record",
                     f"The case '{case_uid}' named in analysis.json is not in '{Path(manifest_path).name}'.",
                     ["Check case_uid in analysis.json against the downloaded file names.",
                      "Or use the download record of the download that holds this case."])
    hashes = []
    for e in entries:
        sha = e.get("sha256")
        if not isinstance(sha, str) or len(sha) != 64:
            raise refuse("Download record is not valid",
                         f"A file entry in '{Path(manifest_path).name}' has no content hash.", _no_manifest_help())
        hashes.append({"sha256": sha, "image_identity_hash": e.get("image_identity_hash"),
                       "relative_path": str(e.get("relative_path") or e.get("filename") or "")})
    refs = sorted({str(e["scan_query_string"]) for e in entries if e.get("scan_query_string")})
    experiments = sorted({experiment_qs_from_scan_qs(r) for r in refs} - {None})
    if len(experiments) > 1:
        raise refuse("Case spans several sessions",
                     f"The case '{case_uid}' appears in {len(experiments)} sessions in the download record; one result must belong to one session.",
                     ["Download only the session this result belongs to, then run the command again."])
    run_id = data.get("run_id") if isinstance(data.get("run_id"), str) else None
    return hashes, refs, (experiments[0] if experiments else None), run_id


def _from_inputs(inputs: Path, case_uid: str) -> List[Dict[str, Any]]:
    folder = Path(inputs)
    if not folder.is_dir():
        raise refuse("Input folder not found", f"The input folder '{folder}' does not exist.", _no_manifest_help())
    hashes = []
    for path in sorted(p for p in folder.rglob("*") if p.is_file()):
        if path.name in _SKIP_NAMES:
            continue
        file_case = case_uid_from_filename(path.name)
        if file_case is not None and file_case != case_uid:
            continue
        hashes.append({"sha256": sha256_of_file(path), "image_identity_hash": image_identity_of_file(path),
                       "relative_path": path.relative_to(folder).as_posix()})
    if not hashes:
        raise refuse("No input files found", f"The input folder '{folder.name}' holds no files for case '{case_uid}'.",
                     _no_manifest_help())
    return hashes


DATASET_MANIFEST_FILENAME = "dataset_manifest.json"


def _project_of(scan_qs: str) -> Optional[str]:
    parts = [p for p in to_pyxnat_qs(scan_qs).split("/") if p]
    if "project" in parts and parts.index("project") + 1 < len(parts):
        return parts[parts.index("project") + 1]
    return None


def _multi_case_provenance(run: Dict[str, Any], manifest: Path, folder: Path) -> None:
    """
    Fill the provenance of a result built from several cases (spec 018, FR-005, FR-006).

    The rows come from ``dataset_manifest.json`` in the result folder; every row
    must name a frame in the (merged) download record and a case listed in
    ``run.cases``.  Unlike a single-case result, the frames may come from many
    sessions; they must all come from one project.
    """
    manifest = Path(manifest)
    try:
        data = json.loads(manifest.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise refuse("Download record not found", f"The download record '{manifest.name}' does not exist.",
                     _no_manifest_help())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        raise refuse("Download record could not be read", f"'{manifest.name}' is not a readable download record.",
                     _no_manifest_help())
    if not isinstance(data, dict) or not isinstance(data.get("files"), list):
        raise refuse("Download record is not valid",
                     f"'{manifest.name}' does not look like a download record from XNAT-Interact.", _no_manifest_help())
    if data.get("complete") is not True:
        raise refuse("Download was not complete",
                     f"'{manifest.name}' says the download did not finish, so its list of inputs cannot be trusted.",
                     ["Download the cases again and let them finish, then run assemble-dataset again."])
    cases = run.get("cases")
    if not isinstance(cases, list) or not cases:
        raise refuse("Descriptor has no list of cases",
                     "A dataset's analysis.json must list its cases in 'run.cases'.",
                     ["Build the dataset folder with 'main.py assemble-dataset', which fills the list for you."])
    case_set = {str(c) for c in cases}
    record = {e["sha256"]: e for e in data["files"]
              if isinstance(e, dict) and isinstance(e.get("sha256"), str) and len(e["sha256"]) == 64}
    rows_path = Path(folder) / DATASET_MANIFEST_FILENAME
    try:
        rows = json.loads(rows_path.read_text(encoding="utf-8")).get("rows")
    except (OSError, UnicodeDecodeError, ValueError, AttributeError):
        rows = None
    if not isinstance(rows, list) or not rows:
        raise refuse("Dataset rows could not be read",
                     f"The folder has no readable list of rows in '{DATASET_MANIFEST_FILENAME}'.",
                     ["Build the dataset folder again with 'main.py assemble-dataset'."])
    hashes: List[Dict[str, Any]] = []
    refs = set()
    for i, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            raise refuse("Dataset row is not valid", f"Row {i} of '{DATASET_MANIFEST_FILENAME}' is not a set of fields.",
                         ["Build the dataset folder again with 'main.py assemble-dataset'."])
        if str(row.get("case_uid")) not in case_set:
            raise refuse("Dataset row names an unlisted case",
                         f"Row {i} of '{DATASET_MANIFEST_FILENAME}' belongs to case '{row.get('case_uid')}', "
                         "which analysis.json does not list in 'run.cases'.",
                         ["Build the dataset folder again with 'main.py assemble-dataset'."])
        entry = record.get(row.get("sha256"))
        if entry is None:
            raise refuse("Dataset row not in the download record",
                         f"Row {i} of '{DATASET_MANIFEST_FILENAME}' names a frame that is not in '{manifest.name}'.",
                         ["Build the dataset folder again from the download records with 'main.py assemble-dataset'."])
        hashes.append({"sha256": entry["sha256"], "image_identity_hash": entry.get("image_identity_hash"),
                       "relative_path": str(row.get("relative_path") or entry.get("relative_path") or "")})
        if entry.get("scan_query_string"):
            refs.add(str(entry["scan_query_string"]))
    projects = sorted({_project_of(r) for r in refs} - {None})
    if len(projects) != 1:
        raise refuse("Dataset spans several projects" if projects else "Unknown XNAT project",
                     f"The frames of this dataset come from {len(projects)} XNAT projects; one dataset must come from one project.",
                     ["Use only the download records of one project and run assemble-dataset again."])
    run["provenance"] = "recorded"
    run["input_refs"] = sorted(refs)
    run["manifest_run_id"] = data.get("run_id") if isinstance(data.get("run_id"), str) else None
    run["project_query_string"] = conventions.project_qs(projects[0])
    run["source_hashes"] = hashes


def fill_provenance(descriptor: Dict[str, Any], *, manifest: Optional[Path] = None, inputs: Optional[Path] = None,
                    username: Optional[str] = None, atype=None,
                    folder: Optional[Path] = None) -> Tuple[Dict[str, Any], List[str]]:
    """
    Return a copy of *descriptor* with the tool-filled run fields, plus warnings.

    When *atype* spans several cases (``multi_case``), the rows are read from
    ``dataset_manifest.json`` in *folder* (spec 018); otherwise the spec 016
    single-case rules apply unchanged.

    ``producer`` is the real XNAT username of the logged-in account, as the
    spec 015 manifest records it (FR-009, operator ruling 2026-10-03).
    """
    if not isinstance(username, str) or not username.strip():
        raise refuse("Not logged in", "The intake needs the XNAT account name to record who produced the result.",
                     ["Log in with XNAT-Interact, then run the command again."])
    desc = copy.deepcopy(descriptor)
    run = desc["run"]
    case_uid = run["case_uid"]
    warnings: List[str] = []
    if atype is not None and getattr(atype, "multi_case", False):
        # Spec 018: a result built from several cases (a training dataset).
        if manifest is None:
            raise refuse("No download record for the dataset",
                         "A dataset needs the merged download record that assemble-dataset writes into its folder.",
                         ["Build the dataset folder with 'main.py assemble-dataset', then publish that folder."])
        _multi_case_provenance(run, Path(manifest), Path(folder) if folder is not None else Path(manifest).parent)
        run["producer"] = username.strip()
        run["intake_started_at"] = _now()
        run["tool_version"] = TOOL_VERSION
        return desc, warnings
    if manifest is not None:
        hashes, refs, exp_qs, run_id = _from_manifest(Path(manifest), case_uid)
        run["provenance"] = "recorded"
        run["input_refs"] = refs
        run["manifest_run_id"] = run_id
        if exp_qs:
            run["experiment_query_string"] = exp_qs
    elif inputs is not None:
        hashes = _from_inputs(Path(inputs), case_uid)
        run["provenance"] = "reconstructed"
        run.setdefault("input_refs", [])
        run["manifest_run_id"] = None
        warnings.append("No download record was used, so the list of inputs was rebuilt from the input folder "
                        "(provenance is reconstructed, not recorded).")
    else:
        raise refuse("No record of the inputs",
                     "The intake needs to know which downloaded files this result was computed from.",
                     _no_manifest_help())
    run["source_hashes"] = hashes
    run["producer"] = username.strip()
    run["intake_started_at"] = _now()
    run["tool_version"] = TOOL_VERSION
    return desc, warnings
