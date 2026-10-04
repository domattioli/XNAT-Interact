"""
Assemble a training dataset folder from download records (spec 018, FR-011, FR-012).

A training dataset is a list of frames taken from several downloaded cases,
with the split (training, validation or test) each frame belongs to.  It holds
no images: only the content hash of each frame and where it came from.

``assemble_dataset`` reads one or more spec 015 download records and writes a
folder that ``publish-analysis`` can publish straight away:

* ``dataset_manifest.json``: one row per frame,
* ``splits.json``: the split rule and how many rows landed in each split,
* ``download_manifest.json``: all the records merged into one,
* ``analysis.json``: the filled descriptor of type ``training_dataset``.

It never contacts the server.  Every check runs before the first file is
written, so a refusal leaves no half-made folder behind.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from src.services.analysis_intake.errors import refuse
from src.services.analysis_intake.provenance import MANIFEST_FILENAME, _project_of

TYPE_NAME = "training_dataset"
DATASET_MANIFEST = "dataset_manifest.json"
SPLITS_FILE = "splits.json"
DESCRIPTOR_FILE = "analysis.json"
SPLITS = ("training", "validation", "test")
DEFAULT_SPLIT_RULE = "every_nth:3:validation"
CODE_REF = "xnat-interact assemble-dataset"

_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{0,63}$")
_SHA_RE = re.compile(r"^[0-9a-f]{64}$")
_RULE_RE = re.compile(r"^every_nth:(\d+):(validation|test)$")


@dataclass
class DatasetSummary:
    """What ``assemble_dataset`` wrote, for the plain summary on screen."""
    name: str
    folder: Path
    rows: int
    counts: Dict[str, int]
    cases: List[str]
    project: str
    files: List[str] = field(default_factory=list)


def _record_help() -> List[str]:
    return ["Download the cases again with XNAT-Interact and let each download finish.",
            "Then run assemble-dataset with the new download_manifest.json files."]


def project_of(scan_qs: str) -> Optional[str]:
    """Return the project name in a scan address (plural or singular form), or None."""
    return _project_of(scan_qs)


def parse_split_rule(rule: str) -> Tuple[int, str]:
    """Read ``every_nth:<n>:<split>``: every n-th row goes to <split>, the rest to training."""
    match = _RULE_RE.match(str(rule or "").strip())
    if not match or int(match.group(1)) < 2:
        raise refuse("Split rule not understood",
                     f"The split rule '{rule}' is not in the form every_nth:<n>:<split>.",
                     ["Use for example every_nth:3:validation (every third frame to validation, the rest to training).",
                      "n must be 2 or more, and the split must be validation or test."])
    return int(match.group(1)), match.group(2)


def _read_record(path: Path) -> Dict[str, Any]:
    """Read and check one download record; refuse in plain words when it cannot be used."""
    path = Path(path)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise refuse("Download record not found", f"The download record '{path.name}' does not exist.", _record_help())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        raise refuse("Download record could not be read", f"'{path.name}' is not a readable download record.",
                     _record_help())
    if not isinstance(data, dict) or not isinstance(data.get("files"), list) or not data["files"]:
        raise refuse("Download record is not valid",
                     f"'{path.name}' does not look like a download record from XNAT-Interact.", _record_help())
    if data.get("complete") is not True:
        raise refuse("Download was not complete",
                     f"'{path.name}' says the download did not finish, so its list of frames cannot be trusted.",
                     _record_help())
    for entry in data["files"]:
        if not isinstance(entry, dict) or not isinstance(entry.get("sha256"), str) or not _SHA_RE.match(entry["sha256"]):
            raise refuse("Download record has no content hashes",
                         f"A file entry in '{path.name}' has no content hash, so the dataset could not name its frames.",
                         _record_help())
        if not isinstance(entry.get("case_uid"), str) or not entry["case_uid"].strip():
            raise refuse("Download record has a file without a case",
                         f"A file entry in '{path.name}' does not say which case it belongs to.",
                         _record_help())
        if not isinstance(entry.get("scan_query_string"), str) or project_of(entry["scan_query_string"]) is None:
            raise refuse("Download record has a file without an address",
                         f"A file entry in '{path.name}' does not say which XNAT project and scan it came from.",
                         _record_help())
    return data


def assemble_dataset(name: str, manifests: Sequence[Path], out_dir: Path, *,
                     split_rule: str = DEFAULT_SPLIT_RULE, label_source: str = "none") -> DatasetSummary:
    """
    Build a training dataset folder in *out_dir* from the download records *manifests*.

    Rows are de-duplicated by content hash and sorted by case then file path, so
    the same records always give the same dataset and the same split.
    """
    if not isinstance(name, str) or not _NAME_RE.match(name):
        raise refuse("Dataset name not allowed",
                     f"The dataset name '{name}' cannot be used as an XNAT label.",
                     ["Use letters, digits, '_' and '-', start with a letter, at most 64 characters.",
                      "Example: knee_hip_2025"])
    every, target = parse_split_rule(split_rule)
    if not isinstance(label_source, str) or not label_source.strip() or len(label_source) > 100:
        raise refuse("Label source not allowed", "The label source must be short text of at most 100 characters.",
                     ["Leave it out to use 'none', or give a short description such as 'manual_v1'."])
    if not manifests:
        raise refuse("No download records given", "Name at least one download record with --manifest.",
                     _record_help())
    out = Path(out_dir)
    if (out / DESCRIPTOR_FILE).exists():
        raise refuse("analysis.json already exists",
                     f"The folder '{out.name}' already holds an analysis.json, so nothing was written.",
                     ["Choose a new, empty folder for the dataset."])

    records = [(Path(m), _read_record(Path(m))) for m in manifests]
    projects = sorted({project_of(e["scan_query_string"]) for _p, rec in records for e in rec["files"]})
    if len(projects) != 1:
        raise refuse("Download records name several projects",
                     f"The download records hold frames from {len(projects)} XNAT projects ({', '.join(projects)}); "
                     "one dataset must come from one project.",
                     ["Use only the download records of one project."])

    merged_files: List[Dict[str, Any]] = []
    seen = set()
    for _path, rec in records:
        for entry in rec["files"]:
            if entry["sha256"] in seen:
                continue
            seen.add(entry["sha256"])
            merged_files.append(entry)

    def rel_of(entry: Dict[str, Any]) -> str:
        return str(entry.get("relative_path") or entry.get("filename") or "")

    ordered = sorted(merged_files, key=lambda e: (e["case_uid"], rel_of(e)))
    rows: List[Dict[str, Any]] = []
    counts = {s: 0 for s in SPLITS}
    for i, entry in enumerate(ordered):
        split = target if (i + 1) % every == 0 else "training"
        counts[split] += 1
        rows.append({
            "sha256": entry["sha256"],
            "image_identity_hash": entry.get("image_identity_hash"),
            "case_uid": entry["case_uid"],
            "scan_query_string": entry["scan_query_string"],
            "relative_path": rel_of(entry),
            "label_source": label_source,
            "split": split,
        })
    cases = sorted({r["case_uid"] for r in rows})
    run_ids = [rec.get("run_id") for _p, rec in records if isinstance(rec.get("run_id"), str)]

    merged_record = dict(records[0][1])
    merged_record.update({
        "run_id": None,
        "complete": True,  # every input was checked complete above
        "merged_from": run_ids,
        "project": projects[0],
        "selection": [s for _p, rec in records for s in (rec.get("selection") or [])],
        "files": merged_files,
    })
    descriptor = {
        "descriptor_version": "1",
        "type_name": TYPE_NAME,
        "type_version": 1,
        "run": {
            "case_uid": name,
            "cases": cases,
            "code_ref": CODE_REF,
            "parameters": {"source_manifest_run_ids": run_ids, "split_rule": f"every_nth:{every}:{target}",
                           "label_source": label_source},
            "notes": "",
            "supersedes": None,
        },
    }

    out.mkdir(parents=True, exist_ok=True)
    written = {
        DATASET_MANIFEST: {"dataset_name": name, "rows": rows},
        SPLITS_FILE: {"rule": f"every_nth:{every}:{target}", "counts": counts},
        MANIFEST_FILENAME: merged_record,
        DESCRIPTOR_FILE: descriptor,
    }
    for filename, doc in written.items():
        (out / filename).write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
    return DatasetSummary(name, out, len(rows), counts, cases, projects[0], list(written))
