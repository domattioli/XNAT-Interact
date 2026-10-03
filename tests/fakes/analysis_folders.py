"""
Synthetic analysis result folders for the spec 016 tests.

Everything here is made up: synthetic DICOM frames, a download record shaped
like the spec 015 manifest, and "dirty" variants that the intake must refuse.
No real patient data, server address or account name appears in this file.
"""
from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np

from app.logic.download_manifest import build_file_entry
from tests.synthetic_data import make_phi_dicom_dataset, make_synthetic_dicom

PROJECT = "FAKE_PROJECT"
SUBJECT = "SUBJ_A"
EXPERIMENT = "EXP_A"
EXPERIMENT_QS = f"/project/{PROJECT}/subject/{SUBJECT}/experiment/{EXPERIMENT}"
SCAN_QS = f"{EXPERIMENT_QS}/scan/0"
CASE_UID = "CASEA"
USERNAME = "student_a"


@dataclass
class AnalysisCase:
    """One synthetic case: input frames, a download record and an output folder."""
    root: Path
    inputs: Path
    manifest: Path
    output: Path
    frames: List[Path]

    def entries(self) -> List[Dict[str, Any]]:
        return json.loads(self.manifest.read_text(encoding="utf-8"))["files"]


def make_frames(folder: Path, case_uid: str = CASE_UID, n_frames: int = 3) -> List[Path]:
    """Write *n_frames* synthetic DICOM frames named like a real download (``NNNN-<uid>.dcm``)."""
    folder.mkdir(parents=True, exist_ok=True)
    return [make_synthetic_dicom(folder / f"{i:04d}-{case_uid}.dcm", seed=i + 1) for i in range(n_frames)]


def synthetic_manifest(dest: Path, frames: List[Path], root: Path, *, scan_qs: str = SCAN_QS,
                       complete: bool = True, extra_entries: Optional[List[Dict[str, Any]]] = None) -> Path:
    """Write a download record shaped like the spec 015 manifest, listing *frames*."""
    files = [build_file_entry(p, root, scan_qs, "DICOM", p.name) for p in frames]
    files.extend(extra_entries or [])
    data = {
        "manifest_version": "1.0", "run_id": "0123456789abcdef0123456789abcdef",
        "finished_at": "2026-10-03T00:00:00.000000Z", "path": "folder", "scope": None,
        "project": PROJECT, "username": None, "server_url": None, "complete": complete,
        "selection": [{"subject": SUBJECT, "experiment": EXPERIMENT, "scan": "0"}],
        "files": files, "empty_scans": [],
        "server_copy": {"status": "not_attempted", "resource_label": None, "filename": None, "reason": None},
    }
    dest.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return dest


def write_analysis_json(folder: Path, type_name: str = "knee_flexion_angle", *, case_uid: str = CASE_UID,
                        notes: str = "", parameters: Optional[Dict[str, Any]] = None,
                        supersedes: Optional[str] = None) -> Path:
    path = folder / "analysis.json"
    path.write_text(json.dumps({
        "descriptor_version": "1", "type_name": type_name, "type_version": 1,
        "run": {"case_uid": case_uid, "code_ref": "git:0123abc", "parameters": parameters or {"smoothing": 3},
                "notes": notes, "supersedes": supersedes},
    }, indent=2), encoding="utf-8")
    return path


def write_angles(folder: Path, identities: List[str], *, angles: Optional[List[float]] = None) -> Path:
    path = folder / "angles.csv"
    angles = angles if angles is not None else [10.0 + 5 * i for i in range(len(identities))]
    lines = ["frame_index,image_identity_hash,angle_degrees"]
    lines += [f"{i},{h},{a}" for i, (h, a) in enumerate(zip(identities, angles))]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def clean_angles_folder(tmp: Path, n_frames: int = 3, *, case_uid: str = CASE_UID) -> AnalysisCase:
    """A clean knee_flexion_angle result: one angle row per downloaded frame."""
    tmp = Path(tmp)
    inputs = tmp / "download"
    frames = make_frames(inputs, case_uid, n_frames)
    manifest = synthetic_manifest(tmp / "download_manifest.json", frames, inputs)
    output = tmp / "results"
    output.mkdir(parents=True, exist_ok=True)
    write_analysis_json(output, case_uid=case_uid)
    case = AnalysisCase(tmp, inputs, manifest, output, frames)
    write_angles(output, [e["image_identity_hash"] for e in case.entries()])
    return case


def consensus_folder(tmp: Path, *, case_uid: str = CASE_UID, mask_dtype=np.uint8) -> AnalysisCase:
    """A segmentation_consensus result holding one whole-number mask."""
    case = clean_angles_folder(tmp, 2, case_uid=case_uid)
    (case.output / "angles.csv").unlink()
    write_analysis_json(case.output, "segmentation_consensus", case_uid=case_uid)
    np.savez(case.output / "mask.npz", mask=np.zeros((4, 4), dtype=mask_dtype))
    return case


# --- dirty variants -----------------------------------------------------------

def dirty_extra_file(tmp: Path) -> AnalysisCase:
    case = clean_angles_folder(tmp)
    (case.output / "scratch.txt").write_text("left over\n", encoding="utf-8")
    return case


def dirty_missing_output(tmp: Path) -> AnalysisCase:
    case = clean_angles_folder(tmp)
    (case.output / "angles.csv").unlink()
    return case


def dirty_copied_dicom(tmp: Path) -> AnalysisCase:
    """A source DICOM (with fake PHI) renamed to look like the result table."""
    case = clean_angles_folder(tmp)
    ds = make_phi_dicom_dataset(seed=9)
    (case.output / "angles.csv").unlink()
    ds.save_as(str(case.output / "angles.csv"))
    return case


def dirty_copied_png(tmp: Path) -> AnalysisCase:
    case = clean_angles_folder(tmp)
    (case.output / "angles.csv").write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 64)
    return case


SYNTHETIC_NAME = "Jane Q Synthetic"


def dirty_notes_name(tmp: Path) -> AnalysisCase:
    case = clean_angles_folder(tmp)
    write_analysis_json(case.output, notes=f"Ran fine.\nPatient {SYNTHETIC_NAME} moved a lot.")
    return case


def dirty_short_csv(tmp: Path) -> AnalysisCase:
    case = clean_angles_folder(tmp)
    write_angles(case.output, [e["image_identity_hash"] for e in case.entries()][:-1])
    return case


DIRTY_BUILDERS = {
    "extra_file": dirty_extra_file,
    "missing_output": dirty_missing_output,
    "copied_dicom": dirty_copied_dicom,
    "copied_png": dirty_copied_png,
    "notes_name": dirty_notes_name,
    "short_csv": dirty_short_csv,
}


def fake_name_classifier(text: str):
    """A deterministic stand-in for the PHI text classifier: flags the synthetic name only."""
    if SYNTHETIC_NAME.lower() in text.lower():
        return True, ["PERSON"]
    return False, []


# --- configuration tables double ---------------------------------------------

class _Table:
    def __init__(self, columns: List[str]) -> None:
        self.columns = list(columns)
        self.rows: List[Dict[str, Any]] = []


class LostUpdateError(Exception):
    """Same class name as the production error, so the catalog recognises it by name."""


class FakeConfigTables:
    """Just enough of ``ConfigTables`` for the catalog: tables, rows, push and pull."""

    def __init__(self, *, registered: bool = True, push_failures: Optional[List[Exception]] = None) -> None:
        self.tables: Dict[str, _Table] = {}
        self.registered = registered
        self.push_failures = list(push_failures or [])
        self.pushes = 0
        self.pulls = 0
        self.server_rows: Dict[str, List[Dict[str, Any]]] = {}

    def table_exists(self, name: str) -> bool:
        return name.upper() in self.tables

    def item_exists(self, table: str, item: str) -> bool:
        return any(r["NAME"] == item.upper() for r in self.tables[table.upper()].rows)

    def add_new_table(self, name: str, extra_column_names=None, verbose=True) -> None:
        assert self.registered, "user not registered"
        assert not self.table_exists(name), "table exists"
        self.tables[name.upper()] = _Table(["NAME", "UID", "CREATED_DATE_TIME", "CREATED_BY"] + [c.upper() for c in extra_column_names or []])

    def add_new_item(self, table: str, item_name: str, item_uid=None, extra_columns_values=None, verbose=True):
        assert self.registered, "user not registered"
        t = self.tables[table.upper()]
        if self.item_exists(table, item_name):
            return False, "exists"
        row = {"NAME": item_name.upper(), "UID": "1.2.3", "CREATED_DATE_TIME": "2026-10-03", "CREATED_BY": "1.2.4"}
        row.update({k.upper(): v for k, v in (extra_columns_values or {}).items()})
        t.rows.append(row)
        return True, "added"

    def push_to_xnat(self, verbose=True) -> bool:
        self.pushes += 1
        if self.push_failures:
            raise self.push_failures.pop(0)
        self.server_rows = {k: [dict(r) for r in t.rows] for k, t in self.tables.items()}
        return True

    def pull_from_xnat(self, write_ffn=None, verbose=True):
        self.pulls += 1
        for name, t in list(self.tables.items()):
            t.rows = [dict(r) for r in self.server_rows.get(name, [])]
        return None


def copy_case(case: AnalysisCase, dest: Path) -> Path:
    shutil.copytree(case.output, dest)
    return dest
