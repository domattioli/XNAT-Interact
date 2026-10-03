"""
Deterministic stand-in analyses for the spec 016 live tests.

``knee_angles_rows`` plays the part of a student's knee-angle code: one row per
input frame, with an angle that depends only on the frame's hash, so two runs
on the same download give the same table.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Dict, List

STUB_CODE_REF = "stub:knee_angles_v1"


def angle_for(frame_hash: str) -> float:
    """A repeatable angle between 0 and 120 degrees derived from *frame_hash*."""
    digest = hashlib.sha256(frame_hash.encode("utf-8")).digest()
    return round(int.from_bytes(digest[:4], "big") / 0xFFFFFFFF * 120.0, 3)


def knee_angles_rows(entries: List[Dict]) -> List[Dict]:
    """One row per manifest entry, in manifest order."""
    rows = []
    for i, e in enumerate(entries):
        ident = e.get("image_identity_hash") or e["sha256"]
        rows.append({"frame_index": i, "image_identity_hash": ident, "angle_degrees": angle_for(ident)})
    return rows


def write_angles_csv(folder: Path, entries: List[Dict]) -> Path:
    path = Path(folder) / "angles.csv"
    lines = ["frame_index,image_identity_hash,angle_degrees"]
    lines += [f"{r['frame_index']},{r['image_identity_hash']},{r['angle_degrees']}" for r in knee_angles_rows(entries)]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path
