"""
Synthetic surgical cases for the live-XNAT integration suite (spec 014).

Wraps the seeded ``make_*_case`` builders in ``tests/synthetic_data.py`` in a
``SyntheticSurgicalCase`` (data-model.md). Each case keeps fluoroscopy files in
``rf/`` and arthroscopy video in ``esv/`` (KNEE_2025 only), because production
publishes each folder as its own session. All data is synthetic.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

import pandas as pd

from tests.synthetic_data import (
    make_hip_2024_case,
    make_knee_2025_case,
    make_radiofluoro_2026_case,
)

CASE_NAMES: Tuple[str, ...] = ("KNEE_2025", "HIP_2024", "RADIOFLUORO_2026")

# 28 intake-form columns, mirrored from tests/stress/driver.py:300-375.
INTAKE_INDEX: Tuple[str, ...] = (
    "Case Name [Optional]",
    "Filer\nHawkID",
    "Operation\nDate",
    "Quality",
    "Institution\nName",
    "Procedure\nName",
    "Epic\nStart\nTime",
    "Epic\nEnd\nTime",
    "Side of\nPatient\nBody",
    "OR Room\nName/\nLocation",
    "Supervising\nSurgeon\nHawkID",
    "Supervising\nSurgeon\nPresence",
    "Performing\nSurgeon\nHawkID",
    "Performing\nSurgeon\n# Years\nExperience",
    "Performing\nSurgeon\n# Prior\nCases",
    "# of\nParticipating\nPerforming\nSurgeons",
    "Performer\nHawkID-Task",
    "Unusual\nFeatures",
    "Diagnotistic\nNotes",
    "Additional\nComments",
    "Skills\nAssessment\nRequested",
    "Assessor\nHawkID",
    "Additional\nAssessment\nDetails",
    "Name/\nType of\nStorage\nDevice",
    "Full Path to Data",
    "Was\nRadiology\nContacted",
    "Radiology\nContact\nDate",
    "Radiology\nContact\nTime",
)


@dataclass
class SyntheticSurgicalCase:
    """One of KNEE_2025 / HIP_2024 / RADIOFLUORO_2026 (data-model.md)."""

    name: str
    subject_label: str
    experiment_label: str
    capture_date: str
    rf_dir: Path
    esv_dir: Optional[Path]
    files: List[Path]
    rf_files: List[Path]
    esv_files: List[Path]
    phi_boxes: Dict[str, Tuple[int, int, int, int]] = field(default_factory=dict)
    phi_kind: Optional[str] = None
    series: Dict[str, str] = field(default_factory=dict)
    expected_absent_tags: Dict[str, List[str]] = field(default_factory=dict)
    expected_frames: Dict[str, int] = field(default_factory=dict)
    expected_dispositions: Dict[str, str] = field(default_factory=dict)
    raw: Dict = field(default_factory=dict)

    @property
    def case_name(self) -> str:
        return self.name

    @property
    def source_files(self) -> List[Path]:
        return self.files

    def session_dir(self, kind: str) -> Path:
        if kind == "rf":
            return self.rf_dir
        if kind == "esv" and self.esv_dir is not None:
            return self.esv_dir
        raise ValueError(f"{self.name} has no {kind!r} session folder")

    def intake_series(self, kind: str = "rf") -> pd.Series:
        """28-field intake form for one session folder (``rf`` or ``esv``)."""
        values = [
            f"{self.name}_{kind}",  # ignored by production (column excluded)
            "testuser",
            self.capture_date,
            "usable",
            "UNIVERSITY_OF_IOWA_HOSPITALS_AND_CLINICS",
            "INTRAMEDULLARY_NAIL-TIBIA",
            "08:00",
            "09:00",
            "RIGHT",
            "OR-1",
            "UNKNOWN",
            "PRESENT",
            "UNKNOWN",
            1,
            0,
            1,
            "{unknown: lead}",
            "none",
            "none",
            f"synthetic {self.name} {kind}",
            "N",
            "NOT-APPLICABLE",
            "none",
            "USB-A",
            str(self.session_dir(kind)),
            "N",
            "",
            "",
        ]
        return pd.Series(values, index=list(INTAKE_INDEX))


_BUILDERS: Dict[str, Callable[[Path], dict]] = {
    "KNEE_2025": make_knee_2025_case,
    "HIP_2024": make_hip_2024_case,
    "RADIOFLUORO_2026": make_radiofluoro_2026_case,
}


def build_case(name: str, tmp_dir: Path) -> SyntheticSurgicalCase:
    """Build case ``name`` under ``tmp_dir`` (seeded, so identical every run)."""
    d = _BUILDERS[name](Path(tmp_dir))
    return SyntheticSurgicalCase(
        name=d["case_name"],
        subject_label=d["subject_label"],
        experiment_label=d["experiment_label"],
        capture_date=d["capture_date"],
        rf_dir=d["rf_dir"],
        esv_dir=d.get("esv_dir"),
        files=list(d["source_files"]),
        rf_files=list(d["rf_files"]),
        esv_files=list(d["esv_files"]),
        phi_boxes=dict(d.get("phi_boxes", {})),
        phi_kind=d.get("phi_kind"),
        series=dict(d.get("series", {})),
        expected_absent_tags=dict(d.get("expected_absent_tags", {})),
        expected_frames=dict(d.get("expected_frames", {})),
        expected_dispositions=dict(d.get("expected_dispositions", {})),
        raw=d,
    )
