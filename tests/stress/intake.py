"""
Intake form construction helpers for stress testing.

Builds ORDataIntakeForm instances from synthetic DICOM directories,
mirroring the pattern from tests/integration/run_roundtrip_push.py.
"""
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.utilities import ConfigTables, XNATLogin
    from src.xnat_resource_data import ORDataIntakeForm


def make_intake(
    uid: str,
    dicom_dir: Path,
    config: ConfigTables,
    login: XNATLogin = None,
    verbose: bool = False,
) -> "ORDataIntakeForm":
    """
    Build an ORDataIntakeForm instance from a synthetic DICOM directory.

    Mirrors the pattern from tests/integration/run_roundtrip_push.py STEP 5-6.
    Constructs a minimal intake-form pd.Series with the exact column names
    and structure expected by ORDataIntakeForm, then passes it to the constructor.

    Parameters
    ----------
    uid : str
        Surgery UID (used as case name and dir identifier).
    dicom_dir : Path
        Directory containing synthesized .dcm files (full path to data).
    config : ConfigTables
        Validated ConfigTables instance.
    login : XNATLogin, optional
        Validated login (required by ORDataIntakeForm constructor).
    verbose : bool
        Whether to enable verbose output.

    Returns
    -------
    ORDataIntakeForm
        Constructed intake form ready for SourceRFSession or publish.
    """
    import pandas as pd
    from src.xnat_resource_data import ORDataIntakeForm

    # Column index from run_roundtrip_push.py STEP 5 — exact newline-separated names
    intake_series_index = [
        "Case Name [Optional]",         # col 0
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
        "Diagnotistic\nNotes",          # note: intentional typo
        "Additional\nComments",
        "Skills\nAssessment\nRequested",
        "Assessor\nHawkID",
        "Additional\nAssessment\nDetails",
        "Name/\nType of\nStorage\nDevice",
        "Full Path to Data",
        "Was\nRadiology\nContacted",
        "Radiology\nContact\nDate",
        "Radiology\nContact\nTime",
    ]

    intake_values = [
        uid,                                       # Case Name
        "testuser",                                # Filer HawkID
        "2024-01-15",                              # Operation Date
        "usable",                                  # Quality
        "UNIVERSITY_OF_IOWA_HOSPITALS_AND_CLINICS",  # Institution Name
        "INTRAMEDULLARY_NAIL-TIBIA",              # Procedure Name
        "08:00",                                   # Epic Start Time
        "09:00",                                   # Epic End Time
        "RIGHT",                                   # Side of Patient Body
        "OR-1",                                    # OR Room Name/Location
        "UNKNOWN",                                 # Supervising Surgeon HawkID
        "PRESENT",                                 # Supervising Surgeon Presence
        "UNKNOWN",                                 # Performing Surgeon HawkID
        1,                                         # Performing Surgeon # Years Experience
        0,                                         # Performing Surgeon # Prior Cases
        1,                                         # # of Participating Performing Surgeons
        "{unknown: lead}",                         # Performer HawkID-Task
        "none",                                    # Unusual Features
        "none",                                    # Diagnotistic Notes
        "none",                                    # Additional Comments
        "N",                                       # Skills Assessment Requested
        "NOT-APPLICABLE",                          # Assessor HawkID
        "none",                                    # Additional Assessment Details
        "USB-A",                                   # Name/Type of Storage Device
        str(dicom_dir),                            # Full Path to Data
        "N",                                       # Was Radiology Contacted
        "",                                        # Radiology Contact Date
        "",                                        # Radiology Contact Time
    ]

    intake_series = pd.Series(intake_values, index=intake_series_index)

    form = ORDataIntakeForm(
        config=config,
        validated_login=login,
        input_data=intake_series,
        verbose=verbose,
        write_file=True,
    )
    return form
