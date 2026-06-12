"""
Reusable stress-test driver functions.

Factored from tests/integration/run_roundtrip_push.py recipe:
  - connect() → establish & bootstrap config (steps 3, 4-pre, 4, 4b)
  - publish_surgery() → intake form + RF session + write + publish (steps 5-9)
  - server_inventory() → REST probe: subjects, experiments, scans counts
  - empty_shells() → subjects/experiments/scans with zero files
"""
from __future__ import annotations

import json
import os
import tempfile
import traceback
from pathlib import Path
from typing import Optional

import pandas as pd
import pydicom
import requests


def connect(
    url: str,
    project: str,
    user: str,
    pwd: str,
    verbose: bool = False,
) -> tuple:
    """
    Establish XNAT connection and bootstrap config if fresh project.

    Mirrors run_roundtrip_push.py steps 3 + 4-pre + 4 + 4b exactly.

    Returns
    -------
    (connection, config)
        connection: pyxnat.Interface
        config: ConfigTables

    Raises on any unrecoverable step (config seeding, connection failure).
    """
    if verbose:
        print(f"[driver.connect] url={url}, project={project}, user={user}")

    # Set env vars BEFORE importing src modules (config.py reads at load time)
    os.environ["XNAT_SERVER_URL"] = url
    os.environ["XNAT_PROJECT_NAME"] = project

    # Pre-create project on server (accept 200/201/409 responses; raise on auth failure)
    try:
        proj_url = f"{url}/data/projects/{project}"
        resp = requests.put(proj_url, auth=(user, pwd), timeout=15)
        if resp.status_code not in (200, 201, 409):
            raise RuntimeError(
                f"Failed to ensure project {project} exists: HTTP {resp.status_code}"
            )
        if verbose:
            print(f"  project {project} ready (HTTP {resp.status_code})")
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Failed to create/verify project {project}: {e}")

    from src.utilities import XNATLogin, XNATConnection, ConfigTables

    # STEP 3: XNATConnection
    login = XNATLogin(
        input_info={"URL": url, "USERNAME": user, "PASSWORD": pwd},
        verbose=verbose,
    )
    if not login.is_valid:
        raise RuntimeError(f"XNATLogin failed for {user} at {url}")

    conn = XNATConnection(
        login_info=login,
        stay_connected=True,
        verbose=verbose,
    )
    if not conn.is_verified or not conn.is_open:
        raise RuntimeError("XNATConnection not verified/open")

    if verbose:
        print(f"  conn verified={conn.is_verified}, open={conn.is_open}")

    # STEP 4-pre: bootstrap config on fresh project
    def _build_seed_config(username: str) -> str:
        """Minimal database_config.json matching _instantiate_json_file logic."""
        from src.utilities import UIDandMetaInfo

        uid_gen = UIDandMetaInfo()
        now = uid_gen.now_datetime

        def new_uid():
            return uid_gen.generate_uid()

        def make_table(names, extra_cols=None):
            rows = []
            for n in names:
                row = {
                    "NAME": n.upper(),
                    "UID": new_uid(),
                    "CREATED_DATE_TIME": now,
                    "CREATED_BY": new_uid(),
                }
                if extra_cols:
                    for ec in extra_cols:
                        row[ec.upper()] = None
                rows.append(row)
            return rows

        acq_sites = [
            "UNIVERSITY_OF_IOWA_HOSPITALS_AND_CLINICS",
            "UNIVERSITY_OF_HOUSTON",
            "AMAZON_MECHANICAL_TURK",
        ]
        groups = [
            "OPEN_REDUCTION_HIP_FRACTURE–DYNAMIC_HIP_SCREW",
            "OPEN_REDUCTION_HIP_FRACTURE–CANNULATED_HIP_SCREW",
            "CLOSED_REDUCTION_HIP_FRACTURE–CANNULATED_HIP_SCREW",
            "PERCUTANEOUS_SACROLIAC_FIXATION",
            "PEDIATRIC_SUPRACONDYLAR_HUMERUS_FRACTURE_REDUCTION_AND_PINNING",
            "OPEN_AND_PERCUTANEOUS_PILON_FRACTURES",
            "INTRAMEDULLARY_NAIL-CMN",
            "INTRAMEDULLARY_NAIL-ANTEGRADE_FEMORAL",
            "INTRAMEDULLARY_NAIL-RETROGRADE_FEMORAL",
            "INTRAMEDULLARY_NAIL-TIBIA",
            "SCAPHOID_FRACTURE",
            "SACROILIAC_SCREW",
            "SLIPPED_CAPITAL_FEMORAL_EPIPHYSIS",
            "SHOULDER_ARTHROSCOPY-PRE_DIAGNOSTIC",
            "SHOULDER_ARTHROSCOPY-POST_DIAGNOSTIC",
            "SHOULDER_ARTHROSCOPY-ROTATOR_CUFF_REPAIR",
            "SHOULDER_ARTHROSCOPY-DISTAL_CLAVICAL_RESECT/SUBACROM_DECOMP",
            "SHOULDER_ARTHROSCOPY-LABRUM",
            "SHOULDER_ARTHROSCOPY-SUPERIOR_LABRUM_ANTERIOR_TO_POSTERIOR",
            "SHOULDER_ARTHROSCOPY-OTHER",
            "KNEE_ARTHROSCOPY-PRE_DIAGNOSTIC",
            "KNEE_ARTHROSCOPY-POST_DIAGNOSTIC",
            "KNEE_ARTHROSCOPY-CARTILAGE_RESURFACING",
            "KNEE_ARTHROSCOPY-MEDIAL_PATELLA_FEMORAL_LIGAMENT",
            "KNEE_ARTHROSCOPY-MENISCAL_TRANSPLANT",
            "KNEE_ARTHROSCOPY-OTHER",
            "HIP_ARTHROSCOPY",
            "ANKLE_ARTHROSCOPY",
        ]
        registered_users = ["DMATTIOLI", "STELONG", "ADMIN", username.upper()]

        tables = {
            "REGISTERED_USERS": make_table(list(dict.fromkeys(registered_users))),
            "ACQUISITION_SITES": make_table(acq_sites),
            "GROUPS": make_table(groups),
            "SUBJECTS": [r | {"ACQUISITION_SITE": None, "GROUP": None} for r in []],
            "IMAGE_HASHES": [
                r | {"SUBJECT": None, "INSTANCE_NUM": None} for r in []
            ],
            "SURGEONS": [
                {
                    "NAME": "UNKNOWN",
                    "UID": new_uid(),
                    "CREATED_DATE_TIME": now,
                    "CREATED_BY": new_uid(),
                    "FIRST_NAME": "UNKNOWN",
                    "LAST_NAME": "UNKNOWN",
                    "MIDDLE_INITIAL": "",
                },
                {
                    "NAME": "NOT-APPLICABLE",
                    "UID": new_uid(),
                    "CREATED_DATE_TIME": now,
                    "CREATED_BY": new_uid(),
                    "FIRST_NAME": "NOT-APPLICABLE",
                    "LAST_NAME": "NOT-APPLICABLE",
                    "MIDDLE_INITIAL": "NA",
                },
            ],
        }
        metadata = {
            "CREATED": now,
            "LAST_MODIFIED": now,
            "CREATED_BY": new_uid(),
            "TABLE_EXTRA_COLUMNS": {
                "SUBJECTS": ["ACQUISITION_SITE", "GROUP"],
                "IMAGE_HASHES": ["SUBJECT", "INSTANCE_NUM"],
                "SURGEONS": ["FIRST_NAME", "LAST_NAME", "MIDDLE_INITIAL"],
            },
        }
        return json.dumps({"metadata": metadata, "tables": tables}, indent=2)

    config_seeded = False
    try:
        proj = conn.server.select.project(project)
        config_resource = proj.resource("config")
        config_file_handle = config_resource.file("database_config.json")
        already_exists = config_file_handle.exists()

        if not already_exists:
            if verbose:
                print(f"  [4-pre] seeding config on fresh project")
            seed_json = _build_seed_config(user)
            seed_path = Path(tempfile.mktemp(suffix=".json", prefix="seed_config_"))
            seed_path.write_text(seed_json, encoding="utf-8")

            config_file_handle.put(
                str(seed_path),
                content="META_DATA",
                format="JSON",
                tags="DOC",
                overwrite=True,
            )
            seed_path.unlink(missing_ok=True)
            config_seeded = True
        else:
            config_seeded = True

        if verbose:
            print(f"  config seeded={config_seeded}")
    except Exception as e:
        raise RuntimeError(f"config seeding failed: {e}")

    # STEP 4: ConfigTables
    if not config_seeded:
        raise RuntimeError("config not seeded")

    cfg = ConfigTables(login_info=login, xnat_connection=conn, verbose=verbose)
    if verbose:
        print(f"  ConfigTables loaded, tables: {cfg.list_of_all_tables()}")

    # STEP 4b: check/seed admin user registration
    is_registered = cfg.is_user_registered(user)
    if not is_registered:
        if verbose:
            print(f"  [4b] registering user {user}")
        cfg.add_new_item("REGISTERED_USERS", user)
        cfg.push_to_xnat(verbose=False)

    return (conn, cfg, login)


def publish_surgery(
    connection,
    config,
    login,
    dicom_dir: Path,
    uid: str,
    verbose: bool = False,
) -> dict:
    """
    Build intake form, SourceRFSession, and publish to XNAT.

    Mirrors run_roundtrip_push.py steps 5-9 for arbitrary DICOM dir + uid.

    Parameters
    ----------
    connection
        pyxnat Interface (conn.server must be open).
    config
        ConfigTables instance.
    login
        XNATLogin instance (validated login).
    dicom_dir : Path
        Directory containing synthesized .dcm files.
    uid : str
        Surgery UID (case name).
    verbose : bool
        Enable verbose output.

    Returns
    -------
    dict
        {'uid': str, 'ok': bool, 'error': str|None, 'seconds': float, 'n_files': int}
        All exceptions caught and recorded; never raises.
    """
    import time

    from src.xnat_experiment_data import ReviewDecision, SourceRFSession
    from src.xnat_resource_data import ORDataIntakeForm

    start_time = time.time()
    result = {"uid": uid, "ok": False, "error": None, "seconds": 0.0, "n_files": 0}

    # REFRESH: ConfigTables must pull latest state from XNAT before processing new surgery.
    # Previous surgeries' hashes are now on XNAT; without refresh, config's in-memory cache
    # will mark new images as duplicates. Reload from XNAT to get fresh state.
    try:
        config.pull_from_xnat(verbose=verbose)
    except Exception as e:
        if verbose:
            print(f"  [refresh] failed (non-blocking): {e}")

    try:
        dicom_dir = Path(dicom_dir)
        n_files = len(list(dicom_dir.glob("*.dcm")))
        result["n_files"] = n_files

        if verbose:
            print(f"  [publish_surgery] uid={uid}, n_files={n_files}")

        # STEP 5: build intake series
        from src.utilities import XNATLogin

        intake_series_index = [
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
        ]

        intake_values = [
            uid,
            "testuser",
            "2024-01-15",
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
            "none",
            "N",
            "NOT-APPLICABLE",
            "none",
            "USB-A",
            str(dicom_dir),
            "N",
            "",
            "",
        ]

        intake_series = pd.Series(intake_values, index=intake_series_index)

        # STEP 6: ORDataIntakeForm
        form = ORDataIntakeForm(
            config=config,
            validated_login=login,
            input_data=intake_series,
            verbose=verbose,
            write_file=True,
        )
        if verbose:
            print(f"  form.uid={form.uid}, saved_ffn={form.saved_ffn}")

        # STEP 7: SourceRFSession
        rf = SourceRFSession(intake_form=form, config=config)
        if not rf.is_valid:
            result["error"] = "SourceRFSession is_valid=False"
            return result

        if verbose:
            print(f"  rf.is_valid=True, shape={rf.df.shape}")

        # STEP 8: rf.write()
        zipped_data, cfg = rf.write(config=config, verbose=verbose)
        if verbose:
            print(f"  write() returned, zipped_data keys: {list(zipped_data.keys())}")

        # STEP 9: rf.publish_to_xnat()
        def auto_confirmer(context_str):
            return (ReviewDecision.CONFIRMED, None)

        rf.publish_to_xnat(
            xnat_connection=connection,
            validated_login=login,
            zipped_data=zipped_data,
            pixel_review_confirmer=auto_confirmer,
            verbose=verbose,
        )

        result["ok"] = True
        if verbose:
            print(f"  publish_to_xnat() succeeded")

    except Exception as e:
        result["error"] = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
        if verbose:
            print(f"  exception: {type(e).__name__}: {e}")

    result["seconds"] = time.time() - start_time
    return result


def server_inventory(url: str, user: str, pwd: str, project: str) -> dict:
    """
    REST probe: count subjects, experiments, and file counts.

    Uses basic auth + JSON endpoints; returns structure:
    {
        'subjects_count': int,
        'experiments_count': int,
        'per_subject_file_counts': {subject_id: int, ...},
    }

    Raises on connection/auth failure.
    """
    auth = (user, pwd)

    def curl_get(path: str):
        """Fetch JSON from XNAT REST."""
        url_full = f"{url}{path}"
        try:
            resp = requests.get(url_full, auth=auth, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except Exception:
            return None

    # Subjects
    subjects_data = curl_get(f"/data/projects/{project}/subjects?format=json")
    subjects_count = 0
    subject_ids = []
    if isinstance(subjects_data, dict):
        results = subjects_data.get("ResultSet", {}).get("Result", [])
        subjects_count = len(results)
        subject_ids = [s.get("ID") for s in results]

    # Experiments
    experiments_data = curl_get(f"/data/projects/{project}/experiments?format=json")
    experiments_count = 0
    if isinstance(experiments_data, dict):
        results = experiments_data.get("ResultSet", {}).get("Result", [])
        experiments_count = len(results)

    # Per-subject file counts
    per_subject_file_counts = {}
    for subj_id in subject_ids:
        count = 0
        try:
            subj_data = curl_get(f"/data/subjects/{subj_id}?format=json")
            if isinstance(subj_data, dict):
                # Count resources → scans → files
                resources = curl_get(f"/data/subjects/{subj_id}/resources?format=json")
                if isinstance(resources, dict):
                    for res in resources.get("ResultSet", {}).get("Result", []):
                        res_label = res.get("label")
                        files_data = curl_get(
                            f"/data/subjects/{subj_id}/resources/{res_label}/files?format=json"
                        )
                        if isinstance(files_data, dict):
                            count += len(
                                files_data.get("ResultSet", {}).get("Result", [])
                            )
        except Exception:
            pass
        per_subject_file_counts[subj_id] = count

    return {
        "subjects_count": subjects_count,
        "experiments_count": experiments_count,
        "per_subject_file_counts": per_subject_file_counts,
    }


def empty_shells(inventory: dict) -> list:
    """
    Return list of subject IDs that exist but have zero files.

    Query the inventory dict for per_subject_file_counts entries with value 0.

    Parameters
    ----------
    inventory : dict
        Output from server_inventory().

    Returns
    -------
    list
        Subject IDs with zero files.
    """
    per_subject = inventory.get("per_subject_file_counts", {})
    return [subj_id for subj_id, count in per_subject.items() if count == 0]
