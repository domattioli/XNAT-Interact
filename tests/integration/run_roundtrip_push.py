"""
Integration harness — real XNAT push round-trip.

Reads creds/URL exclusively from environment variables:
    XNAT_SERVER_URL      (default: http://localhost:8080)
    XNAT_PROJECT_NAME    (default: ITEST_RF)
    XNAT_USERNAME        (default: admin)
    XNAT_PASSWORD        (default: admin)

Run from repo root:
    XNAT_SERVER_URL=http://localhost:8080 \
    XNAT_PROJECT_NAME=ITEST_RF \
    XNAT_USERNAME=admin \
    XNAT_PASSWORD=admin \
    python tests/integration/run_roundtrip_push.py

FINDINGS documented inline as "FINDING:".
DO NOT modify any file under src/ or app/.
"""
from __future__ import annotations

import os
import sys
import json
import traceback
import tempfile
import subprocess
from pathlib import Path
from datetime import datetime

# ---------------------------------------------------------------------------
# Logging — both stdout and a .log file
# ---------------------------------------------------------------------------
LOG_FILE = Path(__file__).parent / "roundtrip_push.log"
_log_fh = open(LOG_FILE, "w", buffering=1)


def log(msg: str = "") -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    _log_fh.write(line + "\n")


def log_exc(label: str) -> None:
    tb = traceback.format_exc()
    log(f"EXCEPTION at '{label}':\n{tb}")


# ---------------------------------------------------------------------------
# Environment / credentials
# ---------------------------------------------------------------------------
XNAT_URL      = os.environ.get("XNAT_SERVER_URL", "http://localhost:8080")
PROJECT_NAME  = os.environ.get("XNAT_PROJECT_NAME", "ITEST_RF")
USERNAME      = os.environ.get("XNAT_USERNAME", "admin")
PASSWORD      = os.environ.get("XNAT_PASSWORD", "admin")

log("=" * 70)
log("XNAT round-trip push harness")
log(f"  URL:     {XNAT_URL}")
log(f"  Project: {PROJECT_NAME}")
log(f"  User:    {USERNAME}")
log("=" * 70)

# Must be set BEFORE any src import — config.py reads env at module-load time
os.environ["XNAT_SERVER_URL"]   = XNAT_URL
os.environ["XNAT_PROJECT_NAME"] = PROJECT_NAME

# ---------------------------------------------------------------------------
# Synthetic DICOM data directory
# ---------------------------------------------------------------------------
SYNTH_DCM_DIR = Path(tempfile.mkdtemp(prefix="xnat_it_dcm_"))
log(f"Synthetic DICOM dir: {SYNTH_DCM_DIR}")

sys.path.insert(0, str(Path(__file__).parent.parent.parent))  # repo root

# ---------------------------------------------------------------------------
# STEP 0 — create synthetic DICOMs
# ---------------------------------------------------------------------------
log("\n--- STEP 0: create synthetic DICOMs ---")
# FINDING: make_synthetic_dicom from tests/synthetic_data.py does NOT set
# InstanceNumber.  SourceRFSession._mine_session_metadata (xnat_experiment_data.py:506)
# accesses dicom_obj.metadata.InstanceNumber directly without a hasattr guard,
# causing AttributeError on any DICOM lacking that field.  This is a bug in the
# original src code.  Harness workaround: write DICOMs directly with InstanceNumber.
try:
    from tests.synthetic_data import make_phi_dicom_dataset
    import pydicom
    for i in range(3):
        p = SYNTH_DCM_DIR / f"frame_{i:03d}.dcm"
        ds = make_phi_dicom_dataset(rows=16, cols=16, seed=i)
        ds.InstanceNumber = str(i + 1)   # required by _mine_session_metadata:506
        ds.StudyDate = "20240115"
        ds.StudyTime = "080000"
        ds.StudyInstanceUID = pydicom.uid.generate_uid()
        ds.SeriesInstanceUID = pydicom.uid.generate_uid()
        ds.SOPInstanceUID = pydicom.uid.generate_uid()
        ds.NumberOfStudyRelatedInstances = 3
        ds.save_as(str(p))
        log(f"  wrote {p} (InstanceNumber={ds.InstanceNumber})")
    log("STEP 0: OK")
except Exception:
    log_exc("step 0 — make_synthetic_dicom")
    log("STEP 0: FAILED — cannot continue without synthetic data")
    sys.exit(1)

# ---------------------------------------------------------------------------
# STEP 1 — import src modules
# ---------------------------------------------------------------------------
log("\n--- STEP 1: import src modules ---")
try:
    from src.services.config import AppConfig
    config_obj = AppConfig.load()
    RESOLVED_URL = config_obj.server_url
    log(f"  AppConfig.server_url (resolved xnat_project_url): '{RESOLVED_URL}'")
    log(f"  AppConfig.project_name: '{config_obj.project_name}'")
    log("STEP 1: OK")
except Exception:
    log_exc("step 1 — import src")
    log("STEP 1: FAILED — cannot continue")
    sys.exit(1)

# ---------------------------------------------------------------------------
# STEP 2 — XNATLogin
# ---------------------------------------------------------------------------
log("\n--- STEP 2: XNATLogin ---")
login = None
try:
    from src.utilities import XNATLogin
    login = XNATLogin(
        input_info={"URL": RESOLVED_URL, "USERNAME": USERNAME, "PASSWORD": PASSWORD},
        verbose=True,
    )
    log(f"  login.is_valid = {login.is_valid}")
    log("STEP 2: OK")
except Exception:
    log_exc("step 2 — XNATLogin")
    log("STEP 2: FAILED")

# ---------------------------------------------------------------------------
# STEP 3 — XNATConnection
# ---------------------------------------------------------------------------
log("\n--- STEP 3: XNATConnection ---")
conn = None
if login and login.is_valid:
    try:
        from src.utilities import XNATConnection
        conn = XNATConnection(login_info=login, stay_connected=True, verbose=True)
        log(f"  conn.is_verified = {conn.is_verified}")
        log(f"  conn.is_open     = {conn.is_open}")
        log("STEP 3: OK")
    except Exception:
        log_exc("step 3 — XNATConnection")
        log("STEP 3: FAILED")
else:
    log("STEP 3: SKIPPED (no valid login)")

# ---------------------------------------------------------------------------
# STEP 4-pre — harness bootstrap: pre-seed config on server if absent
#
# FINDING: ConfigTables.__init__ catches pull_from_xnat failure to detect
# "first-time setup" but only treats FileNotFoundError / KeyError / ValueError
# as first-run indicators (utilities.py:634).  pyxnat raises
# pyxnat.core.errors.DataError("Cannot get file: does not exists") which is
# NOT in that tuple, so the first-run path never fires — ConfigTables always
# re-raises the DataError on a fresh project instead of self-initialising.
#
# A second blocker: _instantiate_json_file() calls
# _validate_login_for_important_functions(assert_librarian=True) which asserts
# accessor_username.lower() in project_owner (utilities.py:847).
# project_owner is hard-coded to ['dmattioli','domattioli','stelong']
# (utilities.py:29).  User 'admin' is not in that list, so even if the
# DataError were treated as first-run the assertion would fail with:
#   AssertionError: Only user(s) ['dmattioli','domattioli','stelong'] can push
#   config file to the xnat server.
#
# HARNESS WORKAROUND (does NOT modify src/): we build the minimal config JSON
# in memory (mimicking what _instantiate_json_file + _initialize_tables would
# produce) and PUT it to the server via pyxnat directly, then register 'admin'
# as a user.  ConfigTables.pull_from_xnat() will then succeed.
# ---------------------------------------------------------------------------
log("\n--- STEP 4-pre: bootstrap config on server if absent ---")

def _build_seed_config(username: str) -> str:
    """
    Construct the minimal database_config.json that ConfigTables expects.
    Mirrors what _instantiate_json_file + _initialize_tables would create.
    """
    import pandas as pd
    from src.utilities import UIDandMetaInfo
    uid_gen = UIDandMetaInfo()
    now = uid_gen.now_datetime

    def new_uid():
        return uid_gen.generate_uid()

    def make_table(names, extra_cols=None):
        rows = []
        for n in names:
            row = {"NAME": n.upper(), "UID": new_uid(), "CREATED_DATE_TIME": now, "CREATED_BY": new_uid()}
            if extra_cols:
                for ec in extra_cols:
                    row[ec.upper()] = None
            rows.append(row)
        return rows

    acq_sites = ["UNIVERSITY_OF_IOWA_HOSPITALS_AND_CLINICS", "UNIVERSITY_OF_HOUSTON", "AMAZON_MECHANICAL_TURK"]
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
    surgeons_names = ["UNKNOWN", "NOT-APPLICABLE", "KARAMM", "KOWALSKIH"]
    registered_users = ["DMATTIOLI", "STELONG", "ADMIN", username.upper()]

    tables = {
        "REGISTERED_USERS": make_table(list(dict.fromkeys(registered_users))),
        "ACQUISITION_SITES": make_table(acq_sites),
        "GROUPS": make_table(groups),
        "SUBJECTS": [r | {"ACQUISITION_SITE": None, "GROUP": None} for r in []],
        "IMAGE_HASHES": [r | {"SUBJECT": None, "INSTANCE_NUM": None} for r in []],
        "SURGEONS": [
            {"NAME": "UNKNOWN", "UID": new_uid(), "CREATED_DATE_TIME": now, "CREATED_BY": new_uid(),
             "FIRST_NAME": "UNKNOWN", "LAST_NAME": "UNKNOWN", "MIDDLE_INITIAL": ""},
            {"NAME": "NOT-APPLICABLE", "UID": new_uid(), "CREATED_DATE_TIME": now, "CREATED_BY": new_uid(),
             "FIRST_NAME": "NOT-APPLICABLE", "LAST_NAME": "NOT-APPLICABLE", "MIDDLE_INITIAL": "NA"},
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
        }
    }
    return json.dumps({"metadata": metadata, "tables": tables}, indent=2)


config_seeded = False
if conn and conn.is_verified and conn.is_open:
    try:
        # Check if config file already exists
        proj = conn.server.select.project(PROJECT_NAME)
        config_resource = proj.resource("config")
        config_file_handle = config_resource.file("database_config.json")
        already_exists = config_file_handle.exists()
        log(f"  config/database_config.json exists on server: {already_exists}")

        if not already_exists:
            log("  FINDING: config/database_config.json absent on fresh project.")
            log("  FINDING: ConfigTables.__init__ cannot self-initialize due to two bugs:")
            log("    Bug 1 (utilities.py:634): pyxnat.DataError not in first-run exception tuple;")
            log("           DataError is re-raised instead of triggering first-time setup path.")
            log("    Bug 2 (utilities.py:704): _instantiate_json_file asserts accessor_username")
            log("           in project_owner (['dmattioli','domattioli','stelong']); 'admin' fails.")
            log("  HARNESS WORKAROUND: seeding minimal config JSON via pyxnat directly.")

            seed_json = _build_seed_config(USERNAME)
            seed_path = Path(tempfile.mktemp(suffix=".json", prefix="seed_config_"))
            seed_path.write_text(seed_json, encoding="utf-8")
            log(f"  seed config written locally to: {seed_path}")

            config_file_handle.put(
                str(seed_path),
                content="META_DATA",
                format="JSON",
                tags="DOC",
                overwrite=True,
            )
            seed_path.unlink(missing_ok=True)
            log("  config/database_config.json seeded on XNAT server")
            config_seeded = True
        else:
            log("  config already present — no seeding needed")
            config_seeded = True

        log("STEP 4-pre: OK")
    except Exception:
        log_exc("step 4-pre — bootstrap config")
        log("STEP 4-pre: FAILED")
else:
    log("STEP 4-pre: SKIPPED (no open+verified connection)")

# ---------------------------------------------------------------------------
# STEP 4 — ConfigTables
# ---------------------------------------------------------------------------
log("\n--- STEP 4: ConfigTables ---")
cfg = None
if config_seeded and conn and conn.is_open:
    try:
        from src.utilities import ConfigTables
        cfg = ConfigTables(login_info=login, xnat_connection=conn, verbose=True)
        log(f"  Tables loaded: {cfg.list_of_all_tables()}")
        log(f"  Registered users: {cfg.list_of_all_items_in_table('REGISTERED_USERS')}")
        log("STEP 4: OK")
    except Exception:
        log_exc("step 4 — ConfigTables")
        log("STEP 4: FAILED")
else:
    log("STEP 4: SKIPPED (config not seeded or connection not open)")

# ---------------------------------------------------------------------------
# STEP 4b — check/seed admin user registration
# ---------------------------------------------------------------------------
if cfg is not None:
    log("\n--- STEP 4b: check/seed admin user registration ---")
    is_registered = cfg.is_user_registered(USERNAME)
    log(f"  is_user_registered('{USERNAME}') = {is_registered}")
    if not is_registered:
        log(f"  '{USERNAME}' not in REGISTERED_USERS — seeding manually via add_new_item")
        try:
            cfg.add_new_item("REGISTERED_USERS", USERNAME)
            log(f"  '{USERNAME}' added to REGISTERED_USERS")
            cfg.push_to_xnat(verbose=False)
            log("  pushed updated config to XNAT")
        except Exception:
            log_exc("step 4b — add admin user")
            log("STEP 4b: partial failure — admin not registered")

# ---------------------------------------------------------------------------
# STEP 5 — Build intake form pd.Series
# ---------------------------------------------------------------------------
log("\n--- STEP 5: build intake-form pd.Series ---")
import pandas as pd

# _read_from_series expects EXACTLY 27 columns after col-0 "Case Name [Optional]".
# Column names use the newline-separator format from the Excel template (xnat_resource_data.py:198).
# The mapping logic uses difflib.get_close_matches (cutoff=0.25), so the harness
# uses the EXACT expected names to guarantee a perfect mapping.
INTAKE_SERIES_INDEX = [
    "Case Name [Optional]",         # col 0 — excluded from mapping (sliced off)
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
    "Diagnotistic\nNotes",          # note: intentional typo matching original code
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

# Procedure must be exactly as seeded in Groups table (after uppercase).
# "Intramedullary_nail-Tibia" → stored uppercase = "INTRAMEDULLARY_NAIL-TIBIA"
PROCEDURE_NAME = "INTRAMEDULLARY_NAIL-TIBIA"
INSTITUTION    = "UNIVERSITY_OF_IOWA_HOSPITALS_AND_CLINICS"

INTAKE_VALUES = [
    "CASE-001",          # Case Name [Optional]
    USERNAME,            # Filer HawkID
    "2024-01-15",        # Operation Date
    "usable",            # Quality
    INSTITUTION,         # Institution Name
    PROCEDURE_NAME,      # Procedure Name
    "08:00",             # Epic Start Time
    "09:00",             # Epic End Time
    "RIGHT",             # Side of Patient Body
    "OR-1",              # OR Room Name/Location
    "UNKNOWN",           # Supervising Surgeon HawkID
    "PRESENT",           # Supervising Surgeon Presence
    "UNKNOWN",           # Performing Surgeon HawkID
    1,                   # Performing Surgeon # Years Experience (int)
    0,                   # Performing Surgeon # Prior Cases (int)
    1,                   # # of Participating Performing Surgeons (int)
    "{unknown: lead}",   # Performer HawkID-Task
    "none",              # Unusual Features
    "none",              # Diagnotistic Notes
    "none",              # Additional Comments
    "N",                 # Skills Assessment Requested
    "NOT-APPLICABLE",    # Assessor HawkID
    "none",              # Additional Assessment Details
    "USB-A",             # Name/Type of Storage Device
    str(SYNTH_DCM_DIR),  # Full Path to Data
    "N",                 # Was Radiology Contacted
    "",                  # Radiology Contact Date
    "",                  # Radiology Contact Time
]

intake_series = pd.Series(INTAKE_VALUES, index=INTAKE_SERIES_INDEX)
log(f"  Series built with {len(intake_series)} entries")
log(f"  Full Path to Data = {SYNTH_DCM_DIR}")
log("STEP 5: OK")

# ---------------------------------------------------------------------------
# STEP 6 — ORDataIntakeForm
# ---------------------------------------------------------------------------
log("\n--- STEP 6: ORDataIntakeForm ---")
form = None
if cfg is not None:
    try:
        from src.xnat_resource_data import ORDataIntakeForm
        form = ORDataIntakeForm(
            config=cfg,
            validated_login=login,
            input_data=intake_series,
            verbose=True,
            write_file=True,
        )
        log(f"  form.uid              = {form.uid}")
        log(f"  form.institution_name = {form.institution_name}")
        log(f"  form.group            = {form.group}")
        log(f"  form.saved_ffn        = {form.saved_ffn}")
        log("STEP 6: OK")
    except Exception:
        log_exc("step 6 — ORDataIntakeForm")
        log("STEP 6: FAILED")
else:
    log("STEP 6: SKIPPED (no ConfigTables)")

# ---------------------------------------------------------------------------
# STEP 7 — SourceRFSession
# ---------------------------------------------------------------------------
log("\n--- STEP 7: SourceRFSession ---")
rf = None
if form is not None and cfg is not None:
    try:
        from src.xnat_experiment_data import SourceRFSession
        rf = SourceRFSession(intake_form=form, config=cfg)
        log(f"  rf.is_valid = {rf.is_valid}")
        log(f"  rf.df shape = {rf.df.shape}")
        if "IS_VALID" in rf.df.columns:
            log(f"  valid rows  = {rf.df['IS_VALID'].sum()}")
        log("STEP 7: OK" if rf.is_valid else "STEP 7: session is_valid=False")
    except Exception:
        log_exc("step 7 — SourceRFSession")
        log("STEP 7: FAILED")
else:
    log("STEP 7: SKIPPED (no form or config)")

# ---------------------------------------------------------------------------
# STEP 8 — rf.write()
# ---------------------------------------------------------------------------
log("\n--- STEP 8: rf.write() ---")
zipped_data = None
if rf is not None and rf.is_valid and cfg is not None:
    try:
        zipped_data, cfg = rf.write(config=cfg, verbose=True)
        log(f"  zipped_data keys: {list(zipped_data.keys())}")
        log("STEP 8: OK")
    except Exception:
        log_exc("step 8 — rf.write()")
        log("STEP 8: FAILED")
else:
    log("STEP 8: SKIPPED")

# ---------------------------------------------------------------------------
# STEP 9 — rf.publish_to_xnat()
# ---------------------------------------------------------------------------
log("\n--- STEP 9: rf.publish_to_xnat() ---")
if rf is not None and zipped_data is not None and conn is not None and conn.is_open:
    try:
        from src.xnat_experiment_data import ReviewDecision

        def auto_confirmer(context_str):
            log(f"  [PHI gate] auto-confirming for: {context_str}")
            return (ReviewDecision.CONFIRMED, None)

        rf.publish_to_xnat(
            xnat_connection=conn,
            validated_login=login,
            zipped_data=zipped_data,
            pixel_review_confirmer=auto_confirmer,
            verbose=True,
        )
        log("STEP 9: OK — publish_to_xnat returned without exception")
    except Exception:
        log_exc("step 9 — rf.publish_to_xnat()")
        log("STEP 9: FAILED")
else:
    log("STEP 9: SKIPPED")

# ---------------------------------------------------------------------------
# STEP 10 — REST probe: what actually landed on the server
# ---------------------------------------------------------------------------
log("\n--- STEP 10: REST probe via curl ---")


def curl_get(path: str) -> dict | str:
    url = f"{XNAT_URL}{path}"
    try:
        result = subprocess.run(
            ["curl", "-s", "-u", f"{USERNAME}:{PASSWORD}", url],
            capture_output=True, text=True, timeout=15,
        )
        raw = result.stdout.strip()
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return raw
    except Exception as exc:
        return f"ERROR: {exc}"


def log_json(label: str, data) -> None:
    log(f"\n  [{label}]")
    if isinstance(data, dict):
        log(f"  {json.dumps(data, indent=2)[:3000]}")
    else:
        log(f"  {str(data)[:3000]}")


subjects = curl_get(f"/data/projects/{PROJECT_NAME}/subjects?format=json")
log_json("subjects", subjects)
subj_count = 0
if isinstance(subjects, dict):
    results = subjects.get("ResultSet", {}).get("Result", [])
    subj_count = len(results)
    log(f"  subjects count: {subj_count}")

experiments = curl_get(f"/data/projects/{PROJECT_NAME}/experiments?format=json")
log_json("experiments", experiments)
exp_count = 0
if isinstance(experiments, dict):
    results = experiments.get("ResultSet", {}).get("Result", [])
    exp_count = len(results)
    log(f"  experiments count: {exp_count}")
    for exp in results[:5]:
        exp_id   = exp.get("ID", "")
        exp_label = exp.get("label", "")
        log(f"\n  Experiment: {exp_label} ({exp_id})")
        scans     = curl_get(f"/data/experiments/{exp_id}/scans?format=json")
        log_json(f"  scans in {exp_id}", scans)
        resources = curl_get(f"/data/experiments/{exp_id}/resources?format=json")
        log_json(f"  resources in {exp_id}", resources)
        files     = curl_get(f"/data/experiments/{exp_id}/resources/SRC/files?format=json")
        log_json(f"  files/SRC in {exp_id}", files)

# Config resource
log("\n  Probing config resource on project...")
config_files = curl_get(f"/data/projects/{PROJECT_NAME}/resources/config/files?format=json")
log_json("config resource files", config_files)

# Subject-level intake-form resource
log("\n  Probing subjects for intake-form resource...")
if isinstance(subjects, dict):
    for subj in subjects.get("ResultSet", {}).get("Result", [])[:5]:
        subj_id    = subj.get("ID", "")
        subj_label = subj.get("label", "")
        log(f"\n  Subject: {subj_label} ({subj_id})")
        intake = curl_get(f"/data/subjects/{subj_id}/resources/INTAKE_FORM/files?format=json")
        log_json(f"  intake-form files for {subj_id}", intake)

log("\nSTEP 10: complete")

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
log("\n" + "=" * 70)
log("HARNESS COMPLETE")
log(f"  subjects on server:    {subj_count}")
log(f"  experiments on server: {exp_count}")
log(f"  full log:              {LOG_FILE}")
log("=" * 70)

_log_fh.close()
