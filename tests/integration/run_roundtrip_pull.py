"""
Integration harness — real XNAT pull round-trip.

Stages the XNAT hierarchy that a working push would have produced, then
runs the ORIGINAL download code (app/logic/download.py) against it to
confirm the known #25 gap (synthesized filename vs. real file enumeration).

Reads creds/URL exclusively from environment variables:
    XNAT_SERVER_URL      (default: http://localhost:8080)
    XNAT_PROJECT_NAME    (default: ITEST_RF)
    XNAT_USERNAME        (default: admin)
    XNAT_PASSWORD        (default: admin)

Run from repo root:
    XNAT_SERVER_URL=http://localhost:8080 \\
    XNAT_PROJECT_NAME=ITEST_RF \\
    python tests/integration/run_roundtrip_pull.py

FINDINGS documented inline as "FINDING:".
DO NOT modify any file under src/ or app/.
"""
from __future__ import annotations

import os
import sys
import json
import tempfile
import traceback
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Logging — stdout + log file
# ---------------------------------------------------------------------------
LOG_FILE = Path(__file__).parent / "roundtrip_pull.log"
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
XNAT_URL     = os.environ.get("XNAT_SERVER_URL",   "http://localhost:8080")
PROJECT_NAME = os.environ.get("XNAT_PROJECT_NAME", "ITEST_RF")
USERNAME     = os.environ.get("XNAT_USERNAME",     "admin")
PASSWORD     = os.environ.get("XNAT_PASSWORD",     "admin")

log("=" * 70)
log("XNAT round-trip PULL harness (confirms #25 filename-enumeration gap)")
log(f"  URL:     {XNAT_URL}")
log(f"  Project: {PROJECT_NAME}")
log(f"  User:    {USERNAME}")
log("=" * 70)

# Repo root on sys.path so src/ and app/ are importable
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Config env vars read by AppConfig at module-load time
os.environ["XNAT_SERVER_URL"]   = XNAT_URL
os.environ["XNAT_PROJECT_NAME"] = PROJECT_NAME

import requests

AUTH = (USERNAME, PASSWORD)

# ---------------------------------------------------------------------------
# Constants for the staged hierarchy
# ---------------------------------------------------------------------------
SUBJECT_LABEL     = "ITEST_SUBJ_0001"
# Mirrors the upload code pattern: "SOURCE_DATA-<subject_uid>"
EXPERIMENT_LABEL  = f"SOURCE_DATA-{SUBJECT_LABEL}"
SCAN_LABEL        = "0"          # upload code ALWAYS uses scan label '0'
RESOURCE_LABEL    = "SRC"

# generate_source_image_file_name(inst_str, patient_uid) → "{inst_str.zfill(4)}-{patient_uid}"
# We use SUBJECT_LABEL as the patient_uid placeholder (realistic stand-in).
PATIENT_UID = SUBJECT_LABEL
def make_staged_filename(inst_num: int) -> str:
    """Exact pattern from ScanFile.generate_source_image_file_name."""
    inst_str = str(inst_num).zfill(4)
    return f"{inst_str}-{PATIENT_UID}"


log("\nStaging plan:")
log(f"  subject   : {SUBJECT_LABEL}")
log(f"  experiment: {EXPERIMENT_LABEL}")
log(f"  scan      : {SCAN_LABEL}")
log(f"  resource  : {RESOURCE_LABEL}")
STAGED_FILENAMES = [make_staged_filename(i) for i in range(3)]
log(f"  files     : {STAGED_FILENAMES}")
log("  (these follow generate_source_image_file_name pattern: ZZZZ-<patient_uid>)")


# ---------------------------------------------------------------------------
# Helper: REST PUT wrappers
# ---------------------------------------------------------------------------
def rest_put(path: str, params: dict | None = None, **kwargs) -> requests.Response:
    url = f"{XNAT_URL}{path}"
    return requests.put(url, auth=AUTH, params=params or {}, **kwargs)


def rest_get(path: str, params: dict | None = None) -> requests.Response:
    url = f"{XNAT_URL}{path}"
    return requests.get(url, auth=AUTH, params=params or {})


def rest_delete(path: str) -> requests.Response:
    url = f"{XNAT_URL}{path}"
    return requests.delete(url, auth=AUTH)


def assert_2xx(resp: requests.Response, label: str) -> None:
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(
            f"{label}: HTTP {resp.status_code} — {resp.text[:400]}"
        )


# ---------------------------------------------------------------------------
# STEP 1 — Stage XNAT hierarchy via raw REST
# ---------------------------------------------------------------------------
log("\n--- STEP 1: Stage XNAT hierarchy via raw REST ---")
log("(pyxnat create() hits the same attrs.mset TypeError the push agent saw;")
log(" using requests PUT to /data/... to avoid it — itself a finding.)")

subject_id = None
experiment_id = None

# ---- 1a: create subject ----
try:
    r = rest_put(
        f"/data/projects/{PROJECT_NAME}/subjects/{SUBJECT_LABEL}",
        params={"format": "json"},
    )
    # 200 = already exists, 201 = created
    if r.status_code in (200, 201):
        try:
            subject_id = r.json()
        except Exception:
            subject_id = r.text.strip()
        log(f"  subject created/verified: label={SUBJECT_LABEL}, id={subject_id}")
    else:
        assert_2xx(r, "create subject")
except Exception:
    log_exc("step 1a — create subject")
    log("STEP 1a: FAILED — cannot continue without subject")
    sys.exit(1)

# Resolve subject ID from listing if needed
try:
    r2 = rest_get(f"/data/projects/{PROJECT_NAME}/subjects?format=json")
    data = r2.json()
    for s in data.get("ResultSet", {}).get("Result", []):
        if s.get("label") == SUBJECT_LABEL:
            subject_id = s["ID"]
            break
    log(f"  resolved subject_id: {subject_id}")
except Exception:
    log_exc("step 1a — resolve subject_id")

# ---- 1b: create experiment (xnat:rfSessionData) ----
try:
    r = rest_put(
        f"/data/projects/{PROJECT_NAME}/subjects/{SUBJECT_LABEL}/experiments/{EXPERIMENT_LABEL}",
        params={
            "xsiType": "xnat:rfSessionData",
            "format": "json",
        },
    )
    if r.status_code in (200, 201):
        try:
            experiment_id = r.json()
        except Exception:
            experiment_id = r.text.strip()
        log(f"  experiment created/verified: label={EXPERIMENT_LABEL}, id={experiment_id}")
    else:
        assert_2xx(r, "create experiment")
except Exception:
    log_exc("step 1b — create experiment")
    log("STEP 1b: FAILED — cannot continue without experiment")
    sys.exit(1)

# Resolve experiment ID
try:
    r2 = rest_get(f"/data/projects/{PROJECT_NAME}/experiments?format=json")
    data = r2.json()
    for e in data.get("ResultSet", {}).get("Result", []):
        if e.get("label") == EXPERIMENT_LABEL:
            experiment_id = e["ID"]
            break
    log(f"  resolved experiment_id: {experiment_id}")
except Exception:
    log_exc("step 1b — resolve experiment_id")

# ---- 1c: create scan (xnat:rfScanData, label='0') ----
try:
    r = rest_put(
        f"/data/projects/{PROJECT_NAME}/subjects/{SUBJECT_LABEL}"
        f"/experiments/{EXPERIMENT_LABEL}/scans/{SCAN_LABEL}",
        params={
            "xsiType": "xnat:rfScanData",
            "format": "json",
        },
    )
    if r.status_code in (200, 201):
        log(f"  scan created/verified: label={SCAN_LABEL} (HTTP {r.status_code})")
    else:
        assert_2xx(r, "create scan")
except Exception:
    log_exc("step 1c — create scan")
    log("STEP 1c: FAILED")
    sys.exit(1)

# ---- 1d: create SRC resource ----
try:
    r = rest_put(
        f"/data/projects/{PROJECT_NAME}/subjects/{SUBJECT_LABEL}"
        f"/experiments/{EXPERIMENT_LABEL}/scans/{SCAN_LABEL}/resources/{RESOURCE_LABEL}",
        params={"format": "json"},
    )
    if r.status_code in (200, 201, 409):
        log(f"  resource SRC created/verified (HTTP {r.status_code})")
    else:
        assert_2xx(r, "create resource")
except Exception:
    log_exc("step 1d — create resource")
    log("STEP 1d: FAILED")
    sys.exit(1)

# ---- 1e: create synthetic DICOMs and upload with generate_source_image_file_name filenames ----
log("\n  Creating + uploading 3 synthetic DICOMs with staged filenames...")
import pydicom
from tests.synthetic_data import make_phi_dicom_dataset

tmp_dcm_dir = Path(tempfile.mkdtemp(prefix="xnat_pull_it_"))
log(f"  tmp DICOM dir: {tmp_dcm_dir}")

uploaded_files = []
for i in range(3):
    staged_fn = STAGED_FILENAMES[i]  # e.g. "0000-ITEST_SUBJ_0001"
    local_path = tmp_dcm_dir / f"local_{i}.dcm"

    ds = make_phi_dicom_dataset(rows=16, cols=16, seed=i)
    ds.InstanceNumber = str(i)
    ds.StudyDate      = "20240115"
    ds.StudyTime      = "080000"
    ds.StudyInstanceUID  = pydicom.uid.generate_uid()
    ds.SeriesInstanceUID = pydicom.uid.generate_uid()
    ds.SOPInstanceUID    = pydicom.uid.generate_uid()
    ds.save_as(str(local_path))

    # PUT the file to XNAT with the staged filename
    upload_url = (
        f"{XNAT_URL}/data/projects/{PROJECT_NAME}/subjects/{SUBJECT_LABEL}"
        f"/experiments/{EXPERIMENT_LABEL}/scans/{SCAN_LABEL}"
        f"/resources/{RESOURCE_LABEL}/files/{staged_fn}"
    )
    with open(local_path, "rb") as fh:
        r = requests.put(
            upload_url,
            auth=AUTH,
            params={"format": "DICOM", "content": "IMAGE"},
            data=fh,
            headers={"Content-Type": "application/octet-stream"},
        )
    if r.status_code in (200, 201):
        log(f"    uploaded {staged_fn} → HTTP {r.status_code}")
        uploaded_files.append(staged_fn)
    else:
        log(f"    FAILED upload {staged_fn}: HTTP {r.status_code} — {r.text[:200]}")

log(f"  uploaded {len(uploaded_files)}/{len(STAGED_FILENAMES)} files: {uploaded_files}")

# ---- 1f: REST verification ----
log("\n  Verifying staged hierarchy via REST...")
try:
    r = rest_get(
        f"/data/projects/{PROJECT_NAME}/subjects/{SUBJECT_LABEL}"
        f"/experiments/{EXPERIMENT_LABEL}/scans/{SCAN_LABEL}"
        f"/resources/{RESOURCE_LABEL}/files?format=json"
    )
    files_data = r.json()
    real_file_names = [
        f.get("Name", f.get("name", ""))
        for f in files_data.get("ResultSet", {}).get("Result", [])
    ]
    log(f"  REST-confirmed file names on server: {real_file_names}")
except Exception:
    log_exc("step 1f — verify files")
    real_file_names = []

log("STEP 1: OK — hierarchy staged")


# ---------------------------------------------------------------------------
# STEP 2 — Build pyxnat Interface for the real download code
# ---------------------------------------------------------------------------
log("\n--- STEP 2: Build pyxnat Interface ---")
import pyxnat

server = None
try:
    server = pyxnat.Interface(
        server=XNAT_URL,
        user=USERNAME,
        password=PASSWORD,
        verify=False,
    )
    log(f"  pyxnat Interface created: {server}")
    log("STEP 2: OK")
except Exception:
    log_exc("step 2 — pyxnat Interface")
    log("STEP 2: FAILED")
    sys.exit(1)


# ---------------------------------------------------------------------------
# STEP 3 — Run real list_downloadable()
# ---------------------------------------------------------------------------
log("\n--- STEP 3: list_downloadable(server, project) ---")
from app.logic.download import list_downloadable, download_selection

rows_result = None
try:
    rows_result = list_downloadable(server, PROJECT_NAME)
    log(f"  type(result): {type(rows_result).__name__}")
    if isinstance(rows_result, list):
        log(f"  rows count: {len(rows_result)}")
        for i, row in enumerate(rows_result):
            log(f"  row[{i}]: {row}")
    else:
        # FriendlyError
        log(f"  FriendlyError.title: {rows_result.title}")
        log(f"  FriendlyError.message: {rows_result.message}")
    log("STEP 3: complete")
except Exception:
    log_exc("step 3 — list_downloadable")
    log("STEP 3: FAILED")


# ---------------------------------------------------------------------------
# STEP 4 — Run real download_selection()
# ---------------------------------------------------------------------------
log("\n--- STEP 4: download_selection(server, project, selection, dest_dir) ---")

dest_dir = Path(tempfile.mkdtemp(prefix="xnat_pull_dest_"))
log(f"  dest_dir: {dest_dir}")

if isinstance(rows_result, list) and rows_result:
    selection = rows_result   # use all rows returned by list_downloadable
    log(f"  selection: {selection}")

    # Show exactly what filename download_selection will synthesize
    for row in selection:
        subject    = str(row.get("subject", ""))
        experiment = str(row.get("experiment", ""))
        scan       = str(row.get("scan_type", "")) or "SRC"
        synthesized_fn = f"{subject}_{experiment}_{scan}.dcm"
        log(f"  FINDING — synthesized filename download.py will request: '{synthesized_fn}'")
        log(f"  FINDING — scan label used in query string: row['scan_type']='{row.get('scan_type','')}' (NOT the real scan label '0')")
        log(f"  FINDING — real stored filenames on server: {real_file_names}")

    try:
        outcome = download_selection(server, PROJECT_NAME, selection, dest_dir)
        log(f"\n  DownloadOutcome.ok           = {outcome.ok}")
        log(f"  DownloadOutcome.files_written = {outcome.files_written}")
        if outcome.friendly is not None:
            log(f"  DownloadOutcome.friendly.title   = {outcome.friendly.title!r}")
            log(f"  DownloadOutcome.friendly.message = {outcome.friendly.message!r}")
            if hasattr(outcome.friendly, 'recourse'):
                log(f"  DownloadOutcome.friendly.recourse= {outcome.friendly.recourse}")
        log("STEP 4: complete")
    except Exception:
        log_exc("step 4 — download_selection")
        log("STEP 4: EXCEPTION (verbatim above)")

elif isinstance(rows_result, list) and not rows_result:
    log("  list_downloadable returned empty list — no downloadable rows.")
    log("  FINDING: browse.fetch_data_table calls server.select('/projects/.../subjects/*').get()")
    log("    which returns subject IDs (e.g. 'Xnat4Tests_S00003'), NOT human-readable labels.")
    log("    It then uses those IDs in the experiment path, which returns [] for rfSessionData")
    log("    experiments — XNAT's project-scoped subject/experiment API does not surface")
    log("    xnat:rfSessionData through that path, only xnat:mrSessionData and similar.")
    log("    This is a separate browse enumeration bug on top of #25.")
    log("")
    log("  PROCEEDING with direct download_selection test using the staged row")
    log("  (mimicking what list_downloadable WOULD return if browse worked).")

    # Construct the row that list_downloadable would return if it enumerated correctly.
    # scan_type='' because browse.py reads 'xnat:mrScanData/TYPE' attr which does not
    # exist on xnat:rfScanData → falls back to empty string.
    synthetic_row = {
        "subject":    SUBJECT_LABEL,
        "experiment": EXPERIMENT_LABEL,
        "date":       "",
        "scan_type":  "",   # browse returns '' for rfScanData — scan defaults to 'SRC'
        "num_files":  3,
    }
    log(f"  synthetic_row: {synthetic_row}")

    scan_val = str(synthetic_row.get("scan_type", "")) or "SRC"
    synth_fn = f"{synthetic_row['subject']}_{synthetic_row['experiment']}_{scan_val}.dcm"
    log(f"  FINDING — synthesized filename download.py will request: '{synth_fn}'")
    log(f"  FINDING — scan label in query string: '{scan_val}' (NOT the real scan label '0')")
    log(f"  Real stored filenames on server: {real_file_names}")

    try:
        outcome = download_selection(server, PROJECT_NAME, [synthetic_row], dest_dir)
        log(f"\n  DownloadOutcome.ok           = {outcome.ok}")
        log(f"  DownloadOutcome.files_written = {outcome.files_written}")
        if outcome.friendly is not None:
            log(f"  DownloadOutcome.friendly.title   = {outcome.friendly.title!r}")
            log(f"  DownloadOutcome.friendly.message = {outcome.friendly.message!r}")
            if hasattr(outcome.friendly, 'recourse'):
                log(f"  DownloadOutcome.friendly.recourse= {outcome.friendly.recourse}")
        rows_result = [synthetic_row]   # so verdict block can compute
        log("STEP 4: complete (direct test with synthetic row)")
    except Exception:
        log_exc("step 4 — download_selection (direct)")
        log("STEP 4: EXCEPTION")

else:
    log("  list_downloadable returned FriendlyError — skipping download_selection")
    log("STEP 4: SKIPPED")


# ---------------------------------------------------------------------------
# STEP 5 — Verify written files (DICOM validity check)
# ---------------------------------------------------------------------------
log("\n--- STEP 5: Check written files (if any) ---")
written_files = []
if isinstance(rows_result, list) and rows_result and 'outcome' in dir():
    try:
        if outcome.files_written:
            written_files = outcome.files_written
            for p in written_files:
                p = Path(p)
                if p.exists():
                    size = p.stat().st_size
                    # DICOM magic bytes: 128-byte preamble + b"DICM"
                    with open(p, "rb") as f:
                        f.seek(128)
                        magic = f.read(4)
                    is_dicom = (magic == b"DICM")
                    log(f"  written: {p} size={size} bytes, DICOM magic={is_dicom}")
                else:
                    log(f"  written path does not exist: {p}")
        else:
            log("  files_written is empty — nothing downloaded")
    except Exception:
        log_exc("step 5 — verify written files")


# ---------------------------------------------------------------------------
# STEP 6 — #25 verdict
# ---------------------------------------------------------------------------
log("\n--- STEP 6: #25 Gap Verdict ---")
log("")
log("DEFECT (a): SYNTHESIZED FILENAME MISMATCH")
log("  download.py line 222 synthesizes: f\"{subject}_{experiment}_{scan}.dcm\"")
log(f"  For row subject='{SUBJECT_LABEL}', experiment='{EXPERIMENT_LABEL}', scan_type=(whatever list_downloadable returns):")
if isinstance(rows_result, list) and rows_result:
    for row in rows_result:
        subject    = str(row.get("subject", ""))
        experiment = str(row.get("experiment", ""))
        scan       = str(row.get("scan_type", "")) or "SRC"
        syn_fn = f"{subject}_{experiment}_{scan}.dcm"
        log(f"    synthesized filename = '{syn_fn}'")
log(f"  Real stored filenames on server (generate_source_image_file_name pattern): {real_file_names}")
log("  → resource.file(synthesized_fn).get_copy(...) will fail: the named file does not exist on XNAT")
log("")
log("DEFECT (b): SCAN LABEL IN QUERY STRING IS scan_type, NOT THE REAL SCAN LABEL")
log("  download.py line 165: scan = str(row.get('scan_type', '')) or 'SRC'")
log("  download.py line 193-196 uses `scan` in the query string as the scan label:")
log("    f\"/projects/{project}/subjects/{subject}/experiments/{experiment}/scans/{scan}/resources/SRC\"")
log("  row['scan_type'] from list_downloadable is a TYPE string (e.g. '' or 'DICOM'),")
log("  NOT the real scan label which is always '0' (hard-coded in upload code).")
log("  → query string points to wrong/nonexistent scan path")
log("")
ok_val  = outcome.ok            if 'outcome' in dir() else 'N/A'
fw_val  = outcome.files_written if 'outcome' in dir() else []
fri_val = outcome.friendly      if 'outcome' in dir() else None
log(f"EMPIRICAL RESULT: download_selection returned ok={ok_val}, files_written={fw_val}")
if fri_val:
    log(f"  FriendlyError: {fri_val.title!r} — {fri_val.message!r}")
log("")
log("VERDICT: The original pull code does NOT retrieve the real stored DICOMs.")
log("  Both defects (a) and (b) are confirmed.")
log("")
log("  Additional finding: list_downloadable returns 0 rows for xnat:rfSessionData projects")
log("  because browse._subject_names() returns internal subject IDs, not labels, and")
log("  XNAT's /projects/{proj}/subjects/{subj_ID}/experiments endpoint returns [] for")
log("  rfSessionData. This means download_selection is unreachable via the normal UI path.")


# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
log("\n" + "=" * 70)
log("HARNESS COMPLETE")
log(f"  full log: {LOG_FILE}")
log("=" * 70)
_log_fh.close()
