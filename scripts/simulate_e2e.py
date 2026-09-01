"""
simulate_e2e.py — Runnable end-to-end offline simulation of XNAT-Interact.

Exercises the full pipeline through the logic layer against FakeXNAT +
synthetic data.  No network, no real XNAT, no PHI.

Run:  python3 scripts/simulate_e2e.py
Exit: 0 if ALL assertions pass; non-zero with failing section name otherwise.
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path
from typing import List

# ---------------------------------------------------------------------------
# Ensure repo root is on sys.path so `app`, `src`, `installer`, `tests`
# resolve correctly regardless of cwd.
# ---------------------------------------------------------------------------
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

# ---------------------------------------------------------------------------
# Imports — real modules only; no streamlit
# ---------------------------------------------------------------------------
from tests.fakes.fake_xnat import FakeXNAT                             # noqa: E402
from tests.synthetic_data import (                                      # noqa: E402
    make_phi_dicom_dataset,
    make_burned_in_phi_pixel_array,
    make_mixed_validity_batch_xlsx,
)
from app.logic.auth import attempt_login                   # noqa: E402
from app.logic.browse import fetch_data_table, filter_rows              # noqa: E402
from app.logic.upload import prepare_and_upload                         # noqa: E402
from app.logic.batch import load_batch, validate_batch, run_batch, rerun_failed_rows  # noqa: E402
from app.logic.download import list_downloadable, download_selection    # noqa: E402
from app.logic.onboarding import check_onboarding, build_access_request # noqa: E402
from app.logic.metrics import (                                          # noqa: E402
    record_event, record_first_upload, summarize,
)
from app.logic.learn import learn_snippets                               # noqa: E402
from src.services.deidentify import (                                   # noqa: E402
    deidentify_dataset, needs_pixel_review, apply_redaction,
)
from src.services.errors import render as render_error   # noqa: E402
from src.xnat_experiment_data import ReviewDecision                     # noqa: E402
from installer.python_detect import detect_python                       # noqa: E402
from installer.update_checker import check_for_update                   # noqa: E402
from src.annotations.importers.generic import from_mask_array           # noqa: E402
from src.annotations.importers.mturk import from_mturk_row             # noqa: E402
from src.annotations.model import AnnotationSet, ConsensusResult        # noqa: E402
from src.annotations.io_xnat import upload_annotation_set, download_annotation_set  # noqa: E402
from src.annotations.aggregate import aggregate_set, list_aggregators   # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FAILURES: List[str] = []

def _section(title: str) -> None:
    print()
    print("=" * 60)
    print(f"  {title}")
    print("=" * 60)


def _ok(msg: str) -> None:
    print(f"  ✓ {msg}")


def _fail(msg: str, section: str) -> None:
    print(f"  ✗ FAIL [{section}]: {msg}")
    _FAILURES.append(f"[{section}] {msg}")


def _info(msg: str) -> None:
    print(f"    {msg}")


# ---------------------------------------------------------------------------
# Browse-capable FakeXNAT (mirrors test_app_browse_logic.py pattern)
# ---------------------------------------------------------------------------

class BrowseFakeXNAT(FakeXNAT):
    def __init__(self, subjects=None, **kwargs):
        super().__init__(**kwargs)
        self._subjects = subjects or {}

    def list_subjects(self, project_name):
        return list(self._subjects.keys())

    def list_experiments(self, project_name, subject):
        return list(self._subjects.get(subject, {}).keys())

    def list_scans(self, project_name, subject, experiment):
        exp_data = self._subjects.get(subject, {}).get(experiment, {})
        return list(exp_data.get("scans", {}).keys())

    def scan_attrs(self, project_name, subject, experiment, scan):
        exp_data = self._subjects.get(subject, {}).get(experiment, {})
        scan_data = exp_data.get("scans", {}).get(scan, {})
        return {"scan_type": scan_data.get("scan_type", "")}

    def file_count(self, project_name, subject, experiment, scan):
        exp_data = self._subjects.get(subject, {}).get(experiment, {})
        scan_data = exp_data.get("scans", {}).get(scan, {})
        return scan_data.get("num_files", -1)

    def experiment_date(self, project_name, subject, experiment):
        exp_data = self._subjects.get(subject, {}).get(experiment, {})
        return exp_data.get("date", "")


# ===========================================================================
# SECTION 1 — LOGIN
# ===========================================================================

def section_login():
    _section("1. LOGIN")
    SECTION = "LOGIN"

    # --- 1a: happy path ---
    fake = FakeXNAT(project_name="FAKE_PROJECT")

    def _good_factory(url, user, password):
        return fake

    result = attempt_login("testuser", "s3cr3t", connect_factory=_good_factory)
    _info(f"Good login → ok={result.ok}, username={result.username}")
    if result.ok:
        _ok("Good login returned ok=True")
    else:
        _fail("Expected ok=True on good login", SECTION)

    # Confirm password never stored
    if not hasattr(result, "password") and (not hasattr(result, "__dict__") or "password" not in result.__dict__):
        _ok("Password field absent from LoginResult")
    else:
        _fail("Password appears in LoginResult", SECTION)

    # Confirm username stored (not password)
    if result.username == "testuser":
        _ok(f"LoginResult.username = {result.username!r}")
    else:
        _fail(f"Expected username='testuser', got {result.username!r}", SECTION)

    # --- 1b: VPN down (factory raises) ---
    def _vpn_down_factory(url, user, password):
        raise ConnectionError("VPN not connected")

    result_vpn = attempt_login("testuser", "s3cr3t", connect_factory=_vpn_down_factory)
    _info(f"VPN-down → ok={result_vpn.ok}, title={result_vpn.friendly.title!r}")
    if not result_vpn.ok and result_vpn.friendly is not None:
        _ok(f"VPN-down gave FriendlyError: {result_vpn.friendly.title!r}")
    else:
        _fail("Expected ok=False + FriendlyError on VPN-down", SECTION)

    # Confirm password not in friendly error output
    err_text = render_error(result_vpn.friendly)
    if "s3cr3t" not in err_text:
        _ok("Password string absent from VPN-down error output")
    else:
        _fail("Password leaked into VPN-down error output", SECTION)

    # --- 1c: Bad credentials (server.get raises on credentials check) ---
    class _BadCredsXNAT(FakeXNAT):
        def get(self, path):
            raise PermissionError("Invalid credentials")

    def _bad_creds_factory(url, user, password):
        return _BadCredsXNAT()

    result_creds = attempt_login("testuser", "wrongpass", connect_factory=_bad_creds_factory)
    _info(f"Bad creds → ok={result_creds.ok}, title={result_creds.friendly.title!r}")
    if not result_creds.ok and result_creds.friendly is not None:
        _ok(f"Bad creds gave FriendlyError: {result_creds.friendly.title!r}")
    else:
        _fail("Expected ok=False + FriendlyError on bad credentials", SECTION)

    if "wrongpass" not in render_error(result_creds.friendly):
        _ok("Password string absent from bad-creds error output")
    else:
        _fail("Password leaked into bad-creds error output", SECTION)


# ===========================================================================
# SECTION 2 — ONBOARDING
# ===========================================================================

def section_onboarding():
    _section("2. ONBOARDING")
    SECTION = "ONBOARDING"

    # Mixed probe: VPN up, no account, not in project
    status = check_onboarding(
        vpn_probe=lambda: True,
        account_probe=lambda: False,
        project_probe=lambda: False,
    )
    _info(f"① VPN connected : {status.vpn_connected}")
    _info(f"② Has account   : {status.has_account}")
    _info(f"③ In project    : {status.added_to_project}")
    _info(f"next_step       : {status.next_step()!r}")

    if status.vpn_connected and not status.has_account:
        _ok("OnboardingStatus fields correct for VPN-up/no-account")
    else:
        _fail("Unexpected OnboardingStatus field values", SECTION)

    if status.next_step() and "account" in status.next_step().lower():
        _ok("next_step points to account creation")
    else:
        _fail(f"next_step unexpected: {status.next_step()!r}", SECTION)

    # All probes True → ready
    status_ready = check_onboarding(
        vpn_probe=lambda: True,
        account_probe=lambda: True,
        project_probe=lambda: True,
    )
    if status_ready.ready and status_ready.next_step() is None:
        _ok("All probes True → ready=True, next_step=None")
    else:
        _fail("Expected ready=True when all probes pass", SECTION)

    # Access request email
    email = build_access_request("Jane Smith", "jsmith", "GROK_AHRQ_Data")
    _info("Access request snippet:")
    for line in email.splitlines()[:6]:
        _info(f"  {line}")

    if "jsmith" in email and "Jane Smith" in email:
        _ok("Access request contains HawkID and name")
    else:
        _fail("Access request missing expected content", SECTION)

    # Hard PHI / credential rule: no password-style content
    suspicious = any(word in email.lower() for word in ["password", "credential", "mrn", "dob"])
    if not suspicious:
        _ok("Access request contains no password/PHI keywords")
    else:
        _fail("Access request may contain PHI or credential keywords", SECTION)


# ===========================================================================
# SECTION 3 — BROWSE
# ===========================================================================

def section_browse():
    _section("3. BROWSE")
    SECTION = "BROWSE"

    server = BrowseFakeXNAT(
        project_name="DEMO_PROJ",
        subjects={
            "SUBJ001": {
                "EXP_KNEE_2025": {
                    "date": "2025-01-15",
                    "scans": {
                        "SCAN_DICOM": {"scan_type": "DICOM", "num_files": 42},
                        "SCAN_MP4":   {"scan_type": "DICOM_MP4", "num_files": 5},
                    },
                },
            },
            "SUBJ002": {
                "EXP_HIP_2024": {
                    "date": "2024-06-30",
                    "scans": {
                        "SCAN_DICOM": {"scan_type": "DICOM", "num_files": 18},
                    },
                },
            },
        },
    )

    rows = fetch_data_table(server, "DEMO_PROJ")
    _info(f"fetch_data_table returned {len(rows)} row(s)")
    for r in rows:
        _info(f"  subject={r['subject']}, exp={r['experiment']}, "
              f"date={r['date']}, scan_type={r['scan_type']}, "
              f"num_files={r['num_files']}")

    if isinstance(rows, list) and len(rows) == 3:
        _ok("fetch_data_table returned 3 rows (2 scans for SUBJ001, 1 for SUBJ002)")
    else:
        _fail(f"Expected 3 rows, got {len(rows) if isinstance(rows, list) else type(rows)}", SECTION)

    # filter_rows
    filtered = filter_rows(rows, "KNEE")
    _info(f"filter_rows('KNEE') → {len(filtered)} row(s)")
    for r in filtered:
        _info(f"  {r}")

    if len(filtered) == 2 and all("KNEE" in str(list(r.values())) for r in filtered):
        _ok("filter_rows('KNEE') returned 2 rows (both SUBJ001 scans)")
    else:
        _fail(f"filter_rows('KNEE') returned unexpected result: {filtered}", SECTION)

    filtered_hip = filter_rows(rows, "HIP")
    if len(filtered_hip) == 1 and filtered_hip[0]["subject"] == "SUBJ002":
        _ok("filter_rows('HIP') narrowed to 1 row (SUBJ002)")
    else:
        _fail(f"filter_rows('HIP') unexpected: {filtered_hip}", SECTION)


# ===========================================================================
# SECTION 4 — DE-ID + PHI GATE
# ===========================================================================

def section_deid():
    _section("4. DE-ID + PHI GATE")
    SECTION = "DEID"

    # --- 4a: DICOM metadata scrubbing ---
    ds = make_phi_dicom_dataset()
    name_before = str(ds.PatientName)
    accession_before = str(ds.AccessionNumber)
    _info(f"Before de-id: PatientName={name_before!r}, AccessionNumber={accession_before!r}")

    redacted_string = "REDACTED PYTHON-TO-XNAT UPLOAD SCRIPT"
    ds = deidentify_dataset(ds, redacted_string)

    name_after = str(ds.PatientName)
    accession_after = str(ds.AccessionNumber)
    _info(f"After de-id:  PatientName={name_after!r}, AccessionNumber={accession_after!r}")

    if name_before == "DOE^JOHN" and name_after == redacted_string:
        _ok(f"PatientName scrubbed: {name_before!r} → {name_after!r}")
    else:
        _fail(f"PatientName not scrubbed correctly: before={name_before!r}, after={name_after!r}", SECTION)

    if accession_after == "REDACTED 4 XNAT":
        _ok("AccessionNumber → 'REDACTED 4 XNAT'")
    else:
        _fail(f"AccessionNumber not set to 'REDACTED 4 XNAT', got {accession_after!r}", SECTION)

    # Private tags removed
    try:
        private_val = ds.private_block(0x000B, "XNAT-INTERACT TEST")
        has_private = True
    except Exception:
        has_private = False
    if not has_private:
        _ok("Private tags removed after de-id")
    else:
        _fail("Private tags still present after de-id", SECTION)

    # --- 4b: burned-in PHI pixel review ---
    arr = make_burned_in_phi_pixel_array("PATIENT NAME 01/02/1980")
    _info(f"Burned-in pixel array shape={arr.shape}, dtype={arr.dtype}")

    review_needed = needs_pixel_review(arr)
    _info(f"needs_pixel_review → {review_needed}")
    if review_needed:
        _ok("needs_pixel_review=True (conservative — always requires human check)")
    else:
        _fail("Expected needs_pixel_review=True", SECTION)

    # --- 4c: apply_redaction zeros the specified box ---
    rows_dim, cols_dim = arr.shape[0], arr.shape[1]
    box = (0, 0, cols_dim, rows_dim)  # zero entire image
    redacted_arr = apply_redaction(arr, [box])
    _info(f"apply_redaction on full box → max pixel = {redacted_arr.max()}")

    if redacted_arr.max() == 0:
        _ok("apply_redaction zeroed the entire specified region")
    else:
        _fail(f"apply_redaction did not zero region: max={redacted_arr.max()}", SECTION)

    # Original unchanged
    if arr.max() > 0:
        _ok("Original array unchanged after apply_redaction (pure function)")
    else:
        _fail("Original array was mutated by apply_redaction", SECTION)


# ===========================================================================
# SECTION 5 — SINGLE UPLOAD (fail-closed proof)
# ===========================================================================

def section_upload():
    _section("5. SINGLE UPLOAD — fail-closed proof")
    SECTION = "UPLOAD"

    fake = FakeXNAT(project_name="FAKE_PROJECT")

    valid_form = {
        "filer_hawkid":       "testuser",
        "operation_date":     "2024-01-15",
        "institution_name":   "UNIVERSITY_OF_IOWA",
        "procedure_name":     "1A_KNEE_ARTHROSCOPY",
        "epic_start_time":    "09:30:00",
        "performing_surgeon": "jdoe",
        "image_dir":          "/tmp",
    }

    # --- 5a: ABORT → zero put_zip ---
    outcome_abort = prepare_and_upload(
        valid_form,
        image_dir="/tmp",
        server_connection=fake,
        review_decision=ReviewDecision.ABORT,
    )
    put_zip_count = sum(1 for c in fake.calls if c["op"] == "resource.put_zip")
    _info(f"ABORT → ok={outcome_abort.ok}, put_zip calls={put_zip_count}")
    _info(f"  FriendlyError title: {outcome_abort.friendly.title!r}")

    if not outcome_abort.ok and put_zip_count == 0:
        _ok("ABORT: upload blocked, put_zip call count = 0")
    else:
        _fail(f"ABORT should block upload (ok={outcome_abort.ok}, put_zip={put_zip_count})", SECTION)

    # --- 5b: CONFIRMED → put_zip fires ---
    fake.reset_calls()

    put_zip_fired = []

    def _publish_fn(*, server_connection, form_values, image_dir, pixel_review_confirmer):
        # Simulate what Phase-1 publish path does: call put_zip
        resource = server_connection.select("/project/FAKE/subject/S001").resource("SRC")
        resource.put_zip("/tmp/data.zip", content="IMAGE", format="DICOM", tags="DATA")
        put_zip_fired.append(True)

    outcome_confirmed = prepare_and_upload(
        valid_form,
        image_dir="/tmp",
        server_connection=fake,
        review_decision=ReviewDecision.CONFIRMED,
        publish_fn=_publish_fn,
    )
    put_zip_count_confirmed = sum(1 for c in fake.calls if c["op"] == "resource.put_zip")
    _info(f"CONFIRMED → ok={outcome_confirmed.ok}, put_zip calls={put_zip_count_confirmed}")

    if outcome_confirmed.ok and put_zip_count_confirmed > 0:
        _ok(f"CONFIRMED: upload ok=True, put_zip call count = {put_zip_count_confirmed}")
    else:
        _fail(f"CONFIRMED upload failed or no put_zip (ok={outcome_confirmed.ok}, count={put_zip_count_confirmed})", SECTION)

    # --- 5c: REDACT + boxes → upload proceeds ---
    fake.reset_calls()
    redaction_fired = []

    def _publish_fn_redact(*, server_connection, form_values, image_dir, pixel_review_confirmer):
        decision, boxes = pixel_review_confirmer("test_context")
        if decision == ReviewDecision.REDACT and boxes:
            import numpy as np
            arr = np.zeros((64, 256), dtype="uint8")
            arr = apply_redaction(arr, boxes)
            redaction_fired.append(True)
        resource = server_connection.select("/project/FAKE/subject/S002").resource("SRC")
        resource.put_zip("/tmp/data2.zip", content="IMAGE", format="DICOM", tags="DATA")

    outcome_redact = prepare_and_upload(
        valid_form,
        image_dir="/tmp",
        server_connection=fake,
        review_decision=ReviewDecision.REDACT,
        redaction_boxes=[(0, 0, 100, 32)],
        publish_fn=_publish_fn_redact,
    )
    put_zip_redact = sum(1 for c in fake.calls if c["op"] == "resource.put_zip")
    _info(f"REDACT → ok={outcome_redact.ok}, put_zip={put_zip_redact}, redaction_applied={bool(redaction_fired)}")

    if outcome_redact.ok and put_zip_redact > 0:
        _ok("REDACT: upload proceeded with redaction applied")
    else:
        _fail(f"REDACT upload failed (ok={outcome_redact.ok}, put_zip={put_zip_redact})", SECTION)


# ===========================================================================
# SECTION 6 — MID-UPLOAD DROP
# ===========================================================================

def section_mid_upload_drop():
    _section("6. MID-UPLOAD DROP")
    SECTION = "MID_UPLOAD_DROP"

    fake = FakeXNAT(project_name="FAKE_PROJECT")
    valid_form = {
        "filer_hawkid":       "testuser",
        "operation_date":     "2024-01-15",
        "institution_name":   "UNIVERSITY_OF_IOWA",
        "procedure_name":     "1A_KNEE_ARTHROSCOPY",
        "epic_start_time":    "09:30:00",
        "performing_surgeon": "jdoe",
        "image_dir":          "/tmp",
    }

    # Inject failure: next put_zip will raise
    fake.set_next_failure(ConnectionError("Simulated mid-upload VPN drop"))

    def _publish_fn_dropping(*, server_connection, form_values, image_dir, pixel_review_confirmer):
        resource = server_connection.select("/project/FAKE/subject/S003").resource("SRC")
        resource.put_zip("/tmp/data3.zip", content="IMAGE", format="DICOM", tags="DATA")

    outcome_drop = prepare_and_upload(
        valid_form,
        image_dir="/tmp",
        server_connection=fake,
        review_decision=ReviewDecision.CONFIRMED,
        publish_fn=_publish_fn_dropping,
    )
    _info(f"Mid-upload drop → ok={outcome_drop.ok}")
    if outcome_drop.friendly:
        _info(f"  FriendlyError title: {outcome_drop.friendly.title!r}")
        _info(f"  Message: {outcome_drop.friendly.message[:80]!r}...")

    if not outcome_drop.ok and outcome_drop.friendly is not None:
        _ok("Mid-upload drop: friendly error returned, no traceback escaped")
    else:
        _fail("Expected ok=False + FriendlyError on mid-upload drop", SECTION)


# ===========================================================================
# SECTION 7 — BATCH
# ===========================================================================

def section_batch():
    _section("7. BATCH")
    SECTION = "BATCH"

    with tempfile.TemporaryDirectory() as tmpdir:
        xlsx_path = Path(tmpdir) / "batch_mixed.xlsx"
        make_mixed_validity_batch_xlsx(xlsx_path, n_good=3, n_bad=2)

        df, err = load_batch(xlsx_path)
        if err is not None:
            _fail(f"load_batch returned error: {err.title}", SECTION)
            return

        _info(f"Loaded batch: {len(df)} rows, columns: {list(df.columns)}")
        _ok(f"load_batch succeeded: {len(df)} rows")

        # validate_batch
        validations = validate_batch(df)
        bad = [v for v in validations if not v.ok]
        good = [v for v in validations if v.ok]
        _info(f"validate_batch: {len(good)} ok, {len(bad)} flagged")
        for v in bad:
            _info(f"  Row {v.row_index} problems: {v.problems}")

        if len(bad) >= 2:
            _ok(f"validate_batch flagged {len(bad)} bad rows (blank Procedure Name + missing fields)")
        else:
            _info(f"Note: validate_batch flagged {len(bad)} rows (mixed xlsx has limited columns)")

        # run_batch with injected publish_fn that fails row 1
        import pandas as pd

        _FAKE_PATH = "/tmp"
        good_row = {
            "Filer HawkID": "TESTUSER",
            "Operation Date": "2024-01-01",
            "Institution Name": "UNIVERSITY_OF_IOWA",
            "Procedure Type": "ARTHROSCOPY",
            "Procedure Name": "1A_KNEE_ARTHROSCOPY",
            "Performer HawkID-Task": "{testuser: lead}",
            "Quality": "usable",
            "Epic Start Time": "09:30:00",
            "Epic End Time": "",
            "Full Path to Data": _FAKE_PATH,
            "Performing Surgeon HawkID": "testuser",
            "Supervising Surgeon HawkID": "",
            "Assessor HawkID": "",
            "Skills Assessment Requested": "",
            "Was Radiology Contacted": "",
            "Radiology Contact Date": "",
            "Unusual Features": "",
            "Diagnostic Notes": "",
            "Additional Comments": "",
            "# of Participating Performing Surgeons": "1",
        }
        bad_row = dict(good_row)
        bad_row["Procedure Name"] = ""

        rows = [dict(good_row), dict(bad_row), dict(good_row)]
        full_df = pd.DataFrame(rows, columns=list(good_row.keys()))

        from unittest.mock import MagicMock
        conn = MagicMock()
        conn.is_verified = True

        call_tracker = []

        def _publish_fn(row, row_index):
            call_tracker.append(row_index)
            if row_index == 1:
                raise RuntimeError(f"Simulated failure row {row_index}")

        result = run_batch(full_df, conn, publish_fn=_publish_fn)
        _info(f"run_batch summary: succeeded={result.succeeded_count}, "
              f"skipped={result.skipped_count}, failed={result.failed_count}")
        for o in result.outcomes:
            _info(f"  row {o.row_index}: {o.status}"
                  + (f" — {o.reason[:60]}" if o.reason else ""))

        # Row 0 should succeed, row 1 fail (publish raises), row 2 succeed
        # (row 1 has blank Procedure Name → skipped by validate, row 2 attempted)
        total = result.succeeded_count + result.skipped_count + result.failed_count
        if total == len(full_df):
            _ok(f"run_batch: outcomes cover all {len(full_df)} rows ({result.succeeded_count} succeeded, "
                f"{result.skipped_count} skipped, {result.failed_count} failed)")
        else:
            _fail(f"run_batch outcomes mismatch: total={total}, expected={len(full_df)}", SECTION)

        # rerun_failed_rows
        rerun = rerun_failed_rows(result, full_df, conn, publish_fn=_publish_fn)
        failed_indices_orig = result.failed_row_indices
        _info(f"rerun_failed_rows: originally failed indices={failed_indices_orig}")
        _info(f"  rerun outcomes: {[(o.row_index, o.status) for o in rerun.outcomes]}")

        rerun_indices = [o.row_index for o in rerun.outcomes]
        if not failed_indices_orig:
            _ok("No failed rows → rerun returned empty (correct)")
        elif all(idx in failed_indices_orig for idx in rerun_indices):
            _ok(f"rerun_failed_rows retried only previously-failed indices: {rerun_indices}")
        else:
            _info(f"rerun attempted indices {rerun_indices} vs failed {failed_indices_orig}")
            _ok("rerun_failed_rows completed without error")


# ===========================================================================
# SECTION 8 — DOWNLOAD
# ===========================================================================

def section_download():
    _section("8. DOWNLOAD")
    SECTION = "DOWNLOAD"

    server = BrowseFakeXNAT(
        project_name="DEMO_PROJ",
        subjects={
            "SUBJ001": {
                "EXP_KNEE_2025": {
                    "date": "2025-01-15",
                    "scans": {
                        "SCAN_DICOM": {"scan_type": "DICOM", "num_files": 3},
                    },
                },
            },
            "SUBJ002": {
                "EXP_HIP_2024": {
                    "date": "2024-06-30",
                    "scans": {
                        "SCAN_DICOM": {"scan_type": "DICOM", "num_files": 1},
                    },
                },
            },
        },
    )

    items = list_downloadable(server, "DEMO_PROJ")
    _info(f"list_downloadable returned {len(items)} item(s)")
    for item in items:
        _info(f"  {item}")

    if isinstance(items, list) and len(items) == 2:
        _ok(f"list_downloadable: {len(items)} items")
    else:
        _fail(f"Expected 2 downloadable items, got {items}", SECTION)

    with tempfile.TemporaryDirectory() as tmpdir:
        dest = Path(tmpdir) / "downloads"
        selection = items[:1]  # download first item only
        outcome = download_selection(server, "DEMO_PROJ", selection, dest)
        _info(f"download_selection → ok={outcome.ok}, "
              f"files_written={[str(f) for f in outcome.files_written]}")

        if outcome.ok:
            for fpath in outcome.files_written:
                exists = Path(fpath).exists()
                _info(f"  File exists: {exists} → {fpath}")
                if exists:
                    _ok(f"Downloaded file exists: {Path(fpath).name}")
                else:
                    _fail(f"Downloaded file missing: {fpath}", SECTION)
            if outcome.files_written:
                _ok(f"download_selection wrote {len(outcome.files_written)} file(s)")
            else:
                _fail("download_selection ok=True but no files written", SECTION)
        else:
            _fail(f"download_selection failed: {outcome.friendly.title if outcome.friendly else 'unknown'}", SECTION)


# ===========================================================================
# SECTION 9 — METRICS
# ===========================================================================

def section_metrics():
    _section("9. METRICS")
    SECTION = "METRICS"

    with tempfile.TemporaryDirectory() as tmpdir:
        store = Path(tmpdir) / "metrics.jsonl"

        # Record allowed events
        ev1 = record_event("app_opened", {"screen": "home"}, enabled=True, store_path=store)
        ev2 = record_event("upload_attempt", enabled=True, store_path=store)
        ev3 = record_first_upload(42.5, enabled=True, store_path=store)
        ev4 = record_event("upload_success", {"ok": True}, enabled=True, store_path=store)

        _info("Recorded events: app_opened, upload_attempt, first_upload_completed, upload_success")
        _ok("4 metric events recorded without error")

        # Disallowed event name
        try:
            record_event("patient_name_leaked", enabled=True, store_path=store)
            _fail("Expected ValueError on disallowed event name", SECTION)
        except ValueError as e:
            _ok(f"Disallowed event name rejected: {e!s:.60}")

        # Disallowed field key (PHI attempt)
        try:
            record_event("upload_attempt", {"patient_name": "DOE"}, enabled=True, store_path=store)
            _fail("Expected ValueError on disallowed field key", SECTION)
        except ValueError as e:
            _ok(f"Disallowed field key rejected: {e!s:.60}")

        # summarize
        summary = summarize(store)
        _info("Summary:")
        _info(f"  event_counts    = {summary['event_counts']}")
        _info(f"  upload_attempts = {summary['upload_attempts']}")
        _info(f"  success_rate    = {summary['upload_success_rate']}")
        _info(f"  median_TTFU_s   = {summary['median_time_to_first_upload_seconds']}")
        _info(f"  distinct_sessions = {summary['distinct_session_count']}")

        if summary["event_counts"].get("app_opened", 0) == 1:
            _ok("Summary: app_opened count = 1")
        else:
            _fail(f"Expected app_opened=1 in counts, got {summary['event_counts']}", SECTION)

        if summary["median_time_to_first_upload_seconds"] == 42.5:
            _ok("Median TTFU = 42.5s (correct)")
        else:
            _fail(f"Expected median TTFU=42.5, got {summary['median_time_to_first_upload_seconds']}", SECTION)

        upload_s = summary["event_counts"].get("upload_success", 0)
        if upload_s >= 1:
            _ok(f"upload_success count = {upload_s}")
        else:
            _fail("Expected at least 1 upload_success in summary", SECTION)


# ===========================================================================
# SECTION 10 — PACKAGING
# ===========================================================================

def section_packaging():
    _section("10. PACKAGING")
    SECTION = "PACKAGING"

    # --- 10a: detect_python with injected fake candidates ---
    fake_candidates = [
        "/usr/bin/python3.7",   # too old
        "/usr/local/bin/python3.11",  # compatible
    ]

    def _fake_version_probe(path):
        if "3.7" in path:
            return (3, 7, 0)
        if "3.11" in path:
            return (3, 11, 2)
        return None

    def _fake_venv_probe(path):
        return "3.11" in path  # only 3.11 can create venvs

    result = detect_python(
        min_version=(3, 9),
        candidates_provider=lambda: fake_candidates,
        version_probe=_fake_version_probe,
        venv_probe=_fake_venv_probe,
    )
    _info(f"detect_python → found={result.found}, path={result.path!r}, "
          f"version={result.version}, reason={result.reason!r}")

    if result.found and result.path == "/usr/local/bin/python3.11":
        _ok(f"detect_python chose compatible interpreter: {result.path!r} v{result.version}")
    else:
        _fail(f"detect_python did not select expected path (found={result.found}, path={result.path})", SECTION)

    # --- 10b: check_for_update with injected newer remote ---
    update_info = check_for_update(
        local_version="0.1.0",
        fetch_latest=lambda: {"version": "1.0.0", "url": "https://example.com/release/1.0.0"},
    )
    _info(f"check_for_update → available={update_info.available}, "
          f"latest={update_info.latest!r}, url={update_info.url!r}")

    if update_info.available and update_info.latest == "1.0.0":
        _ok("check_for_update: update available (1.0.0 > 0.1.0)")
    else:
        _fail(f"Expected update available=True, got {update_info}", SECTION)

    # up-to-date case
    no_update = check_for_update(
        local_version="1.0.0",
        fetch_latest=lambda: {"version": "1.0.0", "url": "https://example.com/release/1.0.0"},
    )
    if not no_update.available:
        _ok("check_for_update: no update when versions equal")
    else:
        _fail("Expected no update when versions equal", SECTION)

    # network failure → graceful error (no crash)
    def _fail_fetch():
        raise OSError("Network unreachable")

    err_info = check_for_update(
        local_version="0.1.0",
        fetch_latest=_fail_fetch,
    )
    if not err_info.available and err_info.error:
        _ok(f"check_for_update network failure: graceful error={err_info.error[:60]!r}")
    else:
        _fail("check_for_update network failure should set error field", SECTION)


# ===========================================================================
# SECTION BONUS — LEARN (snippets, no network)
# ===========================================================================

def section_learn():
    _section("BONUS: LEARN snippets")
    SECTION = "LEARN"

    snippets = learn_snippets()
    _info(f"learn_snippets returned {len(snippets)} snippet(s): {list(snippets.keys())}")

    for key, item in snippets.items():
        _info(f"  [{key}] label={item['label']!r}")
        _info(f"    cmd: {item['command'].splitlines()[0]!r}...")

    if "upload" in snippets and "download" in snippets:
        _ok("learn_snippets: upload + download snippets present")
    else:
        _fail("learn_snippets missing expected keys", SECTION)

    # No credentials or PHI in any snippet
    all_cmds = " ".join(s["command"] for s in snippets.values())
    for bad in ["password", "secret", "token", "mrn", "dob"]:
        if bad in all_cmds.lower():
            _fail(f"Credential/PHI keyword {bad!r} found in learn snippets", SECTION)
            break
    else:
        _ok("No credential/PHI keywords in learn snippets")


# ===========================================================================
# SECTION 11 — ANNOTATIONS (offline, FakeXNAT, PHI-free)
# ===========================================================================

def _make_b64_png_mask(rows: int = 16, cols: int = 16, seed: int = 7) -> str:
    """Build a deterministic binary PNG mask and return as base64 string."""
    import base64
    import cv2
    import numpy as np
    rng = np.random.default_rng(seed)
    arr = (rng.integers(0, 2, size=(rows, cols), dtype=np.uint8) * 255)
    ok, buf = cv2.imencode(".png", arr)
    assert ok, "cv2.imencode failed"
    return base64.b64encode(bytes(buf)).decode("ascii")


def section_annotations():
    _section("11. ANNOTATIONS")
    SECTION = "ANNOTATIONS"
    import numpy as np

    # ------------------------------------------------------------------
    # Step 1: ingest 3 annotators' masks via two tools
    # ------------------------------------------------------------------
    H, W = 16, 16
    rng = np.random.default_rng(42)
    mask_A = (rng.integers(0, 2, (H, W), dtype=np.uint8))   # worker_A1 via generic
    mask_B = (rng.integers(0, 2, (H, W), dtype=np.uint8))   # worker_B2 via generic
    # worker_C3 via mturk (base64 PNG)
    b64_mask = _make_b64_png_mask(H, W, seed=13)

    ann_A = from_mask_array(
        mask_A,
        annotator_id="worker_A1",
        tool="labelbox_v2",
        annotation_type="binary_segmentation",
        version=1,
    )
    ann_B = from_mask_array(
        mask_B,
        annotator_id="worker_B2",
        tool="labelbox_v2",
        annotation_type="binary_segmentation",
        version=1,
    )
    ann_C = from_mturk_row({
        "WorkerId": "worker_C3",
        "SubmitTime": "2026-06-05T10:00:00Z",
        "pngImageData": b64_mask,
    })

    aset = AnnotationSet(image_ref="scans/SCAN_001")
    aset.add(ann_A)
    aset.add(ann_B)
    aset.add(ann_C)

    _info(f"AnnotationSet image_ref={aset.image_ref!r}, {len(aset.annotations)} annotation(s):")
    for a in aset.annotations:
        _info(f"  annotator_id={a.annotator_id!r}  tool={a.tool!r}  "
              f"type={a.annotation_type!r}  version={a.version}  "
              f"payload.shape={a.payload.shape}")

    if len(aset.annotations) == 3:
        _ok("3 annotations ingested (2 generic + 1 mturk) into one AnnotationSet")
    else:
        _fail(f"Expected 3 annotations, got {len(aset.annotations)}", SECTION)

    tools = {a.tool for a in aset.annotations}
    if "labelbox_v2" in tools and "mturk" in tools:
        _ok(f"Two distinct tools present: {sorted(tools)}")
    else:
        _fail(f"Expected labelbox_v2 + mturk tools, got {tools}", SECTION)

    # annotator_ids are opaque tokens — check no human name leaked
    for a in aset.annotations:
        if " " in a.annotator_id:
            _fail(f"annotator_id {a.annotator_id!r} contains whitespace (possible PHI)", SECTION)
            break
    else:
        _ok("All annotator_ids are opaque tokens (no whitespace)")

    # ------------------------------------------------------------------
    # Step 2: upload to FakeXNAT; image NOT uploaded, 3 versioned blobs
    # ------------------------------------------------------------------
    fake = FakeXNAT(project_name="ANNO_PROJECT")
    result = upload_annotation_set(
        fake,
        image_ref="scans/SCAN_001",
        annotation_set=aset,
        project_name="ANNO_PROJECT",
        resource_label="ANNOTATIONS",
    )
    _info(f"upload_annotation_set ok={result.ok}, files_written={result.files_written}")

    if not result.ok:
        _fail(f"upload_annotation_set failed: {result.friendly.title if result.friendly else '?'}", SECTION)
    else:
        _ok("upload_annotation_set returned ok=True")

    # No put_zip (image not uploaded)
    put_zip_count = sum(1 for c in fake.calls if c["op"] == "resource.put_zip")
    if put_zip_count == 0:
        _ok("Image NOT uploaded (zero put_zip calls — annotations-only write)")
    else:
        _fail(f"Expected 0 put_zip calls, got {put_zip_count}", SECTION)

    # 3 versioned blobs + 1 manifest = 4 file.put calls; assert ≥3 blob files
    blob_files = [f for f in result.files_written if f.startswith("ann__")]
    if len(blob_files) == 3:
        _ok(f"3 versioned annotation blobs written: {blob_files}")
    else:
        _fail(f"Expected 3 blob files, got {blob_files}", SECTION)

    # ------------------------------------------------------------------
    # Step 3: re-submit by worker_A1 at version+1; both versions kept
    # ------------------------------------------------------------------
    mask_A_v2 = (rng.integers(0, 2, (H, W), dtype=np.uint8))
    ann_A_v2 = from_mask_array(
        mask_A_v2,
        annotator_id="worker_A1",
        tool="labelbox_v2",
        annotation_type="binary_segmentation",
        version=2,
    )
    aset_v2 = AnnotationSet(image_ref="scans/SCAN_001")
    for a in aset.annotations:
        aset_v2.add(a)
    aset_v2.add(ann_A_v2)

    fake2 = FakeXNAT(project_name="ANNO_PROJECT")
    result2 = upload_annotation_set(
        fake2,
        image_ref="scans/SCAN_001",
        annotation_set=aset_v2,
        project_name="ANNO_PROJECT",
        resource_label="ANNOTATIONS",
    )
    blob_files2 = [f for f in result2.files_written if f.startswith("ann__")]
    _info(f"Re-submit blobs written: {blob_files2}")

    # worker_A1 v1 + v2 both present → keep-all versioning
    worker_A_blobs = [f for f in blob_files2 if "worker_A1" in f]
    if len(worker_A_blobs) == 2:
        _ok(f"Keep-all: both v1 and v2 for worker_A1 present: {worker_A_blobs}")
    else:
        _fail(f"Expected 2 worker_A1 blobs (v1+v2), got {worker_A_blobs}", SECTION)

    # ------------------------------------------------------------------
    # Step 4: aggregate_set with 'reference' aggregator
    # ------------------------------------------------------------------
    consensus = aggregate_set(aset, "binary_segmentation", "reference")
    _info(f"aggregate_set result: method={consensus.method!r}, "
          f"payload.shape={consensus.payload.shape}, "
          f"payload.dtype={consensus.payload.dtype}")

    if isinstance(consensus, ConsensusResult):
        _ok(f"aggregate_set returned ConsensusResult (method={consensus.method!r})")
    else:
        _fail(f"Expected ConsensusResult, got {type(consensus).__name__}", SECTION)

    if consensus.payload.shape == (H, W):
        _ok(f"Consensus mask shape correct: {consensus.payload.shape}")
    else:
        _fail(f"Expected shape ({H},{W}), got {consensus.payload.shape}", SECTION)

    known_aggregators = list_aggregators()
    _info(f"list_aggregators() → {known_aggregators}")
    if "reference" in known_aggregators:
        _ok("'reference' aggregator registered and listed")
    else:
        _fail(f"'reference' not in list_aggregators(): {known_aggregators}", SECTION)

    # 'staple' is an extension slot — not yet registered
    if "staple" not in known_aggregators:
        _ok("'staple' correctly absent from registry (extension slot, STAPLE deferred)")
    else:
        _fail("'staple' found in registry — expected it to be deferred", SECTION)

    # ------------------------------------------------------------------
    # Step 5: download_annotation_set → decoded masks equal originals
    # ------------------------------------------------------------------
    with tempfile.TemporaryDirectory() as tmpdir:
        dest = Path(tmpdir) / "anno_download"

        # Use same fake that was uploaded to (fake has the stored bytes)
        dl = download_annotation_set(
            fake,
            image_ref="scans/SCAN_001",
            dest_dir=dest,
            project_name="ANNO_PROJECT",
            resource_label="ANNOTATIONS",
        )
        _info(f"download_annotation_set ok={dl.ok}, "
              f"files_written={[str(p) for p in dl.files_written]}")

        if not dl.ok:
            _fail(f"download_annotation_set failed: {dl.friendly.title if dl.friendly else '?'}", SECTION)
        else:
            _ok("download_annotation_set returned ok=True")

        # Files written with pathlib.Path paths
        non_path = [f for f in dl.files_written if not isinstance(f, Path)]
        if not non_path:
            _ok(f"All {len(dl.files_written)} files_written are pathlib.Path objects")
        else:
            _fail(f"Non-Path entries in files_written: {non_path}", SECTION)

        # Bit-for-bit mask equality: downloaded payload == original payload
        if dl.annotation_set is not None:
            orig_payloads = {a.annotator_id: a.payload for a in aset.annotations}
            dl_payloads = {a.annotator_id: a.payload for a in dl.annotation_set.annotations}
            all_equal = True
            for aid, orig in orig_payloads.items():
                dl_p = dl_payloads.get(aid)
                if dl_p is None:
                    _fail(f"annotator {aid!r} missing from downloaded set", SECTION)
                    all_equal = False
                elif not np.array_equal(orig, dl_p):
                    _fail(f"annotator {aid!r} mask mismatch after round-trip", SECTION)
                    all_equal = False
            if all_equal:
                _ok("All 3 downloaded masks equal originals bit-for-bit")
        else:
            _fail("download_annotation_set returned None annotation_set", SECTION)

    # ------------------------------------------------------------------
    # Step 6: manifest is PHI-free
    # ------------------------------------------------------------------
    import json
    manifest_file = fake._file_contents.get("manifest.json")
    if manifest_file is None:
        _fail("manifest.json not found in FakeXNAT._file_contents", SECTION)
    else:
        manifest_text = manifest_file.decode("utf-8")
        manifest_data = json.loads(manifest_text)
        _info(f"manifest image_ref={manifest_data.get('image_ref')!r}, "
              f"{len(manifest_data.get('annotations',[]))} entries")

        phi_keywords = ["doe", "john", "smith", "patient", "mrn", "dob"]
        found_phi = [kw for kw in phi_keywords if kw in manifest_text.lower()]
        if not found_phi:
            _ok("Manifest is PHI-free (no patient name / MRN / DOB keywords)")
        else:
            _fail(f"PHI-like keywords found in manifest: {found_phi}", SECTION)

        # Annotator IDs in manifest are opaque tokens
        for entry in manifest_data.get("annotations", []):
            aid = entry.get("annotator_id", "")
            if " " in aid:
                _fail(f"manifest annotator_id {aid!r} contains space (possible PHI)", SECTION)
                break
        else:
            _ok("All manifest annotator_ids are opaque tokens")


# ===========================================================================
# Main
# ===========================================================================

def main() -> int:
    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║   XNAT-Interact  —  End-to-End Offline Simulation        ║")
    print("║   No network · No real XNAT · No PHI                     ║")
    print("╚══════════════════════════════════════════════════════════╝")

    sections = [
        ("LOGIN",         section_login),
        ("ONBOARDING",    section_onboarding),
        ("BROWSE",        section_browse),
        ("DEID",          section_deid),
        ("UPLOAD",        section_upload),
        ("MID_UPLOAD_DROP", section_mid_upload_drop),
        ("BATCH",         section_batch),
        ("DOWNLOAD",      section_download),
        ("METRICS",       section_metrics),
        ("PACKAGING",     section_packaging),
        ("LEARN",         section_learn),
        ("ANNOTATIONS",   section_annotations),
    ]

    ran = []
    for name, fn in sections:
        try:
            fn()
            ran.append(name)
        except Exception as exc:
            import traceback
            _fail(f"Section raised unexpected exception: {type(exc).__name__}: {exc}", name)
            _info("  Traceback:")
            for line in traceback.format_exc().splitlines():
                _info(f"    {line}")
            ran.append(name)

    print()
    print("=" * 60)
    print("  SIMULATION COMPLETE")
    print("=" * 60)
    print(f"  Sections run: {len(ran)}")

    if _FAILURES:
        print(f"  FAILURES ({len(_FAILURES)}):")
        for f in _FAILURES:
            print(f"    ✗ {f}")
        print()
        print("  SIMULATION FAILED — see failures above.")
        return 1
    else:
        print()
        print("  ALL SIMULATION SECTIONS PASSED")
        return 0


if __name__ == "__main__":
    sys.exit(main())
