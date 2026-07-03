"""
app/guided/publish_impl — Publish function factory for the guided upload wizard.

make_publish_fn(server) -> Callable
    Returns a publish_fn matching prepare_and_upload's contract:
    publish_fn(server_connection, form_values, image_dir, pixel_review_confirmer)

FakeXNAT path: creates subject/experiment/scan/resource in fake + registers
the new case so browse lists it immediately.

Real pyxnat path: drives the full pipeline from tests/stress/driver.py
(adapted inline — does NOT import test modules).
"""
from __future__ import annotations

import os
import tempfile
import traceback
from pathlib import Path
from typing import Any, Callable


def make_publish_fn(server: Any) -> Callable:
    """
    Return a publish_fn appropriate for *server*.

    Detects FakeXNAT by checking for the `_experiments` attribute
    (only FakeXNAT has it) and returns the right implementation.
    """
    if hasattr(server, "_experiments"):
        return _make_fake_publish_fn(server)
    else:
        return _make_real_publish_fn(server)


# ---------------------------------------------------------------------------
# FakeXNAT publish path
# ---------------------------------------------------------------------------

def _make_fake_publish_fn(server: Any) -> Callable:
    """
    Return a publish_fn that stores the case in the FakeXNAT archive.

    Creates subject / experiment / scan selectables and seeds a SRC resource
    with placeholder file bytes so browse_view and download both see the new case
    immediately after upload.
    """
    def _publish(
        server_connection: Any,
        form_values: dict,
        image_dir: Any,
        pixel_review_confirmer: Any,
    ) -> None:
        # Build a case identifier from procedure + date + hawkid
        procedure = form_values.get("procedure_name", "PROCEDURE").replace(" ", "_").upper()
        date_str = form_values.get("operation_date", "20240101").replace("-", "")
        hawkid = form_values.get("filer_hawkid", "user").lower()

        # Subject label: SOURCE-<PROCEDURE>_<DATE>_<hawkid>
        subject_label = f"GUIDED_{hawkid}_{date_str}"
        experiment_label = f"SOURCE_DATA-{procedure}_{date_str}"
        scan_id = "0"
        project_name = server_connection.project_name

        # Register subject label so browse can find it
        if subject_label not in server_connection._subject_labels:
            server_connection.seed_subject_label(subject_label, subject_label)

        # Register experiment with RF xsi_type so browse_view lists it
        # Check if experiment already registered
        existing = [e["experiment_label"] for e in server_connection._experiments]
        if experiment_label not in existing:
            server_connection.seed_rf_experiment(
                subject_label=subject_label,
                experiment_label=experiment_label,
                xsi_type="xnat:rfSessionData",
            )

        # Seed resource files from image_dir (or a placeholder if empty)
        image_dir_path = Path(image_dir) if image_dir else None
        files_to_seed = []

        if image_dir_path and image_dir_path.is_dir():
            img_files = sorted(image_dir_path.glob("*.dcm")) + \
                        sorted(image_dir_path.glob("*.dicom")) + \
                        sorted(image_dir_path.glob("*.png")) + \
                        sorted(image_dir_path.glob("*.jpg")) + \
                        sorted(image_dir_path.glob("*.jpeg"))
            for fp in img_files[:50]:  # cap at 50 files for demo
                files_to_seed.append((fp.name, fp.read_bytes()))

        if not files_to_seed:
            # Placeholder so browse shows num_files > 0
            files_to_seed = [("placeholder.dcm", b"FAKE_DICOM_DEMO")]

        # Create the XNAT path objects
        qs = f"/project/{project_name}/subject/{subject_label}/experiment/{experiment_label}/scan/{scan_id}"
        sel = server_connection.select(qs)
        sel.create()
        resource = sel.resource("SRC")
        server_connection.seed_resource_files(resource, files_to_seed)

        # Update demo_data closures — attach list hooks if the server has them
        # (build_demo_server adds callable hooks; we patch them to include new case)
        _patch_server_hooks(server_connection, subject_label, experiment_label, scan_id, len(files_to_seed))

    return _publish


def _patch_server_hooks(server: Any, subject: str, experiment: str, scan_id: str, num_files: int) -> None:
    """Extend the server's list_subjects / list_experiments hooks to include new case."""
    # Only patch if the hooks are the closure-based ones from build_demo_server
    original_list_subjects = getattr(server, "list_subjects", None)
    original_list_experiments = getattr(server, "list_experiments", None)
    original_list_scans = getattr(server, "list_scans", None)
    original_file_count = getattr(server, "file_count", None)

    if callable(original_list_subjects):
        _orig_ls = original_list_subjects
        def new_list_subjects(project_name: str) -> list:
            result = list(_orig_ls(project_name))
            if subject not in result:
                result.append(subject)
            return result
        server.list_subjects = new_list_subjects

    if callable(original_list_experiments):
        _orig_le = original_list_experiments
        def new_list_experiments(project_name: str, subj: str) -> list:
            result = list(_orig_le(project_name, subj))
            if subj == subject and experiment not in result:
                result.append(experiment)
            return result
        server.list_experiments = new_list_experiments

    if callable(original_list_scans):
        _orig_lsc = original_list_scans
        def new_list_scans(project_name: str, subj: str, exp: str) -> list:
            result = list(_orig_lsc(project_name, subj, exp))
            if subj == subject and exp == experiment and scan_id not in result:
                result.append(scan_id)
            return result
        server.list_scans = new_list_scans

    if callable(original_file_count):
        _orig_fc = original_file_count
        def new_file_count(project_name: str, subj: str, exp: str, sc: str) -> int:
            if subj == subject and exp == experiment and sc == scan_id:
                return num_files
            return _orig_fc(project_name, subj, exp, sc)
        server.file_count = new_file_count


# ---------------------------------------------------------------------------
# Real pyxnat publish path
# ---------------------------------------------------------------------------

def _make_real_publish_fn(server: Any) -> Callable:
    """
    Return a publish_fn that drives the real XNAT pipeline.

    Adapts the logic from tests/stress/driver.publish_surgery inline
    (does NOT import from tests/).
    """
    def _publish(
        server_connection: Any,
        form_values: dict,
        image_dir: Any,
        pixel_review_confirmer: Any,
    ) -> None:
        import json
        import time
        import pandas as pd

        from src.xnat_experiment_data import ReviewDecision, SourceRFSession
        from src.xnat_resource_data import ORDataIntakeForm

        # Ensure XNAT_IDENTITY_SALT is set (demo default — must be a valid hex string).
        if not os.environ.get("XNAT_IDENTITY_SALT"):
            os.environ["XNAT_IDENTITY_SALT"] = "64656d6f6f6e6c79646f6e6f747275737474686973696e70726f64756374696f"

        # Resolve connection/config from server_connection
        # server_connection may be a pyxnat Interface or a (conn, cfg, login) tuple
        if isinstance(server_connection, tuple) and len(server_connection) == 3:
            conn, config, login = server_connection
        else:
            # server_connection is a pyxnat Interface; need config from env
            from src.utilities import XNATLogin, XNATConnection, ConfigTables
            url = os.environ.get("XNAT_SERVER_URL", "http://localhost:8080")
            user = os.environ.get("XNAT_USERNAME", "admin")
            pwd = os.environ.get("XNAT_PASSWORD", "admin")
            project = os.environ.get("XNAT_PROJECT_NAME", "DEMO_UI")

            login_obj = XNATLogin(
                input_info={"URL": url, "USERNAME": user, "PASSWORD": pwd},
                verbose=False,
            )
            conn = XNATConnection(
                login_info=login_obj,
                stay_connected=True,
                verbose=False,
            )
            config = ConfigTables(login_info=login_obj, xnat_connection=conn, verbose=False)
            login = login_obj

        # Build intake series from form_values
        uid = (
            f"GUIDED_{form_values.get('filer_hawkid','user')}"
            f"_{form_values.get('operation_date','20240101').replace('-','')}"
            f"_{int(time.time()) % 100000}"
        )

        procedure = form_values.get("procedure_name", "INTRAMEDULLARY_NAIL-TIBIA")
        institution = form_values.get("institution_name", "UNIVERSITY_OF_IOWA_HOSPITALS_AND_CLINICS")
        surgeon = form_values.get("performing_surgeon", "UNKNOWN")
        epic_start = form_values.get("epic_start_time", "08:00")
        op_date = form_values.get("operation_date", "2024-01-15")
        hawkid = form_values.get("filer_hawkid", "testuser")
        quality = form_values.get("scan_quality", "usable") or "usable"

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
            hawkid,
            op_date,
            quality,
            institution,
            procedure,
            epic_start,
            "09:00",
            "RIGHT",
            "OR-1",
            "UNKNOWN",
            "PRESENT",
            surgeon if surgeon else "UNKNOWN",
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
            str(image_dir) if image_dir else "",
            "N",
            "",
            "",
        ]

        intake_series = pd.Series(intake_values, index=intake_series_index)

        # Refresh config
        try:
            config.pull_from_xnat(verbose=False)
        except Exception:
            pass

        # Build ORDataIntakeForm
        form = ORDataIntakeForm(
            config=config,
            validated_login=login,
            input_data=intake_series,
            verbose=False,
            write_file=True,
        )

        # Build SourceRFSession
        rf = SourceRFSession(intake_form=form, config=config)

        if not rf.is_valid:
            from src.services.errors import FriendlyError
            from app.logic.upload import UploadError
            raise UploadError(FriendlyError(
                title="Upload failed — invalid session data",
                message="SourceRFSession validation failed. Check form values.",
                recourse=["Verify all required fields are filled correctly."],
            ))

        # Write + publish
        zipped_data, cfg = rf.write(config=config, verbose=False)

        rf.publish_to_xnat(
            xnat_connection=conn,
            validated_login=login,
            zipped_data=zipped_data,
            pixel_review_confirmer=pixel_review_confirmer,
            verbose=False,
        )

        # Push updated config
        try:
            config.push_to_xnat(verbose=False)
        except Exception:
            pass

    return _publish
