"""
app/guided/demo — Demo mode support for Streamlit guided app.

Provides a fully synthetic XNAT environment for Streamlit Cloud deployment
and offline testing. Runs entirely in-memory with no real XNAT server.

Public API
----------
is_demo_mode() -> bool
    Returns True if XNAT_DEMO_MODE env var is set (case-insensitive).

class DemoConfig
    Config stub with synthetic surgeon/site/group lists for demo mode.

build_demo_server() -> FakeXNAT
    Construct a FakeXNAT seeded with 2-3 synthetic surgeries.

demo_connect_factory(url, user, password) -> FakeXNAT
    Factory for attempt_login() that ignores all args and returns demo server.

demo_login() -> LoginResult
    Run attempt_login() against demo server; returns live LoginResult.
"""
from __future__ import annotations

import os
from typing import Any, List

from app.logic.auth import LoginResult, attempt_login
from src.services.config import AppConfig
from tests.fakes.fake_xnat import FakeXNAT


# ---------------------------------------------------------------------------
# Demo mode detection
# ---------------------------------------------------------------------------

def is_demo_mode() -> bool:
    """
    Return True if XNAT_DEMO_MODE env var is set to a truthy value.

    Case-insensitive: "1", "true", "yes", "on" all return True.
    """
    mode_str = os.environ.get("XNAT_DEMO_MODE", "").lower().strip()
    return mode_str in ("1", "true", "yes", "on")


# ---------------------------------------------------------------------------
# DemoConfig — synthetic surgeon/site/procedure lists
# ---------------------------------------------------------------------------

class DemoConfig:
    """
    Config stub with hardcoded synthetic values for demo mode.

    Implements the list_of_all_items_in_table() interface expected by
    app.logic.upload.dropdown_options().
    """

    _DEMO_TABLES = {
        "surgeons": ["dr_smith", "dr_jones", "dr_okafor"],
        "acquisition_sites": [
            "UNIVERSITY_OF_IOWA_HOSPITALS_AND_CLINICS",
            "MERCY_HOSPITAL",
        ],
        "groups": [
            "PEDIATRIC_SUPRACONDYLAR_HUMERUS_FRACTURE",
            "HIP_ARTHROSCOPY",
            "ANKLE_ORIF",
        ],
    }

    project_name: str = "DEMO_PROJECT"

    def list_of_all_items_in_table(self, table_name: str) -> List[str]:
        """
        Return synthetic list for *table_name*.

        Matches case-insensitively against the table name.
        """
        key = table_name.lower()
        return self._DEMO_TABLES.get(key, [])


# ---------------------------------------------------------------------------
# Demo server builder
# ---------------------------------------------------------------------------

def build_demo_server() -> FakeXNAT:
    """
    Construct and seed a FakeXNAT with synthetic surgeries.

    Returns
    -------
    FakeXNAT with 3 seeded subjects/experiments/scans.
    """
    fake = FakeXNAT(project_name="DEMO_PROJECT", fidelity_mode=True)

    # Data structure: {subject: {experiment: {date, scans: {scan_id: {scan_type, num_files}}}}}
    demo_data = {
        "DEMO_S0001": {
            "SOURCE_DATA-HIP_DEMO_001": {
                "date": "2025-01-15",
                "scans": {
                    "0": {"scan_type": "DICOM", "num_files": 2},
                },
            },
        },
        "DEMO_S0002": {
            "SOURCE_DATA-HUMERUS_DEMO_001": {
                "date": "2024-06-30",
                "scans": {
                    "0": {"scan_type": "DICOM", "num_files": 1},
                },
            },
        },
        "DEMO_S0003": {
            "SOURCE_DATA-ANKLE_DEMO_001": {
                "date": "2025-02-10",
                "scans": {
                    "0": {"scan_type": "DICOM", "num_files": 3},
                },
            },
        },
    }

    # Seed subject labels
    fake.seed_subject_label("DEMO_S0001", "DEMO_S0001")
    fake.seed_subject_label("DEMO_S0002", "DEMO_S0002")
    fake.seed_subject_label("DEMO_S0003", "DEMO_S0003")

    # Seed experiments with types
    fake.seed_rf_experiment(
        subject_label="DEMO_S0001",
        experiment_label="SOURCE_DATA-HIP_DEMO_001",
        xsi_type="xnat:rfSessionData",
    )
    fake.seed_rf_experiment(
        subject_label="DEMO_S0002",
        experiment_label="SOURCE_DATA-HUMERUS_DEMO_001",
        xsi_type="xnat:rfSessionData",
    )
    fake.seed_rf_experiment(
        subject_label="DEMO_S0003",
        experiment_label="SOURCE_DATA-ANKLE_DEMO_001",
        xsi_type="xnat:rfSessionData",
    )

    # Seed resource files for each scan
    for subject, experiments in demo_data.items():
        for exp, exp_data in experiments.items():
            for scan_id, scan_data in exp_data.get("scans", {}).items():
                num_files = scan_data.get("num_files", 0)
                files = [
                    (f"DICOM_{i:04d}.dcm", f"fake_dicom_{subject}_{exp}_{scan_id}_{i}".encode())
                    for i in range(1, num_files + 1)
                ]
                # Get/create the resource and seed files
                qs = f"/project/DEMO_PROJECT/subject/{subject}/experiment/{exp}/scan/{scan_id}"
                sel = fake.select(qs)
                sel.create()
                res = sel.resource("SRC")
                fake.seed_resource_files(res, files)

    # Add list_subjects_with_labels hook for browse.fetch_data_table()
    def list_subjects_with_labels(project_name: str) -> list[tuple]:
        """Return [(internal_id, label), ...] for label resolution."""
        return list(fake._subject_labels.items())

    fake.list_subjects_with_labels = list_subjects_with_labels

    # Add fallback hooks for browse.fetch_data_table() when select().get() is unavailable
    def list_subjects(project_name: str) -> list[str]:
        return list(demo_data.keys())

    def list_experiments(project_name: str, subject: str) -> list[str]:
        return list(demo_data.get(subject, {}).keys())

    def list_scans(project_name: str, subject: str, experiment: str) -> list[str]:
        exp_data = demo_data.get(subject, {}).get(experiment, {})
        return list(exp_data.get("scans", {}).keys())

    def file_count(project_name: str, subject: str, experiment: str, scan: str) -> int:
        exp_data = demo_data.get(subject, {}).get(experiment, {})
        scan_data = exp_data.get("scans", {}).get(scan, {})
        return scan_data.get("num_files", -1)

    def experiment_date(project_name: str, subject: str, experiment: str) -> str:
        exp_data = demo_data.get(subject, {}).get(experiment, {})
        return exp_data.get("date", "")

    def scan_attrs(project_name: str, subject: str, experiment: str, scan: str) -> dict:
        exp_data = demo_data.get(subject, {}).get(experiment, {})
        scan_data = exp_data.get("scans", {}).get(scan, {})
        return {"scan_type": scan_data.get("scan_type", "")}

    fake.list_subjects = list_subjects
    fake.list_experiments = list_experiments
    fake.list_scans = list_scans
    fake.file_count = file_count
    fake.experiment_date = experiment_date
    fake.scan_attrs = scan_attrs

    return fake


# ---------------------------------------------------------------------------
# Demo connect factory for attempt_login()
# ---------------------------------------------------------------------------

def demo_connect_factory(url: str, user: str, password: str) -> FakeXNAT:
    """
    Connect factory for demo mode.

    Ignores url, user, password and returns the demo server.
    For use as connect_factory parameter to attempt_login().
    """
    return build_demo_server()


# ---------------------------------------------------------------------------
# Demo login
# ---------------------------------------------------------------------------

def demo_login() -> LoginResult:
    """
    Authenticate in demo mode.

    Calls attempt_login() with demo credentials and demo server factory.
    Returns LoginResult with demo user authenticated against the in-memory
    demo server.

    Returns
    -------
    LoginResult with ok=True (always), server=demo FakeXNAT, username="demo_user".
    """
    demo_config = AppConfig(
        server_url="http://demo.local",
        project_name="DEMO_PROJECT",
    )

    result = attempt_login(
        username="demo_user",
        password="demo",
        connect_factory=demo_connect_factory,
        config=demo_config,
    )

    return result
