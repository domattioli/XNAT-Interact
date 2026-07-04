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


# Set by demo_login(): the project name browse/download/upload must address.
# Real-XNAT mode → the actual server project (XNAT_PROJECT_NAME); fake → DEMO_PROJECT.
_ACTIVE_PROJECT: str = "DEMO_PROJECT"


def active_project_name() -> str:
    """Project name for the current demo session (real project in live mode)."""
    return _ACTIVE_PROJECT


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

    # Seed annotations for DEMO_S0001 / SOURCE_DATA-HIP_DEMO_001 / scan 0
    # Store under ANNOTATIONS resource so list_image_annotations can find them
    import json
    import numpy as np
    _ann_subject = "DEMO_S0001"
    _ann_experiment = "SOURCE_DATA-HIP_DEMO_001"
    _ann_scan = "0"
    _ann_qs = f"/project/DEMO_PROJECT/subject/{_ann_subject}/experiment/{_ann_experiment}/scan/{_ann_scan}"
    _ann_sel = fake.select(_ann_qs)
    _ann_sel.create()
    _ann_res = _ann_sel.resource("ANNOTATIONS")

    # Build 2 minimal RLE-style annotation blobs
    def _make_rle_blob(seed: int) -> bytes:
        """Make a minimal binary RLE blob (4 bytes header + data)."""
        rng = np.random.default_rng(seed)
        arr = (rng.random((16, 16)) > 0.5).astype(np.uint8)
        flat = arr.ravel().tolist()
        # Simple RLE: list of (value, count) pairs
        result = []
        for val in flat:
            if result and result[-1][0] == val:
                result[-1][1] += 1
            else:
                result.append([val, 1])
        return json.dumps(result).encode()

    _blob_v1 = _make_rle_blob(42)
    _blob_v2 = _make_rle_blob(99)

    _ann_files = [
        ("ann__demo_worker_A__binary_segmentation__v1.rle", _blob_v1),
        ("ann__demo_worker_B__binary_segmentation__v2.rle", _blob_v2),
        ("manifest.json", json.dumps({
            "annotations": [
                {"annotator_id": "demo_worker_A", "annotation_type": "binary_segmentation", "version": 1,
                 "blob_filename": "ann__demo_worker_A__binary_segmentation__v1.rle", "codec": "rle"},
                {"annotator_id": "demo_worker_B", "annotation_type": "binary_segmentation", "version": 2,
                 "blob_filename": "ann__demo_worker_B__binary_segmentation__v2.rle", "codec": "rle"},
            ]
        }).encode()),
    ]
    fake.seed_resource_files(_ann_res, _ann_files)
    # Register in resources registry so select().resource() finds the same instance
    fake._resources[(_ann_subject, _ann_experiment, _ann_scan, "ANNOTATIONS")] = _ann_res

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
# Real XNAT probe + seeding helpers
# ---------------------------------------------------------------------------

class _RealXNATServerProxy:
    """
    Thin proxy wrapping (conn, cfg, login) so publish_impl and other callers
    that check hasattr(server, '_experiments') get the real path (no _experiments).
    Also used by main.py to show the correct sidebar badge.
    """

    def __init__(self, conn: Any, cfg: Any, login: Any, url: str) -> None:
        self._conn = conn
        self._cfg = cfg
        self._login = login
        self._url = url
        self.project_name = os.environ.get("XNAT_PROJECT_NAME", "DEMO_UI")
        self._server = getattr(conn, "server", conn)

    def get(self, path: str) -> Any:  # noqa: A003
        """Forward HTTP GET to real pyxnat server for REST queries."""
        return self._server.get(path)

    def select(self, qs: str) -> Any:
        return self._server.select(qs)

    def disconnect(self) -> None:
        try:
            self._server.disconnect()
        except Exception:
            pass

    def get_conn_tuple(self) -> tuple:
        return (self._conn, self._cfg, self._login)


def _try_real_xnat_server() -> "tuple | None":
    """
    Probe real XNAT. Returns (url, conn, cfg, login) tuple or None.

    Only runs when XNAT_SERVER_URL is set in env.  Non-200 or timeout → None.
    Sets a demo XNAT_IDENTITY_SALT default if unset.
    """
    import logging
    import requests

    url = os.environ.get("XNAT_SERVER_URL", "")
    if not url:
        return None

    user = os.environ.get("XNAT_USERNAME", "admin")
    pwd = os.environ.get("XNAT_PASSWORD", "admin")
    project = os.environ.get("XNAT_PROJECT_NAME", "DEMO_UI")

    # Set default salt if unset (demo only).
    # Must be a valid hex string (32 bytes = 64 hex chars).
    _DEMO_HEX_SALT = "64656d6f6f6e6c79646f6e6f747275737474686973696e70726f64756374696f"
    if not os.environ.get("XNAT_IDENTITY_SALT"):
        logging.getLogger(__name__).warning(
            "XNAT_IDENTITY_SALT not set; using demo default hex salt."
        )
        os.environ["XNAT_IDENTITY_SALT"] = _DEMO_HEX_SALT

    try:
        resp = requests.get(
            f"{url}/data/JSESSION",
            auth=(user, pwd),
            timeout=3,
        )
        if resp.status_code != 200:
            return None
    except Exception:
        return None

    # Real XNAT reachable — connect and bootstrap
    try:
        os.environ["XNAT_SERVER_URL"] = url
        os.environ["XNAT_PROJECT_NAME"] = project

        # Ensure project exists (PUT /data/projects/{project})
        requests.put(
            f"{url}/data/projects/{project}",
            auth=(user, pwd),
            timeout=15,
        )

        from src.utilities import XNATLogin, XNATConnection, ConfigTables

        login_obj = XNATLogin(
            input_info={"URL": url, "USERNAME": user, "PASSWORD": pwd},
            verbose=False,
        )
        conn = XNATConnection(login_info=login_obj, stay_connected=True, verbose=False)

        # Seed config JSON if absent
        proj = conn.server.select.project(project)
        config_file = proj.resource("config").file("database_config.json")
        if not config_file.exists():
            _seed_real_xnat_config(conn, login_obj, project)

        cfg = ConfigTables(login_info=login_obj, xnat_connection=conn, verbose=False)
        if not cfg.is_user_registered(user):
            cfg.add_new_item("REGISTERED_USERS", user)
            cfg.push_to_xnat(verbose=False)

        # Seed 3 synthetic surgeries if project has fewer than 3 experiments
        _seed_real_xnat_cases(conn, cfg, login_obj, url, user, pwd, project)

        return (url, conn, cfg, login_obj)
    except Exception:
        return None


def _seed_real_xnat_config(conn: Any, login_obj: Any, project: str) -> None:
    """Seed a minimal database_config.json on a fresh project."""
    import json
    import tempfile
    from pathlib import Path
    from src.utilities import UIDandMetaInfo

    uid_gen = UIDandMetaInfo()
    now = uid_gen.now_datetime
    user = os.environ.get("XNAT_USERNAME", "admin")

    def new_uid() -> str:
        return uid_gen.generate_uid()

    tables = {
        "REGISTERED_USERS": [
            {"NAME": user.upper(), "UID": new_uid(), "CREATED_DATE_TIME": now, "CREATED_BY": new_uid()},
        ],
        "ACQUISITION_SITES": [
            {"NAME": "UNIVERSITY_OF_IOWA_HOSPITALS_AND_CLINICS", "UID": new_uid(), "CREATED_DATE_TIME": now, "CREATED_BY": new_uid()},
        ],
        "GROUPS": [
            {"NAME": "INTRAMEDULLARY_NAIL-TIBIA", "UID": new_uid(), "CREATED_DATE_TIME": now, "CREATED_BY": new_uid()},
        ],
        "SUBJECTS": [],
        "IMAGE_HASHES": [],
        "SURGEONS": [
            {"NAME": "UNKNOWN", "UID": new_uid(), "CREATED_DATE_TIME": now, "CREATED_BY": new_uid(),
             "FIRST_NAME": "UNKNOWN", "LAST_NAME": "UNKNOWN", "MIDDLE_INITIAL": ""},
            {"NAME": "NOT-APPLICABLE", "UID": new_uid(), "CREATED_DATE_TIME": now, "CREATED_BY": new_uid(),
             "FIRST_NAME": "NOT-APPLICABLE", "LAST_NAME": "NOT-APPLICABLE", "MIDDLE_INITIAL": "NA"},
        ],
    }
    metadata = {
        "CREATED": now, "LAST_MODIFIED": now, "CREATED_BY": new_uid(),
        "TABLE_EXTRA_COLUMNS": {
            "SUBJECTS": ["ACQUISITION_SITE", "GROUP"],
            "IMAGE_HASHES": ["SUBJECT", "INSTANCE_NUM"],
            "SURGEONS": ["FIRST_NAME", "LAST_NAME", "MIDDLE_INITIAL"],
        },
    }
    seed_json = json.dumps({"metadata": metadata, "tables": tables}, indent=2)
    seed_path = Path(tempfile.mktemp(suffix=".json", prefix="seed_config_"))
    seed_path.write_text(seed_json, encoding="utf-8")

    try:
        proj = conn.server.select.project(project)
        proj.resource("config").file("database_config.json").put(
            str(seed_path),
            content="META_DATA",
            format="JSON",
            tags="DOC",
            overwrite=True,
        )
    finally:
        seed_path.unlink(missing_ok=True)


def _seed_real_xnat_cases(
    conn: Any,
    cfg: Any,
    login_obj: Any,
    url: str,
    user: str,
    pwd: str,
    project: str,
) -> None:
    """Seed 3 synthetic surgeries if the project has fewer than 3 experiments."""
    import requests
    import tempfile
    from pathlib import Path

    try:
        resp = requests.get(
            f"{url}/data/experiments?project={project}&format=json",
            auth=(user, pwd),
            timeout=10,
        )
        existing_count = 0
        if resp.status_code == 200:
            data = resp.json()
            results = data.get("ResultSet", {}).get("Result", [])
            existing_count = len(results)
    except Exception:
        existing_count = 0

    if existing_count >= 3:
        return  # already seeded

    # Import factory to generate DICOMs (tests/ is on sys.path at runtime)
    try:
        from tests.stress.factory import make_surgery
    except ImportError:
        return  # factory unavailable — skip seeding silently

    from app.guided.publish_impl import _make_real_publish_fn
    from src.xnat_experiment_data import ReviewDecision as _RD

    _publish = _make_real_publish_fn(None)

    seed_specs = [
        ("DEMO_SEED_001", {10, 11, 12, 13, 14}),
        ("DEMO_SEED_002", {20, 21, 22, 23, 24, 25}),
        ("DEMO_SEED_003", {30, 31, 32, 33}),
    ]

    with tempfile.TemporaryDirectory() as tmpdir:
        for uid, seeds in seed_specs:
            try:
                surgery_dir = make_surgery(uid, seeds, Path(tmpdir), rows=256, cols=256)
                _publish(
                    server_connection=(conn, cfg, login_obj),
                    form_values={
                        "filer_hawkid": user,
                        "operation_date": "2024-01-15",
                        "institution_name": "UNIVERSITY_OF_IOWA_HOSPITALS_AND_CLINICS",
                        "procedure_name": "INTRAMEDULLARY_NAIL-TIBIA",
                        "performing_surgeon": "UNKNOWN",
                        "epic_start_time": "08:00",
                        "scan_quality": "usable",
                    },
                    image_dir=surgery_dir,
                    pixel_review_confirmer=lambda ctx: (_RD.CONFIRMED, None),
                )
            except Exception:
                pass  # non-blocking; demo still usable


# ---------------------------------------------------------------------------
# Demo login
# ---------------------------------------------------------------------------

def demo_login() -> LoginResult:
    """
    Authenticate in demo mode.

    Probes real XNAT first (if XNAT_SERVER_URL is set and reachable).
    Falls back to FakeXNAT if real XNAT is unavailable.

    Returns
    -------
    LoginResult with ok=True (always).
    server is _RealXNATServerProxy (real) or FakeXNAT (fake).
    """
    global _ACTIVE_PROJECT
    # Try real XNAT when XNAT_SERVER_URL is set
    real = _try_real_xnat_server()
    if real is not None:
        url, conn, cfg, login_obj = real
        proxy = _RealXNATServerProxy(conn, cfg, login_obj, url)
        _ACTIVE_PROJECT = os.environ.get("XNAT_PROJECT_NAME", "DEMO_UI")
        return LoginResult(ok=True, username=os.environ.get("XNAT_USERNAME", "admin"), server=proxy, friendly=None)

    # Fall back to FakeXNAT
    _ACTIVE_PROJECT = "DEMO_PROJECT"
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
