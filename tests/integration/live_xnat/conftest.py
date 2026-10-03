"""
Pytest configuration for live-XNAT integration tests.

Session-scoped fixtures for booting and configuring the test XNAT instance.
"""
from __future__ import annotations

import json
import os
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Generator

import pytest
import requests

from tests.integration.live_xnat.staple_stub import register as register_staple_stub
from tests.integration.live_xnat.outcome_summary import OutcomeSummaryWriter

# NOTE: the STAPLE stand-in is registered inside the live_xnat_server fixture,
# not at import time. Registering at collection time leaked "staple" into the
# global aggregator registry and broke the offline test
# tests/test_annotations_aggregate.py::TestT013StapleNotAutoRegistered.


def _poll_xnat_readiness(url: str, timeout: int = 900) -> bool:
    """
    Poll the XNAT root URL for HTTP 200/302 readiness.

    Parameters
    ----------
    url
        Base server URL (e.g., http://localhost:8080).
    timeout
        Maximum seconds to wait.

    Returns
    -------
    bool
        True if server responds 200/302; False if timeout reached.
    """
    start = time.time()
    while time.time() - start < timeout:
        try:
            resp = requests.head(url, timeout=10, allow_redirects=True)
            if resp.status_code in (200, 302):
                return True
        except (requests.RequestException, Exception):
            pass
        time.sleep(10)
    return False


def _build_seed_config(username: str) -> str:
    """
    Build minimal database_config.json for ConfigTables seeding.

    Mirrors the workaround from tests/integration/run_roundtrip_push.py STEP 4-pre.
    """
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
    surgeons_names = ["UNKNOWN", "NOT-APPLICABLE", "KARAMM", "KOWALSKIH"]
    registered_users = ["DMATTIOLI", "STELONG", "ADMIN", username.upper()]

    tables = {
        "REGISTERED_USERS": make_table(list(dict.fromkeys(registered_users))),
        "ACQUISITION_SITES": make_table(acq_sites),
        "GROUPS": make_table(groups),
        "SUBJECTS": [
            r | {"ACQUISITION_SITE": None, "GROUP": None} for r in []
        ],
        "IMAGE_HASHES": [r | {"SUBJECT": None, "INSTANCE_NUM": None} for r in []],
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


CONTAINER_NAME = "xnat-local-it"
CASE_LEDGER_RESOURCE = "LIVE_IT"
CASE_LEDGER_FILE = "case_subjects.json"
# Synthetic, non-secret salt so identity hashes are deterministic across runs.
_TEST_IDENTITY_SALT = "deadbeef" * 8


def _required_env(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        pytest.fail(
            f"{name} is not set. Export XNAT_SERVER_URL, XNAT_PROJECT_NAME, "
            f"XNAT_USERNAME and XNAT_PASSWORD in the shell that runs pytest."
        )
    return value


def _ensure_container(build_script: Path) -> None:
    """Attach, start a stopped container, or build only when none exists (FR-016)."""
    if not build_script.exists():
        pytest.fail(f"XNAT build script not found: {build_script}")
    try:
        running = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}"],
            capture_output=True, text=True, timeout=10,
        ).stdout.split()
        existing = subprocess.run(
            ["docker", "ps", "-a", "--format", "{{.Names}}"],
            capture_output=True, text=True, timeout=10,
        ).stdout.split()
        if CONTAINER_NAME in running:
            print(f"Container {CONTAINER_NAME} already running: attaching")
        elif CONTAINER_NAME in existing:
            print(f"Container {CONTAINER_NAME} stopped: starting (state preserved)")
            subprocess.run(["docker", "start", CONTAINER_NAME], timeout=60, check=True)
        else:
            # build_and_run.sh runs `docker rm -f`; call it only when no container exists.
            print(f"Container {CONTAINER_NAME} absent: building and booting")
            subprocess.run(["bash", str(build_script)], timeout=1200, check=True)
    except Exception as e:  # noqa: BLE001
        pytest.fail(f"Docker command failed for {CONTAINER_NAME}: {e}")


def _read_case_ledger(conn, project_name: str) -> dict:
    """Case name to list of subject uids the harness published (server-side ledger)."""
    uri = f"/data/projects/{project_name}/resources/{CASE_LEDGER_RESOURCE}/files/{CASE_LEDGER_FILE}"
    try:
        raw = conn.server._exec(uri, "GET")
    except Exception:  # noqa: BLE001
        return {}
    try:
        data = json.loads(raw)
    except (ValueError, TypeError):
        return {}
    return data if isinstance(data, dict) else {}


def write_case_ledger(conn, project_name: str, ledger: dict) -> None:
    """Store the case ledger on the server so the next run can reset those subjects."""
    from src.services import xnat_conventions as conventions

    proj_qs = conventions.project_qs(project_name)
    with tempfile.TemporaryDirectory() as td:
        ffn = Path(td) / CASE_LEDGER_FILE
        ffn.write_text(json.dumps(ledger, indent=2, sort_keys=True), encoding="utf-8")
        conn.gateway.put_file(
            proj_qs, CASE_LEDGER_RESOURCE, CASE_LEDGER_FILE, str(ffn),
            content="META_DATA", format="JSON", tags="DOC", overwrite=True,
        )


def _project_subjects(conn, project_name: str) -> dict:
    """Subject label to subject ID for the project."""
    raw = conn.server._exec(f"/data/projects/{project_name}/subjects?format=json", "GET")
    rows = json.loads(raw)["ResultSet"]["Result"]
    return {r["label"]: r["ID"] for r in rows}


def _reset_case_subjects(conn, config_factory, project_name: str, case_names) -> dict:
    """
    FR-002: delete only the subjects this harness published for the three cases,
    then drop their SUBJECTS and IMAGE_HASHES rows from the server config.
    """
    ledger = _read_case_ledger(conn, project_name)
    targets = {u for name in case_names for u in ledger.get(name, [])}
    labels = _project_subjects(conn, project_name)
    removed_subjects, undeletable = [], {}
    for uid in sorted(targets):
        if uid in labels:
            try:
                conn.server._exec(
                    f"/data/projects/{project_name}/subjects/{uid}?removeFiles=true", "DELETE"
                )
                removed_subjects.append(uid)
            except Exception as e:  # noqa: BLE001
                # SPEC-ISSUE-15: the local XNAT 1.9.3 image has no element security
                # for xnat:rfSessionData, so even the admin session gets HTTP 403 here.
                undeletable[uid] = str(e).split("content:")[-1][:200]

    config = config_factory()
    tables = config.tables
    subj = tables["SUBJECTS"]
    hashes = tables["IMAGE_HASHES"]
    n_subj = int(subj["NAME"].isin(targets).sum()) if len(subj) else 0
    n_hash = int(hashes["SUBJECT"].isin(targets).sum()) if len(hashes) else 0
    if n_subj or n_hash:
        tables["SUBJECTS"] = subj[~subj["NAME"].isin(targets)].reset_index(drop=True)
        tables["IMAGE_HASHES"] = hashes[~hashes["SUBJECT"].isin(targets)].reset_index(drop=True)
        if not config.push_to_xnat(verbose=False):
            pytest.fail("Reset could not push the cleaned ConfigTables to XNAT.")
    print(
        f"Reset removed subjects {removed_subjects}, {n_subj} SUBJECTS rows, "
        f"{n_hash} IMAGE_HASHES rows"
    )

    # Re-query: nothing of the case subjects may remain.
    left_server = (set(_project_subjects(conn, project_name)) & targets) - set(undeletable)
    fresh = config_factory().tables
    left_rows = set(fresh["SUBJECTS"]["NAME"]) & targets
    left_hashes = set(fresh["IMAGE_HASHES"]["SUBJECT"]) & targets
    if left_server or left_rows or left_hashes:
        pytest.fail(
            f"Reset incomplete: server subjects {sorted(left_server)}, "
            f"SUBJECTS rows {sorted(left_rows)}, IMAGE_HASHES subjects {sorted(left_hashes)}"
        )
    if undeletable:
        print(
            f"Reset could not delete {len(undeletable)} case subject(s) on the server "
            f"(HTTP 403, SPEC-ISSUE-15); their identity rows were purged: {sorted(undeletable)}"
        )
    for name in case_names:
        ledger[name] = []
    ledger.setdefault("undeletable", [])
    ledger["undeletable"] = sorted(set(ledger["undeletable"]) | set(undeletable))
    write_case_ledger(conn, project_name, ledger)
    return {"subjects": removed_subjects, "subject_rows": n_subj, "hash_rows": n_hash,
            "undeletable": sorted(undeletable)}


@pytest.fixture(scope="session")
def live_xnat_server() -> Generator[dict, None, None]:
    """
    Session fixture: attach to (or start) xnat-local-it, open an admin session,
    ensure TEST_PROJ and its config exist (fatal on failure), reset the three
    case subjects, and register the STAPLE stand-in.

    Yields a dict without the password: server_url, project_name, username,
    connection, login, config, new_session (factory), reset (what was removed).
    """
    server_url = _required_env("XNAT_SERVER_URL")
    project_name = _required_env("XNAT_PROJECT_NAME")
    username = _required_env("XNAT_USERNAME")
    password = _required_env("XNAT_PASSWORD")
    os.environ.setdefault("XNAT_IDENTITY_SALT", _TEST_IDENTITY_SALT)

    _ensure_container(Path(__file__).parent.parent / "xnat_local" / "build_and_run.sh")
    if not _poll_xnat_readiness(server_url, timeout=900):
        pytest.fail(f"server unreachable: {server_url}")
    print(f"XNAT server ready at {server_url}")

    from tests.stress.driver import connect

    try:
        # Creates the project if absent and seeds config; raises on any failure.
        conn, config, login = connect(server_url, project_name, username, password, verbose=False)
    except Exception as e:  # noqa: BLE001
        pytest.fail(f"Connection or config seeding failed for {server_url}: {e}")

    from src.utilities import ConfigTables, XNATConnection, XNATLogin

    info = {
        "server_url": server_url,
        "project_name": project_name,
        "username": username,
        "connection": conn,
        "login": login,
        "config": config,
    }

    def new_session():
        """
        Fresh XNATLogin/XNATConnection. XNATConnection is a process-wide
        singleton (src/utilities.py:380, SPEC-ISSUE-19): opening one closes the
        previous one, so the shared info dict is switched to the new pair.
        """
        lg = XNATLogin(
            input_info={"URL": server_url, "USERNAME": username, "PASSWORD": password},
            verbose=False,
        )
        cn = XNATConnection(login_info=lg, stay_connected=True, verbose=False)
        if not cn.is_verified or not cn.is_open:
            pytest.fail(f"admin session could not be opened: {server_url}")
        info["login"], info["connection"] = lg, cn
        return lg, cn

    def gateway_session():
        """Independent production PyxnatGateway session (for concurrency, T039)."""
        from src.services.xnat_gateway import build_gateway

        gw = build_gateway(server_url, username, password)
        if getattr(gw, "server", None) is None:
            gw.connect()
        return gw

    def config_factory():
        cfg = ConfigTables(info["login"], info["connection"], verbose=False)
        cfg.pull_from_xnat(verbose=False)
        return cfg

    from tests.integration.live_xnat.cases import CASE_NAMES

    reset = _reset_case_subjects(conn, config_factory, project_name, CASE_NAMES)

    from src.annotations.aggregate import _AGGREGATOR_REGISTRY

    register_staple_stub()

    info.update(config_factory=config_factory, new_session=new_session,
                gateway_session=gateway_session, reset=reset)
    yield info

    conn = info["connection"]
    _AGGREGATOR_REGISTRY.pop("staple", None)
    try:
        conn.close()
    except Exception as e:  # noqa: BLE001
        print(f"Warning: Failed to close XNAT connection: {e}")
    # Preserve container state (FR-016): stop, never remove.
    print(f"Tearing down: stopping container {CONTAINER_NAME} (not removing)")
    try:
        subprocess.run(["docker", "stop", CONTAINER_NAME], timeout=60, check=False)
    except Exception as e:  # noqa: BLE001
        print(f"Warning: Failed to stop container: {e}")


@pytest.fixture(scope="session")
def config_tables(live_xnat_server):
    """Fresh ConfigTables pulled from the server (FR-013)."""
    return live_xnat_server["config_factory"]()


@pytest.fixture(scope="session")
def knee_rf_published(live_xnat_server, tmp_path_factory):
    """
    Publish the KNEE_2025 radiofluoro session once per live run.

    Spec 015's download-manifest module and the spec 014 KNEE module both need
    this session on the server. Publishing it twice in one run trips the
    spec 009 duplicate guard, so both modules share this one publish.
    Returns the case object, its work folder and the publish result.
    """
    from tests.integration.live_xnat import helpers as H
    from tests.integration.live_xnat.cases import build_case

    work = tmp_path_factory.mktemp("knee_2025_shared")
    case = build_case("KNEE_2025", work / "src")
    result = H.publish_case_session(live_xnat_server, case, "rf", work)
    H.record_case_subject(live_xnat_server, "KNEE_2025", result.uid)
    return {"case": case, "work": work, "rf": result}


@pytest.fixture(scope="session")
def new_session(live_xnat_server):
    """Factory returning a fresh (XNATLogin, XNATConnection) pair (FR-010)."""
    return live_xnat_server["new_session"]


@pytest.fixture(scope="module")
def outcome_writer() -> OutcomeSummaryWriter:
    """
    Module-scoped fixture for outcome summary writing: one writer per case module.

    Tests in a case module append PipelinePhaseRecords and write the final
    outcome JSON at the end of that case's test sequence. Module scope keeps
    phase records from one case out of another case's summary.
    """
    return OutcomeSummaryWriter()


def pytest_collection_modifyitems(config, items):
    """T041: run the container-restart test after every other live test."""
    last = [i for i in items if "test_teardown_persistence" in i.nodeid]
    if last:
        items[:] = [i for i in items if i not in last] + last


@pytest.fixture
def downloads_cleanup(live_xnat_server):
    """
    Spec 015 (T028): collect DOWNLOADS manifest names a test created on the
    test project and try to delete them afterwards so reruns start clean.

    Deletion is best effort: the local XNAT image refused admin DELETE in
    spec 014 (SPEC-ISSUE-15), and manifest names are unique per run, so a
    leftover never collides.  Any failure is printed as a note, never raised.
    """
    created: list = []
    yield created
    from src.services import xnat_conventions as conventions

    proj_qs = conventions.project_qs(live_xnat_server["project_name"])
    try:
        gw = live_xnat_server["gateway_session"]()
    except Exception as e:  # noqa: BLE001
        print(f"Note: could not open a session to clean DOWNLOADS: {type(e).__name__}")
        return
    for name in created:
        try:
            gw.delete_file(proj_qs, conventions.DOWNLOADS_RESOURCE, name)
        except Exception as e:  # noqa: BLE001
            print(f"Note: could not delete DOWNLOADS/{name} ({type(e).__name__}); left in place")
