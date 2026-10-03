"""
FR-016 / SC-009: server state survives a container stop and start.

Writes a sentinel resource, runs ``docker stop`` then ``docker start`` on
xnat-local-it (never ``rm``), waits for readiness, opens a new session and
checks that TEST_PROJ and the sentinel are still there. The conftest collection
hook runs this module last, so no other live test runs after the restart.
"""
from __future__ import annotations

import subprocess
import tempfile
import uuid
from pathlib import Path

import pytest

from tests.integration.live_xnat import helpers as H
from tests.integration.live_xnat.conftest import CONTAINER_NAME, _poll_xnat_readiness

pytestmark = [pytest.mark.requires_server, pytest.mark.slow]

RESOURCE = "LIVE_IT_SENTINEL"


def test_state_survives_container_restart(live_xnat_server):
    from src.services import xnat_conventions as conventions

    project = live_xnat_server["project_name"]
    proj_qs = conventions.project_qs(project)
    token = uuid.uuid4().hex
    with tempfile.TemporaryDirectory() as td:
        ffn = Path(td) / "sentinel.txt"
        ffn.write_text(token, encoding="utf-8")
        live_xnat_server["connection"].gateway.put_file(
            proj_qs, RESOURCE, "sentinel.txt", str(ffn), content="TEST", format="TXT",
            tags="LIVE_IT", overwrite=True)

    subprocess.run(["docker", "stop", CONTAINER_NAME], timeout=120, check=True)
    subprocess.run(["docker", "start", CONTAINER_NAME], timeout=120, check=True)
    if not _poll_xnat_readiness(live_xnat_server["server_url"], timeout=900):
        pytest.fail(f"server unreachable: {live_xnat_server['server_url']}")

    live_xnat_server["new_session"]()
    projects = H.get_json(live_xnat_server, "/data/projects?format=json")["ResultSet"]["Result"]
    assert any(p.get("ID") == project for p in projects), f"{project} missing after restart"
    body = H.get_bytes(live_xnat_server, f"/data/projects/{project}/resources/{RESOURCE}/files/sentinel.txt")
    assert body.decode("utf-8") == token
