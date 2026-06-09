"""
T017 & T019 — boot_and_verify_local_xnat.sh tests (011 US3).

Tests drive the boot script via subprocess with mocked/stubbed endpoints.
No real XNAT container, no docker startup in unit lane (synthetic probes).

Test inventory
--------------
test_nonlocal_host_refused          — XNAT_LOCAL_HOST=rpacs.example.com → exit 2, nothing started
test_nonlocal_host_stderr_message   — exit 2 stderr names the refusal
test_already_running_skips_boot     — port probe succeeds → skip docker compose, proceed to verify
test_already_running_healthy        — with running XNAT (stubbed probes), report HEALTHY
test_failing_probe_exits_nonzero    — stub auth probe to fail → exit non-zero
test_failing_probe_stderr_names_it  — failed probe name appears in stderr
test_idempotent_no_error            — running twice with running XNAT does not error
test_docker_absent_exits_nonzero    — if docker absent and not already running → exit non-zero
test_all_probes_pass_exits_zero     — all probes mocked to pass → HEALTHY, exit 0
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Optional

import pytest

# Skip entire module if bash/curl not available
pytest.skip(allow_module_level=True) if not os.path.exists("/bin/bash") else None


class BootVerifyRunner:
    """Helper to run the boot script with stubbed probes and optional host override."""

    def __init__(self, script_path: Path):
        self.script_path = script_path

    def run(
        self,
        host: str = "localhost",
        port: str = "8080",
        *,
        probe_up_cmd: Optional[str] = None,
        probe_auth_cmd: Optional[str] = None,
        probe_project_list_cmd: Optional[str] = None,
        probe_roundtrip_cmd: Optional[str] = None,
        compose_up_cmd: Optional[str] = None,
        timeout: int = 30,
    ) -> subprocess.CompletedProcess:
        """
        Run the boot script with given environment overrides.

        Args:
            host: XNAT_LOCAL_HOST
            port: XNAT_LOCAL_PORT
            probe_up_cmd: XNAT_PROBE_UP_CMD (if None, use script default)
            probe_auth_cmd: XNAT_PROBE_AUTH_CMD
            probe_project_list_cmd: XNAT_PROBE_PROJECT_LIST_CMD
            probe_roundtrip_cmd: XNAT_PROBE_ROUNDTRIP_CMD
            compose_up_cmd: XNAT_COMPOSE_UP_CMD (if None, use script default)
            timeout: subprocess timeout in seconds

        Returns:
            CompletedProcess with returncode, stdout, stderr.
        """
        env = os.environ.copy()
        env["XNAT_LOCAL_HOST"] = host
        env["XNAT_LOCAL_PORT"] = port

        if probe_up_cmd is not None:
            env["XNAT_PROBE_UP_CMD"] = probe_up_cmd
        if probe_auth_cmd is not None:
            env["XNAT_PROBE_AUTH_CMD"] = probe_auth_cmd
        if probe_project_list_cmd is not None:
            env["XNAT_PROBE_PROJECT_LIST_CMD"] = probe_project_list_cmd
        if probe_roundtrip_cmd is not None:
            env["XNAT_PROBE_ROUNDTRIP_CMD"] = probe_roundtrip_cmd
        if compose_up_cmd is not None:
            env["XNAT_COMPOSE_UP_CMD"] = compose_up_cmd

        return subprocess.run(
            ["/bin/bash", str(self.script_path)],
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout,
        )


@pytest.fixture
def boot_script():
    """Path to the boot_and_verify_local_xnat.sh script."""
    repo_root = Path(__file__).resolve().parents[1]
    script = repo_root / "scripts" / "boot_and_verify_local_xnat.sh"
    assert script.exists(), f"Script not found: {script}"
    return script


@pytest.fixture
def runner(boot_script):
    """BootVerifyRunner instance."""
    return BootVerifyRunner(boot_script)


# --- Tests ---


def test_nonlocal_host_refused(runner):
    """Non-localhost host → exit 2, nothing started (FR-012)."""
    result = runner.run(
        host="rpacs.example.com",
        probe_up_cmd="true",  # fallback should never be reached
    )
    assert result.returncode == 2


def test_nonlocal_host_stderr_message(runner):
    """Exit 2 stderr explains the refusal."""
    result = runner.run(
        host="rpacs.example.com",
        probe_up_cmd="true",
    )
    assert result.returncode == 2
    assert "not localhost" in result.stderr or "Refusing" in result.stderr


def test_already_running_skips_boot(runner):
    """Port probe success → skip docker compose, proceed to verify (idempotent FR-013)."""
    # Stub all probes to succeed; port-up probe is a success = already running
    result = runner.run(
        host="127.0.0.1",
        probe_up_cmd="true",
        probe_auth_cmd="true",
        probe_project_list_cmd="true",
        probe_roundtrip_cmd="true",
    )
    assert result.returncode == 0
    assert "HEALTHY" in result.stdout


def test_already_running_healthy(runner):
    """Already-running XNAT (all probes stubbed to pass) → HEALTHY."""
    result = runner.run(
        host="localhost",
        probe_up_cmd="true",
        probe_auth_cmd="true",
        probe_project_list_cmd="true",
        probe_roundtrip_cmd="true",
    )
    assert result.returncode == 0
    assert "HEALTHY" in result.stdout
    # Verify per-probe markers
    assert "✓ up" in result.stdout
    assert "✓ auth" in result.stdout
    assert "✓ project-list" in result.stdout
    assert "✓ synthetic-roundtrip" in result.stdout


def test_failing_probe_exits_nonzero(runner):
    """Stub auth probe to fail → exit non-zero (FR-013)."""
    result = runner.run(
        host="localhost",
        probe_up_cmd="true",
        probe_auth_cmd="false",  # fail
        probe_project_list_cmd="true",
        probe_roundtrip_cmd="true",
    )
    assert result.returncode != 0


def test_failing_probe_stderr_names_it(runner):
    """Failed probe name in stderr."""
    result = runner.run(
        host="localhost",
        probe_up_cmd="true",
        probe_auth_cmd="false",
        probe_project_list_cmd="true",
        probe_roundtrip_cmd="true",
    )
    assert result.returncode != 0
    assert "FAILED" in result.stderr
    assert "auth" in result.stderr


def test_idempotent_no_error(runner):
    """Running twice with already-running XNAT does not error."""
    # First run: all probes pass
    result1 = runner.run(
        host="localhost",
        probe_up_cmd="true",
        probe_auth_cmd="true",
        probe_project_list_cmd="true",
        probe_roundtrip_cmd="true",
    )
    assert result1.returncode == 0

    # Second run: same setup, no docker needed
    result2 = runner.run(
        host="localhost",
        probe_up_cmd="true",
        probe_auth_cmd="true",
        probe_project_list_cmd="true",
        probe_roundtrip_cmd="true",
    )
    assert result2.returncode == 0
    assert "HEALTHY" in result2.stdout


def test_docker_absent_exits_nonzero(runner):
    """If docker absent and XNAT not already running → exit non-zero."""
    # Stub port probe to fail (not already running), and compose command to fail
    # to simulate docker absence
    result = runner.run(
        host="localhost",
        probe_up_cmd="false",  # port probe fails → not running
        compose_up_cmd="false",  # simulate docker absent or compose failure
    )
    # With docker missing and port-up failing, the script should exit non-zero
    # (either from docker check or from first probe failure)
    assert result.returncode != 0


def test_all_probes_pass_exits_zero(runner):
    """All probes mocked to pass → HEALTHY, exit 0."""
    result = runner.run(
        host="localhost",
        probe_up_cmd="true",
        probe_auth_cmd="true",
        probe_project_list_cmd="true",
        probe_roundtrip_cmd="true",
    )
    assert result.returncode == 0
    assert "HEALTHY" in result.stdout


def test_project_list_probe_fails(runner):
    """If project-list probe fails → FAILED: project-list."""
    result = runner.run(
        host="localhost",
        probe_up_cmd="true",
        probe_auth_cmd="true",
        probe_project_list_cmd="false",  # fail
        probe_roundtrip_cmd="true",
    )
    assert result.returncode != 0
    assert "FAILED" in result.stderr
    assert "project-list" in result.stderr


def test_roundtrip_probe_fails(runner):
    """If roundtrip probe fails → FAILED: synthetic-roundtrip."""
    result = runner.run(
        host="localhost",
        probe_up_cmd="true",
        probe_auth_cmd="true",
        probe_project_list_cmd="true",
        probe_roundtrip_cmd="false",  # fail
    )
    assert result.returncode != 0
    assert "FAILED" in result.stderr
    assert "synthetic-roundtrip" in result.stderr


def test_up_probe_fails_first(runner):
    """If up probe fails, it's the first failure reported."""
    result = runner.run(
        host="localhost",
        probe_up_cmd="false",  # fail first
        probe_auth_cmd="true",
        probe_project_list_cmd="true",
        probe_roundtrip_cmd="true",
        compose_up_cmd="true",  # stub to avoid docker compose attempt
    )
    assert result.returncode != 0
    assert "FAILED" in result.stderr
    assert "up" in result.stderr
