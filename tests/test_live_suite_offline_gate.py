"""Offline gate for the live-XNAT suite (spec 014, T016 + T044).

With no server configured the live suite must skip cleanly, never touch
docker, and never register the STAPLE stub at import time.
"""
import importlib
import os
import re
import stat
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
LIVE_DIR = REPO_ROOT / "tests" / "integration" / "live_xnat"
SKIP_REASON = "XNAT_SERVER_URL not configured"
PYTESTMARK_LINE = "pytestmark = [pytest.mark.requires_server, pytest.mark.slow]"
OFFLINE_BY_DESIGN = {"test_outcome_summary_schema.py"}
ENV_REMOVE = ("XNAT_SERVER_URL", "XNAT_USERNAME", "XNAT_PASSWORD", "XNAT_PROJECT_NAME")


def test_live_suite_skips_offline_without_docker(tmp_path):
    shim_dir = tmp_path / "bin"
    shim_dir.mkdir()
    log = tmp_path / "docker_shim.log"
    shim = shim_dir / "docker"
    shim.write_text('#!/bin/sh\necho "$@" >> "$DOCKER_SHIM_LOG"\nexit 0\n')
    shim.chmod(shim.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    env = {k: v for k, v in os.environ.items() if k not in ENV_REMOVE}
    env["PATH"] = str(shim_dir) + os.pathsep + env.get("PATH", "")
    env["DOCKER_SHIM_LOG"] = str(log)

    proc = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-rs", "-m", "requires_server",
         "tests/integration/live_xnat", "-p", "no:cacheprovider"],
        cwd=str(REPO_ROOT), env=env, capture_output=True, text=True, timeout=300,
    )
    out = proc.stdout + proc.stderr
    assert proc.returncode == 0, out
    assert re.search(r"\b\d+ skipped\b", out), out
    for word in ("passed", "failed", "error"):
        assert not re.search(r"\b[1-9]\d* %s\b" % word, out), out
    assert SKIP_REASON in out, out
    assert (not log.exists()) or log.read_text().strip() == "", log.read_text()


def test_importing_live_modules_does_not_register_aggregators():
    from src.annotations.aggregate import list_aggregators

    before = list(list_aggregators())
    names = ["tests.integration.live_xnat.conftest", "tests.integration.live_xnat.staple_stub"]
    names += ["tests.integration.live_xnat.%s" % p.stem for p in sorted(LIVE_DIR.glob("test_*.py"))]
    for name in names:
        importlib.import_module(name)
    assert list(list_aggregators()) == before


def test_live_modules_carry_requires_server_slow_pytestmark():
    files = sorted(p for p in LIVE_DIR.glob("test_*.py") if p.name not in OFFLINE_BY_DESIGN)
    assert files, "no live test modules found"
    missing = [p.name for p in files if PYTESTMARK_LINE not in p.read_text()]
    assert not missing, "missing pytestmark in: %s" % missing
