"""
Shared pytest configuration and fixtures for the XNAT-Interact test suite.

Key idea: every test here runs **offline** — no XNAT server, no UIowa VPN, no
real patient data. Anything that would need the live server is either tested
against a fake/stub or explicitly skipped (see the `requires_server` marker).
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Make the repository root importable as `src.*` regardless of where pytest is
# launched from.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


@pytest.fixture
def tmp_workdir(tmp_path: Path) -> Path:
    """A throwaway working directory for files a test wants to create."""
    return tmp_path


@pytest.fixture
def synthetic():
    """Convenience handle to the synthetic-data generators."""
    from tests import synthetic_data

    return synthetic_data


def pytest_collection_modifyitems(config, items):
    """
    T034: Auto-skip requires_server-marked tests when live-server env is not configured.

    Per plan.md Constitution Check Principle IV: CI must run offline by default.
    This hook implements the collection-time skip gate that, combined with
    .github/workflows/tests.yml's marker filter on the default job, ensures
    the live-XNAT suite never runs in the fast CI lane.

    Skipped only when the XNAT_SERVER_URL env var is absent or empty.

    When the URL is set, the tests are NOT skipped even if the server does not
    answer yet: the live_xnat_server fixture owns boot/attach and must fail fast
    with a clear error if the server never becomes reachable (spec 014 edge
    case "server unreachable" / FR-018). Skipping here would turn a boot failure
    into a green opt-in CI lane.
    """
    import os

    server_url = os.environ.get("XNAT_SERVER_URL", "").strip()
    if server_url:
        return
    for item in items:
        if item.get_closest_marker("requires_server"):
            item.add_marker(
                pytest.mark.skip(
                    reason="requires_server marker: XNAT_SERVER_URL not configured (skipped by default in CI)"
                )
            )
