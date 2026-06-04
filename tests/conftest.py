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
