"""
tests/regression_014/test_m10_friendly_errors.py -- #33 M10 backfill (spec 014).

M10 (fixed-no-test per the 2026-07-07 audit, ledger.md): two call sites --
ConfigTables.__init__'s first-run catch (src/utilities.py) and
download_selection's legacy-path except (app/logic/download.py) -- used
broad `except Exception` handling that could mask a real error as
"first-time setup" or swallow it silently.

The fix (present in this branch's history): ConfigTables.__init__ only
treats FileNotFoundError/KeyError/ValueError/pyxnat DataError as first-run;
any other exception is re-raised wrapped in a FriendlyError so it reaches
the user instead of being silently treated as "this is a brand-new
project".

Backfill note (spec 014, User Story 4): the historical pre-fix commit for
this specific classification logic predates this branch's earliest
reachable git history (same shallow-fetch boundary as M7/M8 -- see
ledger.md). This test proves current correctness only.

Offline only. Reuses the ConfigTables.__new__ + mock.patch.object pattern
from tests/test_configtables_bootstrap.py (no real XNATLogin/XNATConnection
instantiated, no network, no PHI).
"""
from __future__ import annotations

import sys
import types
import unittest.mock as mock
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tests.fakes.fake_xnat import FakeXNAT
from tests.test_configtables_bootstrap import _make_login_info, _make_xnat_connection


def test_non_first_run_error_is_surfaced_not_silently_treated_as_first_run(
    capsys: pytest.CaptureFixture,
) -> None:
    """
    A genuine, unrelated error (e.g. a permission failure) during
    pull_from_xnat must NOT be silently treated as "first-time setup" --
    the original exception must still propagate, AND a rendered
    FriendlyError must reach the user (printed) along the way.
    """
    from src.utilities import ConfigTables, UIDandMetaInfo

    fake = FakeXNAT(project_name="FAKE_PROJECT", project_users=["dmattioli"])
    login_info = _make_login_info("dmattioli")
    conn = _make_xnat_connection(login_info, fake, project_users=["dmattioli"])

    class _UnrelatedError(RuntimeError):
        """Stand-in for a genuine, non-first-run failure (e.g. permission denied)."""

    instantiate_called: dict = {}

    with (
        mock.patch.object(UIDandMetaInfo, "__init__", return_value=None),
        mock.patch.object(
            ConfigTables, "pull_from_xnat",
            side_effect=_UnrelatedError("permission denied"),
        ),
        mock.patch.object(
            ConfigTables, "_verify_project_owners_are_registered", return_value=True
        ),
        mock.patch.object(
            ConfigTables, "_instantiate_json_file",
            side_effect=lambda: instantiate_called.__setitem__("called", True),
        ),
        mock.patch.object(ConfigTables, "_initialize_tables"),
        mock.patch.object(ConfigTables, "push_to_xnat"),
    ):
        with pytest.raises(_UnrelatedError):
            ConfigTables(login_info, conn, verbose=False)

    # Must not have been silently swallowed and treated as first-run setup.
    assert not instantiate_called.get("called"), (
        "An unrelated error was silently treated as first-time project setup "
        "instead of being surfaced to the user."
    )
    # A rendered FriendlyError must have reached the user (not a silent pass).
    captured = capsys.readouterr()
    assert "Could not load the XNAT configuration database" in captured.out, (
        f"Expected a rendered FriendlyError to be printed; got stdout: {captured.out!r}"
    )
