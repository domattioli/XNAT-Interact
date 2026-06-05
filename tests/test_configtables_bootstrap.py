"""
tests/test_configtables_bootstrap.py — Offline tests for ConfigTables bootstrap.

T007/T008/T009 (#28): covers two bugs in ConfigTables.__init__:
  1. Fresh project → pyxnat raises DataError("does not exist") → pre-fix
     propagates it instead of treating it as first-run.
     T008 fix: widen _is_first_run_error to include DataError.
  2. Non-whitelisted user (e.g. 'admin') → pre-fix AssertionError from
     hardcoded username list in _validate_login_for_important_functions.
     T009 fix: membership/owner lookup + XNAT_CONFIG_ALLOWLIST env escape hatch.

xfail(strict=True) tests document old behaviour — they FAIL (raise) under
old code; after the fix they don't raise the specific error any more so the
xfail test ALSO fails (did not raise expected exception) → stays xfail.

RULES:
  - NO network.  NO real XNAT server.  NO PHI.
  - Uses unittest.mock.  No real XNATLogin/XNATConnection instantiated.
"""
from __future__ import annotations

import os
import sys
import types
import unittest.mock as mock
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pyxnat.core.errors as _pyxnat_errors
from tests.fakes.fake_xnat import FakeXNAT


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_login_info(username: str) -> types.SimpleNamespace:
    stub = types.SimpleNamespace()
    stub.is_valid = True
    stub.validated_username = username
    stub.validated_password = "fake_password"
    return stub


def _make_xnat_connection(
    login_info: types.SimpleNamespace,
    server: FakeXNAT,
    *,
    project_users: list | None = None,
) -> types.SimpleNamespace:
    stub = types.SimpleNamespace()
    stub.is_open = True
    stub.is_verified = True
    stub.server = server
    stub.xnat_project_name = "FAKE_PROJECT"
    stub.login_info = login_info
    _users = project_users or ["testuser"]
    stub.project_handle = types.SimpleNamespace(
        users=lambda: _users,
        label=lambda: "FAKE_PROJECT",
    )
    return stub


def _build_ct_stub(username: str, project_users: list) -> object:
    """
    Allocate ConfigTables without __init__; inject minimal state for auth check.
    """
    from src.utilities import ConfigTables

    fake = FakeXNAT(project_name="FAKE_PROJECT", project_users=project_users)
    login_info = _make_login_info(username)
    conn = _make_xnat_connection(login_info, fake, project_users=project_users)

    ct = ConfigTables.__new__(ConfigTables)
    ct._login_info = login_info
    ct._xnat_connection = conn
    ct._local_variables = types.SimpleNamespace(
        data_librarian=["dmattioli", "domattioli", "stelong"],
        config_fn="database_config.json",
        config_ffn="/tmp/database_config.json",
        backup_fn="database_config-backup.json",
        xnat_config_folder_name="config",
        xnat_backups_folder_name="backups",
        xnat_project_name="FAKE_PROJECT",
        required_login_keys=[],
        default_meta_table_columns=["NAME", "UID", "DATE", "CREATED_BY"],
        tmp_data_dir=Path("/tmp"),
        cataloged_resources_ffn="/tmp/cataloged.json",
        template_img_dir="/tmp",
        template_img=None,
        acceptable_img_dtypes=[],
        required_img_size_for_hashing=(224, 224),
        mturk_batch_col_names=[],
        redacted_string="REDACTED",
        required_batch_upload_columns={},
    )
    ct._uid = "FAKE_UID"
    return ct


def _auth_error_in_exc(exc: BaseException) -> bool:
    """Return True if *exc* is the auth-specific assertion from _validate_login_for_important_functions."""
    msg = str(exc)
    return (
        "authorized" in msg
        or "Only user" in msg
        or "can push config file" in msg
        or "not authorized" in msg
    )


# ---------------------------------------------------------------------------
# T008: fresh project → DataError treated as first-run
# ---------------------------------------------------------------------------

class TestFreshProjectDataError:

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "T007 red-state: pre-T008 code propagates DataError from pull_from_xnat "
            "because DataError was not in _is_first_run_error.  After T008 fix "
            "DataError is caught → __init__ succeeds → pytest.raises fails (did not "
            "raise) → this xfail test FAILS as expected → stays xfail."
        ),
    )
    def test_data_error_propagates_pre_fix(self) -> None:
        """
        RED (xfail): before T008, DataError from pull_from_xnat was re-raised.
        After T008 it is caught → __init__ succeeds → pytest.raises fails → xfail.
        """
        from src.utilities import ConfigTables, UIDandMetaInfo

        fake = FakeXNAT(project_name="FAKE_PROJECT", project_users=["dmattioli"])
        login_info = _make_login_info("dmattioli")
        conn = _make_xnat_connection(login_info, fake, project_users=["dmattioli"])

        with (
            mock.patch.object(UIDandMetaInfo, "__init__", return_value=None),
            mock.patch.object(
                ConfigTables, "pull_from_xnat",
                side_effect=_pyxnat_errors.DataError("Cannot get file: does not exists"),
            ),
            mock.patch.object(
                ConfigTables, "_verify_project_owners_are_registered", return_value=True
            ),
            mock.patch.object(ConfigTables, "_instantiate_json_file"),
            mock.patch.object(ConfigTables, "_initialize_tables"),
            mock.patch.object(ConfigTables, "push_to_xnat"),
        ):
            # After T008: DataError is caught → __init__ succeeds →
            # pytest.raises fails with "did not raise" → test FAILS → xfail passes.
            with pytest.raises(_pyxnat_errors.DataError):
                ConfigTables(login_info, conn, verbose=False)

    def test_data_error_treated_as_first_run(self) -> None:
        """
        GREEN (T008): DataError caught as first-run, _instantiate_json_file called,
        __init__ succeeds.
        """
        from src.utilities import ConfigTables, UIDandMetaInfo

        fake = FakeXNAT(project_name="FAKE_PROJECT", project_users=["dmattioli"])
        login_info = _make_login_info("dmattioli")
        conn = _make_xnat_connection(login_info, fake, project_users=["dmattioli"])

        init_called: dict = {}

        def _fake_instantiate() -> None:
            init_called["called"] = True

        with (
            mock.patch.object(UIDandMetaInfo, "__init__", return_value=None),
            mock.patch.object(
                ConfigTables, "pull_from_xnat",
                side_effect=_pyxnat_errors.DataError("Cannot get file: does not exists"),
            ),
            mock.patch.object(
                ConfigTables, "_verify_project_owners_are_registered", return_value=True
            ),
            mock.patch.object(
                ConfigTables, "_instantiate_json_file", side_effect=_fake_instantiate
            ),
            mock.patch.object(ConfigTables, "_initialize_tables"),
            mock.patch.object(ConfigTables, "push_to_xnat"),
            mock.patch.object(ConfigTables, "tables", new_callable=mock.PropertyMock, return_value={}),
        ):
            ct = ConfigTables(login_info, conn, verbose=False)

        assert init_called.get("called"), (
            "_instantiate_json_file was NOT called — T008 fix not applied."
        )


# ---------------------------------------------------------------------------
# T009: non-whitelisted user authorized via membership or allowlist
# ---------------------------------------------------------------------------

class TestNonWhitelistedUserAuth:

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "T007 red-state: pre-T009 code raises AssertionError specifically "
            "about authorization ('Only user(s) ... can push').  After T009 fix "
            "the auth assertion is gone; the test checks only for auth-specific "
            "errors and returns without raising if none found — pytest.raises fails "
            "(no auth error raised) → test FAILS → xfail passes."
        ),
    )
    def test_non_whitelisted_member_blocked_pre_fix(self) -> None:
        """
        RED (xfail): before T009, 'admin' got AssertionError about authorization
        ('Only user(s) ... can push config file to the xnat server').

        After T009 fix the auth check passes → the auth-specific message is no
        longer raised → pytest.raises(match=...) fails → test FAILS → xfail.
        """
        ct = _build_ct_stub("admin", ["admin", "dmattioli"])
        with pytest.raises(AssertionError, match=r"Only user|authorized|can push config"):
            ct._instantiate_json_file()

    def test_project_member_authorized_via_allowlist(self) -> None:
        """
        GREEN (T009): XNAT_CONFIG_ALLOWLIST grants 'admin' access.
        """
        ct = _build_ct_stub("admin", ["admin", "dmattioli"])

        with mock.patch.dict(os.environ, {"XNAT_CONFIG_ALLOWLIST": "admin"}):
            try:
                ct._instantiate_json_file()
            except (AssertionError, PermissionError) as exc:
                if _auth_error_in_exc(exc):
                    pytest.fail(
                        f"_instantiate_json_file raised auth error for allowlisted "
                        f"user 'admin' after T009 fix: {exc}"
                    )
            except Exception:
                pass  # further errors OK; auth is what we test.

    def test_allowlisted_non_member_authorized(self) -> None:
        """
        GREEN (T009): user in XNAT_CONFIG_ALLOWLIST but NOT a project member
        is authorized (CI/service-account escape hatch).
        """
        ct = _build_ct_stub("ci_runner", ["dmattioli"])

        with mock.patch.dict(os.environ, {"XNAT_CONFIG_ALLOWLIST": "ci_runner,deploy_bot"}):
            try:
                ct._instantiate_json_file()
            except (AssertionError, PermissionError) as exc:
                if _auth_error_in_exc(exc):
                    pytest.fail(
                        f"_instantiate_json_file raised auth error for allowlisted "
                        f"non-member 'ci_runner' after T009 fix: {exc}"
                    )
            except Exception:
                pass

    def test_project_owner_still_authorized(self) -> None:
        """
        Regression: existing project owners remain authorized after T009 refactor.
        """
        ct = _build_ct_stub("dmattioli", ["dmattioli", "stelong"])

        try:
            ct._instantiate_json_file()
        except (AssertionError, PermissionError) as exc:
            if _auth_error_in_exc(exc):
                pytest.fail(f"Existing owner 'dmattioli' lost auth after T009: {exc}")
        except Exception:
            pass

    def test_unlisted_non_member_still_blocked(self) -> None:
        """
        Security: user not in project_owner, not a member, not in allowlist → blocked.
        """
        ct = _build_ct_stub("random_user", ["dmattioli"])

        env_without_allowlist = {k: v for k, v in os.environ.items()
                                 if k != "XNAT_CONFIG_ALLOWLIST"}
        with mock.patch.dict(os.environ, env_without_allowlist, clear=True):
            auth_error_raised = False
            try:
                ct._instantiate_json_file()
            except (AssertionError, PermissionError) as exc:
                if _auth_error_in_exc(exc):
                    auth_error_raised = True
            except Exception:
                pass

            assert auth_error_raised, (
                "Expected auth error for unlisted/non-member user 'random_user' "
                "but none was raised — security regression in T009 fix."
            )
