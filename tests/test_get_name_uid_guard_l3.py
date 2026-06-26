"""
#33 finding L3 — ConfigTables.get_name guarded on the wrong column.

get_name(table, item_uid) looks up the NAME by the UID column, but its
existence assertion called item_exists(), which checks the NAME column. So a
genuine UID (the normal case where UID != NAME) tripped a false AssertionError.

Fix: guard get_name with uid_exists() (UID-column membership). This test pins
the regression — get_name resolves a real UID, and a non-existent UID raises.

Fully offline: no XNAT server, no VPN, no PHI. We build a ConfigTables shell
via object.__new__ and wire only the _tables dict the lookups touch.
"""
from __future__ import annotations

import pandas as pd
import pytest

from src.utilities import ConfigTables


def _config_tables_with_subjects() -> ConfigTables:
    obj = object.__new__(ConfigTables)
    cols = ["NAME", "UID", "CREATED_DATE_TIME", "CREATED_BY"]
    # UIDs are pydicom-style (digits/underscores) → uppercase-invariant, as in
    # production; NAMEs are stored uppercase by add_new_item.
    obj._tables = {
        "SUBJECTS": pd.DataFrame(
            [["ALICE", "1_2_840_10008_001", "2026-01-01", "uid-lib"],
             ["BOB", "1_2_840_10008_002", "2026-01-01", "uid-lib"]],
            columns=cols,
        ),
    }
    return obj


_UID_ALICE = "1_2_840_10008_001"
_UID_BOB = "1_2_840_10008_002"


class TestGetNameUidGuard:
    def test_get_name_resolves_by_uid(self):
        """The L3 regression: a real UID (UID != NAME) must resolve, not assert."""
        ct = _config_tables_with_subjects()
        assert ct.get_name("SUBJECTS", _UID_ALICE) == "ALICE"
        assert ct.get_name("SUBJECTS", _UID_BOB) == "BOB"

    def test_get_name_table_name_case_insensitive(self):
        ct = _config_tables_with_subjects()
        assert ct.get_name("subjects", _UID_ALICE) == "ALICE"

    def test_get_name_unknown_uid_raises(self):
        ct = _config_tables_with_subjects()
        with pytest.raises(AssertionError):
            ct.get_name("SUBJECTS", "1_2_840_10008_999")

    def test_get_name_does_not_match_on_name_value(self):
        """Passing a NAME where a UID is expected must NOT resolve (no column confusion)."""
        ct = _config_tables_with_subjects()
        with pytest.raises(AssertionError):
            ct.get_name("SUBJECTS", "ALICE")


class TestUidExistsHelper:
    def test_uid_exists_true_false(self):
        ct = _config_tables_with_subjects()
        assert ct.uid_exists("SUBJECTS", _UID_ALICE) is True
        assert ct.uid_exists("SUBJECTS", "1_2_840_10008_999") is False

    def test_uid_exists_missing_uid_column(self):
        ct = object.__new__(ConfigTables)
        ct._tables = {"NOUID": pd.DataFrame({"NAME": ["X"]})}
        assert ct.uid_exists("NOUID", "anything") is False


class TestGetUidStillCorrect:
    """get_uid (NAME -> UID) was already correct; pin it so the L3 fix didn't regress it."""

    def test_get_uid_resolves_by_name(self):
        ct = _config_tables_with_subjects()
        assert ct.get_uid("SUBJECTS", "ALICE") == _UID_ALICE

    def test_get_uid_unknown_name_raises(self):
        ct = _config_tables_with_subjects()
        with pytest.raises(AssertionError):
            ct.get_uid("SUBJECTS", "NOBODY")
