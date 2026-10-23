"""
tests/test_unregistered_user_friendly.py — B3: new (unregistered) user fails softly.

Before: ConfigTables.accessor_uid -> get_uid raised a bare AssertionError
"Item 'X' does not exist in table 'REGISTERED_USERS'". Now it raises
UnregisteredUserError carrying a FriendlyError with a next step.

Offline. Synthetic usernames only.
"""
from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

from src.utilities import ConfigTables, UnregisteredUserError


def _config(username: str) -> ConfigTables:
    cfg = object.__new__(ConfigTables)
    cfg._login_info = SimpleNamespace(validated_username=username, is_valid=True)
    cfg._tables = {"REGISTERED_USERS": pd.DataFrame(
        [["ZZSYNTH_LIBRARIAN", "2_25_1", "2026-01-01", "2_25_1"]],
        columns=["NAME", "UID", "CREATED_DATE_TIME", "CREATED_BY"])}
    return cfg


def test_unregistered_user_gets_friendly_error_not_assertion():
    cfg = _config("zzsynth_newstudent")
    with pytest.raises(UnregisteredUserError) as ei:
        _ = cfg.accessor_uid
    assert not isinstance(ei.value, AssertionError)
    fe = ei.value.friendly
    assert "not registered" in fe.title.lower()
    assert any("Data Librarian" in r and "register" in r for r in fe.recourse)
    assert "does not exist in table" not in str(ei.value)
    assert "Data Librarian" in str(ei.value)


def test_registered_user_still_resolves_uid():
    assert _config("zzsynth_librarian").accessor_uid == "2_25_1"
