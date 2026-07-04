"""Offline regression test for #33 finding L2.

ConfigTables._update_metadata must preserve the original CREATED_BY and record
the modifier under LAST_MODIFIED_BY, instead of overwriting CREATED_BY on every
update. No network, no XNAT server, no PHI — drives the method in isolation.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.utilities import ConfigTables


def _bare_configtables(metadata: dict) -> ConfigTables:
    ct = object.__new__(ConfigTables)
    ct._metadata = metadata
    return ct


def test_l2_update_preserves_original_creator():
    ct = _bare_configtables(
        {"CREATED": "t0", "LAST_MODIFIED": "t0", "CREATED_BY": "UID_A",
         "TABLE_EXTRA_COLUMNS": {}}
    )
    with mock.patch.object(ConfigTables, "accessor_uid",
                           new_callable=mock.PropertyMock, return_value="UID_B"), \
         mock.patch.object(ConfigTables, "now_datetime",
                           new_callable=mock.PropertyMock, return_value="t1"):
        ct._update_metadata()
    assert ct._metadata["CREATED_BY"] == "UID_A"        # original creator preserved
    assert ct._metadata["LAST_MODIFIED_BY"] == "UID_B"  # modifier recorded
    assert ct._metadata["LAST_MODIFIED"] == "t1"


def test_l2_update_backfills_created_by_when_absent():
    ct = _bare_configtables({"CREATED": "t0", "LAST_MODIFIED": "t0",
                             "TABLE_EXTRA_COLUMNS": {}})
    with mock.patch.object(ConfigTables, "accessor_uid",
                           new_callable=mock.PropertyMock, return_value="UID_B"), \
         mock.patch.object(ConfigTables, "now_datetime",
                           new_callable=mock.PropertyMock, return_value="t1"):
        ct._update_metadata()
    assert ct._metadata["CREATED_BY"] == "UID_B"        # back-compat backfill
    assert ct._metadata["LAST_MODIFIED_BY"] == "UID_B"
