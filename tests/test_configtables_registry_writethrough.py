"""
T006 — ConfigTables write-through to SQLite registry (facade, behavior-preserving).

All tests run fully offline: no XNAT server, no VPN, no PHI.
We test only the *additive* registry write-through; the existing ConfigTables
JSON behavior is validated by the 899-test offline suite.

Strategy
--------
We reach inside ConfigTables just enough to inject a real Registry backed by a
tmp-dir SQLite file, then exercise the public add_new_item / item_exists /
list_of_all_items_in_table surface and verify that the registry is populated
alongside — without touching any XNAT or JSON logic.
"""
from __future__ import annotations

import types
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

import pandas as pd
import pytest

from src.services.registry import Registry
from src.utilities import ConfigTables


# ---------------------------------------------------------------------------
# Helpers: build a minimal ConfigTables-like object without XNAT connection
# ---------------------------------------------------------------------------

def _minimal_config_tables(tmp_path: Path) -> ConfigTables:
    """
    Return a ConfigTables instance with:
      - All XNAT I/O bypassed via mocks
      - A real Registry opened in tmp_path/registry.db
      - The IMAGE_HASHES, SUBJECTS, and SURGEONS tables pre-created (empty)

    We bypass __init__ entirely by using object.__new__ and manually wiring
    the same attributes __init__ would set, letting us test only the
    write-through logic without a live XNAT connection.
    """
    obj = object.__new__(ConfigTables)

    # Minimum _local_variables stub (avoids filesystem reads for template_img)
    lv = types.SimpleNamespace(
        tmp_data_dir=str(tmp_path),
        config_fn="database_config.json",
        config_ffn=str(tmp_path / "database_config.json"),
        backup_fn="database_config-backup-.json",
        required_login_keys=["USERNAME", "PASSWORD", "URL"],
        xnat_project_name="test",
        xnat_project_url="https://example.com/xnat/",
        xnat_config_folder_name="config",
        xnat_backups_folder_name="backups",
        default_meta_table_columns=["NAME", "UID", "CREATED_DATE_TIME", "CREATED_BY"],
        template_img_dir="",
        template_img=None,
        acceptable_img_dtypes=[],
        required_img_size_for_hashing=(256, 256),
        mturk_batch_col_names=[],
        redacted_string="REDACTED",
        data_librarian=["testlibrarian"],
        cataloged_resources_ffn="",
    )
    obj._local_variables = lv

    # Stub UIDandMetaInfo._uid
    from pydicom.uid import generate_uid as _gen_uid
    obj._uid = str(_gen_uid()).replace(".", "_")

    # Stub login_info (valid)
    login = MagicMock()
    login.is_valid = True
    login.validated_username = "testlibrarian"
    obj._login_info = login

    # Stub xnat_connection (open + verified)
    conn = MagicMock()
    conn.is_open = True
    conn.is_verified = True
    obj._xnat_connection = conn

    # Bootstrap minimal tables: IMAGE_HASHES, SUBJECTS, SURGEONS
    default_cols = ["NAME", "UID", "CREATED_DATE_TIME", "CREATED_BY"]
    obj._tables = {
        "REGISTERED_USERS": pd.DataFrame(
            [["TESTLIBRARIAN", "uid-lib", "2026-01-01", "uid-lib"]],
            columns=default_cols,
        ),
        "IMAGE_HASHES": pd.DataFrame(
            columns=default_cols + ["SUBJECT", "INSTANCE_NUM"]
        ),
        "SUBJECTS": pd.DataFrame(
            columns=default_cols + ["ACQUISITION_SITE", "GROUP"]
        ),
        "SURGEONS": pd.DataFrame(
            columns=default_cols + ["FIRST_NAME", "LAST_NAME", "MIDDLE_INITIAL"]
        ),
    }
    obj._metadata = {
        "CREATED": "2026-01-01",
        "LAST_MODIFIED": "2026-01-01",
        "CREATED_BY": "uid-lib",
        "TABLE_EXTRA_COLUMNS": {
            "IMAGE_HASHES": ["SUBJECT", "INSTANCE_NUM"],
            "SUBJECTS": ["ACQUISITION_SITE", "GROUP"],
            "SURGEONS": ["FIRST_NAME", "LAST_NAME", "MIDDLE_INITIAL"],
        },
    }
    obj._original_tables = obj._tables.copy()
    obj._server_fingerprint_at_load = None

    # Wire a real Registry into _registry
    obj._registry = Registry(tmp_path / "registry.db")

    return obj


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestImageHashesWriteThrough:
    """add_new_item(IMAGE_HASHES) also populates registry.image_exists."""

    def test_add_image_hash_visible_in_registry(self, tmp_path):
        ct = _minimal_config_tables(tmp_path)
        hash_val = "aabbccdd" * 8  # 64-char synthetic hash
        ct.add_new_item(
            table_name="IMAGE_HASHES",
            item_name=hash_val,
            extra_columns_values={"SUBJECT": "case-001", "INSTANCE_NUM": 1},
            verbose=False,
        )
        # add_new_item uppercases item_name; registry stores the uppercased form.
        assert ct._registry.image_exists(hash_val.upper()) is True

    def test_add_image_hash_json_behavior_preserved(self, tmp_path):
        """Return value and JSON table unchanged: add_new_item still returns (bool, str)."""
        ct = _minimal_config_tables(tmp_path)
        hash_val = "11223344" * 8
        # Must supply extra columns to match the table schema (SUBJECT, INSTANCE_NUM).
        success, msg = ct.add_new_item(
            table_name="IMAGE_HASHES",
            item_name=hash_val,
            extra_columns_values={"SUBJECT": "", "INSTANCE_NUM": None},
            verbose=False,
        )
        assert success is True
        assert isinstance(msg, str)
        # Also appears in in-memory table (JSON side)
        assert ct.item_exists("IMAGE_HASHES", hash_val)

    def test_duplicate_image_hash_not_double_added(self, tmp_path):
        """Second add_new_item returns success=False (already exists) — registry still has exactly one row."""
        ct = _minimal_config_tables(tmp_path)
        hash_val = "cafebabe" * 8
        _extras = {"SUBJECT": "", "INSTANCE_NUM": None}
        ct.add_new_item("IMAGE_HASHES", hash_val, extra_columns_values=_extras, verbose=False)
        ct.add_new_item("IMAGE_HASHES", hash_val, extra_columns_values=_extras, verbose=False)  # duplicate
        stored = hash_val.upper()  # add_new_item uppercases
        assert ct._registry.image_exists(stored) is True
        # Only one row in registry
        cur = ct._registry._conn.execute(
            "SELECT count(*) FROM image_hashes WHERE content_hash = ?", (stored,)
        )
        assert cur.fetchone()[0] == 1

    def test_image_hash_case_key_propagated(self, tmp_path):
        """The SUBJECT extra column is forwarded as case_key to the registry."""
        ct = _minimal_config_tables(tmp_path)
        # Pre-register the subject so the FK constraint is satisfied.
        ct._registry.upsert_case("CASE-XYZ")
        hash_val = "deadbeef" * 8
        ct.add_new_item(
            "IMAGE_HASHES",
            hash_val,
            extra_columns_values={"SUBJECT": "case-xyz", "INSTANCE_NUM": 3},
            verbose=False,
        )
        stored = hash_val.upper()  # add_new_item uppercases
        # Check case_key stored in registry
        cur = ct._registry._conn.execute(
            "SELECT case_key FROM image_hashes WHERE content_hash = ?", (stored,)
        )
        row = cur.fetchone()
        assert row is not None
        assert row[0] == "CASE-XYZ"


class TestSurgeonsWriteThrough:
    """add_new_item(SURGEONS) also populates registry surgeons table."""

    def test_add_surgeon_visible_in_registry(self, tmp_path):
        ct = _minimal_config_tables(tmp_path)
        ct.add_new_item(
            "SURGEONS",
            "karamm",
            extra_columns_values={"FIRST_NAME": "MATTHEW", "LAST_NAME": "KARAM", "MIDDLE_INITIAL": "D"},
            verbose=False,
        )
        cur = ct._registry._conn.execute(
            "SELECT pseudonym FROM surgeons WHERE pseudonym = ?", ("KARAMM",)
        )
        assert cur.fetchone() is not None

    def test_add_surgeon_json_behavior_preserved(self, tmp_path):
        ct = _minimal_config_tables(tmp_path)
        # Must supply extra columns to match the SURGEONS table schema.
        success, msg = ct.add_new_item(
            "SURGEONS", "testdoc",
            extra_columns_values={"FIRST_NAME": "", "LAST_NAME": "", "MIDDLE_INITIAL": ""},
            verbose=False,
        )
        assert success is True
        assert ct.item_exists("SURGEONS", "testdoc")


class TestSubjectsWriteThrough:
    """add_new_item(SUBJECTS) also populates registry cases table."""

    def test_add_subject_visible_in_registry(self, tmp_path):
        ct = _minimal_config_tables(tmp_path)
        ct.add_new_item(
            "SUBJECTS",
            "case-001",
            extra_columns_values={"ACQUISITION_SITE": "UIHC", "GROUP": "DYNAMIC_HIP_SCREW"},
            verbose=False,
        )
        cur = ct._registry._conn.execute(
            "SELECT case_key FROM cases WHERE case_key = ?", ("CASE-001",)
        )
        assert cur.fetchone() is not None

    def test_add_subject_json_behavior_preserved(self, tmp_path):
        ct = _minimal_config_tables(tmp_path)
        # Must supply extra columns to match the SUBJECTS table schema.
        success, msg = ct.add_new_item(
            "SUBJECTS", "case-002",
            extra_columns_values={"ACQUISITION_SITE": "", "GROUP": ""},
            verbose=False,
        )
        assert success is True
        assert ct.item_exists("SUBJECTS", "case-002")


class TestRegistryNoneFailSoft:
    """When _registry is None (e.g. DB unavailable), add_new_item still works normally."""

    def test_add_item_succeeds_when_registry_is_none(self, tmp_path):
        ct = _minimal_config_tables(tmp_path)
        ct._registry = None  # simulate unavailable registry
        success, msg = ct.add_new_item(
            "IMAGE_HASHES", "aabb" * 16,
            extra_columns_values={"SUBJECT": "", "INSTANCE_NUM": None},
            verbose=False,
        )
        assert success is True

    def test_item_exists_unaffected_by_registry_being_none(self, tmp_path):
        ct = _minimal_config_tables(tmp_path)
        ct._registry = None
        ct.add_new_item(
            "SURGEONS", "docfoo",
            extra_columns_values={"FIRST_NAME": "", "LAST_NAME": "", "MIDDLE_INITIAL": ""},
            verbose=False,
        )
        assert ct.item_exists("SURGEONS", "docfoo") is True


class TestNonOverlappingTablesUnaffected:
    """Tables that don't map to the registry (e.g. GROUPS, ACQUISITION_SITES) are unaffected."""

    def test_add_item_to_groups_still_works(self, tmp_path):
        ct = _minimal_config_tables(tmp_path)
        # Add GROUPS table first
        ct._tables["GROUPS"] = pd.DataFrame(
            columns=["NAME", "UID", "CREATED_DATE_TIME", "CREATED_BY"]
        )
        ct._metadata["TABLE_EXTRA_COLUMNS"]["GROUPS"] = []
        success, msg = ct.add_new_item("GROUPS", "new_procedure", verbose=False)
        assert success is True
        assert ct.item_exists("GROUPS", "new_procedure")
