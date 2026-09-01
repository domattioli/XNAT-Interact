"""
Tests for src/services/registry.py — Stage 2a (T005).

All tests run offline: no XNAT server, no VPN, no PHI.
SQLite databases are created in tmp directories and torn down after each test.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from src.services.registry import CrosswalkStore, Registry
from src.services.xnat_gateway import GatewayError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_registry(tmp_path: Path) -> Registry:
    return Registry(tmp_path / "test_registry.db")


def _synthetic_configtables_json() -> dict:
    """
    Minimal ConfigTables JSON mirroring the real shape from src/utilities.py.

    Tables produced by ConfigTables._load / push_to_xnat:
        {
          "metadata": {...},
          "tables": {
            "SURGEONS":     [{"NAME": str, "UID": str, ...}, ...],
            "SUBJECTS":     [{"NAME": str, "UID": str,
                              "ACQUISITION_SITE": str, "GROUP": str, ...}, ...],
            "IMAGE_HASHES": [{"NAME": str, "UID": str,
                              "SUBJECT": str, "INSTANCE_NUM": int, ...}, ...],
          }
        }
    Default meta columns (from ConfigTables.default_meta_table_columns):
        NAME, UID, CREATED_DATE_TIME, CREATED_BY
    Extra columns from _initialize_tables:
        SURGEONS: FIRST_NAME, LAST_NAME, MIDDLE_INITIAL
        SUBJECTS: ACQUISITION_SITE, GROUP
        IMAGE_HASHES: SUBJECT, INSTANCE_NUM
    """
    now = "2026-01-01T00:00:00"
    return {
        "metadata": {
            "CREATED": now,
            "LAST_MODIFIED": now,
            "CREATED_BY": "test-uid",
            "TABLE_EXTRA_COLUMNS": {
                "SURGEONS": ["FIRST_NAME", "LAST_NAME", "MIDDLE_INITIAL"],
                "SUBJECTS": ["ACQUISITION_SITE", "GROUP"],
                "IMAGE_HASHES": ["SUBJECT", "INSTANCE_NUM"],
            },
        },
        "tables": {
            "SURGEONS": [
                {
                    "NAME": "karamm",
                    "UID": "uid-s1",
                    "CREATED_DATE_TIME": now,
                    "CREATED_BY": "test-uid",
                    "FIRST_NAME": "MATTHEW",
                    "LAST_NAME": "KARAM",
                    "MIDDLE_INITIAL": "D",
                },
                {
                    "NAME": "kowalskih",
                    "UID": "uid-s2",
                    "CREATED_DATE_TIME": now,
                    "CREATED_BY": "test-uid",
                    "FIRST_NAME": "HEATHER",
                    "LAST_NAME": "KOWALSKI",
                    "MIDDLE_INITIAL": "R",
                },
            ],
            "SUBJECTS": [
                {
                    "NAME": "case-001",
                    "UID": "uid-sub1",
                    "CREATED_DATE_TIME": now,
                    "CREATED_BY": "test-uid",
                    "ACQUISITION_SITE": "UIHC",
                    "GROUP": "DYNAMIC_HIP_SCREW",
                },
                {
                    "NAME": "case-002",
                    "UID": "uid-sub2",
                    "CREATED_DATE_TIME": now,
                    "CREATED_BY": "test-uid",
                    "ACQUISITION_SITE": "UIHC",
                    "GROUP": "INTERMEDULLARY_NAIL",
                },
            ],
            "IMAGE_HASHES": [
                {
                    "NAME": "aabbcc" * 10 + "aa",  # 62-char synthetic hash
                    "UID": "uid-h1",
                    "CREATED_DATE_TIME": now,
                    "CREATED_BY": "test-uid",
                    "SUBJECT": "case-001",
                    "INSTANCE_NUM": 1,
                },
                {
                    "NAME": "ddeeff" * 10 + "dd",
                    "UID": "uid-h2",
                    "CREATED_DATE_TIME": now,
                    "CREATED_BY": "test-uid",
                    "SUBJECT": "case-001",
                    "INSTANCE_NUM": 2,
                },
                {
                    "NAME": "112233" * 10 + "11",
                    "UID": "uid-h3",
                    "CREATED_DATE_TIME": now,
                    "CREATED_BY": "test-uid",
                    "SUBJECT": "case-002",
                    "INSTANCE_NUM": 1,
                },
            ],
        },
    }


# ---------------------------------------------------------------------------
# T005-A: Schema creation
# ---------------------------------------------------------------------------

class TestSchemaCreation:
    """Registry opens and all five tables exist with the correct columns."""

    def test_all_tables_exist(self, tmp_path):
        reg = _make_registry(tmp_path)
        conn = reg._conn
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        names = {row[0] for row in cur.fetchall()}
        assert {"surgeons", "raters", "cases", "image_hashes", "audit_log"} <= names
        reg.close()

    def test_image_hashes_unique_index_exists(self, tmp_path):
        """UNIQUE constraint on image_hashes.content_hash must be index-backed."""
        reg = _make_registry(tmp_path)
        cur = reg._conn.execute(
            "SELECT sql FROM sqlite_master"
            " WHERE type='table' AND name='image_hashes'"
        )
        ddl = cur.fetchone()[0]
        assert "UNIQUE" in ddl.upper()
        reg.close()

    def test_wal_mode(self, tmp_path):
        reg = _make_registry(tmp_path)
        cur = reg._conn.execute("PRAGMA journal_mode")
        mode = cur.fetchone()[0]
        assert mode == "wal"
        reg.close()

    def test_foreign_keys_on(self, tmp_path):
        reg = _make_registry(tmp_path)
        cur = reg._conn.execute("PRAGMA foreign_keys")
        val = cur.fetchone()[0]
        assert val == 1
        reg.close()

    def test_reopen_idempotent(self, tmp_path):
        """Opening the same DB file twice does not corrupt the schema."""
        db = tmp_path / "reg.db"
        reg1 = Registry(db)
        reg1.upsert_surgeon("surgeon_abc123")
        reg1.close()

        reg2 = Registry(db)
        assert reg2.image_exists("nonexistent") is False
        cur = reg2._conn.execute("SELECT pseudonym FROM surgeons")
        rows = {r[0] for r in cur.fetchall()}
        assert "surgeon_abc123" in rows
        reg2.close()


# ---------------------------------------------------------------------------
# T005-B: image_exists via UNIQUE index
# ---------------------------------------------------------------------------

class TestImageExists:
    """image_exists resolves via UNIQUE index — no full scan."""

    def test_false_when_empty(self, tmp_path):
        reg = _make_registry(tmp_path)
        assert reg.image_exists("deadbeef" * 8) is False
        reg.close()

    def test_true_after_upsert(self, tmp_path):
        reg = _make_registry(tmp_path)
        h = "aabbccdd" * 8
        reg.upsert_image_hash(h)
        assert reg.image_exists(h) is True
        reg.close()

    def test_false_for_different_hash(self, tmp_path):
        reg = _make_registry(tmp_path)
        reg.upsert_image_hash("aabbccdd" * 8)
        assert reg.image_exists("11223344" * 8) is False
        reg.close()

    def test_duplicate_insert_handled_atomically(self, tmp_path):
        """Inserting the same content_hash twice must not raise; row is updated."""
        reg = _make_registry(tmp_path)
        h = "cafecafe" * 8
        reg.upsert_image_hash(h, instance_number=1)
        reg.upsert_image_hash(h, instance_number=2)  # must not raise
        assert reg.image_exists(h) is True
        # Only one row should exist
        cur = reg._conn.execute(
            "SELECT count(*) FROM image_hashes WHERE content_hash = ?", (h,)
        )
        assert cur.fetchone()[0] == 1
        reg.close()

    def test_duplicate_content_hash_rejected_at_db_level(self, tmp_path):
        """
        A raw INSERT (not the upsert wrapper) of a duplicate content_hash
        must raise sqlite3.IntegrityError — proving the UNIQUE constraint
        is enforced by SQLite, not just by the application wrapper.
        """
        reg = _make_registry(tmp_path)
        h = "beef1234" * 8
        reg._conn.execute(
            "INSERT INTO image_hashes(content_hash) VALUES(?)", (h,)
        )
        reg._conn.commit()
        with pytest.raises(sqlite3.IntegrityError):
            reg._conn.execute(
                "INSERT INTO image_hashes(content_hash) VALUES(?)", (h,)
            )
            reg._conn.commit()
        reg.close()


# ---------------------------------------------------------------------------
# T005-C: Transaction rollback
# ---------------------------------------------------------------------------

class TestTransactionRollback:
    """A write that fails mid-operation leaves no partial state."""

    def test_failed_write_leaves_no_partial_state(self, tmp_path):
        """
        Insert a surgeon and a case that references it, then try to insert a
        second case that references a non-existent surgeon with foreign keys ON.
        The second write fails; the registry must contain only the first case.
        """
        reg = _make_registry(tmp_path)
        reg.upsert_surgeon("surgeon_ok")
        reg.upsert_case("case-good", surgeon_pseudonym="surgeon_ok")

        # Attempt to insert a case with a FK violation (no such surgeon)
        with pytest.raises((GatewayError, sqlite3.IntegrityError)):
            with reg._conn:
                reg._conn.execute(
                    "INSERT INTO cases(case_key, surgeon_pseudonym) VALUES(?, ?)",
                    ("case-bad", "surgeon_does_not_exist"),
                )

        # case-good still present; case-bad absent
        cur = reg._conn.execute("SELECT case_key FROM cases")
        keys = {row[0] for row in cur.fetchall()}
        assert "case-good" in keys
        assert "case-bad" not in keys
        reg.close()

    def test_upsert_image_hash_rollback_on_fk_violation(self, tmp_path):
        """
        Inserting an image_hash that references a non-existent case_key with
        foreign keys ON must not silently succeed.
        """
        reg = _make_registry(tmp_path)
        h = "fafafa12" * 8
        # Direct raw insert to force the FK path
        with pytest.raises((sqlite3.IntegrityError, Exception)):
            with reg._conn:
                reg._conn.execute(
                    "INSERT INTO image_hashes(content_hash, case_key) VALUES(?, ?)",
                    (h, "case-ghost"),
                )
        assert reg.image_exists(h) is False
        reg.close()


# ---------------------------------------------------------------------------
# T005-D: Migration
# ---------------------------------------------------------------------------

class TestMigration:
    """migrate_from_configtables imports synthetic ConfigTables JSON correctly."""

    def test_migration_zero_loss(self, tmp_path):
        reg = _make_registry(tmp_path)
        blob = _synthetic_configtables_json()
        report = reg.migrate_from_configtables(blob)
        assert report.parity_ok is True
        assert report.migrated_surgeons == 2
        assert report.migrated_cases == 2
        assert report.migrated_hashes == 3
        reg.close()

    def test_migrated_surgeons_queryable(self, tmp_path):
        reg = _make_registry(tmp_path)
        reg.migrate_from_configtables(_synthetic_configtables_json())
        cur = reg._conn.execute("SELECT pseudonym FROM surgeons")
        pseudonyms = {row[0] for row in cur.fetchall()}
        # NAME values are uppercased during migration
        assert "KARAMM" in pseudonyms
        assert "KOWALSKIH" in pseudonyms
        reg.close()

    def test_migrated_cases_queryable(self, tmp_path):
        reg = _make_registry(tmp_path)
        reg.migrate_from_configtables(_synthetic_configtables_json())
        cur = reg._conn.execute("SELECT case_key FROM cases")
        keys = {row[0] for row in cur.fetchall()}
        assert "CASE-001" in keys
        assert "CASE-002" in keys
        reg.close()

    def test_migrated_image_hashes_queryable(self, tmp_path):
        blob = _synthetic_configtables_json()
        first_hash = blob["tables"]["IMAGE_HASHES"][0]["NAME"]
        reg = _make_registry(tmp_path)
        reg.migrate_from_configtables(blob)
        assert reg.image_exists(first_hash) is True
        reg.close()

    def test_case_image_hashes_after_migration(self, tmp_path):
        blob = _synthetic_configtables_json()
        reg = _make_registry(tmp_path)
        reg.migrate_from_configtables(blob)
        # case-001 has 2 hashes, case-002 has 1 hash
        h001 = reg.case_image_hashes("CASE-001")
        h002 = reg.case_image_hashes("CASE-002")
        assert len(h001) == 2
        assert len(h002) == 1
        reg.close()

    def test_parity_failure_rolls_back(self, tmp_path):
        """If a row lacks NAME, parity fails → registry unchanged."""
        reg = _make_registry(tmp_path)
        blob = _synthetic_configtables_json()
        # Remove NAME from one surgeon row → migrated_surgeons < source_surgeons
        blob["tables"]["SURGEONS"][0]["NAME"] = ""

        with pytest.raises(ValueError, match="parity"):
            reg.migrate_from_configtables(blob)

        # No surgeons should have been committed
        cur = reg._conn.execute("SELECT count(*) FROM surgeons")
        assert cur.fetchone()[0] == 0
        reg.close()

    def test_migration_case_insensitive_table_names(self, tmp_path):
        """Table name lookup is case-insensitive (e.g. 'surgeons' vs 'SURGEONS')."""
        blob = _synthetic_configtables_json()
        # Lowercase all table keys
        blob["tables"] = {k.lower(): v for k, v in blob["tables"].items()}
        reg = _make_registry(tmp_path)
        report = reg.migrate_from_configtables(blob)
        assert report.parity_ok is True
        reg.close()

    def test_idempotent_migration(self, tmp_path):
        """Running migration twice does not duplicate rows (ON CONFLICT DO NOTHING)."""
        blob = _synthetic_configtables_json()
        reg = _make_registry(tmp_path)
        reg.migrate_from_configtables(blob)
        reg.migrate_from_configtables(blob)  # second run

        cur = reg._conn.execute("SELECT count(*) FROM surgeons")
        assert cur.fetchone()[0] == 2  # still 2, not 4
        reg.close()


# ---------------------------------------------------------------------------
# T005-E: CrosswalkStore — put/get round-trips; no HawkID in operational DB
# ---------------------------------------------------------------------------

class TestCrosswalkStore:
    """CrosswalkStore put/get round-trips; operational Registry has no HawkIDs."""

    def test_put_get_roundtrip(self, tmp_path):
        store = CrosswalkStore(tmp_path / "crosswalk.json")
        store.put("surgeon_abc123", "johndoe")
        assert store.get("surgeon_abc123") == "johndoe"

    def test_get_missing_returns_none(self, tmp_path):
        store = CrosswalkStore(tmp_path / "crosswalk.json")
        assert store.get("surgeon_unknown") is None

    def test_put_updates_existing(self, tmp_path):
        store = CrosswalkStore(tmp_path / "crosswalk.json")
        store.put("surgeon_abc", "oldhawkid")
        store.put("surgeon_abc", "newhawkid")
        assert store.get("surgeon_abc") == "newhawkid"

    def test_crosswalk_is_separate_file(self, tmp_path):
        """CrosswalkStore and Registry use distinct files; no HawkID in Registry."""
        db_path = tmp_path / "registry.db"
        xwalk_path = tmp_path / "crosswalk.json"

        reg = Registry(db_path)
        store = CrosswalkStore(xwalk_path)

        store.put("surgeon_abc123", "realhawkid")
        reg.upsert_surgeon("surgeon_abc123")

        # Registry file must not contain the real HawkID
        raw_db = db_path.read_bytes()
        assert b"realhawkid" not in raw_db

        # Crosswalk file contains the HawkID
        xwalk_data = json.loads(xwalk_path.read_text())
        assert xwalk_data.get("surgeon_abc123") == "realhawkid"

        reg.close()

    def test_multiple_pseudonyms(self, tmp_path):
        store = CrosswalkStore(tmp_path / "crosswalk.json")
        store.put("surgeon_aaa", "hawkid_aaa")
        store.put("surgeon_bbb", "hawkid_bbb")
        assert store.get("surgeon_aaa") == "hawkid_aaa"
        assert store.get("surgeon_bbb") == "hawkid_bbb"

    def test_operational_db_no_hawkid_after_migration(self, tmp_path):
        """After migration, no real HawkID from CrosswalkStore appears in DB bytes."""
        db_path = tmp_path / "registry.db"
        xwalk_path = tmp_path / "crosswalk.json"

        reg = Registry(db_path)
        store = CrosswalkStore(xwalk_path)
        store.put("KARAMM", "realkaramhawkid")

        blob = _synthetic_configtables_json()
        reg.migrate_from_configtables(blob)
        reg.close()

        raw_db = db_path.read_bytes()
        assert b"realkaramhawkid" not in raw_db


# ---------------------------------------------------------------------------
# T005-F: Audit log
# ---------------------------------------------------------------------------

class TestAuditLog:
    def test_record_audit_appends(self, tmp_path):
        reg = _make_registry(tmp_path)
        reg.record_audit("testuser", "insert", "image_hash:abc")
        reg.record_audit("testuser", "migrate", "configtables")
        cur = reg._conn.execute("SELECT actor, action FROM audit_log ORDER BY id")
        rows = cur.fetchall()
        assert len(rows) == 2
        assert rows[0] == ("testuser", "insert")
        assert rows[1] == ("testuser", "migrate")
        reg.close()

    def test_audit_log_has_timestamp(self, tmp_path):
        reg = _make_registry(tmp_path)
        reg.record_audit("actor", "action", "target")
        cur = reg._conn.execute("SELECT ts FROM audit_log")
        ts = cur.fetchone()[0]
        assert ts is not None and len(ts) > 10
        reg.close()
