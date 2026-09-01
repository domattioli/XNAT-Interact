"""
T011 + T015 (offline part): Registry contract parametrized over backends.

Backend parametrize:
  sqlite3     — default Registry (raw sqlite3, always run)
  sqlite-core — CoreRegistry on sqlite:/// engine (proves SQLAlchemy Core without PG)
  postgres    — CoreRegistry on XNAT_REGISTRY_PG_DSN (skip if not set)

Also covers the migrate_sqlite_to_pg code path offline (sqlite→sqlite-core).
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from src.services.registry import Registry

sqlalchemy = pytest.importorskip("sqlalchemy", reason="sqlalchemy not installed")


# ---------------------------------------------------------------------------
# Fixtures / parametrize
# ---------------------------------------------------------------------------

def _sqlite3_registry(tmp_path: Path):
    """Default Registry (raw sqlite3 path)."""
    return Registry(tmp_path / "reg.db")


def _sqlite_core_registry(tmp_path: Path):
    """CoreRegistry on a temp sqlite:/// file (SQLAlchemy Core, no PG needed)."""
    from src.services.registry_backend import CoreRegistry, make_engine
    db = tmp_path / "core_reg.db"
    engine = make_engine(f"sqlite:///{db}")
    return CoreRegistry(engine)


def _pg_registry(tmp_path: Path):
    """CoreRegistry on Postgres (skips if no DSN)."""
    dsn = os.environ.get("XNAT_REGISTRY_PG_DSN")
    if not dsn:
        pytest.skip("XNAT_REGISTRY_PG_DSN not set")
    from src.services.registry_backend import CoreRegistry, make_engine
    engine = make_engine(dsn)
    return CoreRegistry(engine)


BACKENDS = ["sqlite3", "sqlite-core", "postgres"]


@pytest.fixture(params=BACKENDS)
def reg(request, tmp_path):
    name = request.param
    if name == "sqlite3":
        r = _sqlite3_registry(tmp_path)
    elif name == "sqlite-core":
        r = _sqlite_core_registry(tmp_path)
    else:
        r = _pg_registry(tmp_path)
    yield r
    if hasattr(r, "close"):
        r.close()


# ---------------------------------------------------------------------------
# Contract tests
# ---------------------------------------------------------------------------

class TestUpsertSurgeon:
    def test_insert_and_idempotent(self, reg):
        reg.upsert_surgeon("S1", role="attending")
        reg.upsert_surgeon("S1", role="attending")  # duplicate — no error

    def test_update_role(self, reg):
        reg.upsert_surgeon("S2", role="resident")
        reg.upsert_surgeon("S2", role="attending")  # update


class TestUpsertRater:
    def test_insert(self, reg):
        reg.upsert_rater("R1", expertise_tier="senior", reliability_weight=0.9)

    def test_idempotent(self, reg):
        reg.upsert_rater("R1", expertise_tier="senior", reliability_weight=0.9)
        reg.upsert_rater("R1", expertise_tier="senior", reliability_weight=0.9)

    def test_update(self, reg):
        reg.upsert_rater("R1", expertise_tier="junior", reliability_weight=0.5)
        reg.upsert_rater("R1", expertise_tier="senior", reliability_weight=0.9)


class TestUpsertCase:
    def test_insert(self, reg):
        reg.upsert_surgeon("S1")
        reg.upsert_case("C1", surgeon_pseudonym="S1", procedure="lap-chole")

    def test_idempotent(self, reg):
        reg.upsert_surgeon("S1")
        reg.upsert_case("C1", surgeon_pseudonym="S1", procedure="lap-chole")
        reg.upsert_case("C1", surgeon_pseudonym="S1", procedure="lap-chole")


class TestImageHash:
    def test_upsert_and_exists(self, reg):
        reg.upsert_surgeon("S1")
        reg.upsert_case("C1", surgeon_pseudonym="S1")
        reg.upsert_image_hash("hash-abc", case_key="C1", orig_sopuid="uid1", instance_number=1)
        assert reg.image_exists("hash-abc") is True

    def test_not_exists(self, reg):
        assert reg.image_exists("nonexistent-hash") is False

    def test_duplicate_content_hash_deduped(self, reg):
        """Duplicate insert on same content_hash must not raise; row is upserted."""
        reg.upsert_surgeon("S1")
        reg.upsert_case("C1", surgeon_pseudonym="S1")
        reg.upsert_image_hash("hash-dup", case_key="C1", orig_sopuid="uid1", instance_number=1)
        # Second insert with same content_hash — ON CONFLICT → update (no exception)
        reg.upsert_image_hash("hash-dup", case_key="C1", orig_sopuid="uid2", instance_number=2)
        assert reg.image_exists("hash-dup") is True

    def test_case_image_hashes(self, reg):
        reg.upsert_surgeon("S1")
        reg.upsert_case("C1", surgeon_pseudonym="S1")
        reg.upsert_image_hash("h1", case_key="C1")
        reg.upsert_image_hash("h2", case_key="C1")
        reg.upsert_image_hash("h3", case_key="C1")
        result = reg.case_image_hashes("C1")
        assert result == {"h1", "h2", "h3"}

    def test_case_image_hashes_empty(self, reg):
        assert reg.case_image_hashes("no-such-case") == set()


class TestRecordAudit:
    def test_appends(self, reg):
        reg.record_audit("actor1", "action1", "target1")
        reg.record_audit("actor2", "action2", "target2")
        # No error raised; two entries written


# ---------------------------------------------------------------------------
# T015 (offline): migrate sqlite → sqlite-core, assert parity
# ---------------------------------------------------------------------------

def test_migrate_sqlite_to_sqlite_core(tmp_path):
    """
    Populate a sqlite3 Registry, migrate to a CoreRegistry on a second sqlite
    file via migrate_sqlite_to_pg, assert row-for-row parity.
    Covers T014/T015 migration code path without Postgres.
    """
    from src.services.registry_backend import migrate_sqlite_to_pg

    src_path = tmp_path / "src.db"
    dst_path = tmp_path / "dst.db"

    # Populate source
    src_reg = Registry(src_path)
    src_reg.upsert_surgeon("DOC1", role="attending")
    src_reg.upsert_surgeon("DOC2", role="resident")
    src_reg.upsert_rater("RAT1", expertise_tier="senior", reliability_weight=0.9)
    src_reg.upsert_case("CASE1", surgeon_pseudonym="DOC1", procedure="lap-chole")
    src_reg.upsert_case("CASE2", surgeon_pseudonym="DOC2", procedure="robotic")
    src_reg.upsert_image_hash("hashA", case_key="CASE1", orig_sopuid="uid-A", instance_number=1)
    src_reg.upsert_image_hash("hashB", case_key="CASE1", orig_sopuid="uid-B", instance_number=2)
    src_reg.upsert_image_hash("hashC", case_key="CASE2", orig_sopuid="uid-C", instance_number=1)
    src_reg.record_audit("test", "populate", "src")
    src_reg.close()

    dst_url = f"sqlite:///{dst_path}"
    report = migrate_sqlite_to_pg(str(src_path), dst_url)

    assert report.parity_ok is True
    assert report.source_surgeons == 2
    assert report.migrated_surgeons == 2
    assert report.source_subjects == 2   # cases
    assert report.source_hashes == 3

    # Verify destination readable via CoreRegistry
    from src.services.registry_backend import CoreRegistry, make_engine
    dst_reg = CoreRegistry(make_engine(dst_url))
    assert dst_reg.image_exists("hashA")
    assert dst_reg.image_exists("hashB")
    assert dst_reg.image_exists("hashC")
    assert dst_reg.case_image_hashes("CASE1") == {"hashA", "hashB"}
    dst_reg.close()


def test_migrate_report_fields(tmp_path):
    """
    Verify MigrationReport fields are accurate after a clean migration.
    """
    from src.services.registry import Registry
    from src.services.registry_backend import migrate_sqlite_to_pg

    src_path = tmp_path / "src2.db"
    dst_path = tmp_path / "dst2.db"

    src_reg = Registry(src_path)
    src_reg.upsert_surgeon("DOC_A")
    src_reg.upsert_case("CASE_A", surgeon_pseudonym="DOC_A")
    src_reg.upsert_image_hash("img-x", case_key="CASE_A")
    src_reg.upsert_image_hash("img-y", case_key="CASE_A")
    src_reg.close()

    report = migrate_sqlite_to_pg(str(src_path), f"sqlite:///{dst_path}")
    assert report.parity_ok is True
    assert report.source_surgeons == 1
    assert report.source_subjects == 1
    assert report.source_hashes == 2
    assert report.migrated_surgeons == 1
    assert report.migrated_cases == 1
    assert report.migrated_hashes == 2
