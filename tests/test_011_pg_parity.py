"""
T015: Postgres parity test (marked pg + slow).

Skips unless XNAT_REGISTRY_PG_DSN is set.
Populates a SQLite registry, migrates to Postgres, asserts row-for-row parity
and UNIQUE constraint enforcement on PG side.
"""
from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

pytestmark = [pytest.mark.pg, pytest.mark.slow]

sqlalchemy = pytest.importorskip("sqlalchemy", reason="sqlalchemy not installed")

PG_DSN = os.environ.get("XNAT_REGISTRY_PG_DSN", "")
if not PG_DSN:
    pytest.skip("XNAT_REGISTRY_PG_DSN not set", allow_module_level=True)


@pytest.fixture(scope="module")
def sqlite_source(tmp_path_factory):
    """Populated SQLite registry for migration tests."""
    tmp = tmp_path_factory.mktemp("pg_parity_src")
    src_path = tmp / "src.db"

    from src.services.registry import Registry
    reg = Registry(src_path)
    reg.upsert_surgeon("DOC_PG1", role="attending")
    reg.upsert_surgeon("DOC_PG2", role="resident")
    reg.upsert_rater("RAT_PG1", expertise_tier="senior", reliability_weight=0.9)
    reg.upsert_case("CASE_PG1", surgeon_pseudonym="DOC_PG1", procedure="lap-chole")
    reg.upsert_case("CASE_PG2", surgeon_pseudonym="DOC_PG2", procedure="robotic")
    reg.upsert_image_hash("pg-hash-A", case_key="CASE_PG1", orig_sopuid="uid-A", instance_number=1)
    reg.upsert_image_hash("pg-hash-B", case_key="CASE_PG1", orig_sopuid="uid-B", instance_number=2)
    reg.upsert_image_hash("pg-hash-C", case_key="CASE_PG2", orig_sopuid="uid-C", instance_number=1)
    reg.record_audit("test", "pg_parity_setup", "src")
    reg.close()
    return src_path


def test_migrate_sqlite_to_pg_parity(sqlite_source):
    from src.services.registry_backend import migrate_sqlite_to_pg

    report = migrate_sqlite_to_pg(str(sqlite_source), PG_DSN)

    assert report.parity_ok is True
    assert report.source_surgeons >= 2
    assert report.migrated_surgeons >= 2
    assert report.source_subjects >= 2
    assert report.source_hashes >= 3


def test_pg_image_exists_after_migrate(sqlite_source):
    from src.services.registry_backend import migrate_sqlite_to_pg, CoreRegistry, make_engine

    migrate_sqlite_to_pg(str(sqlite_source), PG_DSN)
    pg = CoreRegistry(make_engine(PG_DSN))
    assert pg.image_exists("pg-hash-A") is True
    assert pg.image_exists("pg-hash-B") is True
    assert pg.image_exists("pg-hash-C") is True
    assert pg.image_exists("nonexistent") is False
    pg.close()


def test_pg_unique_enforced_after_migrate(sqlite_source):
    """
    Re-inserting a duplicate content_hash via upsert must not raise and
    must not duplicate the row (ON CONFLICT DO UPDATE).
    """
    from src.services.registry_backend import migrate_sqlite_to_pg, CoreRegistry, make_engine
    from sqlalchemy import select, func, text

    migrate_sqlite_to_pg(str(sqlite_source), PG_DSN)
    pg = CoreRegistry(make_engine(PG_DSN))

    # Upsert same hash twice — no exception
    pg.upsert_image_hash("pg-hash-A", case_key="CASE_PG1", orig_sopuid="uid-A-v2", instance_number=99)
    pg.upsert_image_hash("pg-hash-A", case_key="CASE_PG1", orig_sopuid="uid-A-v2", instance_number=99)

    # Count must still be 1 for this hash
    tbl = pg._image_hashes
    stmt = select(func.count()).where(tbl.c.content_hash == "pg-hash-A")
    with pg._engine.connect() as conn:
        count = conn.execute(stmt).scalar()
    assert count == 1
    pg.close()


def test_pg_case_image_hashes(sqlite_source):
    from src.services.registry_backend import migrate_sqlite_to_pg, CoreRegistry, make_engine

    migrate_sqlite_to_pg(str(sqlite_source), PG_DSN)
    pg = CoreRegistry(make_engine(PG_DSN))
    hashes = pg.case_image_hashes("CASE_PG1")
    assert "pg-hash-A" in hashes
    assert "pg-hash-B" in hashes
    pg.close()
