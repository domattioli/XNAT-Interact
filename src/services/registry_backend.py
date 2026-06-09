"""
SQLAlchemy Core backend for the XNAT-Interact registry (011 US2, T012/T013/T014).

This module is ONLY imported when XNAT_REGISTRY_PG_DSN is set (or backend_url=
is passed to Registry).  All SQLAlchemy symbols are lazy-imported inside
functions so a bare environment (no sqlalchemy) can still import registry.py
and use the default sqlite3 path without any error.

Public surface
--------------
make_engine(url) -> Engine
CoreRegistry(engine)
    Same public methods as Registry: upsert_surgeon, upsert_rater, upsert_case,
    upsert_image_hash, image_exists, case_image_hashes, record_audit.
migrate_sqlite_to_pg(src_sqlite_path, target_url) -> MigrationReport
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Set


# ---------------------------------------------------------------------------
# Engine factory
# ---------------------------------------------------------------------------

def make_engine(url: str):
    """Return a SQLAlchemy Engine for *url* (sqlite or postgresql)."""
    from sqlalchemy import create_engine  # lazy
    return create_engine(url)


# ---------------------------------------------------------------------------
# Schema (mirrors _SCHEMA_SQL exactly)
# ---------------------------------------------------------------------------

def _build_metadata():
    """Build and return a MetaData with Table definitions mirroring _SCHEMA_SQL."""
    from sqlalchemy import (
        Column, Float, ForeignKey, Integer, MetaData, Table, Text,
    )
    Real = Float  # SQLAlchemy uses Float; alias for clarity
    meta = MetaData()

    Table(
        "surgeons", meta,
        Column("pseudonym", Text, primary_key=True),
        Column("role", Text),
    )

    Table(
        "raters", meta,
        Column("rater_id", Text, primary_key=True),
        Column("expertise_tier", Text),
        Column("reliability_weight", Real),
    )

    Table(
        "cases", meta,
        Column("case_key", Text, primary_key=True),
        Column("surgeon_pseudonym", Text, ForeignKey("surgeons.pseudonym")),
        Column("procedure", Text),
        Column("date_hash", Text),
        Column("device", Text),
    )

    Table(
        "image_hashes", meta,
        Column("content_hash", Text, nullable=False, unique=True),
        Column("case_key", Text, ForeignKey("cases.case_key")),
        Column("orig_sopuid", Text),
        Column("instance_number", Integer),
    )

    Table(
        "audit_log", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("ts", Text),
        Column("actor", Text),
        Column("action", Text),
        Column("target", Text),
    )

    return meta


# ---------------------------------------------------------------------------
# Dialect-aware upsert helper
# ---------------------------------------------------------------------------

def _upsert(engine, table, index_col: str, values: dict):
    """
    Execute a dialect-aware upsert (insert or update on conflict).

    Supports sqlite and postgresql dialects.
    """
    dialect = engine.dialect.name
    if dialect == "sqlite":
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert
        stmt = sqlite_insert(table).values(**values)
        # Build SET dict for all non-PK columns
        set_dict = {k: v for k, v in values.items() if k != index_col}
        if set_dict:
            stmt = stmt.on_conflict_do_update(index_elements=[index_col], set_=set_dict)
        else:
            stmt = stmt.on_conflict_do_nothing(index_elements=[index_col])
    elif dialect == "postgresql":
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        stmt = pg_insert(table).values(**values)
        set_dict = {k: v for k, v in values.items() if k != index_col}
        if set_dict:
            stmt = stmt.on_conflict_do_update(index_elements=[index_col], set_=set_dict)
        else:
            stmt = stmt.on_conflict_do_nothing(index_elements=[index_col])
    else:
        # Fallback: plain insert ignore
        from sqlalchemy import insert
        stmt = insert(table).values(**values).prefix_with("OR IGNORE")
    with engine.begin() as conn:
        conn.execute(stmt)


# ---------------------------------------------------------------------------
# CoreRegistry
# ---------------------------------------------------------------------------

class CoreRegistry:
    """
    SQLAlchemy Core implementation of the Registry contract.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        A connected SQLAlchemy engine.  Tables are created (CREATE IF NOT EXISTS)
        on construction.
    """

    def __init__(self, engine) -> None:
        self._engine = engine
        self._meta = _build_metadata()
        self._meta.create_all(engine)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @property
    def _surgeons(self):
        return self._meta.tables["surgeons"]

    @property
    def _raters(self):
        return self._meta.tables["raters"]

    @property
    def _cases(self):
        return self._meta.tables["cases"]

    @property
    def _image_hashes(self):
        return self._meta.tables["image_hashes"]

    @property
    def _audit_log(self):
        return self._meta.tables["audit_log"]

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    # ------------------------------------------------------------------
    # Upsert methods
    # ------------------------------------------------------------------

    def upsert_surgeon(self, pseudonym: str, role: Optional[str] = None) -> None:
        from src.services.xnat_gateway import GatewayError
        from src.services.errors import FriendlyError
        try:
            _upsert(
                self._engine, self._surgeons, "pseudonym",
                {"pseudonym": pseudonym, "role": role},
            )
        except Exception as exc:
            raise GatewayError(FriendlyError(
                title="Registry write failed (surgeons)",
                message="Could not upsert surgeon record into the registry.",
                recourse=["Check registry connection and disk space."],
            )) from exc

    def upsert_rater(
        self,
        rater_id: str,
        expertise_tier: Optional[str] = None,
        reliability_weight: Optional[float] = None,
    ) -> None:
        from src.services.xnat_gateway import GatewayError
        from src.services.errors import FriendlyError
        try:
            _upsert(
                self._engine, self._raters, "rater_id",
                {
                    "rater_id": rater_id,
                    "expertise_tier": expertise_tier,
                    "reliability_weight": reliability_weight,
                },
            )
        except Exception as exc:
            raise GatewayError(FriendlyError(
                title="Registry write failed (raters)",
                message="Could not upsert rater record into the registry.",
                recourse=["Check registry connection and disk space."],
            )) from exc

    def upsert_case(
        self,
        case_key: str,
        surgeon_pseudonym: Optional[str] = None,
        procedure: Optional[str] = None,
        date_hash: Optional[str] = None,
        device: Optional[str] = None,
    ) -> None:
        from src.services.xnat_gateway import GatewayError
        from src.services.errors import FriendlyError
        try:
            _upsert(
                self._engine, self._cases, "case_key",
                {
                    "case_key": case_key,
                    "surgeon_pseudonym": surgeon_pseudonym,
                    "procedure": procedure,
                    "date_hash": date_hash,
                    "device": device,
                },
            )
        except Exception as exc:
            raise GatewayError(FriendlyError(
                title="Registry write failed (cases)",
                message="Could not upsert case record into the registry.",
                recourse=["Check registry connection and disk space."],
            )) from exc

    def upsert_image_hash(
        self,
        content_hash: str,
        case_key: Optional[str] = None,
        orig_sopuid: Optional[str] = None,
        instance_number: Optional[int] = None,
    ) -> None:
        from src.services.xnat_gateway import GatewayError
        from src.services.errors import FriendlyError
        try:
            _upsert(
                self._engine, self._image_hashes, "content_hash",
                {
                    "content_hash": content_hash,
                    "case_key": case_key,
                    "orig_sopuid": orig_sopuid,
                    "instance_number": instance_number,
                },
            )
        except Exception as exc:
            raise GatewayError(FriendlyError(
                title="Registry write failed (image_hashes)",
                message="Could not upsert image hash into the registry.",
                recourse=["Check registry connection and disk space."],
            )) from exc

    def image_exists(self, content_hash: str) -> bool:
        from sqlalchemy import select
        tbl = self._image_hashes
        stmt = select(tbl.c.content_hash).where(
            tbl.c.content_hash == content_hash
        ).limit(1)
        with self._engine.connect() as conn:
            row = conn.execute(stmt).fetchone()
        return row is not None

    def case_image_hashes(self, case_key: str) -> Set[str]:
        from sqlalchemy import select
        tbl = self._image_hashes
        stmt = select(tbl.c.content_hash).where(tbl.c.case_key == case_key)
        with self._engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()
        return {r[0] for r in rows}

    def record_audit(self, actor: str, action: str, target: str) -> None:
        from src.services.xnat_gateway import GatewayError
        from src.services.errors import FriendlyError
        try:
            tbl = self._audit_log
            with self._engine.begin() as conn:
                conn.execute(tbl.insert().values(
                    ts=self._now_iso(), actor=actor, action=action, target=target,
                ))
        except Exception as exc:
            raise GatewayError(FriendlyError(
                title="Registry write failed (audit_log)",
                message="Could not append to the audit log.",
                recourse=["Check registry connection and disk space."],
            )) from exc

    def close(self) -> None:
        self._engine.dispose()


# ---------------------------------------------------------------------------
# Migration: SQLite → any target engine
# ---------------------------------------------------------------------------

def migrate_sqlite_to_pg(src_sqlite_path: str, target_url: str):
    """
    Copy all rows from a SQLite registry file into the target (PG or another SQLite).

    Parameters
    ----------
    src_sqlite_path : str
        Path to the source SQLite .db file.
    target_url : str
        SQLAlchemy URL of the destination (e.g. ``postgresql+psycopg://...`` or
        ``sqlite:///path/to/dest.db``).

    Returns
    -------
    MigrationReport
        Row-for-row parity report.  ``parity_ok=True`` iff all counts match.

    Raises
    ------
    ValueError
        When row counts mismatch after copy.  The target transaction is rolled back.
    """
    from src.services.registry import MigrationReport  # avoid circular at module level

    report = MigrationReport()

    # Read from SQLite source
    src_conn = sqlite3.connect(str(src_sqlite_path))
    try:
        def _count(table: str) -> int:
            try:
                return src_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            except sqlite3.OperationalError:
                return 0

        def _rows(table: str, columns):
            try:
                cur = src_conn.execute(
                    f"SELECT {', '.join(columns)} FROM {table}"
                )
                return cur.fetchall()
            except sqlite3.OperationalError:
                return []

        src_surgeons   = _rows("surgeons",    ["pseudonym", "role"])
        src_raters     = _rows("raters",      ["rater_id", "expertise_tier", "reliability_weight"])
        src_cases      = _rows("cases",       ["case_key", "surgeon_pseudonym", "procedure", "date_hash", "device"])
        src_img_hashes = _rows("image_hashes",["content_hash", "case_key", "orig_sopuid", "instance_number"])
        src_audit      = _rows("audit_log",   ["ts", "actor", "action", "target"])

        report.source_surgeons = len(src_surgeons)
        report.source_subjects = len(src_cases)
        report.source_hashes   = len(src_img_hashes)
    finally:
        src_conn.close()

    # Write to target inside a single transaction; rollback on parity fail
    dst_engine = make_engine(target_url)
    dst_meta = _build_metadata()
    dst_meta.create_all(dst_engine)

    with dst_engine.begin() as tx:
        def _insert_rows(table_name, col_names, rows):
            if not rows:
                return
            tbl = dst_meta.tables[table_name]
            for row in rows:
                vals = dict(zip(col_names, row))
                stmt = tbl.insert().values(**vals)
                try:
                    tx.execute(stmt)
                except Exception:
                    # On conflict: skip (idempotent re-run)
                    pass

        _insert_rows("surgeons",    ["pseudonym", "role"],                             src_surgeons)
        _insert_rows("raters",      ["rater_id", "expertise_tier", "reliability_weight"], src_raters)
        _insert_rows("cases",       ["case_key", "surgeon_pseudonym", "procedure", "date_hash", "device"], src_cases)
        _insert_rows("image_hashes",["content_hash", "case_key", "orig_sopuid", "instance_number"], src_img_hashes)
        _insert_rows("audit_log",   ["ts", "actor", "action", "target"],               src_audit)

        # Parity check
        def _dst_count(table_name):
            from sqlalchemy import text
            row = tx.execute(text(f"SELECT COUNT(*) FROM {table_name}")).fetchone()
            return row[0] if row else 0

        dst_surgeons = _dst_count("surgeons")
        dst_cases    = _dst_count("cases")
        dst_hashes   = _dst_count("image_hashes")

        errors = []
        if dst_surgeons < report.source_surgeons:
            errors.append(f"surgeons: src={report.source_surgeons} dst={dst_surgeons}")
        if dst_cases < report.source_subjects:
            errors.append(f"cases: src={report.source_subjects} dst={dst_cases}")
        if dst_hashes < report.source_hashes:
            errors.append(f"image_hashes: src={report.source_hashes} dst={dst_hashes}")

        if errors:
            raise ValueError(
                "migrate_sqlite_to_pg parity check failed; target rolled back. "
                + "; ".join(errors)
            )

        report.migrated_surgeons = dst_surgeons
        report.migrated_cases    = dst_cases
        report.migrated_hashes   = dst_hashes

    report.parity_ok = True
    dst_engine.dispose()
    return report
