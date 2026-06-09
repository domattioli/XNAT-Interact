"""
SQLite-backed registry for XNAT-Interact (Feature 009, Stage 2a — T003/T004).

Replaces the ConfigTables JSON blob with a schema-enforced, UNIQUE-indexed,
transactional SQLite database (FR-009, FR-010, US3).

Public API
----------
Registry(db_path)
    Open or create the operational SQLite registry at *db_path*.

    Schema (created on first open):
        surgeons(pseudonym TEXT PRIMARY KEY, role TEXT)
        raters(rater_id TEXT PRIMARY KEY, expertise_tier TEXT,
               reliability_weight REAL)
        cases(case_key TEXT PRIMARY KEY,
              surgeon_pseudonym TEXT REFERENCES surgeons(pseudonym),
              procedure TEXT, date_hash TEXT, device TEXT)
        image_hashes(content_hash TEXT UNIQUE NOT NULL,
                     case_key TEXT REFERENCES cases(case_key),
                     orig_sopuid TEXT, instance_number INTEGER)
        audit_log(id INTEGER PRIMARY KEY AUTOINCREMENT,
                  ts TEXT, actor TEXT, action TEXT, target TEXT)

    Methods:
        upsert_surgeon(pseudonym, role=None) -> None
        upsert_rater(rater_id, expertise_tier=None, reliability_weight=None) -> None
        upsert_case(case_key, surgeon_pseudonym=None, procedure=None,
                    date_hash=None, device=None) -> None
        upsert_image_hash(content_hash, case_key=None, orig_sopuid=None,
                         instance_number=None) -> None
        image_exists(content_hash) -> bool
        case_image_hashes(case_key) -> set[str]
        record_audit(actor, action, target) -> None
        migrate_from_configtables(configtables_json) -> MigrationReport

CrosswalkStore(path)
    Separate, path-injected file store for the pseudonym <-> HawkID crosswalk.
    NOT the operational DB.  Must be access-controlled/encrypted in deployment
    (encryption is a deployment concern — this impl stores plaintext JSON
    behind a restricted filesystem path; see SECURITY NOTE below).

    Methods:
        put(pseudonym, hawkid) -> None
        get(pseudonym) -> str | None

SECURITY NOTE
-------------
CrosswalkStore uses a plain JSON file.  In production this file MUST be:
  - Owned by the data librarian account (mode 600 or equivalent).
  - Stored on an encrypted volume or wrapped by a secrets-management system.
  - Never co-located with the operational registry DB.
The operational DB (Registry) MUST NOT contain real HawkIDs; the crosswalk is
the only place they live (FR-011).

MigrationReport
    Named dataclass returned by migrate_from_configtables:
        source_surgeons  : int   (rows in source Surgeons table)
        migrated_surgeons: int
        source_subjects  : int   (rows in source Subjects/subjects table)
        migrated_cases   : int
        source_hashes    : int   (rows in source IMAGE_HASHES table)
        migrated_hashes  : int
        parity_ok        : bool  (True when all counts match)
        notes            : list[str]
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Set

from src.services.errors import FriendlyError
from src.services.xnat_gateway import GatewayError


# ---------------------------------------------------------------------------
# MigrationReport
# ---------------------------------------------------------------------------

@dataclass
class MigrationReport:
    """Row-for-row parity report produced by Registry.migrate_from_configtables."""
    source_surgeons: int = 0
    migrated_surgeons: int = 0
    source_subjects: int = 0
    migrated_cases: int = 0
    source_hashes: int = 0
    migrated_hashes: int = 0
    parity_ok: bool = False
    notes: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

_SCHEMA_SQL = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS surgeons (
    pseudonym TEXT PRIMARY KEY,
    role      TEXT
);

CREATE TABLE IF NOT EXISTS raters (
    rater_id          TEXT PRIMARY KEY,
    expertise_tier    TEXT,
    reliability_weight REAL
);

CREATE TABLE IF NOT EXISTS cases (
    case_key           TEXT PRIMARY KEY,
    surgeon_pseudonym  TEXT REFERENCES surgeons(pseudonym),
    procedure          TEXT,
    date_hash          TEXT,
    device             TEXT
);

CREATE TABLE IF NOT EXISTS image_hashes (
    content_hash    TEXT UNIQUE NOT NULL,
    case_key        TEXT REFERENCES cases(case_key),
    orig_sopuid     TEXT,
    instance_number INTEGER
);

CREATE TABLE IF NOT EXISTS audit_log (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    ts      TEXT,
    actor   TEXT,
    action  TEXT,
    target  TEXT
);
"""


def _open_connection(db_path: str | Path) -> sqlite3.Connection:
    """Open a WAL-mode, foreign-keys-ON connection; raise GatewayError on failure."""
    try:
        conn = sqlite3.connect(str(db_path), check_same_thread=False)
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn
    except sqlite3.Error as exc:
        raise GatewayError(
            FriendlyError(
                title="Registry database unavailable",
                message=(
                    f"Could not open the SQLite registry at '{db_path}'. "
                    "The file may be locked, on a read-only filesystem, or "
                    "corrupted."
                ),
                recourse=[
                    "Ensure the directory exists and is writable.",
                    "Check that no other process holds an exclusive lock.",
                    "Contact the Data Librarian if the problem persists.",
                ],
            )
        ) from exc


class Registry:
    """
    SQLite-backed operational registry.

    Opens (or creates) the database at *db_path*, applies the schema, and
    provides transactional upsert methods for each table.

    Parameters
    ----------
    db_path : str | Path
        Filesystem path to the SQLite file.  Created if it does not exist.
    """

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = Path(db_path)
        try:
            self._conn = _open_connection(self._db_path)
            self._apply_schema()
        except GatewayError:
            raise
        except sqlite3.Error as exc:
            raise GatewayError(
                FriendlyError(
                    title="Registry schema initialisation failed",
                    message=(
                        "The SQLite registry opened but the schema could not "
                        "be applied.  The database may be corrupted."
                    ),
                    recourse=["Delete the registry file and let it be recreated."],
                )
            ) from exc

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _apply_schema(self) -> None:
        with self._conn:
            self._conn.executescript(_SCHEMA_SQL)

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    # ------------------------------------------------------------------
    # Upsert methods (all transactional)
    # ------------------------------------------------------------------

    def upsert_surgeon(self, pseudonym: str, role: Optional[str] = None) -> None:
        """Insert or replace a surgeon row."""
        try:
            with self._conn:
                self._conn.execute(
                    "INSERT INTO surgeons(pseudonym, role) VALUES(?, ?)"
                    " ON CONFLICT(pseudonym) DO UPDATE SET role=excluded.role",
                    (pseudonym, role),
                )
        except sqlite3.Error as exc:
            raise GatewayError(
                FriendlyError(
                    title="Registry write failed (surgeons)",
                    message="Could not upsert surgeon record into the registry.",
                    recourse=["Check registry file permissions and disk space."],
                )
            ) from exc

    def upsert_rater(
        self,
        rater_id: str,
        expertise_tier: Optional[str] = None,
        reliability_weight: Optional[float] = None,
    ) -> None:
        """Insert or replace a rater row."""
        try:
            with self._conn:
                self._conn.execute(
                    "INSERT INTO raters(rater_id, expertise_tier, reliability_weight)"
                    " VALUES(?, ?, ?)"
                    " ON CONFLICT(rater_id) DO UPDATE SET"
                    "   expertise_tier=excluded.expertise_tier,"
                    "   reliability_weight=excluded.reliability_weight",
                    (rater_id, expertise_tier, reliability_weight),
                )
        except sqlite3.Error as exc:
            raise GatewayError(
                FriendlyError(
                    title="Registry write failed (raters)",
                    message="Could not upsert rater record into the registry.",
                    recourse=["Check registry file permissions and disk space."],
                )
            ) from exc

    def upsert_case(
        self,
        case_key: str,
        surgeon_pseudonym: Optional[str] = None,
        procedure: Optional[str] = None,
        date_hash: Optional[str] = None,
        device: Optional[str] = None,
    ) -> None:
        """Insert or replace a case row."""
        try:
            with self._conn:
                self._conn.execute(
                    "INSERT INTO cases(case_key, surgeon_pseudonym, procedure,"
                    "   date_hash, device) VALUES(?, ?, ?, ?, ?)"
                    " ON CONFLICT(case_key) DO UPDATE SET"
                    "   surgeon_pseudonym=excluded.surgeon_pseudonym,"
                    "   procedure=excluded.procedure,"
                    "   date_hash=excluded.date_hash,"
                    "   device=excluded.device",
                    (case_key, surgeon_pseudonym, procedure, date_hash, device),
                )
        except sqlite3.Error as exc:
            raise GatewayError(
                FriendlyError(
                    title="Registry write failed (cases)",
                    message="Could not upsert case record into the registry.",
                    recourse=["Check registry file permissions and disk space."],
                )
            ) from exc

    def upsert_image_hash(
        self,
        content_hash: str,
        case_key: Optional[str] = None,
        orig_sopuid: Optional[str] = None,
        instance_number: Optional[int] = None,
    ) -> None:
        """
        Insert or replace an image-hash row.

        The UNIQUE constraint on *content_hash* is the O(1) dedup index
        (US3 SC-001, FR-009).  A duplicate insert is handled atomically via
        ON CONFLICT — no partial write occurs.
        """
        try:
            with self._conn:
                self._conn.execute(
                    "INSERT INTO image_hashes(content_hash, case_key,"
                    "   orig_sopuid, instance_number) VALUES(?, ?, ?, ?)"
                    " ON CONFLICT(content_hash) DO UPDATE SET"
                    "   case_key=excluded.case_key,"
                    "   orig_sopuid=excluded.orig_sopuid,"
                    "   instance_number=excluded.instance_number",
                    (content_hash, case_key, orig_sopuid, instance_number),
                )
        except sqlite3.Error as exc:
            raise GatewayError(
                FriendlyError(
                    title="Registry write failed (image_hashes)",
                    message="Could not upsert image hash into the registry.",
                    recourse=["Check registry file permissions and disk space."],
                )
            ) from exc

    # ------------------------------------------------------------------
    # Query methods
    # ------------------------------------------------------------------

    def image_exists(self, content_hash: str) -> bool:
        """
        Return True if *content_hash* is already in the registry.

        Resolved via the UNIQUE index on image_hashes.content_hash —
        O(1) lookup, no full-table scan (US3, FR-009, SC-001).
        """
        cur = self._conn.execute(
            "SELECT 1 FROM image_hashes WHERE content_hash = ? LIMIT 1",
            (content_hash,),
        )
        return cur.fetchone() is not None

    def case_image_hashes(self, case_key: str) -> Set[str]:
        """Return the set of content hashes belonging to *case_key*."""
        cur = self._conn.execute(
            "SELECT content_hash FROM image_hashes WHERE case_key = ?",
            (case_key,),
        )
        return {row[0] for row in cur.fetchall()}

    # ------------------------------------------------------------------
    # Audit log
    # ------------------------------------------------------------------

    def record_audit(self, actor: str, action: str, target: str) -> None:
        """Append one row to the audit_log (append-only by convention)."""
        try:
            with self._conn:
                self._conn.execute(
                    "INSERT INTO audit_log(ts, actor, action, target)"
                    " VALUES(?, ?, ?, ?)",
                    (self._now_iso(), actor, action, target),
                )
        except sqlite3.Error as exc:
            raise GatewayError(
                FriendlyError(
                    title="Registry write failed (audit_log)",
                    message="Could not append to the audit log.",
                    recourse=["Check registry file permissions and disk space."],
                )
            ) from exc

    # ------------------------------------------------------------------
    # Migration (T004)
    # ------------------------------------------------------------------

    def migrate_from_configtables(self, configtables_json: dict) -> MigrationReport:
        """
        Import an existing ConfigTables JSON blob into the SQLite schema.

        The JSON is expected to have the shape produced by
        ``ConfigTables.save()``:

            {
              "metadata": {...},
              "tables": {
                "SURGEONS":     [{"NAME": ..., "UID": ..., ...}, ...],
                "SUBJECTS":     [{"NAME": ..., "UID": ...,
                                  "ACQUISITION_SITE": ..., "GROUP": ...}, ...],
                "IMAGE_HASHES": [{"NAME": ..., "UID": ...,
                                  "SUBJECT": ..., "INSTANCE_NUM": ...}, ...],
                ...
              }
            }

        Table-name matching is case-insensitive.  Unrecognised tables are
        skipped (noted in the report).

        After import, row-for-row parity is asserted (counts must match source).
        If parity fails, all migrated rows are rolled back and a ``ValueError``
        is raised.

        Returns
        -------
        MigrationReport
            Counts and parity result.

        Raises
        ------
        ValueError
            When parity check fails (counts mismatch).  The registry is left
            unchanged.
        GatewayError
            On SQLite I/O failure during migration.
        """
        report = MigrationReport()
        tables: dict = configtables_json.get("tables", {})

        # Normalise table-name lookup (case-insensitive)
        def _find(names):
            upper_map = {k.upper(): k for k in tables}
            for n in names:
                if n.upper() in upper_map:
                    return tables[upper_map[n.upper()]]
            return []

        surgeons_rows = _find(["SURGEONS", "Surgeons"])
        subjects_rows = _find(["SUBJECTS", "subjects"])
        hashes_rows   = _find(["IMAGE_HASHES"])

        report.source_surgeons = len(surgeons_rows)
        report.source_subjects = len(subjects_rows)
        report.source_hashes   = len(hashes_rows)

        # All-or-nothing: wrap everything in a single savepoint so we can
        # roll back completely on parity failure.
        try:
            with self._conn:
                # -- surgeons -------------------------------------------------
                for row in surgeons_rows:
                    name = (row.get("NAME") or "").strip()
                    if not name:
                        report.notes.append(
                            f"Skipped surgeon row with empty NAME: {row}"
                        )
                        continue
                    # Legacy rows store cleartext names — migrate as pseudonym
                    # placeholder.  The real pseudonym assignment happens during
                    # the Stage 4/5 ingest rewrite; here we preserve the legacy
                    # key verbatim so the row is not lost.
                    self._conn.execute(
                        "INSERT INTO surgeons(pseudonym, role) VALUES(?, ?)"
                        " ON CONFLICT(pseudonym) DO NOTHING",
                        (name.upper(), None),
                    )
                    report.migrated_surgeons += 1

                # -- cases (from Subjects table) ------------------------------
                for row in subjects_rows:
                    name = (row.get("NAME") or "").strip()
                    if not name:
                        report.notes.append(
                            f"Skipped subject row with empty NAME: {row}"
                        )
                        continue
                    acq_site = row.get("ACQUISITION_SITE") or row.get("acquisition_site")
                    group    = row.get("GROUP") or row.get("group")
                    # Map Subjects → cases; procedure = group, device = acq_site
                    self._conn.execute(
                        "INSERT INTO cases(case_key, surgeon_pseudonym,"
                        "   procedure, date_hash, device) VALUES(?, ?, ?, ?, ?)"
                        " ON CONFLICT(case_key) DO NOTHING",
                        (name.upper(), None, group, None, acq_site),
                    )
                    report.migrated_cases += 1

                # -- image_hashes ---------------------------------------------
                for row in hashes_rows:
                    name = (row.get("NAME") or "").strip()
                    if not name:
                        report.notes.append(
                            f"Skipped image_hash row with empty NAME: {row}"
                        )
                        continue
                    subject = (row.get("SUBJECT") or row.get("subject") or "").strip()
                    instance_num = row.get("INSTANCE_NUM") or row.get("instance_num")
                    try:
                        instance_num = int(instance_num) if instance_num is not None else None
                    except (TypeError, ValueError):
                        instance_num = None
                    case_key = subject.upper() if subject else None
                    self._conn.execute(
                        "INSERT INTO image_hashes(content_hash, case_key,"
                        "   orig_sopuid, instance_number) VALUES(?, ?, ?, ?)"
                        " ON CONFLICT(content_hash) DO NOTHING",
                        (name, case_key, None, instance_num),
                    )
                    report.migrated_hashes += 1

                # -- parity check ---------------------------------------------
                _parity_errors = []
                if report.migrated_surgeons != report.source_surgeons:
                    _parity_errors.append(
                        f"Surgeons: source={report.source_surgeons},"
                        f" migrated={report.migrated_surgeons}"
                    )
                if report.migrated_cases != report.source_subjects:
                    _parity_errors.append(
                        f"Subjects/cases: source={report.source_subjects},"
                        f" migrated={report.migrated_cases}"
                    )
                if report.migrated_hashes != report.source_hashes:
                    _parity_errors.append(
                        f"ImageHashes: source={report.source_hashes},"
                        f" migrated={report.migrated_hashes}"
                    )

                if _parity_errors:
                    # Raise inside the `with self._conn` block → automatic rollback
                    raise ValueError(
                        "Migration parity check failed; registry unchanged. "
                        + "; ".join(_parity_errors)
                    )

        except ValueError:
            raise
        except sqlite3.Error as exc:
            raise GatewayError(
                FriendlyError(
                    title="Registry migration failed",
                    message=(
                        "A database error occurred during ConfigTables migration. "
                        "No changes were committed."
                    ),
                    recourse=["Check registry file permissions and disk space."],
                )
            ) from exc

        report.parity_ok = True
        self.record_audit("migration", "migrate_from_configtables", "configtables_json")
        return report

    # ------------------------------------------------------------------
    # Teardown
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close the underlying SQLite connection."""
        try:
            self._conn.close()
        except sqlite3.Error:
            pass


# ---------------------------------------------------------------------------
# CrosswalkStore
# ---------------------------------------------------------------------------

class CrosswalkStore:
    """
    Librarian-only pseudonym <-> HawkID crosswalk store.

    SECURITY: This store holds the ONLY copy of real HawkIDs.  It MUST be:
      - Stored at a path accessible only to the data librarian account.
      - Encrypted at rest in production via the opt-in passphrase/key arg
        (011 FR-003: AEAD envelope via crosswalk_crypto).
      - Never co-located with the operational Registry DB.
      - Never committed to source control.

    Parameters
    ----------
    path : str | Path
        Path to the crosswalk file.  Created if it does not exist.
    passphrase : str | None
        Librarian passphrase.  When supplied the store is encrypted at rest
        (AEAD envelope via scrypt KDF).  Never logged or persisted.
    key : bytes | None
        Pre-derived 32-byte AES key.  Use instead of *passphrase* when you
        already hold the derived key.  Mutually exclusive with *passphrase*.

    When neither *passphrase* nor *key* is given the store behaves exactly
    as before (plain JSON) — the 009 unkeyed path is byte-unchanged (F2).
    """

    def __init__(
        self,
        path: str | Path,
        *,
        passphrase: "str | None" = None,
        key: "bytes | None" = None,
    ) -> None:
        self._path = Path(path)
        if passphrase is not None and key is not None:
            raise ValueError("Supply either passphrase or key, not both.")
        self._passphrase = passphrase
        self._key = key  # pre-derived key (bypasses KDF)
        self._kdf_salt: "bytes | None" = None  # loaded from envelope on first read

    # ------------------------------------------------------------------
    # Internal key helpers
    # ------------------------------------------------------------------

    def _is_keyed(self) -> bool:
        return self._passphrase is not None or self._key is not None

    def _get_key(self, kdf_salt: bytes) -> bytes:
        """Return the AES key, deriving from passphrase if needed."""
        if self._key is not None:
            return self._key
        from src.services.crosswalk_crypto import derive_key
        return derive_key(self._passphrase, kdf_salt)  # type: ignore[arg-type]

    # ------------------------------------------------------------------
    # Load / save
    # ------------------------------------------------------------------

    def _load(self) -> dict:
        if not self._path.exists():
            return {}
        raw = self._path.read_bytes()
        if not self._is_keyed():
            # Unkeyed — legacy plaintext JSON path (byte-unchanged, F2)
            return json.loads(raw.decode("utf-8"))
        # Keyed path
        from src.services.crosswalk_crypto import (
            extract_kdf_salt,
            is_legacy_plaintext,
            open_envelope,
        )
        if is_legacy_plaintext(raw):
            # Auto-detect legacy plaintext → read it, schedule re-seal on next save
            data = json.loads(raw.decode("utf-8"))
            # Store a fresh salt so next _save will seal properly
            import os
            self._kdf_salt = os.urandom(16)
            return data
        # Normal sealed envelope
        self._kdf_salt = extract_kdf_salt(raw)
        key = self._get_key(self._kdf_salt)
        return open_envelope(raw, key)

    def _save(self, data: dict) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        if not self._is_keyed():
            # Unkeyed — plain JSON (byte-unchanged legacy path, F2)
            with self._path.open("w", encoding="utf-8") as fh:
                json.dump(data, fh, indent=2)
            return
        # Keyed path — AEAD envelope
        import os
        from src.services.crosswalk_crypto import seal_with_salt
        if self._kdf_salt is None:
            self._kdf_salt = os.urandom(16)
        key = self._get_key(self._kdf_salt)
        blob = seal_with_salt(data, key, self._kdf_salt)
        self._path.write_bytes(blob)

    def put(self, pseudonym: str, hawkid: str) -> None:
        """
        Store *hawkid* under *pseudonym*.

        The operational DB MUST NOT be queried to derive this mapping — this
        method is the only authorised write path for crosswalk data (FR-011).
        """
        data = self._load()
        data[pseudonym] = hawkid
        self._save(data)

    def get(self, pseudonym: str) -> Optional[str]:
        """
        Return the HawkID for *pseudonym*, or None if not found.

        Access to this method MUST be restricted to librarian-role callers.
        """
        return self._load().get(pseudonym)
