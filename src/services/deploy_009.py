"""
011 US1 — 009 deploy service (T007).

``deploy(config_json_path, registry_path, *, archive=True) -> MigrationReport``

Loads a ConfigTables JSON, runs Registry.migrate_from_configtables (parity +
rollback), records an audit entry (counts only — NO PHI), then archives the
source JSON to ``<path>.archived-<UTC-timestamp>.json``.

Parity failure → registry untouched, raise (do NOT archive).
"""
from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Union

from src.services.registry import MigrationReport, Registry


def deploy(
    config_json_path: Union[str, Path],
    registry_path: Union[str, Path],
    *,
    archive: bool = True,
) -> MigrationReport:
    """
    Migrate a ConfigTables JSON into the SQLite registry.

    Parameters
    ----------
    config_json_path:
        Path to the ConfigTables JSON file to migrate.
    registry_path:
        Path to the target SQLite registry (created if absent).
    archive:
        When True (default) the source JSON is moved to
        ``<config_json_path>.archived-<UTC-timestamp>.json`` on success.
        Set False only in tests that want to inspect the original path.

    Returns
    -------
    MigrationReport
        Row counts from the migration.

    Raises
    ------
    ValueError
        When parity check fails.  Registry is unchanged; source NOT archived.
    FileNotFoundError
        When config_json_path does not exist.
    """
    config_json_path = Path(config_json_path)
    registry_path = Path(registry_path)

    if not config_json_path.exists():
        raise FileNotFoundError(f"ConfigTables JSON not found: {config_json_path}")

    configtables_json = json.loads(config_json_path.read_text(encoding="utf-8"))

    reg = Registry(registry_path)
    try:
        # May raise ValueError on parity failure (rolls back internally)
        report = reg.migrate_from_configtables(configtables_json)
    except (ValueError, Exception):
        reg.close()
        raise

    # Audit entry — counts only, NO PHI
    summary = (
        f"deploy-009-migrate: "
        f"surgeons={report.migrated_surgeons}/"
        f"{report.source_surgeons} "
        f"cases={report.migrated_cases}/"
        f"{report.source_subjects} "
        f"hashes={report.migrated_hashes}/{report.source_hashes}"
    )
    reg.record_audit("librarian", "deploy-009-migrate", summary)
    reg.close()

    # Archive source JSON (rename, not delete)
    if archive:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        archive_path = config_json_path.with_name(
            config_json_path.name + f".archived-{ts}.json"
        )
        shutil.move(str(config_json_path), str(archive_path))

    return report
