"""
011 US1 — CLI entrypoint for the 009 deploy service (T008).

Usage:
    python -m src.cli.deploy_009 <config_json> <registry_db>

Passphrase (for encrypted crosswalk):
    Read from XNAT_CROSSWALK_PASSPHRASE env var ONLY, or via interactive
    no-echo prompt. NEVER accepted as a CLI argument (F5 / FR-002).

Exit codes:
    0 — success
    1 — failure (error printed to stderr)
"""
from __future__ import annotations

import argparse
import getpass
import os
import sys


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="deploy_009",
        description="Migrate ConfigTables JSON into the 009 SQLite registry.",
    )
    parser.add_argument("config_json", help="Path to ConfigTables JSON file.")
    parser.add_argument("registry_db", help="Path to target SQLite registry file.")
    parser.add_argument(
        "--no-archive",
        action="store_true",
        help="Do not archive the source JSON after migration (testing only).",
    )
    # NOTE: no --passphrase flag — passphrase ONLY via env or prompt (F5)
    args = parser.parse_args(argv)

    # Passphrase: env first, then prompt (never argv)
    passphrase: str | None = os.environ.get("XNAT_CROSSWALK_PASSPHRASE")
    if passphrase is None and sys.stdin.isatty():
        try:
            passphrase = getpass.getpass("Crosswalk passphrase (leave blank to skip encryption): ")
            if passphrase == "":
                passphrase = None
        except (EOFError, KeyboardInterrupt):
            passphrase = None

    try:
        from src.services.deploy_009 import deploy
        report = deploy(
            args.config_json,
            args.registry_db,
            archive=not args.no_archive,
        )
        print(
            f"Migration complete: "
            f"surgeons={report.migrated_surgeons}/{report.source_surgeons} "
            f"cases={report.migrated_cases}/{report.source_subjects} "
            f"hashes={report.migrated_hashes}/{report.source_hashes}"
        )
        return 0
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(_main())
