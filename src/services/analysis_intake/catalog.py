"""
The ANALYSES catalog (spec 016, FR-019).

One row per confirmed publish, stored in the project configuration tables
beside SUBJECTS and IMAGE_HASHES and pushed through the same path.  That path
refuses to overwrite a configuration someone else changed meanwhile; in that
case the catalog pulls a fresh copy, adds the row again and retries once.
"""
from __future__ import annotations

from typing import Any, Dict

from src.services.analysis_intake.errors import refuse

ANALYSES_TABLE = "ANALYSES"
CATALOG_COLUMNS = ["TYPE_NAME", "TYPE_VERSION", "CASE_UID", "PLACEMENT_USED", "PRODUCER",
                   "SOURCE_HASH_COUNT", "SUPERSEDES"]
_DEFAULT_COLUMNS = {"NAME", "UID", "CREATED_DATE_TIME", "CREATED_BY"}


def _is_lost_update(exc: BaseException) -> bool:
    return any(cls.__name__ == "LostUpdateError" for cls in type(exc).__mro__)


def _retry_help():
    return ["Your result is on XNAT and confirmed; only the catalog row is missing.",
            "Run the same command again later: the catalog step will add the row without uploading twice.",
            "If it keeps failing, contact the Data Librarian."]


def catalog_values(outcome, descriptor: Dict[str, Any]) -> Dict[str, str]:
    run = descriptor["run"]
    return {
        "TYPE_NAME": str(descriptor["type_name"]),
        "TYPE_VERSION": str(descriptor["type_version"]),
        "CASE_UID": str(run["case_uid"]),
        "PLACEMENT_USED": str(outcome.placement_used),
        "PRODUCER": str(run.get("producer") or ""),
        "SOURCE_HASH_COUNT": str(len(run.get("source_hashes") or [])),
        "SUPERSEDES": str(run.get("supersedes") or ""),
    }


def _add_row(config_tables, label: str, values: Dict[str, str]) -> bool:
    """Add the row locally.  Returns False when the row was already there."""
    try:
        if not config_tables.table_exists(ANALYSES_TABLE):
            config_tables.add_new_table(ANALYSES_TABLE, list(CATALOG_COLUMNS), verbose=False)
        if config_tables.item_exists(ANALYSES_TABLE, label):
            return False
        columns = [c for c in config_tables.tables[ANALYSES_TABLE].columns if c.upper() not in _DEFAULT_COLUMNS]
        ordered = {c: values.get(c.upper(), "") for c in columns}
        ok, _msg = config_tables.add_new_item(ANALYSES_TABLE, label, extra_columns_values=ordered, verbose=False)
        return bool(ok)
    except AssertionError:
        raise refuse("Catalog not updated",
                     "Your account is not registered in the project configuration, so the catalog row could not be added.",
                     _retry_help())


def record_in_catalog(outcome, descriptor: Dict[str, Any], config_tables) -> bool:
    """Add and push the catalog row.  Returns False when the row was already there."""
    label = outcome.label
    values = catalog_values(outcome, descriptor)
    added = False
    for attempt in (1, 2):
        # The row may already be present locally from an earlier attempt whose
        # push failed, so the push always runs; pushing an unchanged table is harmless.
        added = _add_row(config_tables, label, values) or added
        try:
            pushed = config_tables.push_to_xnat(verbose=False)
        except Exception as exc:  # noqa: BLE001
            if _is_lost_update(exc) and attempt == 1:
                try:
                    config_tables.pull_from_xnat(verbose=False)
                except Exception:  # noqa: BLE001
                    break
                continue
            break
        if pushed is False:
            break
        return added
    raise refuse("Catalog not updated", "The project catalog could not be saved on XNAT.", _retry_help())
