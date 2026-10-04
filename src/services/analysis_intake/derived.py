"""
Which published items were made from one frame? (spec 018, FR-013, FR-014)

``find_derived(hash_value, gateway=..., config_tables=...)`` reads every row of
the ANALYSES catalog, downloads that item's ``analysis.json`` and looks for the
hash among its ``run.source_hashes``.  It answers questions such as "this frame
turned out to be wrong; which datasets and results used it?".

This function only reads.  It never uploads, changes or deletes anything on
the server, and its temporary download folder is always removed.
"""
from __future__ import annotations

import json
import re
import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Tuple

from src.services.analysis_intake.catalog import ANALYSES_TABLE
from src.services.analysis_intake.errors import refuse

_HASH_RE = re.compile(r"^[0-9a-fA-F]{64}$")
DESCRIPTOR_NAME = "analysis.json"


@dataclass
class DerivedItem:
    """One published item that lists the hash among its inputs."""
    type_name: str
    label: str
    placement_used: str
    query_string: str
    resource_label: str
    matched_on: str          # "sha256" or "image_identity_hash"


@dataclass
class DerivedReport:
    """The answer: matching items, how many rows were checked, and rows that could not be checked."""
    hash: str
    items: List[DerivedItem] = field(default_factory=list)
    checked: int = 0
    unchecked: List[Tuple[str, str]] = field(default_factory=list)   # (label, plain reason)


def _catalog_rows(config_tables) -> List[Dict[str, Any]]:
    """Return the ANALYSES rows as plain dictionaries with upper-case keys."""
    try:
        if not config_tables.table_exists(ANALYSES_TABLE):
            return []
        table = config_tables.tables[ANALYSES_TABLE]
        if hasattr(table, "to_dict"):          # the real table is a pandas DataFrame
            rows = table.to_dict("records")
        else:                                  # the offline test double
            rows = list(table.rows)
    except Exception:  # noqa: BLE001 - any failure means the catalog cannot be read
        raise refuse("Catalog could not be read",
                     "The ANALYSES catalog in the project configuration could not be read.",
                     ["Log in again and retry.", "If it keeps failing, contact the Data Librarian."])
    return [{str(k).upper(): v for k, v in row.items()} for row in rows]


def _text(value: Any) -> str:
    """A table cell as text; empty cells (None or NaN) become ''."""
    if value is None or (isinstance(value, float) and value != value):
        return ""
    return str(value).strip()


def find_derived(hash_value: str, *, gateway, config_tables) -> DerivedReport:
    """
    Return every cataloged item whose inputs include *hash_value* (a content or image identity hash).

    Rows that cannot be checked (no address recorded, or ``analysis.json`` could
    not be downloaded or read) are listed in ``unchecked`` with a reason.
    """
    if not isinstance(hash_value, str) or not _HASH_RE.match(hash_value.strip()):
        raise refuse("Not a frame hash",
                     "The value given is not a frame hash (64 characters of 0-9 and a-f).",
                     ["Copy the sha256 or image_identity_hash of the frame from its download record."])
    wanted = hash_value.strip().lower()
    report = DerivedReport(hash=wanted)
    for row in _catalog_rows(config_tables):
        name = _text(row.get("NAME"))
        query_string = _text(row.get("QUERY_STRING"))
        resource_label = _text(row.get("RESOURCE_LABEL"))
        if not query_string or not resource_label:
            report.unchecked.append((name, "the catalog row does not record where the item lives "
                                           "(it was added before spec 018)"))
            continue
        tmp = Path(tempfile.mkdtemp(prefix="xnat_derived_"))
        try:
            try:
                paths = gateway.download_resource(query_string, resource_label, tmp)
                descriptor_path = next((Path(p) for p in paths if Path(p).name == DESCRIPTOR_NAME), None)
                if descriptor_path is None:
                    raise FileNotFoundError(DESCRIPTOR_NAME)
                descriptor = json.loads(descriptor_path.read_text(encoding="utf-8"))
                run = descriptor["run"]
                hashes = run.get("source_hashes") or []
            except Exception:  # noqa: BLE001 - one unreadable item must not stop the search
                report.unchecked.append((name, "its analysis.json could not be downloaded or read"))
                continue
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
        report.checked += 1
        matched_on = ""
        for entry in hashes:
            if not isinstance(entry, dict):
                continue
            if str(entry.get("sha256") or "").lower() == wanted:
                matched_on = "sha256"
                break
            if str(entry.get("image_identity_hash") or "").lower() == wanted:
                matched_on = "image_identity_hash"
                break
        if matched_on:
            report.items.append(DerivedItem(
                type_name=_text(row.get("TYPE_NAME")) or str(descriptor.get("type_name") or ""),
                label=str(run.get("label") or name),
                placement_used=_text(row.get("PLACEMENT_USED")) or str(run.get("placement_used") or ""),
                query_string=query_string, resource_label=resource_label, matched_on=matched_on))
    return report
