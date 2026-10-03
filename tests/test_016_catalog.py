"""Spec 016 US1: the ANALYSES catalog."""
import pytest

from src.services.analysis_intake import IntakeRefusal, PublishOutcome, record_in_catalog
from src.services.analysis_intake.catalog import CATALOG_COLUMNS
from tests.fakes.analysis_folders import FakeConfigTables, LostUpdateError, USERNAME


def _pair(label="knee_flexion_angle__v1"):
    desc = {"type_name": "knee_flexion_angle", "type_version": 1,
            "run": {"case_uid": "CASEA", "producer": USERNAME, "source_hashes": [{}, {}], "supersedes": None}}
    return PublishOutcome(label, "assessor", "/q", "KNEE_FLEXION_ANGLE"), desc


def test_table_created_once_with_columns():
    ct = FakeConfigTables()
    out, desc = _pair()
    assert record_in_catalog(out, desc, ct) is True
    out2, desc2 = _pair("knee_flexion_angle__v2")
    record_in_catalog(out2, desc2, ct)
    rows = ct.server_rows["ANALYSES"]
    assert len(rows) == 2
    for col in CATALOG_COLUMNS:
        assert col in rows[0]
    assert rows[0]["PRODUCER"] == USERNAME and rows[0]["SOURCE_HASH_COUNT"] == "2"


def test_retry_is_idempotent():
    ct = FakeConfigTables()
    out, desc = _pair()
    record_in_catalog(out, desc, ct)
    assert record_in_catalog(out, desc, ct) is False
    assert len(ct.server_rows["ANALYSES"]) == 1


def test_lost_update_pulls_and_retries_once():
    ct = FakeConfigTables(push_failures=[LostUpdateError("changed meanwhile")])
    out, desc = _pair()
    assert record_in_catalog(out, desc, ct) is True
    assert ct.pulls == 1 and ct.pushes == 2
    assert len(ct.server_rows["ANALYSES"]) == 1


def test_second_refusal_gives_next_step():
    ct = FakeConfigTables(push_failures=[LostUpdateError("a"), LostUpdateError("b")])
    out, desc = _pair()
    with pytest.raises(IntakeRefusal) as exc:
        record_in_catalog(out, desc, ct)
    assert exc.value.friendly.recourse


def test_unregistered_user_refused_softly():
    ct = FakeConfigTables(registered=False)
    out, desc = _pair()
    with pytest.raises(IntakeRefusal):
        record_in_catalog(out, desc, ct)


def test_default_columns_match_production_metatables():
    """Found live 2026-10-03: the production date column is CREATED_DATE_TIME, not DATE."""
    import re
    from pathlib import Path
    from src.services.analysis_intake.catalog import _DEFAULT_COLUMNS

    src = Path("src/utilities.py").read_text(encoding="utf-8")
    m = re.search(r"'default_meta_table_columns'\s*:\s*\[([^\]]+)\]", src)
    assert m, "production default columns not found"
    production = {c.strip().strip("'\"") for c in m.group(1).split(",")}
    assert _DEFAULT_COLUMNS == production
