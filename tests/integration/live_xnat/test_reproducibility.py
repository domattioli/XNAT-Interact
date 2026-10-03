"""
SC-008: two completed runs of a case produce the same outcome summary, apart
from timestamps. Reads the two newest ``outcomes/<CASE>-<timestamp>.json``
files per case; skips until two runs exist.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.integration.live_xnat.cases import CASE_NAMES

pytestmark = [pytest.mark.requires_server, pytest.mark.slow]

OUTCOMES = Path(__file__).parent / "outcomes"


def _strip(summary: dict) -> dict:
    out = {k: v for k, v in summary.items() if k != "run_timestamp"}
    out["phases"] = [{k: v for k, v in p.items() if k != "timestamp"} for p in summary.get("phases", [])]
    return out


@pytest.mark.parametrize("case_name", CASE_NAMES)
def test_two_newest_runs_match(case_name):
    files = sorted(OUTCOMES.glob(f"{case_name}-*.json"))
    if len(files) < 2:
        pytest.skip("needs two completed runs")
    older, newer = (json.loads(p.read_text()) for p in files[-2:])
    assert _strip(older) == _strip(newer), f"{files[-2].name} and {files[-1].name} differ"
