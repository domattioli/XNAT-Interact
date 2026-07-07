"""
tests/regression_014/test_m8_stale_count_race.py -- #33 M8 backfill (spec 014).

M8 (fixed-no-test per the 2026-07-07 audit, ledger.md): download_selection
used to compare enumerated files against the row's stale browse-time
`num_files` count. If a file was added to the resource between browse and
download, the live count would disagree with the stale row count, producing
a spurious "count mismatch" FriendlyError even though the download itself
would have succeeded fine.

The fix (present in this branch's history) prefers the *live* per-resource
count (`resource.num_files()`) over the stale row-supplied count, falling
back to the row value only when a live count is unavailable.

Backfill note (spec 014, User Story 4): the historical pre-fix commit is
not reachable in this clone's git history (same shallow-fetch boundary
noted in test_m7_stale_dir_contents.py / ledger.md). This test proves the
fix's current correctness only.

Offline only -- FakeXNAT, no PHI.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from app.logic.download import download_selection, DownloadOutcome
from tests.fakes.fake_xnat import FakeXNAT


def test_row_declares_stale_count_but_live_resource_has_more_files(tmp_path: Path) -> None:
    scan_qs = "/projects/PROJ/subjects/SUBJ/experiments/EXP/scans/0"
    fake = FakeXNAT()
    fake.create(scan_qs)
    sel = fake._selectables[scan_qs]
    resource = sel.resource("SRC")

    # A file was added to the resource AFTER the row was browsed -- the row
    # (browse-time snapshot) still says 2 files, but the live resource
    # actually has 3.
    fake.seed_resource_files(
        resource,
        [("a.dcm", b"\x01"), ("b.dcm", b"\x02"), ("c.dcm", b"\x03")],
    )

    row = {
        "subject": "SUBJ",
        "experiment": "EXP",
        "scan_type": "0",
        "num_files": 2,  # stale browse-time count
    }

    outcome: DownloadOutcome = download_selection(
        fake, "PROJ", [row], tmp_path, resource_label="SRC",
    )

    # Pre-fix (comparing against the stale row count of 2): a spurious
    # mismatch FriendlyError would fire even though nothing is actually
    # wrong. Post-fix: the live count (3) is authoritative, so all 3 files
    # download cleanly.
    assert outcome.ok, (
        f"Expected the live per-resource count to be authoritative, not the "
        f"stale browse-time row count; got failure: {outcome.friendly}"
    )
    assert len(outcome.files_written) == 3
