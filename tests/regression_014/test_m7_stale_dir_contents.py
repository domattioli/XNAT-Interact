"""
tests/regression_014/test_m7_stale_dir_contents.py -- #33 M7 backfill (spec 014).

M7 (fixed-no-test per the 2026-07-07 audit, ledger.md): download_resource
used to return dest_dir.iterdir() directly, which included any pre-existing
unrelated files already sitting in dest_dir -- not just the newly downloaded
ones. The fix (present in this branch's history, commit f365524 and later)
isolates downloads into a dest_dir / "_xnat_download" sub-directory.

Backfill note (spec 014, User Story 4): the historical pre-fix commit for
this fix is the parent of f365524, which is NOT reachable in this clone's
git history (shallow-fetch boundary -- verified via `git cat-file -e`).
This test therefore proves the fix's CURRENT correctness only; it cannot
be run against a genuine pre-fix baseline. Recorded honestly in ledger.md
rather than claiming a fail-then-pass proof that isn't possible here.

Offline only -- FakeXNAT, no PHI.

Existing coverage note: tests/test_xnat_gateway.py already has an
M7-labeled test for the zip-vs-individual-files behavior. This test targets
the *other* half of M7: stale pre-existing directory contents must not be
included in the result.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from tests.fakes.fake_xnat import FakeXNAT


def test_stale_preexisting_file_in_dest_dir_not_included_in_result(tmp_path: Path) -> None:
    scan_qs = "/projects/P/subjects/S/experiments/E/scans/0"
    fake = FakeXNAT()
    fake.create(scan_qs)
    sel = fake._selectables[scan_qs]
    resource = sel.resource("SRC")
    fake.seed_resource_files(resource, [("real.dcm", b"\x01\x02")])

    # A stale, unrelated file already sitting in dest_dir before download --
    # e.g. left over from a prior, unrelated operation.
    stale_file = tmp_path / "unrelated_leftover.txt"
    stale_file.write_text("not part of this download")

    written = fake.download_resource(scan_qs, "SRC", tmp_path)

    written_names = {p.name for p in written}
    assert "unrelated_leftover.txt" not in written_names, (
        f"Stale pre-existing file was included in the download result: {written}"
    )
    assert "real.dcm" in written_names
    # The stale file must still exist untouched on disk -- just excluded
    # from the *result*, not deleted.
    assert stale_file.exists()
