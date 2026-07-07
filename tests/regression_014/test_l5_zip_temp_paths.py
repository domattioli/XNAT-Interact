"""
tests/regression_014/test_l5_zip_temp_paths.py -- #33 L5 regression (spec 014).

L5 (audit-flagged 'unclear', investigation confirmed real and still-open):
assemble_zip() staged downloaded files into a tempfile.TemporaryDirectory(),
then returned those staging paths as DownloadOutcome.files_written. The
TemporaryDirectory's cleanup runs as the call stack unwinds through any
`return` inside the `with` block -- so by the time the caller inspects
files_written, every path in it points into an already-deleted directory.

Baseline evidence (pre-fix, this branch before the L5 fix commit): the test
below FAILS (files_written paths do not exist) -- see ledger.md for the
recorded run.

Offline only. FakeXNAT, no PHI.
"""
from __future__ import annotations

import sys
import zipfile
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.logic.download import assemble_zip, list_downloadable, DownloadOutcome
from src.services.errors import FriendlyError
from tests.test_download_full_series import (
    SeededFullSeriesFakeXNAT,
    _make_fake_files,
    PROJECT,
    SUBJECT,
    EXPERIMENT,
)


@pytest.fixture()
def single_scan_server() -> SeededFullSeriesFakeXNAT:
    server = SeededFullSeriesFakeXNAT(
        project_name=PROJECT,
        subjects={
            SUBJECT: {
                EXPERIMENT: {
                    "date": "2026-01-01",
                    "scans": {"0": {"scan_type": "0", "num_files": 3}},
                },
            },
        },
    )
    res = server.get_scan_resource(SUBJECT, EXPERIMENT, "0", "SRC")
    server.seed_resource_files(res, _make_fake_files(3, prefix="scan0"))
    return server


def test_files_written_paths_all_exist_after_return(
    single_scan_server: SeededFullSeriesFakeXNAT, tmp_path: Path
) -> None:
    rows = list_downloadable(single_scan_server, PROJECT)
    if isinstance(rows, FriendlyError):
        pytest.fail(f"list_downloadable failed: {rows.message}")
    exp_rows = [r for r in rows if r.get("experiment") == EXPERIMENT]

    zip_path = tmp_path / "surgery.zip"
    outcome: DownloadOutcome = assemble_zip(
        single_scan_server, PROJECT, exp_rows, zip_path, scope="source",
    )
    assert outcome.ok, f"assemble_zip failed: {outcome.friendly}"

    # Pre-fix: files_written held tmp_path-staged Paths that no longer exist
    # once the TemporaryDirectory's __exit__ has run -- this loop is what
    # fails against the pre-fix code.
    for fp in outcome.files_written:
        assert fp.exists(), (
            f"DownloadOutcome.files_written contains a path that does not "
            f"exist on disk: {fp} -- it pointed into a deleted temp dir."
        )

    # The one artifact that genuinely outlives the call is the zip itself.
    assert zip_path in outcome.files_written
    with zipfile.ZipFile(zip_path) as zf:
        assert len(zf.namelist()) == 3
