"""
tests/regression_014/test_h8_zip_integrity.py — #33 H8 regression (spec 014).

H8 (still-open per the 2026-07-07 audit, ledger.md): app/logic/download.py's
assemble_zip() (a) silently `continue`s past a resource that enumerates zero
files with no signal to the caller, and (b) has no cleanup if the zip-write
loop raises partway through — leaving a truncated zip at zip_dest.

Baseline evidence (pre-fix, this branch before the H8 fix commit): both
tests below FAIL — see ledger.md for the recorded run.

NO network. FakeXNAT + tests/test_download_full_series.py's
SeededFullSeriesFakeXNAT fixtures reused. NO PHI.
"""
from __future__ import annotations

import sys
import zipfile
from pathlib import Path
from typing import Any, List

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
def empty_and_nonempty_scan_server() -> SeededFullSeriesFakeXNAT:
    """One scan with real files, one scan with zero files enumerated."""
    server = SeededFullSeriesFakeXNAT(
        project_name=PROJECT,
        subjects={
            SUBJECT: {
                EXPERIMENT: {
                    "date": "2026-01-01",
                    "scans": {
                        "0": {"scan_type": "0", "num_files": 2},
                        "1": {"scan_type": "1", "num_files": 0},
                    },
                },
            },
        },
    )
    res0 = server.get_scan_resource(SUBJECT, EXPERIMENT, "0", "SRC")
    server.seed_resource_files(res0, _make_fake_files(2, prefix="scan0"))
    res1 = server.get_scan_resource(SUBJECT, EXPERIMENT, "1", "SRC")
    server.seed_resource_files(res1, [])
    return server


def _experiment_rows(server: Any) -> List[dict]:
    rows = list_downloadable(server, PROJECT)
    if isinstance(rows, FriendlyError):
        pytest.fail(f"list_downloadable failed: {rows.message}")
    exp_rows = [r for r in rows if r.get("experiment") == EXPERIMENT]
    assert exp_rows
    return exp_rows


class TestH8EmptyResourceNotSilent:
    """#33 H8 (a): an empty-resource skip must be visible, not silent."""

    def test_empty_resource_recorded_as_warning_not_silently_dropped(
        self, empty_and_nonempty_scan_server: SeededFullSeriesFakeXNAT, tmp_path: Path
    ) -> None:
        zip_path = tmp_path / "surgery.zip"
        outcome: DownloadOutcome = assemble_zip(
            empty_and_nonempty_scan_server,
            PROJECT,
            _experiment_rows(empty_and_nonempty_scan_server),
            zip_path,
            scope="source",
        )
        # The non-empty scan's 2 files still get zipped.
        assert outcome.ok, f"assemble_zip failed: {outcome.friendly}"
        assert zip_path.exists()
        with zipfile.ZipFile(zip_path) as zf:
            assert len(zf.namelist()) == 2

        # Pre-fix: DownloadOutcome had no way to signal the omitted resource —
        # this assertion is what fails against the pre-fix code.
        assert outcome.warnings, (
            "Expected a warning recording that scan '1' enumerated zero files "
            "and was omitted from the zip — omissions must never be silent."
        )
        assert any("scan '1'" in w or "'1'" in w for w in outcome.warnings), outcome.warnings


class TestH8PartialZipCleanup:
    """#33 H8 (b): a mid-write error must not leave a truncated zip on disk."""

    def test_write_error_leaves_no_partial_zip(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
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

        zip_path = tmp_path / "surgery.zip"

        import zipfile as _zipfile_module

        original_write = _zipfile_module.ZipFile.write
        call_count = {"n": 0}

        def _flaky_write(self: Any, filename: Any, arcname: Any = None, *args: Any, **kwargs: Any) -> Any:
            call_count["n"] += 1
            if call_count["n"] == 2:
                raise OSError("simulated disk-full mid-write")
            return original_write(self, filename, arcname, *args, **kwargs)

        monkeypatch.setattr(_zipfile_module.ZipFile, "write", _flaky_write)

        outcome: DownloadOutcome = assemble_zip(
            server, PROJECT, _experiment_rows(server), zip_path, scope="source",
        )

        assert not outcome.ok
        assert outcome.friendly is not None

        # Pre-fix: zip_dest itself was opened directly and left truncated on
        # disk after the exception. Post-fix: nothing is left at zip_dest,
        # and no stray ".partial" temp file survives either.
        assert not zip_path.exists(), (
            "A truncated zip was left behind at zip_dest after a mid-write error."
        )
        leftover_partials = list(tmp_path.glob("*.partial"))
        assert not leftover_partials, f"Leftover partial zip file(s): {leftover_partials}"
