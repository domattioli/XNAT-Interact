"""
Offline tests for app/logic/batch (Phase 2 US4).

Covers:
  - load_batch parses a valid xlsx and returns a DataFrame
  - load_batch returns FriendlyError on a bad/empty file, never raises
  - validate_batch flags bad rows pre-upload (blank Procedure Name)
  - validate_batch marks good rows as ok
  - run_batch with injected publish_fn: good rows succeed, bad rows reported,
    batch NOT aborted
  - run_batch: succeeded + failed counts match expectations
  - rerun_failed_rows attempts only the failed indices
  - rerun_failed_rows with zero failures returns empty BatchRunResult

NO streamlit imports.  No network.  No PHI.

NOTE on synthetic data
----------------------
make_mixed_validity_batch_xlsx produces rows with these columns:
  Filer HawkID, Operation Date, Institution Name, Procedure Type,
  Procedure Name, Performer HawkID-Task, Quality.

Required-field checks in validate_batch also test Epic Start Time and
Full Path to Data — which are absent from the synthetic xlsx. So:
  - _make_full_df() builds DataFrames with ALL required fields (for
    validate_batch / run_batch correctness tests).
  - make_mixed_validity_batch_xlsx is used for load_batch / file-I/O tests only.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pandas as pd

from app.logic.batch import (
    RowValidation,
    load_batch,
    rerun_failed_rows,
    run_batch,
    validate_batch,
)
from src.batch_upload import BatchRunResult
from src.services.errors import FriendlyError
from tests.synthetic_data import make_mixed_validity_batch_xlsx


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fake_connection(verified: bool = True) -> MagicMock:
    conn = MagicMock()
    conn.is_verified = verified
    return conn


def _publish_fn_always_ok(row: Any, row_index: int) -> None:
    """Publish fn that always succeeds."""
    pass


def _publish_fn_always_fail(row: Any, row_index: int) -> None:
    raise RuntimeError(f"Simulated failure row {row_index}")


def _make_full_df(n_good: int, n_bad: int) -> pd.DataFrame:
    """
    Build a DataFrame with ALL required columns so validate_batch can
    distinguish good (all fields filled) from bad (blank Procedure Name).

    Good rows:  all required fields filled.
    Bad rows:   Procedure Name blank — triggers the required-column check.
    """
    _FAKE_PATH = "/tmp"  # /tmp always exists — satisfies the path-exists check
    good_row = {
        "Filer HawkID": "TESTUSER",
        "Operation Date": "2024-01-01",
        "Institution Name": "UNIVERSITY_OF_IOWA",
        "Procedure Type": "ARTHROSCOPY",
        "Procedure Name": "1A_KNEE_ARTHROSCOPY",
        "Performer HawkID-Task": "{testuser: lead}",
        "Quality": "usable",
        "Epic Start Time": "09:30:00",
        "Epic End Time": "",
        "Full Path to Data": _FAKE_PATH,
        "Performing Surgeon HawkID": "testuser",
        "Supervising Surgeon HawkID": "",
        "Assessor HawkID": "",
        "Skills Assessment Requested": "",
        "Was Radiology Contacted": "",
        "Radiology Contact Date": "",
        "Unusual Features": "",
        "Diagnostic Notes": "",
        "Additional Comments": "",
        "# of Participating Performing Surgeons": "1",
    }
    bad_row = dict(good_row)
    bad_row["Procedure Name"] = ""   # intentionally blank

    rows = []
    g, b = 0, 0
    for i in range(n_good + n_bad):
        if g < n_good and (b >= n_bad or i % 2 == 0):
            rows.append(dict(good_row))
            g += 1
        else:
            rows.append(dict(bad_row))
            b += 1

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# load_batch tests
# ---------------------------------------------------------------------------

class TestLoadBatch:
    def test_parses_valid_xlsx(self, tmp_path: Path) -> None:
        p = make_mixed_validity_batch_xlsx(tmp_path / "batch.xlsx", n_good=3, n_bad=2)
        df, err = load_batch(p)
        assert err is None
        assert df is not None
        assert isinstance(df, pd.DataFrame)

    def test_returns_correct_row_count(self, tmp_path: Path) -> None:
        p = make_mixed_validity_batch_xlsx(tmp_path / "batch.xlsx", n_good=4, n_bad=3)
        df, err = load_batch(p)
        assert err is None
        assert len(df) == 7

    def test_columns_present(self, tmp_path: Path) -> None:
        p = make_mixed_validity_batch_xlsx(tmp_path / "batch.xlsx", n_good=2, n_bad=1)
        df, err = load_batch(p)
        assert err is None
        assert "Procedure Name" in df.columns
        assert "Filer HawkID" in df.columns

    def test_bad_file_returns_friendly_error(self, tmp_path: Path) -> None:
        bad = tmp_path / "notanxlsx.xlsx"
        bad.write_bytes(b"this is not a valid xlsx file at all")
        df, err = load_batch(bad)
        assert df is None
        assert err is not None
        assert isinstance(err, FriendlyError)

    def test_bad_file_does_not_raise(self, tmp_path: Path) -> None:
        bad = tmp_path / "corrupt.xlsx"
        bad.write_bytes(b"\x00\x01\x02\x03 garbage")
        # Must not raise
        df, err = load_batch(bad)
        assert df is None

    def test_nonexistent_file_returns_friendly_error(self, tmp_path: Path) -> None:
        missing = tmp_path / "no_such_file.xlsx"
        df, err = load_batch(missing)
        assert df is None
        assert isinstance(err, FriendlyError)

    def test_friendly_error_has_recourse(self, tmp_path: Path) -> None:
        bad = tmp_path / "bad.xlsx"
        bad.write_bytes(b"garbage")
        _, err = load_batch(bad)
        assert err is not None
        assert len(err.recourse) > 0


# ---------------------------------------------------------------------------
# validate_batch tests
# (use _make_full_df so all required columns are present)
# ---------------------------------------------------------------------------

class TestValidateBatch:
    def test_returns_one_result_per_row(self) -> None:
        df = _make_full_df(n_good=3, n_bad=2)
        results = validate_batch(df)
        assert len(results) == 5

    def test_bad_rows_flagged(self) -> None:
        """Rows with blank Procedure Name must have ok=False."""
        df = _make_full_df(n_good=3, n_bad=2)
        results = validate_batch(df)
        bad = [r for r in results if not r.ok]
        assert len(bad) == 2

    def test_good_rows_ok(self) -> None:
        """Rows with all required fields present must have ok=True."""
        df = _make_full_df(n_good=3, n_bad=2)
        results = validate_batch(df)
        good_indices = set(df[df["Procedure Name"].astype(str).str.strip() != ""].index)
        good_results = [r for r in results if r.row_index in good_indices]
        assert all(r.ok for r in good_results)

    def test_bad_rows_have_problems(self) -> None:
        df = _make_full_df(n_good=2, n_bad=3)
        results = validate_batch(df)
        for r in results:
            if not r.ok:
                assert len(r.problems) > 0

    def test_row_validation_type(self) -> None:
        df = _make_full_df(n_good=1, n_bad=1)
        results = validate_batch(df)
        assert all(isinstance(r, RowValidation) for r in results)

    def test_all_good_batch_all_ok(self) -> None:
        """All-good batch: every row validates ok."""
        df = _make_full_df(n_good=4, n_bad=0)
        results = validate_batch(df)
        assert all(r.ok for r in results)

    def test_row_index_sequential(self) -> None:
        df = _make_full_df(n_good=2, n_bad=2)
        results = validate_batch(df)
        indices = [r.row_index for r in results]
        assert indices == sorted(indices)

    def test_validate_batch_uses_xlsx_from_load_batch(self, tmp_path: Path) -> None:
        """Smoke: validate_batch works on a DataFrame from load_batch."""
        p = make_mixed_validity_batch_xlsx(tmp_path / "batch.xlsx", n_good=2, n_bad=2)
        df, err = load_batch(p)
        assert err is None
        results = validate_batch(df)
        assert len(results) == 4
        # Bad rows (blank Procedure Name) flagged
        proc_bad = [
            r for r in results
            if not r.ok and any("Procedure Name" in prob for prob in r.problems)
        ]
        assert len(proc_bad) == 2


# ---------------------------------------------------------------------------
# run_batch tests
# (use _make_full_df so required-field checks match good/bad rows correctly)
# ---------------------------------------------------------------------------

class TestRunBatch:
    def test_returns_batch_run_result(self) -> None:
        df = _make_full_df(n_good=2, n_bad=1)
        conn = _fake_connection()
        result = run_batch(df, conn, publish_fn=_publish_fn_always_ok)
        assert isinstance(result, BatchRunResult)

    def test_good_rows_succeed(self) -> None:
        """Good rows must succeed when publish_fn succeeds."""
        df = _make_full_df(n_good=3, n_bad=2)
        conn = _fake_connection()
        result = run_batch(df, conn, publish_fn=_publish_fn_always_ok)
        assert result.succeeded_count == 3

    def test_bad_rows_reported_failed(self) -> None:
        """Rows with blank Procedure Name must be recorded as failed, not silently skipped."""
        df = _make_full_df(n_good=3, n_bad=2)
        conn = _fake_connection()
        result = run_batch(df, conn, publish_fn=_publish_fn_always_ok)
        assert result.failed_count == 2

    def test_batch_not_aborted_all_rows_covered(self) -> None:
        """Total outcome count must equal total row count (no early abort)."""
        n_good, n_bad = 4, 3
        df = _make_full_df(n_good=n_good, n_bad=n_bad)
        conn = _fake_connection()
        result = run_batch(df, conn, publish_fn=_publish_fn_always_ok)
        assert len(result.outcomes) == n_good + n_bad

    def test_no_exception_escapes(self) -> None:
        """run_batch must not raise even when publish_fn raises on every row."""
        df = _make_full_df(n_good=3, n_bad=0)
        conn = _fake_connection()
        # Must not raise
        result = run_batch(df, conn, publish_fn=_publish_fn_always_fail)
        assert isinstance(result, BatchRunResult)

    def test_publish_exception_recorded_as_failure(self) -> None:
        """Publish-fn failures recorded as failed; batch continues to all rows."""
        df = _make_full_df(n_good=3, n_bad=0)
        conn = _fake_connection()
        result = run_batch(df, conn, publish_fn=_publish_fn_always_fail)
        assert result.failed_count == 3
        assert result.succeeded_count == 0

    def test_only_rows_filter_skips_others(self) -> None:
        """only_rows=[0] → rows 1..n skipped."""
        df = _make_full_df(n_good=4, n_bad=0)
        conn = _fake_connection()
        result = run_batch(df, conn, publish_fn=_publish_fn_always_ok, only_rows=[0])
        skipped = [o for o in result.outcomes if o.status == "skipped"]
        assert len(skipped) == 3

    def test_failure_reason_non_empty(self) -> None:
        df = _make_full_df(n_good=1, n_bad=2)
        conn = _fake_connection()
        result = run_batch(df, conn, publish_fn=_publish_fn_always_ok)
        for f in result.failures:
            assert f.reason, f"Failure for row {f.row_index} has empty reason"


# ---------------------------------------------------------------------------
# rerun_failed_rows tests
# (use _make_full_df so required-field checks match good/bad rows correctly)
# ---------------------------------------------------------------------------

class TestRerunFailedRows:
    def test_attempts_only_failed_indices(self) -> None:
        n_good, n_bad = 3, 2
        df = _make_full_df(n_good=n_good, n_bad=n_bad)
        conn = _fake_connection()

        first = run_batch(df, conn, publish_fn=_publish_fn_always_ok)
        failed_indices = set(first.failed_row_indices)
        assert len(failed_indices) == n_bad

        second = rerun_failed_rows(first, df, conn, publish_fn=_publish_fn_always_ok)
        # non-skipped outcomes are exactly the originally-failed rows
        attempted = {o.row_index for o in second.outcomes if o.status != "skipped"}
        assert attempted == failed_indices

    def test_skips_originally_good_rows(self) -> None:
        n_good, n_bad = 3, 2
        df = _make_full_df(n_good=n_good, n_bad=n_bad)
        conn = _fake_connection()

        first = run_batch(df, conn, publish_fn=_publish_fn_always_ok)
        second = rerun_failed_rows(first, df, conn, publish_fn=_publish_fn_always_ok)

        skipped = [o for o in second.outcomes if o.status == "skipped"]
        assert len(skipped) == n_good

    def test_empty_failures_returns_empty_result(self) -> None:
        """When first run has zero failures, rerun returns empty BatchRunResult."""
        df = _make_full_df(n_good=3, n_bad=0)
        conn = _fake_connection()

        first = run_batch(df, conn, publish_fn=_publish_fn_always_ok)
        assert first.failed_count == 0

        second = rerun_failed_rows(first, df, conn, publish_fn=_publish_fn_always_ok)
        assert len(second.outcomes) == 0

    def test_returns_batch_run_result(self) -> None:
        df = _make_full_df(n_good=2, n_bad=2)
        conn = _fake_connection()
        first = run_batch(df, conn, publish_fn=_publish_fn_always_ok)
        second = rerun_failed_rows(first, df, conn, publish_fn=_publish_fn_always_ok)
        assert isinstance(second, BatchRunResult)

    def test_does_not_raise(self) -> None:
        """rerun_failed_rows must not raise under any circumstances."""
        df = _make_full_df(n_good=1, n_bad=1)
        conn = _fake_connection()
        first = run_batch(df, conn, publish_fn=_publish_fn_always_ok)
        # Must not raise even with a failing publish_fn on rerun
        second = rerun_failed_rows(first, df, conn, publish_fn=_publish_fn_always_fail)
        assert isinstance(second, BatchRunResult)
