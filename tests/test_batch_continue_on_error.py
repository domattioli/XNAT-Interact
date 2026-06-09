"""
Offline tests for continue-on-error batch-upload semantics.

Tests cover:
  - N bad rows + M good rows → M succeed, N reported failed, no exception escapes
  - Batch is NOT aborted early (all rows processed)
  - BatchRunResult summary counts correct
  - Persistence: JSON written next to source file
  - Failed-only re-run: exactly the previously-failed indices attempted
  - `only_rows` filter: non-requested rows recorded as skipped
  - `rerun_failed` helper with empty-failure result returns empty BatchRunResult

No network, no XNAT server, no PHI.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, List
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from src.batch_upload import BatchRunResult, BatchUploadRepresentation, RowOutcome, rerun_failed


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_batch_repr(
    tmp_path: Path,
    n_good: int,
    n_bad: int,
) -> "BatchUploadRepresentation":
    """
    Build a BatchUploadRepresentation bypassing __init__ (no real config/xlsx
    needed).  Internal DataFrames are built directly so tests remain offline.

    Good rows have no errors.  Bad rows have a fabricated error in
    'Procedure Name'.
    """
    inst = BatchUploadRepresentation.__new__(BatchUploadRepresentation)

    # Minimal xlsx on disk (used for result_path naming only)
    xlsx_path = tmp_path / "test_batch.xlsx"
    xlsx_path.write_bytes(b"")  # placeholder — not parsed by upload_sessions
    inst._ffn = xlsx_path

    # Build _df: n_good + n_bad rows with the columns upload_sessions reads.
    # Column names use spaces (as read from xlsx); upload_sessions makes its own
    # \n-separated copy internally.
    n_rows = n_good + n_bad
    cols = [
        "Filer HawkID",
        "Operation Date",
        "Institution Name",
        "Procedure Type",
        "Procedure Name",
        "Performer HawkID-Task",
        "Quality",
        "Epic Start Time",
        "Epic End Time",
        "Full Path to Data",
        "Performing Surgeon HawkID",
        "Supervising Surgeon HawkID",
        "Assessor HawkID",
        "Skills Assessment Requested",
        "Was Radiology Contacted",
        "Radiology Contact Date",
        "Unusual Features",
        "Diagnostic Notes",
        "Additional Comments",
        "# of Participating Performing Surgeons",
    ]
    data: dict = {c: [""] * n_rows for c in cols}
    # Set Procedure Name for good rows; leave bad rows blank
    for i in range(n_rows):
        if i < n_good:
            data["Procedure Name"][i] = "ARTHROSCOPY_TEST"
        # bad rows stay "" — triggers the validation error path
    inst._df = pd.DataFrame(data)

    # Build _errors and _warnings DataFrames (same shape, default empty string
    # which is falsy — matches BatchUploadRepresentation._log_issue convention)
    inst._errors = pd.DataFrame(
        data=[[""] * len(cols) for _ in range(n_rows)],
        columns=cols,
    )
    inst._warnings = inst._errors.copy()

    # Inject errors into bad rows (indices n_good .. n_good+n_bad-1)
    for i in range(n_good, n_rows):
        inst._errors.at[i, "Procedure Name"] = [
            f"'Procedure Name' is blank or not registered (synthetic test row {i})."
        ]

    # Stub generate_summary — just needs to return a 5-tuple; called for side
    # effects (building rows_with_errors) which upload_sessions rebuilds itself
    # from _errors, so a no-op return is fine.
    inst.generate_summary = lambda write_to_file=False: (True, "", "", "", "")  # type: ignore[method-assign]

    return inst


def _fake_connection(verified: bool = True) -> MagicMock:
    """Minimal XNATConnection stand-in."""
    conn = MagicMock()
    conn.is_verified = verified
    return conn


def _fake_publish_fn(succeed_indices: "set[int]"):
    """
    Return a _publish_fn(row, row_index) that succeeds for row indices in
    succeed_indices and raises for all others.  Tracks calls.
    """
    called: List[int] = []

    def _fn(row: Any, row_index: int) -> None:
        called.append(row_index)
        if row_index not in succeed_indices:
            raise RuntimeError(f"Simulated failure for row {row_index}")

    _fn.called_list = called  # type: ignore[attr-defined]
    return _fn


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_workdir(tmp_path: Path) -> Path:
    return tmp_path


# ---------------------------------------------------------------------------
# Core continue-on-error tests
# ---------------------------------------------------------------------------

class TestContinueOnError:
    """N bad + M good rows → no exception, M succeed, N fail."""

    def test_no_exception_escapes(self, tmp_workdir: Path) -> None:
        inst = _make_batch_repr(tmp_workdir, n_good=3, n_bad=2)
        conn = _fake_connection()

        # _publish_fn never called for bad rows (they fail at validation);
        # always succeeds for good rows.
        def _ok_publish(row, row_index):
            pass

        result = inst.upload_sessions(
            config=MagicMock(),
            validated_login=MagicMock(),
            xnat_connection=conn,
            verbose=False,
            _publish_fn=_ok_publish,
        )
        assert isinstance(result, BatchRunResult)

    def test_good_rows_succeed(self, tmp_workdir: Path) -> None:
        inst = _make_batch_repr(tmp_workdir, n_good=4, n_bad=1)
        conn = _fake_connection()

        def _ok_publish(row, row_index):
            pass

        result = inst.upload_sessions(
            config=MagicMock(),
            validated_login=MagicMock(),
            xnat_connection=conn,
            verbose=False,
            _publish_fn=_ok_publish,
        )
        assert result.succeeded_count == 4

    def test_bad_rows_reported_failed(self, tmp_workdir: Path) -> None:
        inst = _make_batch_repr(tmp_workdir, n_good=3, n_bad=2)
        conn = _fake_connection()

        def _ok_publish(row, row_index):
            pass

        result = inst.upload_sessions(
            config=MagicMock(),
            validated_login=MagicMock(),
            xnat_connection=conn,
            verbose=False,
            _publish_fn=_ok_publish,
        )
        assert result.failed_count == 2

    def test_failure_reason_populated(self, tmp_workdir: Path) -> None:
        inst = _make_batch_repr(tmp_workdir, n_good=2, n_bad=3)
        conn = _fake_connection()

        def _ok_publish(row, row_index):
            pass

        result = inst.upload_sessions(
            config=MagicMock(),
            validated_login=MagicMock(),
            xnat_connection=conn,
            verbose=False,
            _publish_fn=_ok_publish,
        )
        for failure in result.failures:
            assert failure.reason, "Every failure must have a non-empty reason"

    def test_offending_field_populated_for_validation_errors(self, tmp_workdir: Path) -> None:
        inst = _make_batch_repr(tmp_workdir, n_good=2, n_bad=2)
        conn = _fake_connection()

        def _ok_publish(row, row_index):
            pass

        result = inst.upload_sessions(
            config=MagicMock(),
            validated_login=MagicMock(),
            xnat_connection=conn,
            verbose=False,
            _publish_fn=_ok_publish,
        )
        for failure in result.failures:
            assert failure.offending_field == "Procedure Name"

    def test_all_rows_processed_not_aborted_early(self, tmp_workdir: Path) -> None:
        """Total outcome count must equal total row count (no early abort)."""
        n_good, n_bad = 5, 3
        inst = _make_batch_repr(tmp_workdir, n_good=n_good, n_bad=n_bad)
        conn = _fake_connection()

        def _ok_publish(row, row_index):
            pass

        result = inst.upload_sessions(
            config=MagicMock(),
            validated_login=MagicMock(),
            xnat_connection=conn,
            verbose=False,
            _publish_fn=_ok_publish,
        )
        assert len(result.outcomes) == n_good + n_bad

    def test_publish_exception_recorded_as_failure_not_raised(self, tmp_workdir: Path) -> None:
        """
        When _publish_fn raises on a good row, that row is recorded as failed
        and the batch continues — no exception escapes.
        """
        inst = _make_batch_repr(tmp_workdir, n_good=3, n_bad=0)
        conn = _fake_connection()

        call_log: List[int] = []

        def _failing_publish(row, row_index):
            call_log.append(1)
            raise RuntimeError("simulated upload failure")

        result = inst.upload_sessions(
            config=MagicMock(),
            validated_login=MagicMock(),
            xnat_connection=conn,
            verbose=False,
            _publish_fn=_failing_publish,
        )
        assert result.failed_count == 3
        assert result.succeeded_count == 0
        assert len(call_log) == 3, "publish_fn must be called for every good row even after failures"

    def test_mixed_publish_failures(self, tmp_workdir: Path) -> None:
        """
        Good rows where _publish_fn raises are failed; rows that don't raise
        succeed.  Bad (validation-error) rows are failed regardless.
        """
        n_good, n_bad = 4, 2
        inst = _make_batch_repr(tmp_workdir, n_good=n_good, n_bad=n_bad)
        conn = _fake_connection()

        # Fail on row index 1 (second good row)
        call_count = [0]

        def _selective_publish(row, row_index):
            call_count[0] += 1
            if call_count[0] == 2:  # second good-row call
                raise ValueError("deliberate row-1 failure")

        result = inst.upload_sessions(
            config=MagicMock(),
            validated_login=MagicMock(),
            xnat_connection=conn,
            verbose=False,
            _publish_fn=_selective_publish,
        )
        # 4 good rows: 1 publish failure → 3 succeed, 1 fail from publish
        # 2 bad rows: 2 validation failures
        assert result.succeeded_count == 3
        assert result.failed_count == 3  # 1 publish + 2 validation

    def test_unverified_connection_raises(self, tmp_workdir: Path) -> None:
        inst = _make_batch_repr(tmp_workdir, n_good=1, n_bad=0)
        conn = _fake_connection(verified=False)

        with pytest.raises(ConnectionError):
            inst.upload_sessions(
                config=MagicMock(),
                validated_login=MagicMock(),
                xnat_connection=conn,
                verbose=False,
            )


# ---------------------------------------------------------------------------
# Persistence tests
# ---------------------------------------------------------------------------

class TestPersistence:
    def test_json_written_next_to_source(self, tmp_workdir: Path) -> None:
        inst = _make_batch_repr(tmp_workdir, n_good=2, n_bad=1)
        conn = _fake_connection()

        def _ok_publish(row, row_index):
            pass

        result = inst.upload_sessions(
            config=MagicMock(),
            validated_login=MagicMock(),
            xnat_connection=conn,
            verbose=False,
            _publish_fn=_ok_publish,
        )
        assert result.result_path is not None
        assert result.result_path.exists()

    def test_json_content_valid(self, tmp_workdir: Path) -> None:
        inst = _make_batch_repr(tmp_workdir, n_good=2, n_bad=2)
        conn = _fake_connection()

        def _ok_publish(row, row_index):
            pass

        result = inst.upload_sessions(
            config=MagicMock(),
            validated_login=MagicMock(),
            xnat_connection=conn,
            verbose=False,
            _publish_fn=_ok_publish,
        )
        data = json.loads(result.result_path.read_text(encoding="utf-8"))
        assert data["succeeded"] == 2
        assert data["failed"] == 2
        assert len(data["outcomes"]) == 4

    def test_load_round_trips(self, tmp_workdir: Path) -> None:
        inst = _make_batch_repr(tmp_workdir, n_good=1, n_bad=1)
        conn = _fake_connection()

        def _ok_publish(row, row_index):
            pass

        result = inst.upload_sessions(
            config=MagicMock(),
            validated_login=MagicMock(),
            xnat_connection=conn,
            verbose=False,
            _publish_fn=_ok_publish,
        )
        loaded = BatchRunResult.load(result.result_path)
        assert loaded.succeeded_count == result.succeeded_count
        assert loaded.failed_count == result.failed_count
        assert loaded.failed_row_indices == result.failed_row_indices

    def test_persist_explicit_path(self, tmp_workdir: Path) -> None:
        result = BatchRunResult(outcomes=[
            RowOutcome(row_index=0, status="succeeded"),
            RowOutcome(row_index=1, status="failed", reason="test"),
        ])
        out = tmp_workdir / "custom-result.json"
        written = result.persist(path=out)
        assert written == out
        assert out.exists()


# ---------------------------------------------------------------------------
# Re-run (only_rows / rerun_failed) tests
# ---------------------------------------------------------------------------

class TestRerun:
    def test_only_rows_skips_non_requested(self, tmp_workdir: Path) -> None:
        inst = _make_batch_repr(tmp_workdir, n_good=4, n_bad=0)
        conn = _fake_connection()

        def _ok_publish(row, row_index):
            pass

        result = inst.upload_sessions(
            config=MagicMock(),
            validated_login=MagicMock(),
            xnat_connection=conn,
            verbose=False,
            _publish_fn=_ok_publish,
            only_rows=[0, 2],
        )
        skipped = [o for o in result.outcomes if o.status == "skipped"]
        assert len(skipped) == 2  # rows 1 and 3 skipped

    def test_only_rows_attempts_requested(self, tmp_workdir: Path) -> None:
        inst = _make_batch_repr(tmp_workdir, n_good=4, n_bad=0)
        conn = _fake_connection()

        def _ok_publish(row, row_index):
            pass

        result = inst.upload_sessions(
            config=MagicMock(),
            validated_login=MagicMock(),
            xnat_connection=conn,
            verbose=False,
            _publish_fn=_ok_publish,
            only_rows=[1, 3],
        )
        succeeded = [o for o in result.outcomes if o.status == "succeeded"]
        assert len(succeeded) == 2

    def test_rerun_failed_targets_exactly_failed_rows(self, tmp_workdir: Path) -> None:
        n_good, n_bad = 3, 2
        inst = _make_batch_repr(tmp_workdir, n_good=n_good, n_bad=n_bad)
        conn = _fake_connection()

        attempted: List[int] = []

        def _ok_publish(row, row_index):
            pass

        # First run: n_bad rows fail validation
        first = inst.upload_sessions(
            config=MagicMock(),
            validated_login=MagicMock(),
            xnat_connection=conn,
            verbose=False,
            _publish_fn=_ok_publish,
        )
        assert first.failed_count == n_bad
        failed_indices = first.failed_row_indices

        # Second run via rerun_failed
        second = rerun_failed(
            first,
            inst,
            config=MagicMock(),
            validated_login=MagicMock(),
            xnat_connection=conn,
            verbose=False,
            _publish_fn=_ok_publish,
        )
        # The failed rows (validation errors) are still validation errors on re-run
        attempted_indices = [o.row_index for o in second.outcomes if o.status != "skipped"]
        assert set(attempted_indices) == set(failed_indices)

    def test_rerun_with_no_failures_returns_empty(self, tmp_workdir: Path) -> None:
        inst = _make_batch_repr(tmp_workdir, n_good=2, n_bad=0)
        conn = _fake_connection()

        def _ok_publish(row, row_index):
            pass

        first = inst.upload_sessions(
            config=MagicMock(),
            validated_login=MagicMock(),
            xnat_connection=conn,
            verbose=False,
            _publish_fn=_ok_publish,
        )
        assert first.failed_count == 0

        second = rerun_failed(
            first,
            inst,
            config=MagicMock(),
            validated_login=MagicMock(),
            xnat_connection=conn,
            verbose=False,
            _publish_fn=_ok_publish,
        )
        assert len(second.outcomes) == 0

    def test_rerun_skips_originally_good_rows(self, tmp_workdir: Path) -> None:
        n_good, n_bad = 3, 2
        inst = _make_batch_repr(tmp_workdir, n_good=n_good, n_bad=n_bad)
        conn = _fake_connection()

        def _ok_publish(row, row_index):
            pass

        first = inst.upload_sessions(
            config=MagicMock(),
            validated_login=MagicMock(),
            xnat_connection=conn,
            verbose=False,
            _publish_fn=_ok_publish,
        )
        second = rerun_failed(
            first,
            inst,
            config=MagicMock(),
            validated_login=MagicMock(),
            xnat_connection=conn,
            verbose=False,
            _publish_fn=_ok_publish,
        )
        skipped_in_rerun = [o for o in second.outcomes if o.status == "skipped"]
        assert len(skipped_in_rerun) == n_good


# ---------------------------------------------------------------------------
# BatchRunResult unit tests (no upload_sessions call needed)
# ---------------------------------------------------------------------------

class TestBatchRunResult:
    def _make_result(self) -> BatchRunResult:
        return BatchRunResult(outcomes=[
            RowOutcome(row_index=0, status="succeeded"),
            RowOutcome(row_index=1, status="failed", reason="bad field", offending_field="Filer HawkID"),
            RowOutcome(row_index=2, status="succeeded"),
            RowOutcome(row_index=3, status="skipped", reason="not in only_rows filter"),
            RowOutcome(row_index=4, status="failed", reason="upload error"),
        ])

    def test_succeeded_count(self):
        r = self._make_result()
        assert r.succeeded_count == 2

    def test_failed_count(self):
        r = self._make_result()
        assert r.failed_count == 2

    def test_skipped_count(self):
        r = self._make_result()
        assert r.skipped_count == 1

    def test_failures_property(self):
        r = self._make_result()
        assert len(r.failures) == 2
        assert all(o.status == "failed" for o in r.failures)

    def test_failed_row_indices(self):
        r = self._make_result()
        assert set(r.failed_row_indices) == {1, 4}

    def test_summary_contains_counts(self):
        r = self._make_result()
        s = r.summary()
        assert "succeeded" in s
        assert "failed" in s
        assert "skipped" in s

    def test_summary_lists_failure_reasons(self):
        r = self._make_result()
        s = r.summary()
        assert "bad field" in s
        assert "upload error" in s


# ---------------------------------------------------------------------------
# Synthetic data generator test
# ---------------------------------------------------------------------------

class TestMixedValidityGenerator:
    def test_file_written(self, tmp_workdir: Path) -> None:
        from tests.synthetic_data import make_mixed_validity_batch_xlsx
        p = make_mixed_validity_batch_xlsx(tmp_workdir / "mixed.xlsx", n_good=3, n_bad=2)
        assert p.exists()

    def test_row_count(self, tmp_workdir: Path) -> None:
        from tests.synthetic_data import make_mixed_validity_batch_xlsx
        p = make_mixed_validity_batch_xlsx(tmp_workdir / "mixed.xlsx", n_good=4, n_bad=3)
        df = pd.read_excel(p)
        assert len(df) == 7

    def test_bad_rows_have_blank_procedure_name(self, tmp_workdir: Path) -> None:
        from tests.synthetic_data import make_mixed_validity_batch_xlsx
        p = make_mixed_validity_batch_xlsx(tmp_workdir / "mixed.xlsx", n_good=2, n_bad=3)
        df = pd.read_excel(p).fillna("")
        blank_count = (df["Procedure Name"].astype(str).str.strip() == "").sum()
        assert blank_count == 3

    def test_good_rows_have_procedure_name(self, tmp_workdir: Path) -> None:
        from tests.synthetic_data import make_mixed_validity_batch_xlsx
        p = make_mixed_validity_batch_xlsx(tmp_workdir / "mixed.xlsx", n_good=3, n_bad=2)
        df = pd.read_excel(p).fillna("")
        non_blank = (df["Procedure Name"].astype(str).str.strip() != "").sum()
        assert non_blank == 3
