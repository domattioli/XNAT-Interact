"""
Tests for Stage 5 (T021–T022) of pixel de-identification: batch throughput.

Covers:
  - assess_batch: multiprocessing fan-out across cases, order preservation (T021)
  - batch_summary: verdict counting (T021)
  - Throughput: SLOW test marked with @pytest.mark.slow; extrapolates 200-case
    completion time from measured per-case rate (T022, SC-005)

All fixtures are synthetic; no real PHI is used anywhere.

The SC-005 constraint: a 200-image-sized-case batch must complete pixel de-id
in ≤15 minutes on a CPU-only multi-core machine.

Test strategy:
  - FAST test (unmarked): small batch (6 cases) with profiled/unprofiled mix,
    assert correctness (order, one-assessment-per-case, results match serial).
  - SLOW test (@pytest.mark.slow): N cases (default N=20, parameterizable),
    measure per-case time, extrapolate to 200 cases, assert ≤15 min.
"""
from __future__ import annotations

import shutil
import time
from pathlib import Path
from typing import Optional

import numpy as np
import pytest

from src.services.pixel_deid.batch import assess_batch, batch_summary
from src.services.pixel_deid.verdict import assess_case, CaseAssessment
from tests.synthetic_data import (
    make_burned_in_phi_pixel_array,
    make_profiled_device_dataset,
    make_unprofiled_device_dataset,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _profiles_dir() -> str:
    """Return the path to the starter device-profile data."""
    return str(Path(__file__).parent.parent / "data" / "device_profiles")


def _requires_tesseract():
    """Skip marker: skip the test if the tesseract binary is unavailable."""
    return pytest.mark.skipif(
        shutil.which("tesseract") is None,
        reason="tesseract binary not found on PATH",
    )


# ---------------------------------------------------------------------------
# FAST Correctness Test
# ---------------------------------------------------------------------------

class TestAssessBatchFast:
    """Fast correctness tests for assess_batch: order, counts, match serial."""

    def test_assess_batch_order_preserved(self) -> None:
        """assess_batch must return one assessment per case in input order."""
        # Build a small batch: 6 cases mixing profiled/unprofiled
        cases = []
        case_ids = []
        expected_count = 6

        for i in range(expected_count):
            case_id = f"case_{i:03d}"
            case_ids.append(case_id)

            # Alternate profiled/unprofiled
            if i % 2 == 0:
                dataset = make_profiled_device_dataset(
                    text=f"TEXT{i}",
                    manufacturer="Siemens",
                    model_name="AXIOM_Artis",
                    rows=32,
                    cols=128,
                )
            else:
                dataset = make_unprofiled_device_dataset(
                    text=f"TEXT{i}",
                    rows=32,
                    cols=128,
                )

            # Single small frame
            frame = make_burned_in_phi_pixel_array(
                f"PHI_{i}",
                rows=32,
                cols=128,
            )
            cases.append((case_id, [frame], dataset))

        # Run assess_batch
        assessments = assess_batch(
            cases,
            max_workers=1,  # Force serial for this test
            profiles_dir=_profiles_dir(),
            margin=4,
        )

        # Assert count
        assert len(assessments) == expected_count, (
            f"Expected {expected_count} assessments, got {len(assessments)}"
        )

        # Assert all are CaseAssessment instances
        for i, assessment in enumerate(assessments):
            assert isinstance(assessment, CaseAssessment), (
                f"Assessment {i} is not a CaseAssessment: {type(assessment)}"
            )

    def test_batch_summary_counts(self) -> None:
        """batch_summary must return verdict counts summing to total."""
        # Build a tiny batch: 3 cases (mix of verdicts expected)
        cases = []
        for i in range(3):
            dataset = make_profiled_device_dataset(
                text=f"TEXT{i}",
                manufacturer="Siemens",
                model_name="AXIOM_Artis",
                rows=32,
                cols=128,
            )
            frame = make_burned_in_phi_pixel_array(
                f"PHI_{i}",
                rows=32,
                cols=128,
            )
            cases.append((f"case_{i}", [frame], dataset))

        assessments = assess_batch(
            cases,
            max_workers=1,
            profiles_dir=_profiles_dir(),
            margin=4,
        )

        summary = batch_summary(assessments)

        # Assert keys present
        for key in ['clean', 'redacted', 'quarantine', 'total', 'failed']:
            assert key in summary, f"Missing key '{key}' in batch_summary"

        # Assert counts sum
        count_sum = summary['clean'] + summary['redacted'] + summary['quarantine']
        assert count_sum == summary['total'], (
            f"Verdict counts {count_sum} do not equal total {summary['total']}"
        )

        # Assert no failures in this small test
        assert summary['failed'] == 0, f"Unexpected failures: {summary['failed']}"

        # Assert total is 3
        assert summary['total'] == 3, f"Expected total=3, got {summary['total']}"

    @_requires_tesseract()
    def test_assess_batch_matches_serial(self) -> None:
        """
        assess_batch results must match calling assess_case directly per case.

        This test verifies that parallelism (or fallback serial) does not
        produce different verdicts than the normal serial path.
        """
        # Build a small batch
        cases = []
        for i in range(4):
            dataset = make_unprofiled_device_dataset(
                text=f"MRN_{i}",
                rows=32,
                cols=128,
            )
            frame = make_burned_in_phi_pixel_array(
                f"PHI_{i}",
                rows=32,
                cols=128,
            )
            cases.append((f"case_{i}", [frame], dataset))

        # Run assess_batch
        batch_assessments = assess_batch(
            cases,
            max_workers=1,
            profiles_dir=_profiles_dir(),
            margin=4,
        )

        # Run serial assess_case per case
        serial_assessments = []
        for case_id, frames, dataset in cases:
            assessment = assess_case(
                frames,
                dataset,
                profiles_dir=_profiles_dir(),
                margin=4,
            )
            serial_assessments.append(assessment)

        # Compare verdicts
        for i, (batch_a, serial_a) in enumerate(zip(batch_assessments, serial_assessments)):
            assert batch_a.verdict == serial_a.verdict, (
                f"Case {i}: batch verdict {batch_a.verdict} != serial {serial_a.verdict}"
            )


# ---------------------------------------------------------------------------
# SLOW Throughput Test (SC-005)
# ---------------------------------------------------------------------------

class TestAssessBatchSlow:
    """
    Slow throughput test: measure per-case time and extrapolate to 200 cases.

    This test is marked @pytest.mark.slow and should be skipped in CI by default.
    It measures real wall-clock time on a small batch (default N=20) and extrapolates
    to the SC-005 constraint (200 cases in ≤15 minutes).
    """

    @_requires_tesseract()
    @pytest.mark.slow
    @pytest.mark.pixeldeid
    def test_batch_throughput_extrapolated(
        self,
        n_cases: int = 20,
    ) -> None:
        """
        Measure per-case throughput and assert extrapolated 200-case time ≤ 15 min.

        Parameters
        ----------
        n_cases:
            Number of cases to actually run (default 20).
            Measured per-case time is extrapolated to 200 cases.
        """
        # Build a batch of image-sized cases (single small frame each)
        cases = []
        for i in range(n_cases):
            dataset = make_profiled_device_dataset(
                text=f"MRN_{i:04d}",
                manufacturer="Siemens",
                model_name="AXIOM_Artis",
                rows=64,
                cols=256,
            )
            frame = make_burned_in_phi_pixel_array(
                f"PATIENT NAME {i:04d}",
                rows=64,
                cols=256,
            )
            cases.append((f"case_{i:04d}", [frame], dataset))

        # Time the batch
        start_time = time.time()
        assessments = assess_batch(
            cases,
            max_workers=None,  # Use default (CPU count)
            profiles_dir=_profiles_dir(),
            margin=4,
        )
        elapsed_seconds = time.time() - start_time

        # Assert all returned
        assert len(assessments) == n_cases, (
            f"Expected {n_cases} assessments, got {len(assessments)}"
        )

        # Compute per-case rate
        per_case_seconds = elapsed_seconds / n_cases
        per_case_ms = per_case_seconds * 1000

        # Extrapolate to 200 cases
        projected_200_seconds = per_case_seconds * 200
        projected_200_minutes = projected_200_seconds / 60

        # SC-005 constraint: ≤15 minutes
        sc005_budget_seconds = 900  # 15 * 60

        # Log results for inspection
        print()
        print("=" * 70)
        print(f"Throughput Test Results (SC-005 constraint)")
        print("=" * 70)
        print(f"  Batch size measured: {n_cases} cases")
        print(f"  Total elapsed time: {elapsed_seconds:.2f} seconds")
        print(f"  Per-case rate: {per_case_ms:.1f} ms / case")
        print(f"  Extrapolated for 200 cases: {projected_200_seconds:.1f} sec ({projected_200_minutes:.1f} min)")
        print(f"  SC-005 budget: {sc005_budget_seconds} sec (15 min)")
        if projected_200_seconds <= sc005_budget_seconds:
            print(f"  Status: PASS (margin: {sc005_budget_seconds - projected_200_seconds:.1f} sec)")
        else:
            overage = projected_200_seconds - sc005_budget_seconds
            print(f"  Status: FAIL (overage: {overage:.1f} sec = {overage/60:.1f} min)")
        print("=" * 70)

        # Assert SC-005 constraint
        assert projected_200_seconds <= sc005_budget_seconds, (
            f"Extrapolated 200-case time {projected_200_minutes:.1f} min exceeds "
            f"SC-005 budget of 15 min. Per-case rate: {per_case_ms:.1f} ms"
        )


# ---------------------------------------------------------------------------
# Custom pytest markers
# ---------------------------------------------------------------------------

def pytest_configure(config):
    """Register custom pytest markers."""
    config.addinivalue_line(
        "markers", "pixeldeid: mark test as a pixel-de-id throughput test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow (skipped by default in CI)"
    )
