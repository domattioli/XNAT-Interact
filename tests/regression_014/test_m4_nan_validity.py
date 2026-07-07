"""
tests/regression_014/test_m4_nan_validity.py -- #33 M4 regression (spec 014).

M4 (still-open per the 2026-07-07 audit, ledger.md):
src/xnat_experiment_data.py used `df['IS_VALID'] == False` to find invalid
rows before writing. NaN/None cells (which can appear if IS_VALID's dtype
degrades to object) are never `== False`, so they silently passed the gate
as if valid.

Baseline evidence (pre-fix, this branch before the M4 fix commit): the test
below FAILS -- see ledger.md for the recorded run.

Offline only -- pure pandas, no server, no PHI.
"""
from __future__ import annotations

import math

import pandas as pd
import pytest

from src.xnat_experiment_data import _find_invalid_rows


def test_nan_is_valid_cell_is_treated_as_invalid():
    df = pd.DataFrame(
        {
            "FN": ["a.dcm", "b.dcm", "c.dcm"],
            # Object dtype (mixed True/False/NaN) -- the exact degradation
            # that made `== False` blind to the NaN row pre-fix.
            "IS_VALID": pd.array([True, False, math.nan], dtype="object"),
        }
    )

    invalid = _find_invalid_rows(df)

    # Pre-fix: invalid would only contain the explicit False row ("b.dcm"),
    # missing "c.dcm" entirely -- this assertion is what fails against the
    # pre-fix code.
    assert set(invalid["FN"]) == {"b.dcm", "c.dcm"}, (
        f"Expected both the False row and the NaN row to be flagged invalid, "
        f"got: {list(invalid['FN'])}"
    )


def test_none_is_valid_cell_is_treated_as_invalid():
    df = pd.DataFrame(
        {
            "FN": ["a.dcm", "b.dcm"],
            "IS_VALID": pd.array([True, None], dtype="object"),
        }
    )
    invalid = _find_invalid_rows(df)
    assert list(invalid["FN"]) == ["b.dcm"]


def test_all_true_rows_yield_no_invalid_rows():
    df = pd.DataFrame({"FN": ["a.dcm", "b.dcm"], "IS_VALID": [True, True]})
    assert _find_invalid_rows(df).empty
