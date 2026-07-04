"""
tests/test_session_df_index_m3.py -- #33 M3 regression.

_mine_session_metadata ended with `sort_values(by='NEW_FN')` but never
reset the index.  The downstream write() loop uses `for idx in range(len(df))`
with label-based `.loc[idx]`, so a sorted/gapped (non-contiguous) index made
.loc[idx] read the wrong row or raise KeyError.  This guards the reset_index fix:
after mining, the dataframe index must be a contiguous RangeIndex 0..n-1.

Offline only. No real server, no real PHI. No pyxnat.
"""
from __future__ import annotations

import types

import pandas as pd
import pytest
from types import SimpleNamespace
from typing import Any

import src.xnat_experiment_data as _mod


class _FakeMetadata:
    """Mimics a pydicom FileDataset metadata holder; rejects duplicate tags."""

    def __init__(self) -> None:
        self._private_tags: dict[tuple, tuple[str, Any]] = {}

    def add_new(self, tag: tuple, vr: str, value: Any) -> None:
        if tag in self._private_tags:
            raise ValueError(f"Tag {tag} already exists")
        self._private_tags[tag] = (vr, value)


def _make_dicom_obj(sop_tail: str, fn_key: str) -> SimpleNamespace:
    """Minimal SourceDicomDeIdentified stand-in with all stashable metadata."""
    obj = SimpleNamespace(
        metadata=_FakeMetadata(),
        StudyDate="20240101",
        StudyTime="120000",
        StudyInstanceUID="1.2.3.4." + sop_tail,
        SeriesInstanceUID="1.2.3.4.5." + sop_tail,
        SOPInstanceUID="1.2.3.4.5.6." + sop_tail,
        NumberOfStudyRelatedInstances=3,
        ContentTime="120000",
        _derived_metadata={"DATETIME": "2024-01-01 12:00:00"},
        image=SimpleNamespace(hash_str="aabbcc" + sop_tail),
    )
    # NEW_FN is fn_key-derived so the post-sort order is deterministic and
    # (by construction below) the reverse of row order — proves the sort runs.
    obj.generate_source_image_file_name = lambda inst_str, uid, k=fn_key: f"{k}-{uid}"
    return obj


class _MinimalRFSession:
    """Drives the real _mine_session_metadata with no file I/O."""

    def __init__(self, df: pd.DataFrame, uid: str = "9.9.9") -> None:
        self._df = df
        self.intake_form = SimpleNamespace(
            uid=uid, operation_date="20240101", epic_start_time="120000",
        )

    @property
    def df(self) -> pd.DataFrame:
        return self._df

    def run(self) -> None:
        types.MethodType(_mod.SourceRFSession._mine_session_metadata, self)()


def _build_df(index: list[int]) -> pd.DataFrame:
    n = len(index)
    rows = []
    for i in range(n):
        # Descending zero-padded keys: row 0 gets the largest, so NEW_FN sort
        # reverses row order -- a non-trivial permutation that exercises the sort.
        fn_key = f"{n - i:04d}"
        rows.append({
            "FN": f"frame_{i}.dcm", "EXT": ".dcm", "NEW_FN": "",
            "OBJECT": _make_dicom_obj(str(i), fn_key),
            "IS_VALID": True, "IS_QUESTIONABLE": False,
            "DATE": None, "SERIES_TIME": None, "INSTANCE_TIME": None,
            "INSTANCE_NUM": None, "ContentTime": None, "InstanceNumber": None,
        })
    return pd.DataFrame(rows, index=index)


def test_contiguous_index_after_mining_clean_input():
    session = _MinimalRFSession(_build_df([0, 1, 2]))
    session.run()
    assert list(session.df.index) == [0, 1, 2]


def test_contiguous_index_after_mining_gapped_input():
    # Gapped index simulates rows dropped before mining; pre-fix this leaves a
    # non-contiguous index so the write() range(len)+.loc[idx] loop KeyErrors.
    session = _MinimalRFSession(_build_df([0, 2, 5]))
    session.run()
    assert list(session.df.index) == [0, 1, 2]


def test_write_loop_access_pattern_is_safe():
    # Reproduce the write() access pattern (range(len) + label .loc) post-mining.
    session = _MinimalRFSession(_build_df([3, 7, 11]))
    session.run()
    for idx in range(len(session.df)):
        # Must not raise KeyError.
        assert session.df.loc[idx, "NEW_FN"] is not None


def test_rows_in_sorted_newfn_order_after_mining():
    # NEW_FN keys are descending by row, so after sort they ascend and the
    # contiguous index must line up with sorted NEW_FN values.
    session = _MinimalRFSession(_build_df([0, 2, 5]))
    session.run()
    new_fns = list(session.df["NEW_FN"])
    assert new_fns == sorted(new_fns)
    assert list(session.df.index) == [0, 1, 2]
