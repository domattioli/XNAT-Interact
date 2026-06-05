"""
tests/test_session_metadata_guard.py — #30 InstanceNumber guard regression.

T016 (RED FIRST): a DICOM lacking InstanceNumber in its metadata causes
AttributeError at xnat_experiment_data.py:506 today.

T017 fix: hasattr-guard metadata.InstanceNumber like its sibling tags;
default/derive an instance index when absent.

Offline only. No real server, no real PHI. No pyxnat.
"""
from __future__ import annotations

import pandas as pd
import pytest
from types import SimpleNamespace
from typing import Any


# ---------------------------------------------------------------------------
# Minimal stubs — reproduce just enough of the dicom_obj / session shape
# that _mine_session_metadata can run without a real DICOM file on disk.
# ---------------------------------------------------------------------------

class _FakeMetadata:
    """Mimics a pydicom FileDataset with configurable tag presence."""

    def __init__(self, *, has_instance_number: bool = True, instance_number: str = "1") -> None:
        self._has_instance_number = has_instance_number
        if has_instance_number:
            self.InstanceNumber = instance_number
        self._private_tags: list = []

    def add_new(self, tag: Any, vr: str, value: Any) -> None:
        self._private_tags.append((tag, vr, value))

    def __hasattr_shim(self, name: str) -> bool:
        return hasattr(self, name)


def _make_dicom_obj(
    *,
    has_instance_number: bool = True,
    instance_number: str = "1",
    uid: str = "1.2.3.4.5",
) -> SimpleNamespace:
    """Build a minimal stand-in for a SourceDicomDeIdentified row object."""
    meta = _FakeMetadata(
        has_instance_number=has_instance_number,
        instance_number=instance_number,
    )
    obj = SimpleNamespace(
        metadata=meta,
        ContentTime="120000",
        InstanceNumber=instance_number if has_instance_number else None,
        _derived_metadata={"DATETIME": "2024-01-01 12:00:00"},
        image=SimpleNamespace(hash_str="aabbcc"),
    )
    # Mimic hasattr behaviour: only expose InstanceNumber when requested.
    if not has_instance_number:
        del obj.InstanceNumber

    # generate_source_image_file_name is a static method on the real class;
    # stub it here so _mine_session_metadata:526 can call it.
    def _gen_fn(inst_str: str, patient_uid: str) -> str:
        return f"{patient_uid}_{inst_str}.dcm"

    obj.generate_source_image_file_name = _gen_fn
    return obj


def _build_session_df(dicom_obj: Any) -> pd.DataFrame:
    """One-row dataframe as expected by _mine_session_metadata."""
    cols = {
        "FN": "frame_001.dcm",
        "EXT": ".dcm",
        "NEW_FN": "",
        "OBJECT": dicom_obj,
        "IS_VALID": True,
        "IS_QUESTIONABLE": False,
        "DATE": None,
        "SERIES_TIME": None,
        "INSTANCE_TIME": None,
        "INSTANCE_NUM": None,
        "ContentTime": None,
        "InstanceNumber": None,
    }
    return pd.DataFrame([cols])


class _MinimalRFSessionForMetadata:
    """
    Thin stand-in for SourceRFSession that exposes only the attributes and
    methods accessed by _mine_session_metadata.  Avoids file I/O entirely.
    """

    def __init__(self, dicom_obj: Any, uid: str = "1.2.3.4.5") -> None:
        self._df = _build_session_df(dicom_obj)
        self.intake_form = SimpleNamespace(
            uid=uid,
            operation_date="20240101",
            epic_start_time="120000",
        )

    @property
    def df(self) -> pd.DataFrame:
        return self._df

    def _mine_session_metadata(self) -> None:
        # Direct copy of the method under test from src/xnat_experiment_data.py
        # so we can exercise the real source code.
        from src.xnat_experiment_data import SourceRFSession  # noqa: F401 (import smoke)
        # Call the actual method on ourself — but since we're not a real
        # SourceRFSession we patch it in below.
        raise NotImplementedError("must call via real class — see test body")


# ---------------------------------------------------------------------------
# T016 — RED test: today's code raises AttributeError on missing InstanceNumber
# ---------------------------------------------------------------------------

class TestInstanceNumberGuard:

    def _run_mine_session_metadata(self, session: Any) -> None:
        """Drive the real _mine_session_metadata on the session stub."""
        import types
        import src.xnat_experiment_data as _mod

        # Bind the real method to our stub instance so we exercise the real code
        # path without instantiating the full class (which needs file I/O).
        bound = types.MethodType(_mod.SourceRFSession._mine_session_metadata, session)
        bound()

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "T016 red-state documentation: pre-fix code raises AttributeError on "
            "metadata.InstanceNumber when tag absent.  After T017 fix the error is "
            "gone (xfail strict=True: expected to fail here, so xfail passes in "
            "the green state).  Preserves the regression evidence."
        ),
    )
    def test_missing_instance_number_raises_today(self) -> None:
        """T016 RED: dicom_obj.metadata with no InstanceNumber → AttributeError
        at xnat_experiment_data.py:526 before the hasattr guard is added."""
        dicom_obj = _make_dicom_obj(has_instance_number=False)
        session = _MinimalRFSessionForMetadata(dicom_obj)

        # Pre-fix: AttributeError because metadata.InstanceNumber is accessed
        # without a hasattr guard.  This test documents the bug.
        # After T017 fix this test must pass WITHOUT raising.
        with pytest.raises(AttributeError):
            self._run_mine_session_metadata(session)

    def test_present_instance_number_no_error(self) -> None:
        """Baseline: a DICOM with InstanceNumber must complete without error."""
        dicom_obj = _make_dicom_obj(has_instance_number=True, instance_number="42")
        session = _MinimalRFSessionForMetadata(dicom_obj)

        # Must not raise regardless of fix state.
        self._run_mine_session_metadata(session)
        assert session._df.iloc[0]["NEW_FN"] != "", "NEW_FN should be set"


# ---------------------------------------------------------------------------
# T017 — GREEN tests: after fix, missing InstanceNumber defaults/derives index
# ---------------------------------------------------------------------------

class TestInstanceNumberGuardPostFix:
    """These pass once the hasattr guard is applied (T017)."""

    def _run_mine_session_metadata(self, session: Any) -> None:
        import types
        import src.xnat_experiment_data as _mod
        bound = types.MethodType(_mod.SourceRFSession._mine_session_metadata, session)
        bound()

    def test_missing_instance_number_no_attribute_error(self) -> None:
        """After T017 fix: DICOM without InstanceNumber completes without crash."""
        dicom_obj = _make_dicom_obj(has_instance_number=False)
        session = _MinimalRFSessionForMetadata(dicom_obj)

        # Must NOT raise after the fix.
        self._run_mine_session_metadata(session)

    def test_new_fn_derived_when_instance_number_absent(self) -> None:
        """After fix: NEW_FN is set (non-empty) even when InstanceNumber absent."""
        dicom_obj = _make_dicom_obj(has_instance_number=False)
        session = _MinimalRFSessionForMetadata(dicom_obj)

        self._run_mine_session_metadata(session)
        new_fn = session._df.iloc[0]["NEW_FN"]
        assert new_fn != "", f"NEW_FN should be derived/defaulted; got: {new_fn!r}"
