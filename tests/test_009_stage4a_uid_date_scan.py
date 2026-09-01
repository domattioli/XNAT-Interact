"""
tests/test_009_stage4a_uid_date_scan.py — Feature 009 Stage 4a acceptance tests.

Covers SC-001, SC-002, and T014 (FR-014) scan-param:

SC-001  N-frame session → N distinct SOPInstanceUIDs (FR-001).
SC-002  No readable original StudyDate persists on uploaded object after ingest (FR-003).
T014    scan param is user-selectable; default='0'; non-default routes to correct label.

Offline only. No real server, no PHI.
"""
from __future__ import annotations

import pandas as pd
import pytest
from types import SimpleNamespace
from typing import Any


# ---------------------------------------------------------------------------
# Shared helpers (mirror test_session_metadata_guard.py shape)
# ---------------------------------------------------------------------------

class _FakeMetadata:
    """Mimics a pydicom FileDataset with list-based private tag storage."""

    def __init__(self, *, instance_number: str = "1") -> None:
        self.InstanceNumber = instance_number
        self._private_tags: list = []

    def add_new(self, tag: Any, vr: str, value: Any) -> None:
        self._private_tags.append((tag, vr, value))


def _make_dicom_obj(
    *,
    idx: int = 0,
    uid: str = "1.2.3",
    study_date: str = "20240101",
    study_time: str = "120000",
    sop_uid: str = "9.8.7.6.5",
    series_uid: str = "9.8.7.6",
    study_uid: str = "9.8.7",
) -> SimpleNamespace:
    meta = _FakeMetadata(instance_number=str(idx + 1))
    obj = SimpleNamespace(
        metadata=meta,
        ContentTime="120000",
        InstanceNumber=str(idx + 1),
        StudyDate=study_date,
        StudyTime=study_time,
        StudyInstanceUID=study_uid,
        SeriesInstanceUID=series_uid,
        SOPInstanceUID=sop_uid,
        _derived_metadata={"DATETIME": "2024-01-01 12:00:00"},
        image=SimpleNamespace(hash_str=f"hash{idx:04d}"),
    )

    def _gen_fn(inst_str: str, patient_uid: str) -> str:
        return f"{patient_uid}_{inst_str}.dcm"

    obj.generate_source_image_file_name = _gen_fn
    return obj


def _build_df(dicom_objs: list) -> pd.DataFrame:
    rows = []
    for obj in dicom_objs:
        rows.append({
            "FN": f"frame_{id(obj)}.dcm",
            "EXT": ".dcm",
            "NEW_FN": "",
            "OBJECT": obj,
            "IS_VALID": True,
            "IS_QUESTIONABLE": False,
            "DATE": None,
            "SERIES_TIME": None,
            "INSTANCE_TIME": None,
            "INSTANCE_NUM": None,
            "ContentTime": None,
            "InstanceNumber": None,
        })
    return pd.DataFrame(rows)


class _MultiFrameRFSession:
    """Thin stand-in for SourceRFSession with N frames."""

    def __init__(self, dicom_objs: list, uid: str = "2.99.88.77") -> None:
        self._df = _build_df(dicom_objs)
        self.intake_form = SimpleNamespace(
            uid=uid,
            operation_date="20240601",
            epic_start_time="090000",
        )

    @property
    def df(self) -> pd.DataFrame:
        return self._df

    def _run(self) -> None:
        import types
        import src.xnat_experiment_data as _mod
        types.MethodType(_mod.SourceRFSession._mine_session_metadata, self)()


# ---------------------------------------------------------------------------
# SC-001 — N frames → N distinct SOPInstanceUIDs
# ---------------------------------------------------------------------------

class TestUniqueSOPInstanceUID:
    """SC-001: N-frame session produces N distinct SOPInstanceUIDs."""

    def _collect_sop_uids(self, session: _MultiFrameRFSession) -> list[str]:
        session._run()
        return [row["OBJECT"].SOPInstanceUID for _, row in session._df.iterrows()]

    def test_single_frame_has_sop_uid(self) -> None:
        """1-frame session: SOPInstanceUID is set and non-empty."""
        obj = _make_dicom_obj(idx=0, uid="2.1.1")
        session = _MultiFrameRFSession([obj], uid="2.1.1")
        sop_uids = self._collect_sop_uids(session)
        assert len(sop_uids) == 1
        assert sop_uids[0], "SOPInstanceUID must not be empty"

    def test_two_frames_distinct_sop_uids(self) -> None:
        """2-frame session: each frame gets a different SOPInstanceUID."""
        objs = [_make_dicom_obj(idx=i, uid="2.2.2") for i in range(2)]
        session = _MultiFrameRFSession(objs, uid="2.2.2")
        sop_uids = self._collect_sop_uids(session)
        assert len(set(sop_uids)) == 2, f"Expected 2 distinct UIDs; got: {sop_uids}"

    def test_five_frames_all_distinct_sop_uids(self) -> None:
        """5-frame session: all 5 SOPInstanceUIDs are distinct (SC-001)."""
        n = 5
        objs = [_make_dicom_obj(idx=i, uid="2.3.3") for i in range(n)]
        session = _MultiFrameRFSession(objs, uid="2.3.3")
        sop_uids = self._collect_sop_uids(session)
        assert len(sop_uids) == n, f"Expected {n} frames in output"
        assert len(set(sop_uids)) == n, (
            f"Expected {n} distinct SOPInstanceUIDs; "
            f"got {len(set(sop_uids))} unique values: {sop_uids}"
        )

    def test_sop_uid_deterministic_same_input(self) -> None:
        """Same intake UID + frame index → same SOPInstanceUID on re-run (idempotent)."""
        obj_a = _make_dicom_obj(idx=0, uid="2.4.4")
        session_a = _MultiFrameRFSession([obj_a], uid="2.4.4")
        session_a._run()
        uid_a = session_a._df.iloc[0]["OBJECT"].SOPInstanceUID

        obj_b = _make_dicom_obj(idx=0, uid="2.4.4")
        session_b = _MultiFrameRFSession([obj_b], uid="2.4.4")
        session_b._run()
        uid_b = session_b._df.iloc[0]["OBJECT"].SOPInstanceUID

        assert uid_a == uid_b, "Same inputs must produce same SOPInstanceUID (deterministic)"

    def test_original_sop_uid_stashed_as_private_tag(self) -> None:
        """FR-002: original SOPInstanceUID stashed in (0x0019, 0x1005) LT tag."""
        orig_sop = "9.8.7.6.5.4.3"
        obj = _make_dicom_obj(idx=0, uid="2.5.5", sop_uid=orig_sop)
        session = _MultiFrameRFSession([obj], uid="2.5.5")
        session._run()

        meta = session._df.iloc[0]["OBJECT"].metadata
        tags = {tag: (vr, val) for tag, vr, val in meta._private_tags}
        assert (0x0019, 0x1005) in tags, "Old_SOPInstanceUID private tag must exist"
        vr, val = tags[(0x0019, 0x1005)]
        assert vr == "LT"
        assert orig_sop in val, f"Original SOP UID {orig_sop!r} not found in stashed value: {val!r}"

    def test_ten_frames_all_distinct_sop_uids(self) -> None:
        """10-frame session: all 10 SOPInstanceUIDs are distinct."""
        n = 10
        objs = [_make_dicom_obj(idx=i, uid="2.6.6") for i in range(n)]
        session = _MultiFrameRFSession(objs, uid="2.6.6")
        sop_uids = self._collect_sop_uids(session)
        assert len(set(sop_uids)) == n, (
            f"Expected {n} distinct SOPInstanceUIDs; "
            f"got {len(set(sop_uids))}: {sop_uids}"
        )


# ---------------------------------------------------------------------------
# SC-002 — No readable Old_StudyDate after ingest
# ---------------------------------------------------------------------------

class TestNoReadableOldStudyDate:
    """SC-002: original StudyDate must not persist as readable text on upload."""

    def _run_session(self, obj: Any, uid: str = "3.1.1") -> Any:
        session = _MultiFrameRFSession([obj], uid=uid)
        session._run()
        return session._df.iloc[0]["OBJECT"].metadata

    def test_no_old_study_date_tag(self) -> None:
        """FR-003: (0x0019,0x1001) must NOT contain a readable 'Old_StudyDate:' string.
        It may contain a CaseDateHash or be absent entirely."""
        orig_date = "20230115"
        obj = _make_dicom_obj(idx=0, uid="3.1.1", study_date=orig_date)
        meta = self._run_session(obj, uid="3.1.1")

        for tag, vr, val in meta._private_tags:
            val_str = str(val)
            assert "Old_StudyDate" not in val_str, (
                f"Readable 'Old_StudyDate' found in tag {tag}: {val_str!r}. "
                "HIPAA identifier must not persist on uploaded object."
            )

    def test_original_date_not_in_any_private_tag(self) -> None:
        """No private tag should contain the literal original date string."""
        orig_date = "20230115"
        obj = _make_dicom_obj(idx=0, uid="3.2.2", study_date=orig_date)
        meta = self._run_session(obj, uid="3.2.2")

        for tag, vr, val in meta._private_tags:
            val_str = str(val)
            # The hash of the date is fine; the literal date is not.
            # Hash will never equal 8-digit date string.
            if "Old_StudyDate" in val_str or (orig_date in val_str and "Hash" not in val_str):
                pytest.fail(
                    f"Readable original date {orig_date!r} found in tag {tag}: {val_str!r}"
                )

    def test_case_date_hash_present_when_salt_configured(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """When XNAT_IDENTITY_SALT is set, (0x0019,0x1001) contains CaseDateHash."""
        monkeypatch.setenv("XNAT_IDENTITY_SALT", "deadbeef" * 8)

        orig_date = "20230115"
        obj = _make_dicom_obj(idx=0, uid="3.3.3", study_date=orig_date)
        session = _MultiFrameRFSession([obj], uid="3.3.3")
        session._run()
        meta = session._df.iloc[0]["OBJECT"].metadata

        tags = {tag: (vr, val) for tag, vr, val in meta._private_tags}
        # (0x0019,0x1001) should exist and contain 'CaseDateHash'
        assert (0x0019, 0x1001) in tags, "CaseDateHash private tag must exist when salt configured"
        vr, val = tags[(0x0019, 0x1001)]
        assert "CaseDateHash" in str(val), (
            f"Expected 'CaseDateHash' in tag value; got: {val!r}"
        )
        # Must not contain readable date
        assert orig_date not in str(val), (
            f"Readable date {orig_date!r} must not appear in CaseDateHash tag: {val!r}"
        )

    def test_no_crash_when_salt_absent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """FR-003 fail-soft: missing XNAT_IDENTITY_SALT → no crash, readable date still stripped."""
        monkeypatch.delenv("XNAT_IDENTITY_SALT", raising=False)

        obj = _make_dicom_obj(idx=0, uid="3.4.4", study_date="20230115")
        session = _MultiFrameRFSession([obj], uid="3.4.4")
        # Must not raise:
        session._run()

        meta = session._df.iloc[0]["OBJECT"].metadata
        for tag, vr, val in meta._private_tags:
            assert "Old_StudyDate" not in str(val), (
                f"Readable 'Old_StudyDate' must be stripped even without salt; found in {tag}"
            )


# ---------------------------------------------------------------------------
# T014 / FR-014 — scan param is user-selectable
# ---------------------------------------------------------------------------

class TestScanParamSelectable:
    """FR-014: _generate_queries scan param defaults to '0', accepts overrides."""

    def _make_connection(self, project: str = "PROJ") -> SimpleNamespace:
        return SimpleNamespace(xnat_project_name=project)

    def _make_session(self) -> Any:
        """Minimal SourceRFSession-shaped object."""
        import src.xnat_experiment_data as _mod

        class _Stub:
            intake_form = SimpleNamespace(uid="4.1.1")
            schema_prefix_str = "rf"
            scan_type_label = "DICOM"

            def _generate_queries(self, xnat_connection, scan=None):
                return _mod.SourceRFSession._generate_queries(self, xnat_connection, scan=scan)

        return _Stub()

    def test_default_scan_is_zero(self) -> None:
        """No scan arg → scan_label='0' in the generated query string."""
        session = self._make_session()
        conn = self._make_connection()
        _, _, scan_qs, _, _ = session._generate_queries(conn)
        assert "/0" in scan_qs, (
            f"Default scan should route to scan '0'; scan_qs={scan_qs!r}"
        )

    def test_custom_scan_routes_correctly(self) -> None:
        """scan='2' routes to scan label '2' in the query string."""
        session = self._make_session()
        conn = self._make_connection()
        _, _, scan_qs, _, _ = session._generate_queries(conn, scan="2")
        assert "/2" in scan_qs, (
            f"Custom scan='2' should appear in scan_qs; got: {scan_qs!r}"
        )
        assert "/0" not in scan_qs.split("/scan/")[1] if "/scan/" in scan_qs else True, (
            "Custom scan must not fall back to '0'"
        )

    def test_scan_none_uses_default(self) -> None:
        """Explicit scan=None → same as no argument → default '0'."""
        session = self._make_session()
        conn = self._make_connection()
        qs_implicit = session._generate_queries(conn)
        qs_explicit_none = session._generate_queries(conn, scan=None)
        assert qs_implicit == qs_explicit_none, (
            "scan=None must behave identically to omitting scan"
        )

    def test_publish_to_xnat_accepts_scan_param(self) -> None:
        """publish_to_xnat signature accepts scan keyword arg without TypeError."""
        import inspect
        import src.xnat_experiment_data as _mod
        sig = inspect.signature(_mod.SourceRFSession.publish_to_xnat)
        assert "scan" in sig.parameters, (
            "publish_to_xnat must accept 'scan' keyword argument (FR-014)"
        )
        param = sig.parameters["scan"]
        assert param.default is None, "scan default must be None (maps to SCAN_DEFAULT='0')"
