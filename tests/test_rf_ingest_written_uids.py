"""
tests/test_rf_ingest_written_uids.py — UIDs and times land in the written DICOM file.

Regression: SourceRFSession._mine_session_metadata set StudyTime, StudyInstanceUID,
SeriesInstanceUID and SOPInstanceUID on the SourceDicomDeIdentified wrapper, not on
its .metadata Dataset, so none reached the uploaded file. Earlier tests used a
SimpleNamespace stand-in that hid this. This test drives the real wrapper over
synthetic twin files, writes each Dataset to disk and reads it back.

Offline only. Synthetic data only.
"""
from __future__ import annotations

from pathlib import Path
from types import MethodType, SimpleNamespace

import pandas as pd
import pydicom
import pytest

from tests import synthetic_twins as tw

INTAKE_UID = "2_25_123456789012345678901234567890"
DICOM_UID = INTAKE_UID.replace("_", ".")
EPIC_START = "09:30"
EPIC_TM = "093000"
TIME_TAGS = ("StudyTime", "SeriesTime", "ContentTime", "AcquisitionTime", "InstanceCreationTime")


@pytest.fixture(scope="module")
def twins(tmp_path_factory):
    return tw.make_all_twins(tmp_path_factory.mktemp("twins_uid"))


def _wrappers_for_case(m):
    from src.services.deidentify import deidentify_dataset
    from src.xnat_scan_data import SourceDicomDeIdentified

    rows = []
    for f in m["files"]:
        if f["role"] not in ("dicom", "duplicate") or f["path"] in m["expected_parse_error"]:
            continue
        ds = pydicom.dcmread(str(Path(m["folder"]) / f["path"]))
        obj = object.__new__(SourceDicomDeIdentified)
        obj._metadata, obj._derived_metadata, obj._image = ds, {}, object()
        obj._parse_for_derived_metadata()
        deidentify_dataset(obj._metadata, "REDACTED PYTHON-TO-XNAT UPLOAD SCRIPT")
        rows.append({"FN": f["path"], "OBJECT": obj, "IS_VALID": True, "IS_QUESTIONABLE": False,
                     "NEW_FN": "", "ContentTime": None, "InstanceNumber": None,
                     "ORIG": {k: str(getattr(ds, k, "")) for k in
                              ("StudyInstanceUID", "SeriesInstanceUID", "SOPInstanceUID")}})
    return rows


def test_written_file_carries_new_uids_times_and_old_uid_stash(twins, tmp_path, monkeypatch):
    import src.xnat_experiment_data as xed

    monkeypatch.setenv("XNAT_IDENTITY_SALT", "ab" * 32)
    case = next(m for m in twins.values() if len(_wrappers_for_case(m)) >= 2)
    rows = _wrappers_for_case(case)
    origs = [r.pop("ORIG") for r in rows]
    session = SimpleNamespace(
        _df=pd.DataFrame(rows),
        intake_form=SimpleNamespace(uid=INTAKE_UID, operation_date="2026-02-03", epic_start_time=EPIC_START),
    )
    session.df = session._df
    MethodType(xed.SourceRFSession._mine_session_metadata, session)()

    sops = []
    for i, (_, row) in enumerate(session._df.iterrows()):
        out = tmp_path / f"out_{i}.dcm"
        pydicom.dcmwrite(str(out), row["OBJECT"].metadata)
        back = pydicom.dcmread(str(out))
        assert str(back.StudyInstanceUID) == DICOM_UID
        assert str(back.SeriesInstanceUID) == DICOM_UID
        assert str(back.SOPInstanceUID).startswith("2.25.")
        sops.append(str(back.SOPInstanceUID))
        for kw in TIME_TAGS:
            if kw == "StudyTime" or kw in back:
                assert str(getattr(back, kw)) == EPIC_TM, kw
        stash = {el.tag.element: str(el.value) for el in back if el.tag.group == 0x0019}
        # Operator decision: the Old_*UID stashes stay readable.
        assert stash.get(0x1003, "").startswith("Old_StudyInstanceUID: ")
        assert stash.get(0x1004, "").startswith("Old_SeriesInstanceUID: ")
        assert stash.get(0x1005, "").startswith("Old_SOPInstanceUID: ")
    # FR-001: every instance gets its own SOPInstanceUID.
    assert len(set(sops)) == len(sops) >= 2
    assert all(o["SOPInstanceUID"] not in sops for o in origs if o["SOPInstanceUID"])


def test_time_and_uid_helpers():
    import src.xnat_experiment_data as xed

    assert xed._epic_start_time_as_dicom_tm("09:30") == "093000"
    assert xed._epic_start_time_as_dicom_tm("093015") == "093015"
    assert xed._epic_start_time_as_dicom_tm("") == ""
    assert xed._intake_uid_as_dicom_uid("2_25_77") == "2.25.77"
