"""
Synthetic "twin" fluoro cases with a ground-truth manifest (spec 020).

Each twin mirrors the *structure* of a real GE fluoro case (file layout,
modalities, device identity strings, quirks). Every identifying value is
invented, carries a ``SYNTH`` or ``ZZTWIN`` marker, and is listed in the
manifest, so tests can measure precision and recall of de-identification.

No real header value, pixel, UID, name or date is used here.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

import numpy as np

from tests.synthetic_data import _det_uid

CASE_IDS = ("015", "086", "089")

MANUFACTURER = "GE Healthcare"
MODEL_SUPERC = "Surgery ESP 21 cm FPD Super-C"
MODEL_VAS = "990001VAS 8"
KEY_SUPERC = "ge_healthcare_surgery_esp_21_cm_fpd_super-c"
KEY_VAS = "ge_healthcare_990001vas_8"

RF_SOP = "1.2.840.10008.5.1.4.1.1.12.2"  # X-Ray Radiofluoroscopic Image Storage
SC_SOP = "1.2.840.10008.5.1.4.1.1.7"     # Secondary Capture Image Storage

ROWS, COLS = 128, 256
PLANT_VALUE = 255

# Banner boxes (x0, y0, x1, y1), exclusive end. Twin profiles in
# tests/fixtures/device_profiles_twins/ use exactly these.
BOXES = {
    KEY_SUPERC: [[0, 4, 150, 22]],
    KEY_VAS: [[8, 100, 120, 120], [200, 4, 250, 18]],
}

# Invented dates (19000101 range), listed here and in every manifest.
D_STUDY, D_CONTENT, D_BIRTH, D_ICD = "19000101", "19000102", "19000103", "19000104"
INVENTED_DATES = [D_STUDY, D_CONTENT, D_BIRTH, D_ICD]

# Contract tags the pipeline must redact vs. known gaps outside the contract.
REDACTED_PN = ("PatientName", "ReferringPhysicianName", "PerformingPhysicianName", "OperatorsName")
REDACTED_OTHER = ("PatientID", "AccessionNumber", "StudyID", "InstitutionName", "PatientBirthDate",
                  "StationName", "DeviceSerialNumber", "InstitutionAddress", "OtherPatientIDs")
GAP_TAGS = ()  # every planted header item must be redacted
PRIVATE_CREATOR_TAG = (0x0009, 0x0010)
PRIVATE_VALUE_TAG = (0x0009, 0x1001)
PRIVATE_KEYWORD = "Private_0009_1001"


def _device(key: str) -> str:
    return MODEL_VAS if key == KEY_VAS else MODEL_SUPERC


def _pixels(case_id: str, name: str, device_key: str, n_frames: int) -> np.ndarray:
    """uint8 frames: seeded noise 20-80 (fresh per frame) plus static 255 plant blocks."""
    seed = int(case_id) * 1000 + sum(ord(c) for c in name)
    rng = np.random.default_rng(seed)
    out = rng.integers(20, 81, size=(n_frames, ROWS, COLS), dtype=np.uint8)
    for x0, y0, x1, y1 in BOXES[device_key]:
        out[:, y0:y1, x0:x1] = PLANT_VALUE
    return out


def _build_dataset(case_id: str, name: str, *, device_key: str, modality: str, sop_class: str,
                   n_frames: int, instance: int, dates: str):
    """dates: 'full' | 'none' | 'icd' (only InstanceCreationDate)."""
    from pydicom.dataset import FileDataset, FileMetaDataset
    from pydicom.uid import ExplicitVRLittleEndian

    sop_uid = _det_uid("twin", case_id, "sop", name)
    meta = FileMetaDataset()
    meta.MediaStorageSOPClassUID = sop_class
    meta.MediaStorageSOPInstanceUID = sop_uid
    meta.TransferSyntaxUID = ExplicitVRLittleEndian
    meta.ImplementationClassUID = _det_uid("twin", "impl")
    ds = FileDataset(name, {}, file_meta=meta, preamble=b"\x00" * 128)
    ds.SOPClassUID = sop_class
    ds.SOPInstanceUID = sop_uid
    ds.StudyInstanceUID = _det_uid("twin", case_id, "study")
    ds.SeriesInstanceUID = _det_uid("twin", case_id, "series", modality)
    ds.Modality = modality
    ds.Manufacturer = MANUFACTURER
    ds.ManufacturerModelName = _device(device_key)
    ds.InstanceNumber = str(instance)
    ds.SeriesDescription = "SYNTH TWIN"
    # Planted contract tags (must be redacted).
    ds.PatientName = f"ZZTWIN^SYNTH^{case_id}{instance}"
    ds.ReferringPhysicianName = f"ZZTWIN^REFERRER^{case_id}{instance}"
    ds.PerformingPhysicianName = f"ZZTWIN^PERFORMER^{case_id}{instance}"
    ds.OperatorsName = f"ZZTWIN^OPERATOR^{case_id}{instance}"
    ds.PatientID = f"ZZTWIN-PID-{case_id}-{instance}"
    ds.AccessionNumber = f"ZZTWIN-ACC-{case_id}-{instance}"
    ds.StudyID = f"ZZTWIN-SID-{case_id}-{instance}"
    ds.add_new(PRIVATE_CREATOR_TAG, "LO", "ZZTWIN PRIVATE")
    ds.add_new(PRIVATE_VALUE_TAG, "LO", f"ZZTWIN-PRIV-{case_id}-{instance}")
    # Planted known-gap tags (outside the current de-id contract).
    ds.InstitutionName = f"ZZTWIN HOSPITAL {case_id}"
    ds.PatientBirthDate = D_BIRTH
    ds.StationName = f"ZZTWIN-STN-{case_id}"
    ds.DeviceSerialNumber = f"ZZTWIN-SER-{case_id}"
    ds.InstitutionAddress = f"ZZTWIN 1 SYNTH STREET {case_id}"
    ds.OtherPatientIDs = f"ZZTWIN-OPID-{case_id}-{instance}"
    # Dates.
    if dates == "full":
        ds.StudyDate, ds.StudyTime = D_STUDY, "010101"
        ds.ContentDate, ds.ContentTime = D_CONTENT, "010202"
    elif dates == "none":
        ds.StudyDate = ds.ContentDate = ds.SeriesDate = ds.AcquisitionDate = ""
        ds.InstanceCreationDate = ""
    elif dates == "icd":
        ds.StudyDate = ds.ContentDate = ds.SeriesDate = ds.AcquisitionDate = ""
        ds.InstanceCreationDate, ds.InstanceCreationTime = D_ICD, "010303"
    # Pixels.
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.Rows, ds.Columns = ROWS, COLS
    ds.BitsAllocated = ds.BitsStored = 8
    ds.HighBit = 7
    ds.PixelRepresentation = 0
    if n_frames > 1:
        ds.NumberOfFrames = str(n_frames)
    ds.PixelData = _pixels(case_id, name, device_key, n_frames).tobytes()
    return ds


# (name, role, modality, sop_class, n_frames, dates, copy_of)
def _layout(case_id: str):
    sc, rf = SC_SOP, RF_SOP
    if case_id == "015":
        return KEY_SUPERC, [
            ("rf_001.dcm", "dicom", "RF", rf, 1, "full", None),
            ("rf_002.dcm", "dicom", "RF", rf, 1, "full", None),
            ("rf_001_copy", "duplicate", "RF", rf, 1, "full", "rf_001.dcm"),
            ("rf_002_copy", "duplicate", "RF", rf, 1, "full", "rf_002.dcm"),
            ("sc_nodate.dcm", "dicom", "OT", sc, 1, "none", None),
            ("sc_icd_only.dcm", "dicom", "OT", sc, 1, "icd", None),
            ("sidecar_1.json", "non_dicom", "", "", 0, "", None),
            ("sidecar_2.json", "non_dicom", "", "", 0, "", None),
        ]
    if case_id == "086":
        return KEY_VAS, [
            ("rf_multi.dcm", "dicom", "RF", rf, 8, "full", None),
            ("rf_single.dcm", "dicom", "RF", rf, 1, "full", None),
        ]
    if case_id == "089":
        return KEY_VAS, [
            ("ot_single.dcm", "dicom", "OT", sc, 1, "full", None),
            ("rf_multi.dcm", "dicom", "RF", rf, 6, "full", None),
        ]
    raise ValueError(f"Unknown twin case_id {case_id!r}; use one of {CASE_IDS}.")


def make_twin_case(tmp_dir, case_id: str) -> dict:
    """Write one twin case under ``tmp_dir`` and return its ground-truth manifest."""
    device_key, layout = _layout(case_id)
    folder = Path(tmp_dir) / f"twin_{case_id}"
    folder.mkdir(parents=True, exist_ok=True)

    files: List[dict] = []
    plants: List[dict] = []
    controls: List[dict] = []
    pixel_plants: List[dict] = []
    written: Dict[str, bytes] = {}
    parse_errors: List[str] = []
    counts = dict(files=0, dicom=0, non_dcm_extension=0, secondary_capture=0, multiframe=0,
                  empty_study_date=0, rf=0, ot=0)

    for idx, (name, role, modality, sop, n_frames, dates, copy_of) in enumerate(layout, start=1):
        path = folder / name
        counts["files"] += 1
        if Path(name).suffix.lower() != ".dcm":
            counts["non_dcm_extension"] += 1
        if role == "non_dicom":
            path.write_text(json.dumps({"note": "SYNTH TWIN sidecar", "case": case_id}, sort_keys=True))
            files.append(dict(path=name, role=role, modality="", sop_class="", n_frames=0, device_key=""))
            continue
        if role == "duplicate":
            path.write_bytes(written[copy_of])
            src_idx = next(i for i, l in enumerate(layout, start=1) if l[0] == copy_of)
        else:
            ds = _build_dataset(case_id, name, device_key=device_key, modality=modality, sop_class=sop,
                                n_frames=n_frames, instance=idx, dates=dates)
            ds.save_as(str(path), enforce_file_format=True)
            written[name] = path.read_bytes()
            src_idx = idx
        files.append(dict(path=name, role=role, modality=modality, sop_class=sop,
                          n_frames=n_frames, device_key=device_key))
        counts["dicom"] += 1
        counts["secondary_capture"] += int(sop == SC_SOP)
        counts["multiframe"] += int(n_frames > 1)
        counts["rf"] += int(modality == "RF")
        counts["ot"] += int(modality == "OT")
        counts["empty_study_date"] += int(dates in ("none", "icd"))
        if dates == "none":
            parse_errors.append(name)

        inst = src_idx
        for kw in REDACTED_PN + REDACTED_OTHER:
            plants.append(dict(file=name, tag=kw, value=_expected_value(case_id, kw, inst), expected="redacted"))
        plants.append(dict(file=name, tag=PRIVATE_KEYWORD, value=f"ZZTWIN-PRIV-{case_id}-{inst}", expected="redacted"))
        for kw in GAP_TAGS:
            plants.append(dict(file=name, tag=kw, value=_expected_value(case_id, kw, inst), expected="known_gap"))
        for kw, val in (("Modality", modality), ("Manufacturer", MANUFACTURER),
                        ("ManufacturerModelName", _device(device_key)), ("SOPClassUID", sop),
                        ("SOPInstanceUID", _det_uid("twin", case_id, "sop", written_name(name, copy_of))),
                        ("InstanceNumber", str(inst)), ("SeriesDescription", "SYNTH TWIN"),
                        ("Rows", ROWS), ("Columns", COLS)):
            controls.append(dict(file=name, tag=kw, value=val))
        if n_frames > 1:
            controls.append(dict(file=name, tag="NumberOfFrames", value=str(n_frames)))
        for box in BOXES[device_key]:
            pixel_plants.append(dict(file=name, frame="all", box=list(box)))

    return dict(
        case_id=case_id, folder=str(folder), device_key=device_key, invented_dates=list(INVENTED_DATES),
        files=files, header_plants=plants, control_tags=controls, pixel_plants=pixel_plants,
        expected_census=dict(counts), expected_parse_error=parse_errors,
    )


def written_name(name: str, copy_of) -> str:
    return copy_of or name


def _expected_value(case_id: str, kw: str, inst: int) -> str:
    return {
        "PatientName": f"ZZTWIN^SYNTH^{case_id}{inst}",
        "ReferringPhysicianName": f"ZZTWIN^REFERRER^{case_id}{inst}",
        "PerformingPhysicianName": f"ZZTWIN^PERFORMER^{case_id}{inst}",
        "OperatorsName": f"ZZTWIN^OPERATOR^{case_id}{inst}",
        "PatientID": f"ZZTWIN-PID-{case_id}-{inst}",
        "AccessionNumber": f"ZZTWIN-ACC-{case_id}-{inst}",
        "StudyID": f"ZZTWIN-SID-{case_id}-{inst}",
        "InstitutionName": f"ZZTWIN HOSPITAL {case_id}",
        "PatientBirthDate": D_BIRTH,
        "StationName": f"ZZTWIN-STN-{case_id}",
        "DeviceSerialNumber": f"ZZTWIN-SER-{case_id}",
        "InstitutionAddress": f"ZZTWIN 1 SYNTH STREET {case_id}",
        "OtherPatientIDs": f"ZZTWIN-OPID-{case_id}-{inst}",
    }[kw]


def make_all_twins(tmp_dir) -> Dict[str, dict]:
    """Build all three twin cases; returns {case_id: manifest}."""
    return {cid: make_twin_case(tmp_dir, cid) for cid in CASE_IDS}
