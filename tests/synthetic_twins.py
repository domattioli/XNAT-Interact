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
# OCR-only twin: a device with NO profile, one single-frame file, so neither the
# profile nor the cross-frame consensus tier can find its text; only OCR can.
MODEL_OCR = "ZZTWIN OCR Unit"
KEY_OCR = "ge_healthcare_zztwin_ocr_unit"
OCR_CASE_ID = "099"
OCR_BOX = (40, 56, 210, 76)

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
# Date tags that ingest sets to the intake operation date (ADR 0001).
INGEST_DATE_TAGS = ("StudyDate", "SeriesDate", "ContentDate", "AcquisitionDate", "InstanceCreationDate")

# Contract tags the pipeline must redact vs. known gaps outside the contract.
REDACTED_PN = ("PatientName", "ReferringPhysicianName", "PerformingPhysicianName", "OperatorsName")
REDACTED_OTHER = ("PatientID", "AccessionNumber", "StudyID", "InstitutionName", "PatientBirthDate",
                  "StationName", "DeviceSerialNumber", "InstitutionAddress", "OtherPatientIDs")
GAP_TAGS = ()  # every planted header item must be redacted
PRIVATE_CREATOR_TAG = (0x0009, 0x0010)
PRIVATE_VALUE_TAG = (0x0009, 0x1001)
PRIVATE_KEYWORD = "Private_0009_1001"


def _device(key: str) -> str:
    return {KEY_VAS: MODEL_VAS, KEY_OCR: MODEL_OCR}.get(key, MODEL_SUPERC)


# Fake burned-in text (all invented, ZZTWIN-marked). The 7-digit run reads as an
# MRN to the classifier, so OCR-found text is always treated as PHI.
TEXT_BG = 8  # dark but non-zero, so a masked (0) box is told apart from the background


def box_text(case_id: str, box_index: int) -> str:
    """Invented PHI-like banner text for box ``box_index`` of a twin case."""
    return (f"ZZTWIN {case_id}0001", f"ZZ{case_id}")[min(box_index, 1)]


def _font(height: int):
    from PIL import ImageFont

    return ImageFont.load_default(size=max(8, min(14, height - 4)))


def render_text_block(frame: np.ndarray, box, text: str) -> None:
    """Draw ``text`` bright (PLANT_VALUE) on dark (TEXT_BG) inside ``box`` (x0, y0, x1, y1), in place.

    Uses Pillow's bundled font so the glyphs are the same on every machine and
    real Tesseract can read them (spec 020 B2: the old solid blocks never
    exercised OCR).
    """
    from PIL import Image, ImageDraw

    x0, y0, x1, y1 = box
    tile = Image.new("L", (x1 - x0, y1 - y0), 0)
    ImageDraw.Draw(tile).text((3, 1), text, fill=255, font=_font(y1 - y0))
    # Binarise the anti-aliased glyphs: every text pixel is exactly PLANT_VALUE.
    frame[y0:y1, x0:x1] = np.where(np.asarray(tile) >= 128, PLANT_VALUE, TEXT_BG).astype(np.uint8)


def _pixels(case_id: str, name: str, device_key: str, n_frames: int, extra_text=()) -> np.ndarray:
    """uint8 frames: seeded noise 20-80 (fresh per frame) plus static bright-on-dark text plants.

    ``extra_text``: ((x0, y0, x1, y1), text) pairs drawn outside any device profile box.
    """
    seed = int(case_id) * 1000 + sum(ord(c) for c in name)
    rng = np.random.default_rng(seed)
    out = rng.integers(20, 81, size=(n_frames, ROWS, COLS), dtype=np.uint8)
    blocks = [(box, box_text(case_id, i)) for i, box in enumerate(BOXES.get(device_key, []))]
    blocks += list(extra_text)
    for frame in out:
        for box, text in blocks:
            render_text_block(frame, box, text)
    return out


def _build_dataset(case_id: str, name: str, *, device_key: str, modality: str, sop_class: str,
                   n_frames: int, instance: int, dates: str, extra_text=()):
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
    ds.PixelData = _pixels(case_id, name, device_key, n_frames, extra_text).tobytes()
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
        for i, box in enumerate(BOXES.get(device_key, [])):
            pixel_plants.append(dict(file=name, frame="all", box=list(box), text=box_text(case_id, i)))

    return dict(
        case_id=case_id, folder=str(folder), device_key=device_key, invented_dates=list(INVENTED_DATES),
        files=files, header_plants=plants, control_tags=controls, pixel_plants=pixel_plants,
        expected_census=dict(counts), expected_parse_error=parse_errors,
        # ADR 0001: after ingest every date tag equals the intake operation date; no invented date survives.
        ingest_date_rule=dict(tags=list(INGEST_DATE_TAGS), expected="operation_date"),
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


def make_ocr_only_case(tmp_dir) -> dict:
    """One single-frame RF file on an unprofiled device with burned-in text.

    Not in CASE_IDS: it exists to prove the OCR tier alone catches burned-in
    text. Returns a small manifest with the text box and the planted string.
    """
    folder = Path(tmp_dir) / f"twin_{OCR_CASE_ID}"
    folder.mkdir(parents=True, exist_ok=True)
    name = "rf_ocr_only.dcm"
    label, phi = "MRN ", f"{OCR_CASE_ID}0001 {OCR_CASE_ID}0002"  # two invented MRN-shaped IDs
    text = label + phi
    ds = _build_dataset(OCR_CASE_ID, name, device_key=KEY_OCR, modality="RF", sop_class=RF_SOP,
                        n_frames=1, instance=1, dates="full", extra_text=((OCR_BOX, text),))
    ds.save_as(str(folder / name), enforce_file_format=True)
    return dict(case_id=OCR_CASE_ID, folder=str(folder), device_key=KEY_OCR,
                files=[dict(path=name, role="dicom", n_frames=1, device_key=KEY_OCR)],
                pixel_plants=[dict(file=name, frame="all", box=list(OCR_BOX), text=text,
                                   phi_text=phi, phi_box=_text_extent(OCR_BOX, text, label))])


def _text_extent(box, text: str, prefix: str):
    """(x0, y0, x1, y1) frame pixels of ``text`` minus its leading ``prefix``, as render_text_block draws it."""
    from PIL import Image, ImageDraw

    bx0, by0, bx1, by1 = box
    font = _font(by1 - by0)
    tile = Image.new("L", (bx1 - bx0, by1 - by0), 0)
    ImageDraw.Draw(tile).text((3, 1), text, fill=255, font=font)
    ys, xs = np.nonzero(np.asarray(tile) >= 128)
    start = 3 + int(font.getlength(prefix))
    xs_phi = xs[xs >= start]
    ys_phi = ys[xs >= start]
    return [bx0 + int(xs_phi.min()), by0 + int(ys_phi.min()), bx0 + int(xs_phi.max()) + 1, by0 + int(ys_phi.max()) + 1]
