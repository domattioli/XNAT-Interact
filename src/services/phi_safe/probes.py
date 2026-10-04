"""
Probes over a folder of real imaging files. Every return value is a dict that
passes :func:`gate.check`, so nothing a probe learned about a patient can be
printed. Traversal is sorted and no probe uses time or randomness, so two runs
over the same folder give the same report.

Probes never write, move or modify any file.
"""
from __future__ import annotations

import contextlib
import io
import os
import re
import warnings
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .gate import ALLOWED_KEYS, SafeReport, check, digest

IDENTIFYING_TAGS: Tuple[str, ...] = (
    "PatientName", "PatientID", "PatientBirthDate", "OtherPatientIDs", "PatientAddress",
    "AccessionNumber", "InstitutionName", "ReferringPhysicianName", "PerformingPhysicianName",
    "OperatorsName", "StudyDate", "SeriesDate", "AcquisitionDate", "ContentDate", "StudyID",
    "StationName", "InstitutionAddress", "DeviceSerialNumber",
)
MODALITY_KEYS = ("RF", "XA", "OT", "CR", "DX", "SC")
SECONDARY_CAPTURE_SOP = "1.2.840.10008.5.1.4.1.1.7"
REASON_CODES = (
    ("unprofiled device, no positive clean evidence", "unprofiled_no_evidence"),
    ("phi-text outside maskable region", "phi_outside_mask"),
    ("profile/detector disagreement", "profile_detector_disagree"),
    ("profiled device, no maskable regions found", "clean"),
)
MAX_BOXES = 12
MAX_FRAMES_PER_CASE = 40


@contextlib.contextmanager
def silenced():
    """Swallow stdout, stderr and warnings while a library touches real data.

    pydicom and cv2 can print tag values or file names in warnings. None of
    that may reach the terminal, so every probe body runs inside this guard.
    """
    sink = io.StringIO()
    with warnings.catch_warnings(), contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
        warnings.simplefilter("ignore")
        yield


def _files(folder: Path) -> List[Path]:
    return sorted(p for p in Path(folder).rglob("*") if p.is_file())


def _is_dicom(path: Path) -> bool:
    try:
        with path.open("rb") as handle:
            handle.seek(128)
            return handle.read(4) == b"DICM"
    except OSError:
        return False


def _read(path: Path, *, pixels: bool):
    import pydicom

    return pydicom.dcmread(str(path), stop_before_pixels=not pixels)


def _present(value: Any) -> bool:
    if value is None:
        return False
    text = str(value).strip()
    return text not in ("", "Anonymous", "ANONYMOUS")


def census(folder: Path) -> Dict[str, Any]:
    """Counts of what the folder holds and which identifying tags are filled."""
    files = _files(folder)
    out: Dict[str, Any] = {
        "files": len(files), "dicom": 0, "parsed": 0, "parse_failed": 0,
        "non_dcm_extension": 0, "secondary_capture": 0, "burned_in_yes": 0,
        "multiframe": 0, "empty_study_date": 0,
    }
    modalities = {k: 0 for k in MODALITY_KEYS + ("other",)}
    tags = {t: 0 for t in IDENTIFYING_TAGS}
    with silenced():
        for path in files:
            if path.suffix.lower() != ".dcm":
                out["non_dcm_extension"] += 1
            if not _is_dicom(path):
                continue
            out["dicom"] += 1
            try:
                ds = _read(path, pixels=False)
            except Exception:  # noqa: BLE001 - count, never surface
                out["parse_failed"] += 1
                continue
            out["parsed"] += 1
            mod = str(ds.get("Modality", "")).strip()
            modalities[mod if mod in MODALITY_KEYS else "other"] += 1
            sop = str(getattr(ds, "file_meta", {}).get("MediaStorageSOPClassUID", "")).strip()
            if sop == SECONDARY_CAPTURE_SOP:
                out["secondary_capture"] += 1
            if str(ds.get("BurnedInAnnotation", "")).strip().upper() == "YES":
                out["burned_in_yes"] += 1
            try:
                if int(ds.get("NumberOfFrames", 1) or 1) > 1:
                    out["multiframe"] += 1
            except (TypeError, ValueError):
                pass
            if not _present(ds.get("StudyDate", None)):
                out["empty_study_date"] += 1
            for tag in IDENTIFYING_TAGS:
                if _present(ds.get(tag, None)):
                    tags[tag] += 1
    out["modalities"] = modalities
    out["identifying_tags"] = tags
    return check(out)


def _device_id(ds) -> Optional[str]:
    from src.services.pixel_deid import profiles

    return profiles.device_id_for(ds)


def _loaded_profiles(profiles_dir: str) -> Dict[str, dict]:
    from src.services.pixel_deid import profiles

    try:
        return profiles.load_profiles(profiles_dir)
    except Exception:  # noqa: BLE001
        return {}


def devices(folder: Path, *, profiles_dir: str = "data/device_profiles") -> Dict[str, Any]:
    """Distinct acquisition devices, as digests of the device key, and whether each has a profile."""
    found: Dict[str, Dict[str, Any]] = {}
    with silenced():
        loaded = _loaded_profiles(profiles_dir)
        for path in _files(folder):
            if not _is_dicom(path):
                continue
            try:
                ds = _read(path, pixels=False)
            except Exception:  # noqa: BLE001
                continue
            key = _device_id(ds) or "none"
            entry = found.setdefault(digest(key), {"profiled": key in loaded, "frames_sampled": 0})
            entry["frames_sampled"] += 1
    rows = [{"device_digest": d, **v} for d, v in sorted(found.items())]
    return check({"devices": rows})


def detectors(*, model_dir: str = "models/craft") -> Dict[str, Any]:
    """Whether the text detectors the verdict engine relies on are installed."""
    tesseract = False
    with silenced():
        try:
            import pytesseract  # noqa: F401

            pytesseract.get_tesseract_version()
            tesseract = True
        except Exception:  # noqa: BLE001
            tesseract = False
    onnx = any(Path(model_dir).glob("*.onnx")) if Path(model_dir).is_dir() else False
    return check({"tesseract": tesseract, "onnx_model": onnx})


def _reason_code(reason: str) -> str:
    for prefix, code in REASON_CODES:
        if reason.startswith(prefix):
            return code
    return "other"


def verdicts(folder: Path, *, profiles_dir: str = "data/device_profiles") -> Dict[str, Any]:
    """Run the production pixel assessor on the first frame of each device."""
    from src.services.pixel_deid.verdict import assess_case

    seen: Dict[str, Dict[str, Any]] = {}
    with silenced():
        for path in _files(folder):
            if not _is_dicom(path):
                continue
            try:
                ds = _read(path, pixels=True)
                key = digest(_device_id(ds) or "none")
                if key in seen:
                    continue
                frames = ds.pixel_array
                if frames.ndim == 2:
                    frames = frames[None, ...]
                assessment = assess_case(list(frames[:1]), ds, profiles_dir=profiles_dir)
            except Exception:  # noqa: BLE001
                continue
            seen[key] = {
                "device_digest": key,
                "verdict": str(getattr(assessment.verdict, "value", assessment.verdict)),
                "reason": _reason_code(str(assessment.reason)),
                "tiers_fired": len(assessment.tiers_fired),
                "regions": len(assessment.regions),
            }
    rows = [seen[k] for k in sorted(seen)]
    for row in rows:
        if row["verdict"] not in ("confirmed", "redact", "abort", "quarantine", "clean"):
            row["verdict"] = "other"
        if row["verdict"] == "clean":
            row["verdict"] = "confirmed"
    return check({"verdicts": rows})


def _frames_for_device(folder: Path, limit: int) -> Dict[str, Tuple[Tuple[int, int], List[Any]]]:
    """Collect up to ``limit`` frames per device, keyed by device digest, as 2-D uint8 arrays."""
    import numpy as np

    out: Dict[str, Tuple[Tuple[int, int], List[Any]]] = {}
    for path in _files(folder):
        if not _is_dicom(path):
            continue
        try:
            ds = _read(path, pixels=True)
            arr = ds.pixel_array
        except Exception:  # noqa: BLE001
            continue
        if arr.ndim == 3 and arr.shape[-1] in (3, 4):
            arr = arr.mean(axis=-1)
        frames = arr if arr.ndim == 3 else arr[None, ...]
        key = digest(_device_id(ds) or "none")
        shape = (int(frames.shape[1]), int(frames.shape[2]))
        bucket = out.setdefault(key, (shape, []))
        if bucket[0] != shape:
            continue
        for frame in frames:
            if len(bucket[1]) >= limit:
                break
            f = frame.astype(np.float64)
            lo, hi = float(f.min()), float(f.max())
            scaled = np.zeros_like(f, dtype=np.uint8) if hi <= lo else ((f - lo) / (hi - lo) * 255).astype(np.uint8)
            bucket[1].append(scaled)
    return out


def _components(mask) -> List[Tuple[int, int, int, int]]:
    """Bounding boxes of connected components, largest first, merged when touching after dilation."""
    import numpy as np

    try:
        import cv2
    except Exception:  # noqa: BLE001
        cv2 = None
    m = mask.astype(np.uint8)
    if cv2 is not None:
        kernel = np.ones((5, 15), dtype=np.uint8)
        m = cv2.dilate(m, kernel, iterations=2)
        n, labels, stats, _ = cv2.connectedComponentsWithStats(m, connectivity=8)
        boxes = []
        for i in range(1, n):
            x, y, w, h, area = (int(v) for v in stats[i])
            if area >= 40 and w >= 8:
                boxes.append((x, y, x + w, y + h))
    else:
        ys, xs = np.nonzero(m)
        boxes = [] if len(xs) == 0 else [(int(xs.min()), int(ys.min()), int(xs.max()) + 1, int(ys.max()) + 1)]
    boxes.sort(key=lambda b: (-(b[2] - b[0]) * (b[3] - b[1]), b[1], b[0]))
    return boxes[:MAX_BOXES]


def banner(folder: Path, *, limit: int = MAX_FRAMES_PER_CASE) -> Dict[str, Any]:
    """Find candidate burned-in text regions without reading any text.

    Burned-in annotation stays fixed while anatomy moves. Pixels whose value is
    constant across every sampled frame and brighter than the frame median form
    the banner mask. With a single frame, the mask falls back to bright,
    high-contrast pixels in the outer 20 percent border, where devices print
    their banners. Output is boxes and counts only.
    """
    import numpy as np

    rows: List[Dict[str, Any]] = []
    with silenced():
        for key, (shape, frames) in sorted(_frames_for_device(folder, limit).items()):
            if not frames:
                continue
            stack = np.stack(frames).astype(np.int16)
            h, w = shape
            if len(frames) >= 3:
                spread = stack.max(axis=0) - stack.min(axis=0)
                bright = np.median(stack, axis=0) > (np.median(stack) + 40)
                mask = (spread <= 2) & bright
            else:
                frame = stack[0]
                border = np.zeros((h, w), dtype=bool)
                bh, bw = max(1, h // 5), max(1, w // 5)
                border[:bh, :] = border[-bh:, :] = True
                border[:, :bw] = border[:, -bw:] = True
                mask = (frame > (int(np.median(frame)) + 80)) & border
            fraction = int(round(100.0 * float(mask.mean())))
            rows.append({
                "device_digest": key, "rows": h, "cols": w, "frames_used": len(frames),
                "constant_fraction_pct": fraction, "boxes": _components(mask),
            })
    return check({"banner": rows})


def intake(folder: Path) -> Dict[str, Any]:
    """What the production intake would do with these files, by rule, without running it."""
    files = _files(folder)
    accepted = sum(1 for p in files if p.suffix.lower() == ".dcm")
    return check({"intake": {
        "files": len(files),
        "accepted": accepted,
        "rejected_extension": len(files) - accepted,
    }})


def run_all(folder: Path, *, profiles_dir: str = "data/device_profiles", model_dir: str = "models/craft") -> SafeReport:
    """Run every probe over ``folder`` and return the gated report."""
    report = SafeReport()
    probes: Dict[str, Any] = {}
    probes.update(census(folder))
    probes.update(devices(folder, profiles_dir=profiles_dir))
    probes.update(detectors(model_dir=model_dir))
    probes.update(verdicts(folder, profiles_dir=profiles_dir))
    probes.update(banner(folder))
    probes.update(intake(folder))
    report.put("probes", probes)
    return report
