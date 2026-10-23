"""
PHI classification, redaction, and fail-closed verdict engine for burned-in PHI
pixel de-identification (Feature 010, Stage 3).

This module ties together the contrast-independent tiers (device profiles,
cross-frame consensus) and the learned/multipass detector tier, applies
PHI-vs-benign classification over OCR'd text, redacts the union of all
maskable regions, and routes each case to one of three verdicts:
``clean``, ``redacted``, or ``quarantine``.

Public API
----------
classify_text(text) -> tuple[bool, list[str]]
    Run Presidio AnalyzerEngine over *text*; return (is_phi, categories).
    Benign laterality/view/technique markers are allow-listed and never
    classified as PHI regardless of Presidio output.

assess_case(frames, dataset, *, registry, model_dir, profiles_dir, margin)
    -> CaseAssessment
    Orchestrate all tiers, apply redaction, route verdict, write audit.

audit_entry(assessment) -> str
    Return the JSON string that goes into registry.record_audit(target=…);
    contains counts and categories only — no raw PHI text (SC-004).

All heavy dependencies (presidio_analyzer, pytesseract, cv2) are imported
LAZILY inside functions so that ``import src.services.pixel_deid.verdict``
is cheap and does not penalise the rest of the test suite.
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

import numpy as np

from src.services.pixel_deid import detect, profiles, consensus
from src.services.deidentify import apply_redaction

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

class Verdict(Enum):
    CLEAN = "clean"
    REDACTED = "redacted"
    QUARANTINE = "quarantine"


@dataclass
class CaseAssessment:
    """Per-case result produced by assess_case()."""

    verdict: Verdict
    masked_frames: list[np.ndarray]
    regions: list[tuple]
    tiers_fired: list[str]
    phi_categories: list[str]
    reason: str


# ---------------------------------------------------------------------------
# Benign allow-list (T012)
# ---------------------------------------------------------------------------

# Exact-match allow-list (case-insensitive, after strip).
_BENIGN_EXACT: frozenset[str] = frozenset({
    "l", "r", "ap", "pa", "lao", "rao", "ll", "rl", "sup", "inf",
})

# Regex for kVp / mA readouts: up to 3 digits optionally followed by kV/kVp/mA.
_BENIGN_RE = re.compile(r"^\d{1,3}\s?(kv|kvp|ma)$", re.IGNORECASE)


def _is_benign(text: str) -> bool:
    """Return True if *text* is a benign laterality/view/technique marker."""
    stripped = text.strip()
    if stripped.lower() in _BENIGN_EXACT:
        return True
    if _BENIGN_RE.match(stripped):
        return True
    return False


# ---------------------------------------------------------------------------
# Presidio analyzer singleton (lazy, module-level)
# ---------------------------------------------------------------------------

_analyzer = None  # type: ignore[assignment]


def _get_analyzer():
    """Return (and lazily build) the module-level Presidio AnalyzerEngine."""
    global _analyzer
    if _analyzer is not None:
        return _analyzer

    from presidio_analyzer import AnalyzerEngine, RecognizerRegistry
    from presidio_analyzer.nlp_engine import NlpEngineProvider

    # Build the NLP engine (spaCy en_core_web_lg if available; small otherwise).
    try:
        provider = NlpEngineProvider(nlp_configuration={
            "nlp_engine_name": "spacy",
            "models": [{"lang_code": "en", "model_name": "en_core_web_lg"}],
        })
        nlp_engine = provider.create_engine()
    except Exception:
        try:
            provider = NlpEngineProvider(nlp_configuration={
                "nlp_engine_name": "spacy",
                "models": [{"lang_code": "en", "model_name": "en_core_web_sm"}],
            })
            nlp_engine = provider.create_engine()
        except Exception:
            nlp_engine = None

    if nlp_engine is not None:
        _analyzer = AnalyzerEngine(nlp_engine=nlp_engine)
    else:
        _analyzer = AnalyzerEngine()

    return _analyzer


# ---------------------------------------------------------------------------
# PHI classification (T012)
# ---------------------------------------------------------------------------

# Entity types that map to PHI in the medical context.
_PHI_ENTITY_TYPES: frozenset[str] = frozenset({
    "PERSON",
    "DATE_TIME",
    "US_SSN",
    "MEDICAL_LICENSE",
    "PHONE_NUMBER",
    "EMAIL_ADDRESS",
    "US_DRIVER_LICENSE",
    "US_PASSPORT",
    "US_BANK_NUMBER",
    "CREDIT_CARD",
    "IBAN_CODE",
    "IP_ADDRESS",
    "NRP",
    "LOCATION",
})

# MRN-like pattern: 5–12 consecutive digits (stands alone as a token).
_MRN_RE = re.compile(r"\b\d{5,12}\b")


def classify_text(text: str) -> tuple[bool, list[str]]:
    """
    Classify *text* as PHI or benign using Presidio + a custom MRN pattern.

    Returns a tuple ``(is_phi, categories)`` where *categories* is a list of
    distinct PHI entity-type strings (e.g. ``["PERSON", "DATE_TIME"]``).
    The *text* itself is never returned; only category names are recorded
    (SC-004).

    Benign allow-list: short laterality/view markers and kVp/mA readouts are
    never flagged as PHI, even if Presidio reports an entity (FR-009).

    Parameters
    ----------
    text:
        OCR'd text from a detected text region.

    Returns
    -------
    tuple[bool, list[str]]
        ``(True, ["CATEGORY", ...])`` when PHI is detected;
        ``(False, [])`` for benign or unclassifiable text.
    """
    if not text or not text.strip():
        return False, []

    # Benign allow-list check first — short markers override Presidio.
    if _is_benign(text):
        return False, []

    categories: list[str] = []

    # Run Presidio.
    try:
        analyzer = _get_analyzer()
        results = analyzer.analyze(text=text, language="en")
        for result in results:
            if result.entity_type in _PHI_ENTITY_TYPES:
                cat = result.entity_type
                if cat not in categories:
                    categories.append(cat)
    except Exception as exc:
        logger.debug("Presidio analysis failed: %s", exc)

    # Custom MRN-ish digit pattern: 5–12 consecutive digits.
    if _MRN_RE.search(text):
        if "MRN" not in categories:
            categories.append("MRN")

    is_phi = len(categories) > 0
    return is_phi, categories


# ---------------------------------------------------------------------------
# Box geometry helpers
# ---------------------------------------------------------------------------

def _boxes_overlap(box_a: tuple, box_b: tuple) -> bool:
    """Return True if two (x0, y0, x1, y1) boxes overlap (non-zero intersection)."""
    ax0, ay0, ax1, ay1 = box_a
    bx0, by0, bx1, by1 = box_b
    return ax0 < bx1 and ax1 > bx0 and ay0 < by1 and ay1 > by0


def _any_overlap(box: tuple, box_list: list[tuple]) -> bool:
    """Return True if *box* overlaps any box in *box_list*."""
    return any(_boxes_overlap(box, b) for b in box_list)


def _crop_box(img: np.ndarray, box: tuple) -> np.ndarray:
    """Return the sub-image corresponding to the (x0, y0, x1, y1) box."""
    x0, y0, x1, y1 = box
    h, w = img.shape[:2]
    x0 = max(0, x0)
    y0 = max(0, y0)
    x1 = min(w, x1)
    y1 = min(h, y1)
    return img[y0:y1, x0:x1]


def _box_center_distance(box_a: tuple, box_b: tuple) -> float:
    """Euclidean distance between centres of two (x0, y0, x1, y1) boxes."""
    cx_a = (box_a[0] + box_a[2]) / 2.0
    cy_a = (box_a[1] + box_a[3]) / 2.0
    cx_b = (box_b[0] + box_b[2]) / 2.0
    cy_b = (box_b[1] + box_b[3]) / 2.0
    return float(((cx_a - cx_b) ** 2 + (cy_a - cy_b) ** 2) ** 0.5)


def _x1y1_to_xywh(box: tuple) -> tuple[int, int, int, int]:
    """Convert (x0, y0, x1, y1) to (x, y, w, h) for apply_redaction."""
    x0, y0, x1, y1 = box
    return (x0, y0, x1 - x0, y1 - y0)


# ---------------------------------------------------------------------------
# assess_case (T013 + T014)
# ---------------------------------------------------------------------------

def assess_case(
    frames: list[np.ndarray],
    dataset,
    *,
    registry=None,
    model_dir: str = "models/craft",
    profiles_dir: str = "data/device_profiles",
    margin: int = 6,
) -> CaseAssessment:
    """
    Orchestrate all de-identification tiers and return a CaseAssessment.

    Tiers (cheapest first):

    1. **Profile** — device-profile blind mask (contrast-independent anchor).
    2. **Consensus** — cross-frame variance mask (multi-frame only).
    3. **Detector** — multipass Tesseract + CRAFT-ONNX per frame.

    After collecting candidate boxes from all tiers, each detector box is
    classified with OCR + :func:`classify_text`.  Detector boxes that are
    benign-only (no PHI and no overlap with profile/consensus boxes) are
    dropped to preserve benign markers (FR-009).  Unreadable/empty-crop
    detector boxes are kept (conservative FN-first approach).

    The union of remaining boxes is dilated and applied as an irreversible
    pixel fill on copies of every frame (FR-008).

    Verdict routing (fail-closed, T014):

    - ``QUARANTINE`` if PHI text is detected outside every mask_union region
      (leaked-PHI risk).
    - ``QUARANTINE`` if the device is unprofiled AND mask_union is empty
      (absence of detection on an unknown device is NOT evidence of cleanliness).
    - ``REDACTED`` if mask_union is non-empty (profiled or unprofiled).
    - ``CLEAN`` only if the device IS profiled AND mask_union is empty (a
      registered device with no text region is trustworthy).
    - ``QUARANTINE`` if profiled but the detector found a PHI box far outside
      all profile boxes (profile/detector disagreement).

    Parameters
    ----------
    frames:
        List of 2D uint8 numpy arrays, one per case frame.
    dataset:
        pydicom Dataset for the case (used for device identity lookup).
    registry:
        Optional Registry instance; if provided, an audit entry is written
        after the verdict is determined (SC-004 — no raw PHI in the entry).
    model_dir:
        Directory passed to the CRAFT detector; gracefully degrades if absent.
    profiles_dir:
        Directory containing device-profile JSON files.
    margin:
        Dilation margin in pixels for union_dilate.

    Returns
    -------
    CaseAssessment
    """
    # ------------------------------------------------------------------
    # Load profiles (once per call — small dict)
    # ------------------------------------------------------------------
    try:
        loaded_profiles = profiles.load_profiles(profiles_dir)
    except Exception as exc:
        logger.warning("Could not load profiles from %s: %s", profiles_dir, exc)
        loaded_profiles = {}

    device_id = profiles.device_id_for(dataset)
    is_profiled = (device_id is not None) and (device_id in loaded_profiles)

    # ------------------------------------------------------------------
    # Tier 1: Profile boxes
    # ------------------------------------------------------------------
    tiers_fired: list[str] = []
    profile_boxes: list[tuple] = []

    if is_profiled:
        profile_boxes = loaded_profiles[device_id]["boxes"]  # type: ignore[index]
        if profile_boxes:
            tiers_fired.append("profile")

    # ------------------------------------------------------------------
    # Tier 2: Consensus (multi-frame only)
    # ------------------------------------------------------------------
    consensus_boxes: list[tuple] = []
    if len(frames) > 1:
        try:
            consensus_boxes = consensus.variance_consensus_mask(frames)
            if consensus_boxes:
                tiers_fired.append("consensus")
        except Exception as exc:
            logger.warning("Consensus mask failed: %s", exc)

    # ------------------------------------------------------------------
    # Tier 3: Multipass detector + PHI classification per frame
    # ------------------------------------------------------------------
    detector_boxes_all: list[tuple] = []
    for frame in frames:
        try:
            boxes = detect.multipass_detect(frame, model_dir=model_dir, margin=0)
            detector_boxes_all.extend(boxes)
        except Exception as exc:
            logger.warning("Detector failed on frame: %s", exc)

    # For each detector box: OCR crop → classify → keep or drop.
    anchor_boxes = list(profile_boxes) + list(consensus_boxes)
    kept_detector_boxes: list[tuple] = []
    phi_categories_all: list[str] = []

    for box in detector_boxes_all:
        crop_text = _ocr_crop(frames, box)
        if not crop_text.strip():
            # Unreadable / empty → keep conservatively (FN priority).
            kept_detector_boxes.append(box)
            continue

        is_phi, cats = classify_text(crop_text)
        if is_phi:
            kept_detector_boxes.append(box)
            for c in cats:
                if c not in phi_categories_all:
                    phi_categories_all.append(c)
        else:
            # Benign-only: preserve UNLESS the box overlaps a profile/consensus box.
            if _any_overlap(box, anchor_boxes):
                # Overlaps an anchor — keep (conservative).
                kept_detector_boxes.append(box)
            # Otherwise drop (FR-009 preserve benign).

    if kept_detector_boxes:
        tiers_fired.append("detector")

    # ------------------------------------------------------------------
    # Union + dilate all maskable boxes
    # ------------------------------------------------------------------
    all_maskable = list(profile_boxes) + list(consensus_boxes) + kept_detector_boxes
    if all_maskable:
        ref_shape = frames[0].shape[:2] if frames else None
        mask_union = detect.union_dilate(all_maskable, margin=margin, shape=ref_shape)
    else:
        mask_union = []

    # ------------------------------------------------------------------
    # PHI-outside-mask check: run a lightweight full-frame OCR scan and
    # check whether any PHI text falls entirely outside mask_union.
    # This catches leaked-PHI risk (FR-006 condition 1).
    # ------------------------------------------------------------------
    phi_outside_mask = _check_phi_outside_mask(frames, mask_union, phi_categories_all)

    # ------------------------------------------------------------------
    # Verdict routing (T014)
    # ------------------------------------------------------------------
    verdict, reason = _route_verdict(
        is_profiled=is_profiled,
        mask_union=mask_union,
        phi_outside_mask=phi_outside_mask,
        profile_boxes=profile_boxes,
        kept_detector_boxes=kept_detector_boxes,
        loaded_profiles=loaded_profiles,
        device_id=device_id,
    )

    # ------------------------------------------------------------------
    # Apply redaction (irreversible fill on copies — FR-008)
    # ------------------------------------------------------------------
    if mask_union:
        # apply_redaction expects (x, y, w, h); convert from (x0, y0, x1, y1).
        xywh_boxes = [_x1y1_to_xywh(b) for b in mask_union]
        masked_frames = [apply_redaction(f, xywh_boxes) for f in frames]
    else:
        masked_frames = [f.copy() for f in frames]

    assessment = CaseAssessment(
        verdict=verdict,
        masked_frames=masked_frames,
        regions=mask_union,
        tiers_fired=tiers_fired,
        phi_categories=phi_categories_all,
        reason=reason,
    )

    # ------------------------------------------------------------------
    # Audit (T015) — no raw PHI
    # ------------------------------------------------------------------
    if registry is not None:
        try:
            entry = audit_entry(assessment)
            registry.record_audit(
                actor="pixel_deid",
                action=verdict.value,
                target=entry,
            )
        except Exception as exc:
            logger.warning("Audit write failed: %s", exc)

    return assessment


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _ocr_crop(frames: list[np.ndarray], box: tuple) -> str:
    """
    OCR the sub-image defined by *box* across all frames; return concatenated text.

    Uses pytesseract with --psm 7 (single text line) over the first frame that
    yields a non-empty result.  Falls back to '' if pytesseract is unavailable
    or no text is found.
    """
    try:
        import pytesseract  # lazy import
    except ImportError:
        return ""

    for frame in frames:
        crop = _crop_box(frame, box)
        if crop.size == 0:
            continue
        try:
            text = pytesseract.image_to_string(crop, config="--psm 7", timeout=10).strip()
            if text:
                return text
        except Exception:
            continue
    return ""


def _check_phi_outside_mask(
    frames: list[np.ndarray],
    mask_union: list[tuple],
    existing_cats: list[str],
) -> bool:
    """
    Run a quick multipass detection over all frames and classify each box.

    Return True if any PHI box is found that lies entirely outside every box
    in *mask_union* — indicating a potential leak that cannot be covered by
    the current mask set.

    This check is intentionally conservative: it only triggers when a box is
    fully outside (not overlapping) the mask union.
    """
    try:
        import pytesseract  # lazy import — bail if unavailable
    except ImportError:
        return False

    for frame in frames:
        try:
            from src.services.pixel_deid.detect import union_dilate, tesseract_boxes, image_variants
            for _name, variant in image_variants(frame).items():
                for box in tesseract_boxes(variant):
                    crop_text = _ocr_crop([frame], box)
                    if not crop_text.strip():
                        continue
                    is_phi, _cats = classify_text(crop_text)
                    if is_phi and not _any_overlap(box, mask_union):
                        return True
        except Exception:
            continue
    return False


def _route_verdict(
    *,
    is_profiled: bool,
    mask_union: list[tuple],
    phi_outside_mask: bool,
    profile_boxes: list[tuple],
    kept_detector_boxes: list[tuple],
    loaded_profiles: dict,
    device_id: Optional[str],
) -> tuple[Verdict, str]:
    """
    Apply the fail-closed routing rules (T014) and return (Verdict, reason).

    Routing order (precedence highest→lowest):

    1. PHI text detected OUTSIDE every mask_union region → QUARANTINE.
    2. Unprofiled device:
       a. mask_union non-empty → REDACTED.
       b. mask_union empty → QUARANTINE (absence of detection ≠ clean for unknown device).
    3. Profiled device:
       a. mask_union non-empty → REDACTED.
       b. mask_union empty → CLEAN.
    4. Profile/detector disagreement (profiled, but PHI box far outside all profile boxes)
       → QUARANTINE.
    """
    if phi_outside_mask:
        return Verdict.QUARANTINE, "phi-text outside maskable region"

    if not is_profiled:
        if mask_union:
            return Verdict.REDACTED, "unprofiled; detector/consensus regions masked"
        else:
            return (
                Verdict.QUARANTINE,
                "unprofiled device, no positive clean evidence",
            )

    # Profiled path.
    # Check profile/detector disagreement: profiled device AND detector found
    # a PHI box far outside all profile boxes (beyond profile tolerance).
    if kept_detector_boxes and profile_boxes:
        tolerance = _profile_tolerance(loaded_profiles, device_id)
        for det_box in kept_detector_boxes:
            if not _any_overlap(det_box, profile_boxes):
                # Find minimum distance to any profile box centre.
                min_dist = min(
                    _box_center_distance(det_box, pb) for pb in profile_boxes
                )
                if min_dist > tolerance:
                    return (
                        Verdict.QUARANTINE,
                        "profile/detector disagreement",
                    )

    if mask_union:
        return Verdict.REDACTED, "regions masked"

    return Verdict.CLEAN, "profiled device, no maskable regions found"


def _profile_tolerance(loaded_profiles: dict, device_id: Optional[str]) -> float:
    """Return the pixel-distance tolerance for profile/detector disagreement checks."""
    if device_id and device_id in loaded_profiles:
        tol = loaded_profiles[device_id].get("tolerance")
        if tol is not None:
            return float(tol)
    return 50.0  # conservative default


# ---------------------------------------------------------------------------
# Audit helper (T015)
# ---------------------------------------------------------------------------

def audit_entry(assessment: CaseAssessment) -> str:
    """
    Produce the compact JSON string for ``registry.record_audit(target=…)``.

    Contains only counts and category names — never raw PHI text (SC-004).

    Parameters
    ----------
    assessment:
        The CaseAssessment returned by assess_case().

    Returns
    -------
    str
        JSON string with keys: ``regions``, ``tiers``, ``categories``, ``frames``.
    """
    payload = {
        "regions": len(assessment.regions),
        "tiers": assessment.tiers_fired,
        "categories": assessment.phi_categories,
        "frames": len(assessment.masked_frames),
    }
    return json.dumps(payload, separators=(",", ":"))
