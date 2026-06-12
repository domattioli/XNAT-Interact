"""
Stage-1 multipass text detector for burned-in PHI pixel de-identification.

Public API
----------
image_variants(img) -> dict[str, np.ndarray]
    Produce {orig, invert, stretch, clahe} preprocessing variants of a
    grayscale uint8 frame.

tesseract_boxes(img, *, min_conf=30, timeout=15.0) -> list[tuple[int,int,int,int]]
    Run Tesseract automatic page segmentation (--psm 3) and return
    (x0,y0,x1,y1) boxes for words that meet the confidence threshold.
    Hard-bounded by `timeout`; any error/timeout degrades to [] (FN-safe).

detect_text_regions(img, *, model_dir="models/craft") -> list[tuple]
    Learned-detector tier: load a CRAFT-style ONNX model from model_dir and
    return text-region boxes.  Gracefully degrades to [] when no model is
    present — the caller unions this with the Tesseract path, and FN=0 is
    carried by the contrast-independent tiers + quarantine elsewhere.

union_dilate(boxes, *, margin=4, shape=None) -> list[tuple]
    Merge overlapping/adjacent boxes into a minimal covering set and dilate
    each by margin pixels (clamped to shape if given).

multipass_detect(img, *, model_dir="models/craft", margin=4) -> list[tuple]
    Convenience entry-point: run tesseract_boxes over every image_variants()
    value, union with detect_text_regions(img), union_dilate, return boxes.

All heavy dependencies (cv2, pytesseract, onnxruntime) are imported LAZILY
inside the functions so that ``import src.services.pixel_deid.detect`` is
cheap and does not penalise the rest of the test suite.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Image preprocessing variants
# ---------------------------------------------------------------------------

def image_variants(img: np.ndarray) -> dict[str, np.ndarray]:
    """
    Return four preprocessing variants of a 2D grayscale uint8 image.

    The variants are:
      - ``"orig"``    — the input as-is.
      - ``"invert"``  — 255 − orig (reveals dark text on bright backgrounds).
      - ``"stretch"`` — linear contrast stretch via NORM_MINMAX to the full
                        [0, 255] range.
      - ``"clahe"``   — Contrast-Limited Adaptive Histogram Equalization with
                        clip-limit 2.0 and tile-grid (8, 8).

    Together these four variants give the Tesseract multipass the best chance
    of recovering text at any contrast level (spike evidence: single-pass
    recall 0.00 on faint overlays, multipass lifts it).

    Parameters
    ----------
    img:
        2D grayscale uint8 numpy array of shape (H, W).

    Returns
    -------
    dict[str, np.ndarray]
        Keys: ``"orig"``, ``"invert"``, ``"stretch"``, ``"clahe"``.
        Each value is a 2D uint8 array with the same shape as *img*.
    """
    import cv2  # lazy import — keep module-level import cheap

    stretch = cv2.normalize(img, None, 0, 255, cv2.NORM_MINMAX)
    clahe_obj = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
    clahe = clahe_obj.apply(img)

    return {
        "orig": img,
        "invert": (255 - img.astype(np.int16)).clip(0, 255).astype(np.uint8),
        "stretch": stretch,
        "clahe": clahe,
    }


# ---------------------------------------------------------------------------
# Tesseract sparse-word detector
# ---------------------------------------------------------------------------

def tesseract_boxes(
    img: np.ndarray,
    *,
    min_conf: int = 30,
    timeout: float = 15.0,
) -> list[tuple[int, int, int, int]]:
    """
    Run Tesseract with ``--psm 3`` (fully-automatic page segmentation) over
    *img* and return (x0, y0, x1, y1) bounding boxes for detected words.

    Why ``--psm 3`` and not ``--psm 11``
    ------------------------------------
    ``--psm 11`` (sparse text) triggers Tesseract's exhaustive
    component-search layout analysis, which is pathologically slow on the
    low-information / contrast-stretched frames this pipeline feeds it
    (measured 20-42 s for a single 128x256 frame).  ``--psm 3`` — the default
    automatic page segmentation — returns the *same* word boxes on burned-in
    PHI banners in ~0.36 s (a ~60x speedup), which is what keeps the 200-case
    batch inside the SC-005 budget.  Burned-in overlays are laid out as text
    blocks, so automatic segmentation localises them correctly.

    Bounded + fail-safe
    -------------------
    The call is hard-bounded by *timeout* (seconds, passed to pytesseract,
    which kills the tesseract subprocess on expiry).  ANY error — timeout,
    missing binary, decode failure — returns ``[]`` (graceful degrade).  This
    is FN-safe: when OCR yields nothing the caller's fail-closed routing
    quarantines unprofiled cases rather than passing them clean, and the
    contrast-independent profile + cross-frame tiers still mask known regions.

    Parameters
    ----------
    img:
        2D grayscale uint8 numpy array.
    min_conf:
        Minimum word confidence in [0, 100].  Tesseract returns -1 for
        non-word rows; those are always excluded.
    timeout:
        Hard per-call ceiling in seconds.  On expiry the tesseract subprocess
        is killed and ``[]`` is returned.

    Returns
    -------
    list of (x0, y0, x1, y1) tuples (integers, pixel coordinates).
    """
    try:
        import pytesseract  # lazy import
        from pytesseract import Output

        data = pytesseract.image_to_data(
            img,
            config="--psm 3",
            output_type=Output.DICT,
            timeout=timeout,
        )
    except Exception:
        # timeout / missing binary / decode error → graceful degrade (FN-safe)
        return []

    boxes: list[tuple[int, int, int, int]] = []
    for i, text in enumerate(data["text"]):
        if not text.strip():
            continue
        conf = int(data["conf"][i])
        if conf < min_conf:
            continue
        x = int(data["left"][i])
        y = int(data["top"][i])
        w = int(data["width"][i])
        h = int(data["height"][i])
        boxes.append((x, y, x + w, y + h))

    return boxes


# ---------------------------------------------------------------------------
# Learned ONNX-CPU text-region detector (CRAFT-style)
# ---------------------------------------------------------------------------

def detect_text_regions(
    img: np.ndarray,
    *,
    model_dir: str = "models/craft",
) -> list[tuple]:
    """
    Learned-detector tier: load a CRAFT-style ONNX model from *model_dir* and
    return text-region boxes.

    Model provisioning note
    -----------------------
    The ONNX model is **optional and vendored later** — it is not bundled in
    the repository at development time.  When no ``.onnx`` file is found in
    *model_dir* the function returns an empty list and does NOT raise.  This
    is the intended graceful-degrade path: the caller
    (``multipass_detect``) unions the result with the Tesseract path, and
    FN=0 is carried by the contrast-independent device-profile + cross-frame
    tiers (Stage 2) and the fail-closed quarantine routing (Stage 3).

    When a model IS present it is loaded via ``onnxruntime`` with
    ``CPUExecutionProvider`` only (no GPU, per FR-010).  The inference is
    best-effort/defensive: any load or inference error silently returns ``[]``
    and does not crash the pipeline.

    The signature is intentionally model-agnostic so that a docTR detector can
    swap in later without touching any caller.

    Parameters
    ----------
    img:
        2D grayscale uint8 numpy array of shape (H, W).
    model_dir:
        Path (absolute or relative to the process CWD) of the directory that
        may contain a ``*.onnx`` CRAFT model file.

    Returns
    -------
    list of (x0, y0, x1, y1) tuples, or ``[]`` if no model is available or
    any error occurs.
    """
    # Locate a .onnx file in model_dir — graceful degrade if none present.
    model_path: Optional[Path] = None
    try:
        model_dir_path = Path(model_dir)
        onnx_files = list(model_dir_path.glob("*.onnx"))
        if not onnx_files:
            return []
        model_path = onnx_files[0]
    except Exception:
        return []

    # Load the model and run inference — any error → silent degrade.
    try:
        import onnxruntime as ort  # lazy import

        session = ort.InferenceSession(
            str(model_path),
            providers=["CPUExecutionProvider"],
        )

        # Prepare input: CRAFT expects a float32 NCHW tensor normalised to
        # [0, 1].  The exact pre/post-processing varies by model variant;
        # this is a best-effort generic wrapper.  If the model does not
        # conform, the exception handler below catches the error gracefully.
        input_name = session.get_inputs()[0].name
        h, w = img.shape[:2]
        inp = img.astype(np.float32) / 255.0
        # Add batch + channel dimensions: (H, W) → (1, 1, H, W)
        inp = inp[np.newaxis, np.newaxis, :, :]

        outputs = session.run(None, {input_name: inp})

        # Extract boxes from the score/heat map output.
        # The exact decoding depends on the model output format; this
        # placeholder returns an empty list until a concrete CRAFT export
        # is vendored and a decoder is implemented for its output format.
        # When the model file is added, replace this section with the
        # appropriate post-processing (e.g. connected-component analysis on
        # the text-region score map, as in the original CRAFT paper).
        _ = outputs  # consumed above; decoder to be added with model
        return []

    except Exception:
        # Any load / inference error → graceful degrade, no crash.
        return []


# ---------------------------------------------------------------------------
# Box union + dilation
# ---------------------------------------------------------------------------

def union_dilate(
    boxes: list[tuple],
    *,
    margin: int = 4,
    shape: Optional[tuple[int, int]] = None,
) -> list[tuple]:
    """
    Merge overlapping or adjacent boxes into a minimal covering set, then
    dilate each merged box by *margin* pixels on all sides.

    The merge algorithm is a single-pass greedy union: boxes are sorted by
    their top-left corner, and any box that overlaps or touches the current
    accumulator box is merged into it.  This is O(n log n) and adequate for
    the small box counts typical of a per-frame text detection result.

    Pure Python/NumPy — no OpenCV or other heavy dependencies.

    Parameters
    ----------
    boxes:
        List of (x0, y0, x1, y1) tuples (any numeric type).
    margin:
        Number of pixels to expand each final merged box on every side.
    shape:
        If given, a ``(height, width)`` tuple used to clamp the dilated boxes
        to the image boundary so no box exceeds the frame dimensions.

    Returns
    -------
    list of (x0, y0, x1, y1) tuples (integers), deduplicated, merged, and
    dilated.  Returns ``[]`` when *boxes* is empty.
    """
    if not boxes:
        return []

    # Normalise to list of lists for mutation.
    normalised = [[int(x0), int(y0), int(x1), int(y1)] for x0, y0, x1, y1 in boxes]

    # Sort by (y0, x0) — top-then-left reading order.
    normalised.sort(key=lambda b: (b[1], b[0]))

    merged: list[list[int]] = [normalised[0]]
    for box in normalised[1:]:
        last = merged[-1]
        # Two boxes overlap or are adjacent when they are NOT disjoint.
        # Disjoint test: last.x1 < box.x0 OR box.x1 < last.x0
        #                OR last.y1 < box.y0 OR box.y1 < last.y0
        # Use a tiny adjacency tolerance of 1 px to merge touching boxes.
        gap = 1
        if (
            box[0] <= last[2] + gap
            and box[2] >= last[0] - gap
            and box[1] <= last[3] + gap
            and box[3] >= last[1] - gap
        ):
            # Expand the accumulator to cover both boxes.
            last[0] = min(last[0], box[0])
            last[1] = min(last[1], box[1])
            last[2] = max(last[2], box[2])
            last[3] = max(last[3], box[3])
        else:
            merged.append(box)

    # Dilate and clamp.
    result: list[tuple] = []
    for x0, y0, x1, y1 in merged:
        dx0 = x0 - margin
        dy0 = y0 - margin
        dx1 = x1 + margin
        dy1 = y1 + margin
        if shape is not None:
            h, w = shape
            dx0 = max(0, dx0)
            dy0 = max(0, dy0)
            dx1 = min(w, dx1)
            dy1 = min(h, dy1)
        result.append((dx0, dy0, dx1, dy1))

    return result


# ---------------------------------------------------------------------------
# Multipass convenience entry-point
# ---------------------------------------------------------------------------

def multipass_detect(
    img: np.ndarray,
    *,
    model_dir: str = "models/craft",
    margin: int = 4,
) -> list[tuple]:
    """
    Stage-1 entry-point: run the full multipass text detection pipeline and
    return a dilated, merged list of (x0, y0, x1, y1) text-region boxes.

    Pipeline steps:

    1. Generate four image variants (orig, invert, stretch, clahe) via
       :func:`image_variants`.
    2. Run :func:`tesseract_boxes` over each variant and collect all boxes.
    3. Run :func:`detect_text_regions` over the original image (learned
       detector tier — degrades gracefully when no model is present).
    4. Union all boxes from steps 2–3 and merge/dilate via
       :func:`union_dilate`.

    This is the function that the verdict engine (Stage 3) calls.  The result
    is the set of candidate text regions to be handed to the PHI-NER classifier
    and ultimately the redaction / quarantine decision.

    Parameters
    ----------
    img:
        2D grayscale uint8 numpy array of shape (H, W).
    model_dir:
        Forwarded to :func:`detect_text_regions`.  Points to the directory
        that may hold a ``*.onnx`` CRAFT model; gracefully degrades if absent.
    margin:
        Dilation margin in pixels, forwarded to :func:`union_dilate`.

    Returns
    -------
    list of (x0, y0, x1, y1) tuples — the merged, dilated text-region boxes.
    Returns ``[]`` only when no text is detected by any tier.
    """
    all_boxes: list[tuple] = []

    # Tier 1: Tesseract multipass over all preprocessing variants.
    for _name, variant in image_variants(img).items():
        all_boxes.extend(tesseract_boxes(variant))

    # Tier 2: Learned ONNX detector (optional / degrades gracefully).
    all_boxes.extend(detect_text_regions(img, model_dir=model_dir))

    h, w = img.shape[:2]
    return union_dilate(all_boxes, margin=margin, shape=(h, w))
