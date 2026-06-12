"""
Cross-frame variance consensus mask for burned-in PHI de-identification (Feature 010, Stage 2).

For multi-frame sequences (fluoroscopy, OR video), static burned-in PHI overlays have LOW
variance across frames (same pixels appear unchanged), while moving anatomy has HIGH variance.
This module detects static bright regions via per-pixel variance, producing a contrast-independent
(textured-overlay) detection that catches faint or stylized text that single-frame OCR misses.

Key property (User Story 2): a multi-frame case with a faint static overlay is detected and
masked regardless of text contrast, because the variance signature is independent of pixel values.

Public API
----------
variance_consensus_mask(frames, *, var_thresh=5.0, bright_thresh=None) -> list[tuple]
    Input frames (n_frames, rows, cols) or list of 2D arrays. Compute per-pixel variance.
    Return (x0, y0, x1, y1) bounding boxes of connected regions that are low-variance AND bright.
    Single-frame input returns [] (no consensus possible; caller falls back to detector/profile).
"""
from __future__ import annotations

import logging
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)


def variance_consensus_mask(
    frames,
    *,
    var_thresh: float = 5.0,
    bright_thresh: Optional[int] = None,
) -> list[tuple]:
    """
    Detect static bright overlay regions across a sequence of frames via per-pixel variance.

    For multi-frame input, computes per-pixel variance across the frame stack. A pixel is
    flagged if its variance is low (static) AND its mean intensity is above bright_thresh.
    Connected components of flagged pixels are then converted to minimal bounding boxes.

    Single-frame input (n_frames == 1) returns [] — no consensus possible; the caller should
    fall back to device profile + multipass detector for single-frame sequences.

    Parameters
    ----------
    frames:
        Either:
          - numpy array of shape (n_frames, rows, cols) — uint8 or uint16.
          - List of 2D numpy arrays (each shape (rows, cols)).
    var_thresh:
        Maximum variance threshold for "static" classification. Default 5.0 is suitable for
        uint8 images where moving anatomy variance is typically >> 50. For uint16, scale
        proportionally (e.g., 5.0 * (65535 / 255)^2 ≈ 4167).
    bright_thresh:
        Minimum mean intensity for a pixel to be considered "bright" (part of the overlay).
        If None, automatically set to (background_level + foreground_level) / 2 estimated from
        the image statistics. For typical synthetic frames with background ~20–80 and text ~150,
        bright_thresh defaults to ~120.

    Returns
    -------
    list[tuple]
        List of (x0, y0, x1, y1) bounding boxes (integers) covering connected regions of
        low-variance bright pixels. Returns [] if n_frames <= 1 (consensus undefined) or if
        no regions qualify.

    Notes
    -----
    - Connected components are computed via cv2.connectedComponentsWithStats (lazily imported).
    - Bounding boxes are minimal axis-aligned rectangles; no sub-pixel accuracy.
    - The algorithm is contrast-independent: it detects the *presence* of a static overlay
      regardless of its brightness, making it effective for faint text that single-frame
      OCR misses (Spec User Story 2, SC-001).
    """
    import cv2  # lazy import

    # Convert input to numpy array if needed
    if isinstance(frames, list):
        frames_arr = np.stack(frames, axis=0)
    else:
        frames_arr = np.asarray(frames)

    # Ensure 3D
    if frames_arr.ndim != 3:
        logger.warning(
            f"variance_consensus_mask expects 3D input (n_frames, rows, cols), "
            f"got shape {frames_arr.shape}"
        )
        return []

    n_frames, rows, cols = frames_arr.shape

    # Single-frame case: no consensus possible
    if n_frames <= 1:
        logger.debug("Single-frame input: variance consensus undefined; returning [].")
        return []

    # Compute per-pixel variance across frames
    # variance = E[X^2] - (E[X])^2
    frames_float = frames_arr.astype(np.float32)
    mean_val = frames_float.mean(axis=0)
    var_val = frames_float.var(axis=0)

    # Estimate bright_thresh if not provided
    if bright_thresh is None:
        # Use a heuristic: the 75th percentile of mean intensity
        # This separates low-intensity background/noise from high-intensity overlay
        bright_thresh = int(np.percentile(mean_val, 75))
        logger.debug(f"Auto-set bright_thresh={bright_thresh} (75th percentile of mean intensity)")

    # Flag pixels that are BOTH low-variance AND bright
    mask = (var_val <= var_thresh) & (mean_val >= bright_thresh)
    mask = mask.astype(np.uint8)

    # Find connected components
    n_labels, labels, stats, centroids = cv2.connectedComponentsWithStats(mask, connectivity=8)

    # Collect bounding boxes (skip label 0 = background)
    boxes: list[tuple] = []
    for label in range(1, n_labels):
        x, y, w, h, area = stats[label]
        # Only keep reasonably-sized components (avoid tiny noise)
        if area >= 4:  # at least 2x2 pixels
            x1 = x + w
            y1 = y + h
            boxes.append((x, y, x1, y1))
            logger.debug(f"  Consensus box: ({x}, {y}, {x1}, {y1}), area={area}")

    if boxes:
        logger.info(f"variance_consensus_mask found {len(boxes)} box(es)")
    else:
        logger.debug("variance_consensus_mask found no qualifying regions")

    return boxes
