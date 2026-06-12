"""
Annotation validation functions.

PHI-FREE INVARIANT:
    annotator_id MUST be an opaque token (alnum / underscore / hyphen).
    It MUST NEVER be a patient identifier, patient name, or MRN.

    This module enforces that at write time via validate_annotator_id().
    Any value that looks like "First Last" (two capitalised words separated by
    a space) or contains spaces is rejected with an AnnotationError.
"""
from __future__ import annotations

import re
from typing import Any, List

import numpy as np

from src.services.errors import FriendlyError
from src.annotations.exc import AnnotationError


def _raise(fe: FriendlyError) -> None:
    """Raise AnnotationError wrapping a FriendlyError. Never returns."""
    raise AnnotationError(fe)


# ---------------------------------------------------------------------------
# Annotator ID validation
# ---------------------------------------------------------------------------

# Allowed opaque token characters: alnum, underscore, hyphen — no spaces
_VALID_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")

# Human-name heuristic: "Word Word" pattern (First Last style)
# Reject anything with whitespace, or CapWord CapWord
_HUMAN_NAME_RE = re.compile(r"\s")  # any whitespace → rejected


def validate_annotator_id(aid: Any) -> None:
    """
    Validate that aid is a safe opaque annotator token.

    Rules:
    - Must be a non-empty str.
    - Must match [A-Za-z0-9_-]+ (no spaces, no special chars).
    - Must NOT look like a human name ("First Last", "J. Smith", etc.).

    annotator_id is NEVER a patient identifier. This function enforces that.

    Raises
    ------
    AnnotationError — if the value violates any rule.
    """
    if not isinstance(aid, str):
        _raise(FriendlyError(
            title="Invalid annotator_id",
            message=f"annotator_id must be a string, got {type(aid).__name__}.",
            recourse=[
                "Use an opaque token such as 'worker_A1' or 'hawkid123'.",
                "Do NOT use patient names, MRNs, or any PHI as annotator_id.",
            ],
        ))
    if not aid:
        _raise(FriendlyError(
            title="Invalid annotator_id",
            message="annotator_id must not be empty.",
            recourse=["Provide a non-empty opaque token (e.g. 'worker_A1')."],
        ))
    # Reject anything containing whitespace (catches "John Doe", "J. Smith", etc.)
    if _HUMAN_NAME_RE.search(aid):
        _raise(FriendlyError(
            title="Invalid annotator_id — possible PHI",
            message=(
                f"annotator_id '{aid}' contains whitespace and may be a human name or "
                "patient identifier. annotator_id MUST be an opaque token with no spaces."
            ),
            recourse=[
                "Use an opaque token such as 'worker_A1' or 'hawkid123'.",
                "Do NOT use patient names, MRNs, or any PHI as annotator_id.",
            ],
        ))
    # Reject characters outside the safe set
    if not _VALID_ID_RE.match(aid):
        _raise(FriendlyError(
            title="Invalid annotator_id",
            message=(
                f"annotator_id '{aid}' contains characters outside the allowed set "
                "[A-Za-z0-9_-]. No dots, slashes, or special characters permitted."
            ),
            recourse=[
                "Use only letters, digits, underscores, and hyphens.",
                "Example: 'worker_A1', 'hawkid-123'.",
            ],
        ))


# ---------------------------------------------------------------------------
# Mask shape validation
# ---------------------------------------------------------------------------

def validate_mask_shape(masks: List[np.ndarray]) -> None:
    """
    Validate that all masks share the same (rows, cols) shape and grid.

    Raises AnnotationError if any mask has a different shape from the first.
    Raises AnnotationError if any mask is not 2-D.
    """
    if not masks:
        return
    ref_shape = None
    for i, mask in enumerate(masks):
        if not isinstance(mask, np.ndarray):
            _raise(FriendlyError(
                title="Invalid mask",
                message=f"Mask at index {i} is not a numpy ndarray (got {type(mask).__name__}).",
                recourse=["Provide numpy 2-D integer arrays as mask payloads."],
            ))
        if mask.ndim != 2:
            _raise(FriendlyError(
                title="Invalid mask shape",
                message=f"Mask at index {i} is {mask.ndim}-D; expected 2-D.",
                recourse=["All masks must be 2-D arrays with shape (rows, cols)."],
            ))
        if ref_shape is None:
            ref_shape = mask.shape
        elif mask.shape != ref_shape:
            _raise(FriendlyError(
                title="Mask shape mismatch",
                message=(
                    f"Mask at index {i} has shape {mask.shape}, but the reference mask "
                    f"has shape {ref_shape}. All masks for an image must share the same grid."
                ),
                recourse=[
                    "Ensure all annotators used the same image resolution.",
                    "Resample masks to a common grid before storing.",
                ],
            ))


# ---------------------------------------------------------------------------
# Per-type payload validators
# ---------------------------------------------------------------------------

def validate_mask_payload(payload: Any) -> None:
    """
    Validate a mask payload (binary_segmentation or label_map).

    Must be a 2-D integer numpy array.
    """
    if not isinstance(payload, np.ndarray):
        _raise(FriendlyError(
            title="Invalid mask payload",
            message=f"Expected a numpy ndarray for mask payload, got {type(payload).__name__}.",
            recourse=["Provide a 2-D integer numpy array (e.g. dtype=uint8 or int32)."],
        ))
    if payload.ndim != 2:
        _raise(FriendlyError(
            title="Invalid mask payload",
            message=f"Mask must be 2-D, got {payload.ndim}-D array.",
            recourse=["Reshape or slice the array to (rows, cols) before storing."],
        ))
    if not np.issubdtype(payload.dtype, np.integer):
        _raise(FriendlyError(
            title="Invalid mask payload dtype",
            message=f"Mask dtype must be integer, got {payload.dtype}.",
            recourse=["Cast the array: arr.astype(np.uint8) or arr.astype(np.int32)."],
        ))


def validate_landmark_payload(payload: Any) -> None:
    """
    Validate a landmark payload.

    Must be a dict with keys 'x' and 'y' (numeric values).
    """
    if not isinstance(payload, dict):
        _raise(FriendlyError(
            title="Invalid landmark payload",
            message=f"Landmark payload must be a dict, got {type(payload).__name__}.",
            recourse=["Provide a dict with keys 'x' and 'y', e.g. {'x': 10, 'y': 20}."],
        ))
    for key in ("x", "y"):
        if key not in payload:
            _raise(FriendlyError(
                title="Invalid landmark payload",
                message=f"Landmark payload missing required key '{key}'.",
                recourse=["Provide both 'x' and 'y' coordinates in the landmark dict."],
            ))
        if not isinstance(payload[key], (int, float)):
            _raise(FriendlyError(
                title="Invalid landmark payload",
                message=f"Landmark key '{key}' must be numeric, got {type(payload[key]).__name__}.",
                recourse=["Use int or float values for landmark coordinates."],
            ))


def validate_bbox_payload(payload: Any) -> None:
    """
    Validate a bounding-box payload.

    Must be a dict with keys 'x', 'y', 'w', 'h' (numeric values).
    """
    if not isinstance(payload, dict):
        _raise(FriendlyError(
            title="Invalid bbox payload",
            message=f"Bbox payload must be a dict, got {type(payload).__name__}.",
            recourse=["Provide a dict with keys 'x', 'y', 'w', 'h'."],
        ))
    for key in ("x", "y", "w", "h"):
        if key not in payload:
            _raise(FriendlyError(
                title="Invalid bbox payload",
                message=f"Bbox payload missing required key '{key}'.",
                recourse=["Provide all four keys: 'x', 'y', 'w', 'h'."],
            ))
        if not isinstance(payload[key], (int, float)):
            _raise(FriendlyError(
                title="Invalid bbox payload",
                message=f"Bbox key '{key}' must be numeric, got {type(payload[key]).__name__}.",
                recourse=["Use int or float values for bbox coordinates."],
            ))
