"""
annotations.importers.generic — generic mask-array importer.

Converts a raw numpy mask array from any tool into a canonical Annotation.

PHI-FREE: annotator_id is validated as an opaque token (no patient names).
"""
from __future__ import annotations

from typing import Optional

import numpy as np

from src.services.errors import FriendlyError
from src.annotations.exc import AnnotationError
from src.annotations.model import Annotation
from src.annotations.validate import validate_annotator_id, validate_mask_payload


def _raise(fe: FriendlyError) -> None:
    raise AnnotationError(fe)


def from_mask_array(
    arr: np.ndarray,
    *,
    annotator_id: str,
    tool: str,
    annotation_type: Optional[str] = None,
    created_at: Optional[str] = None,
    version: int = 1,
) -> Annotation:
    """
    Convert a raw 2-D numpy mask array into a canonical Annotation.

    Parameters
    ----------
    arr             : 2-D integer numpy array (uint8 for binary, int32 for multi-label).
    annotator_id    : Opaque token — validated; must not be a human name or PHI.
    tool            : Tool name string (e.g. 'labelbox_v2', 'itk-snap').
    annotation_type : 'binary_segmentation' or 'label_map'.
                      If None, inferred: 2 unique values → 'binary_segmentation',
                      >2 unique values → 'label_map'.
    created_at      : ISO-8601 timestamp string. Defaults to UTC now if None.
    version         : Integer version, default 1.

    Returns
    -------
    Annotation with validated payload (not encoded — call .encode() if blob needed).

    Raises
    ------
    AnnotationError on bad input (non-2D array, bad annotator_id, unknown type, etc.).
    """
    # Validate annotator_id first — PHI guard
    try:
        validate_annotator_id(annotator_id)
    except AnnotationError:
        raise

    # Validate array shape/dtype
    try:
        validate_mask_payload(arr)
    except AnnotationError:
        raise

    # Infer annotation_type if not provided
    if annotation_type is None:
        n_unique = len(np.unique(arr))
        if n_unique <= 2:
            annotation_type = "binary_segmentation"
        else:
            annotation_type = "label_map"
    else:
        # Validate that the requested type is a known mask type
        from src.annotations.registry import get_type
        try:
            entry = get_type(annotation_type)
        except AnnotationError:
            raise
        # Only mask types (rle codec) are valid for mask arrays
        if entry.codec != "rle":
            _raise(FriendlyError(
                title="Wrong annotation type for mask array",
                message=(
                    f"annotation_type '{annotation_type}' uses codec '{entry.codec}', "
                    "but mask arrays require the 'rle' codec "
                    "(i.e. 'binary_segmentation' or 'label_map')."
                ),
                recourse=[
                    "Use annotation_type='binary_segmentation' or 'label_map' for mask arrays.",
                    "Use from_mask_array only for 2-D pixel masks.",
                ],
            ))

    # Normalise to integer dtype if needed
    if not np.issubdtype(arr.dtype, np.integer):
        _raise(FriendlyError(
            title="Invalid mask dtype",
            message=f"Mask array dtype must be integer, got {arr.dtype}.",
            recourse=["Cast: arr.astype(np.uint8) for binary, arr.astype(np.int32) for multi-label."],
        ))

    # Default created_at to UTC now
    if created_at is None:
        from datetime import datetime, timezone
        created_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    return Annotation(
        annotator_id=annotator_id,
        tool=tool,
        annotation_type=annotation_type,
        created_at=created_at,
        version=version,
        payload=arr,
    )
