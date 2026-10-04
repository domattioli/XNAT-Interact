"""
app/logic/annotations — pure annotation logic, NO streamlit import.

Tested offline against FakeXNAT.  All server I/O is injected via the
``server`` parameter (real pyxnat.Interface in production; FakeXNAT in tests).

Public API
----------
list_image_annotations(server, image_ref, dest_dir=None)
    -> AnnotationSet | FriendlyError
    Download AnnotationSet for image_ref from XNAT. Uses a tmp dir if no dest.

import_annotations(items)
    -> list[Annotation]
    Dispatch each item to the right importer by source kind:
      - dict with 'WorkerId' key → mturk.from_mturk_row
      - dict with 'mask_array' key → generic.from_mask_array
      - Annotation → pass through

run_consensus(annotation_set, annotation_type, aggregator_name=None)
    -> ConsensusResult | FriendlyError
    Aggregate via aggregate.aggregate_set.
    aggregator_name=None → uses the type's registered default.
    list_aggregator_names() exposes available names.

upload_annotations(server, image_ref, annotation_set)
    -> UploadResult
    Delegate to io_xnat.upload_annotation_set.

list_aggregator_names()
    -> list[str]
    Introspect registered aggregators (thin wrapper).
"""
from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any, List, Optional, Union

from src.services.errors import FriendlyError, handle as _handle
from src.annotations.model import Annotation, AnnotationSet, ConsensusResult
from src.annotations.io_xnat import (
    ANNOTATIONS_LABEL,
    download_annotation_set,
    list_annotation_resources,
    upload_annotation_set,
    UploadResult,
)
from src.annotations.aggregate import list_aggregators, aggregate_set
from src.annotations.exc import AnnotationError


# ---------------------------------------------------------------------------
# list_aggregator_names — thin introspection wrapper
# ---------------------------------------------------------------------------

def list_aggregator_names() -> List[str]:
    """Return sorted list of all registered aggregator names."""
    return list_aggregators()


# ---------------------------------------------------------------------------
# list_image_annotations
# ---------------------------------------------------------------------------

def load_annotation_sets(
    server: Any,
    image_ref: str,
    dest_dir: Optional[Any] = None,
) -> Union[AnnotationSet, FriendlyError]:
    """
    Download every annotation set on *image_ref* and merge them into one (spec 019, FR-019).

    People's annotations live in the shared ``ANNOTATIONS`` resource; each model
    version's predictions live in their own ``ANNOTATIONS_<annotator>`` resource.
    Review and consensus need all of them, so every annotation resource on the
    scan is downloaded and the annotations are put into one ``AnnotationSet``.

    When the scan's resources cannot be listed (an older server object or test
    double), only the default ``ANNOTATIONS`` resource is read, as before.
    A resource that cannot be read is skipped as long as another one could be
    read; when none can be read, the first problem is returned.

    Returns
    -------
    AnnotationSet on success; FriendlyError on any failure (never raises).
    """
    try:
        labels = list_annotation_resources(server, image_ref)
    except Exception:  # noqa: BLE001 - no listing means "read the default resource"
        labels = []
    if not labels:
        labels = [ANNOTATIONS_LABEL]

    merged: Optional[AnnotationSet] = None
    first_problem: Optional[FriendlyError] = None
    with tempfile.TemporaryDirectory() as _tmp:
        for label in labels:
            if dest_dir is None:
                target: Any = Path(_tmp) / label
            else:
                # Keep the default resource where callers have always found it;
                # other resources get a subfolder so same-named files stay apart.
                target = Path(dest_dir) if label == ANNOTATIONS_LABEL else Path(dest_dir) / label
            result = download_annotation_set(server, image_ref, target, resource_label=label)
            if not result.ok or result.annotation_set is None:
                first_problem = first_problem or result.friendly
                continue
            if merged is None:
                merged = AnnotationSet(image_ref=result.annotation_set.image_ref)
            merged.annotations.extend(result.annotation_set.annotations)
    if merged is None:
        return first_problem  # type: ignore[return-value]
    return merged


def list_image_annotations(
    server: Any,
    image_ref: str,
    dest_dir: Optional[Any] = None,
) -> Union[AnnotationSet, FriendlyError]:
    """
    Download the AnnotationSet for *image_ref* from *server*.

    Since spec 019 this gathers every annotation resource on the scan (people's
    and each model's) through ``load_annotation_sets``.

    Parameters
    ----------
    server    : pyxnat.Interface or FakeXNAT.
    image_ref : Opaque XNAT scan query string (not a patient ID).
    dest_dir  : Optional destination directory.  A temp dir is used if None.

    Returns
    -------
    AnnotationSet on success; FriendlyError on any failure (never raises).
    """
    return load_annotation_sets(server, image_ref, dest_dir)


# ---------------------------------------------------------------------------
# import_annotations
# ---------------------------------------------------------------------------

def import_annotations(items: List[Any]) -> List[Annotation]:
    """
    Convert a mixed list of raw items into canonical Annotations.

    Dispatch rules (checked in order):
      1. item is already an Annotation → pass through.
      2. item is a dict/mapping with 'WorkerId' key → mturk.from_mturk_row.
      3. item is a dict with 'mask_array' key → generic.from_mask_array
         (requires 'annotator_id' and 'tool' keys too).
      4. Unknown shape → skip (no error; caller sees shorter list).

    Raises nothing — individual failures are silently skipped.

    Returns
    -------
    list[Annotation]
    """
    from src.annotations.importers.mturk import from_mturk_row
    from src.annotations.importers.generic import from_mask_array

    annotations: List[Annotation] = []
    for item in items:
        if isinstance(item, Annotation):
            annotations.append(item)
            continue
        if isinstance(item, dict):
            if "WorkerId" in item:
                try:
                    annotations.append(from_mturk_row(item))
                except (AnnotationError, Exception):
                    pass
                continue
            if "mask_array" in item:
                try:
                    ann = from_mask_array(
                        item["mask_array"],
                        annotator_id=item.get("annotator_id", "unknown"),
                        tool=item.get("tool", "generic"),
                        annotation_type=item.get("annotation_type"),
                        created_at=item.get("created_at"),
                        version=item.get("version", 1),
                    )
                    annotations.append(ann)
                except (AnnotationError, Exception):
                    pass
                continue
    return annotations


# ---------------------------------------------------------------------------
# run_consensus
# ---------------------------------------------------------------------------

def run_consensus(
    annotation_set: AnnotationSet,
    annotation_type: str,
    aggregator_name: Optional[str] = None,
) -> Union[ConsensusResult, FriendlyError]:
    """
    Run consensus aggregation over *annotation_set* for *annotation_type*.

    Parameters
    ----------
    annotation_set   : AnnotationSet with at least one annotation.
    annotation_type  : Registered type name (e.g. 'binary_segmentation').
    aggregator_name  : Aggregator name; None → type's default_aggregator.

    Returns
    -------
    ConsensusResult on success; FriendlyError on any failure (never raises).
    """
    try:
        return aggregate_set(annotation_set, annotation_type, aggregator_name)
    except AnnotationError as exc:
        fe = exc.args[0] if exc.args and isinstance(exc.args[0], FriendlyError) else FriendlyError(
            title="Consensus failed",
            message=str(exc),
            recourse=[
                "Check that the annotation_type is registered.",
                "Ensure at least one annotation of the requested type is present.",
                "Use list_aggregator_names() to see available aggregators.",
            ],
        )
        return fe
    except Exception as exc:
        return _handle(
            exc,
            title="Consensus aggregation failed",
            message=(
                f"An unexpected error occurred while aggregating annotations "
                f"for type '{annotation_type}'."
            ),
            recourse=[
                "Ensure the annotation set contains at least one annotation of the requested type.",
                "Check that the annotation payloads are valid arrays.",
                "Use list_aggregator_names() to see available aggregators.",
            ],
            context=f"run_consensus, annotation_type={annotation_type}, aggregator={aggregator_name}",
        )


# ---------------------------------------------------------------------------
# upload_annotations
# ---------------------------------------------------------------------------

def upload_annotations(
    server: Any,
    image_ref: str,
    annotation_set: AnnotationSet,
) -> Union[UploadResult, FriendlyError]:
    """
    Upload *annotation_set* blobs + manifest to XNAT.

    Parameters
    ----------
    server          : pyxnat.Interface or FakeXNAT.
    image_ref       : Opaque XNAT scan query string.
    annotation_set  : AnnotationSet to upload.

    Returns
    -------
    UploadResult (ok=True on full success); FriendlyError on catastrophic error.
    Never raises.
    """
    try:
        return upload_annotation_set(server, image_ref, annotation_set)
    except Exception as exc:
        return _handle(
            exc,
            title="Annotation upload failed",
            message=(
                "An unexpected error prevented the annotation upload. "
                "Check your server connection and try again."
            ),
            recourse=[
                "Verify the XNAT server is reachable.",
                "Re-run the upload.",
                "Contact the Data Librarian if the problem persists.",
            ],
            context=f"upload_annotations, image_ref={image_ref}",
        )
