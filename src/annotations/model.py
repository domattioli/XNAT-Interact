"""
Core annotation data model.

Classes
-------
Annotation      — single annotation from one annotator at one version.
AnnotationSet   — ordered collection of annotations for one image reference.
ConsensusResult — result of an aggregation pass (produced by Task B, stored here).

PHI-FREE: image_ref is an opaque XNAT scan reference (not a patient identifier).
          annotator_id is validated as an opaque token (see validate.py).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

from src.services.errors import FriendlyError
from src.annotations.exc import AnnotationError


def _raise(fe: FriendlyError) -> None:
    """Raise AnnotationError wrapping a FriendlyError. Never returns."""
    raise AnnotationError(fe)


# ---------------------------------------------------------------------------
# Annotation
# ---------------------------------------------------------------------------

@dataclass
class Annotation:
    """
    One annotation from one annotator at one version.

    Fields
    ------
    annotator_id    : Opaque token — NEVER a patient identifier.
    tool            : Tool that produced this annotation (e.g. 'labelbox_v2').
    annotation_type : Registered type name (e.g. 'binary_segmentation').
    created_at      : ISO-8601 timestamp string.
    version         : Integer version (monotonically increasing per annotator+type).
    derived         : True if this annotation was computed from other annotations
                      (e.g. consensus output), not produced directly by a human.
    payload         : Decoded annotation object:
                        - np.ndarray for binary_segmentation / label_map
                        - dict for landmark / bbox
    blob            : Encoded bytes (None until explicitly encoded via a codec).
    codec           : Codec name used to produce blob (None if blob is None).
    """
    annotator_id: str
    tool: str
    annotation_type: str
    created_at: str        # ISO-8601 string, e.g. "2026-06-05T12:00:00Z"
    version: int
    derived: bool = False
    payload: Any = field(default=None)
    blob: Optional[bytes] = field(default=None, repr=False)
    codec: Optional[str] = field(default=None)

    def encode(self) -> None:
        """
        Encode payload → blob using the registered codec for this annotation_type.
        Sets self.blob and self.codec. Idempotent.
        """
        from src.annotations.registry import get_type
        from src.annotations.codecs import get_codec

        entry = get_type(self.annotation_type)
        codec_cls = get_codec(entry.codec)
        self.blob = codec_cls.encode(self.payload)
        self.codec = entry.codec

    def decode(self) -> None:
        """
        Decode blob → payload using self.codec. Idempotent.
        Requires self.blob and self.codec to be set.
        """
        if self.blob is None or self.codec is None:
            _raise(FriendlyError(
                title="Cannot decode annotation",
                message="blob and codec must both be set before calling decode().",
                recourse=["Call encode() first, or provide blob + codec when constructing."],
            ))
        from src.annotations.codecs import get_codec
        codec_cls = get_codec(self.codec)
        self.payload = codec_cls.decode(self.blob)


# ---------------------------------------------------------------------------
# AnnotationSet
# ---------------------------------------------------------------------------

@dataclass
class AnnotationSet:
    """
    Ordered collection of annotations for a single image reference.

    image_ref   : Opaque XNAT scan reference (e.g. scan UID or experiment ID).
                  Not a patient identifier.
    annotations : List of Annotation objects (insertion order preserved).
    """
    image_ref: str
    annotations: List[Annotation] = field(default_factory=list)

    # ------------------------------------------------------------------
    # Mutators
    # ------------------------------------------------------------------

    def add(self, annotation: Annotation) -> None:
        """Append an annotation. Validates annotator_id before adding."""
        from src.annotations.validate import validate_annotator_id
        validate_annotator_id(annotation.annotator_id)
        self.annotations.append(annotation)

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def by_annotator(self) -> Dict[str, List[Annotation]]:
        """
        Return {annotator_id: [Annotation, ...]} grouped by annotator.
        Within each group, order is insertion order.
        """
        result: Dict[str, List[Annotation]] = {}
        for ann in self.annotations:
            result.setdefault(ann.annotator_id, []).append(ann)
        return result

    def latest_per_annotator(self) -> Dict[Tuple[str, str], Annotation]:
        """
        Return the highest-version Annotation per (annotator_id, annotation_type) pair.

        Multiple annotations per user are allowed (different types or incrementing
        versions). "Latest" = highest version number per (annotator, type).

        Returns
        -------
        dict keyed by (annotator_id, annotation_type) → Annotation with max version.
        """
        best: Dict[Tuple[str, str], Annotation] = {}
        for ann in self.annotations:
            key = (ann.annotator_id, ann.annotation_type)
            if key not in best or ann.version > best[key].version:
                best[key] = ann
        return best

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def to_manifest(self) -> dict:
        """
        Serialise to a JSON-compatible manifest dict (blobs excluded).

        The manifest records metadata for all annotations. Blobs are
        stored separately (e.g. as files) and re-attached via from_manifest().
        """
        entries = []
        for i, ann in enumerate(self.annotations):
            entry = {
                "index": i,
                "annotator_id": ann.annotator_id,
                "tool": ann.tool,
                "annotation_type": ann.annotation_type,
                "created_at": ann.created_at,
                "version": ann.version,
                "derived": ann.derived,
                "codec": ann.codec,
                # payload is NOT serialised to manifest — it lives in the blob
            }
            entries.append(entry)
        return {
            "image_ref": self.image_ref,
            "annotations": entries,
        }

    @classmethod
    def from_manifest(
        cls,
        manifest: dict,
        blobs: Optional[Dict[int, bytes]] = None,
    ) -> "AnnotationSet":
        """
        Reconstruct an AnnotationSet from a manifest dict + optional blob map.

        Parameters
        ----------
        manifest : dict produced by to_manifest().
        blobs    : Optional {index: bytes} map. If provided, each Annotation's
                   blob is restored and payload is decoded from it.
        """
        aset = cls(image_ref=manifest["image_ref"])
        for entry in manifest.get("annotations", []):
            idx = entry["index"]
            blob = blobs.get(idx) if blobs else None
            ann = Annotation(
                annotator_id=entry["annotator_id"],
                tool=entry["tool"],
                annotation_type=entry["annotation_type"],
                created_at=entry["created_at"],
                version=entry["version"],
                derived=entry.get("derived", False),
                blob=blob,
                codec=entry.get("codec"),
            )
            if blob is not None and ann.codec is not None:
                ann.decode()
            aset.annotations.append(ann)
        return aset


# ---------------------------------------------------------------------------
# ConsensusResult
# ---------------------------------------------------------------------------

@dataclass
class ConsensusResult:
    """
    Result of an aggregation pass over an AnnotationSet.

    Produced by Task B aggregators; stored here as a data carrier.

    Fields
    ------
    payload         : Aggregated annotation payload (same type as individual payloads).
    per_annotator   : {annotator_id: individual_payload} — the inputs used.
    method          : Name of the aggregator method used (e.g. 'majority_vote').
    """
    payload: Any
    per_annotator: Dict[str, Any]
    method: str
