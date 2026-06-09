"""
annotations — core annotation model, lossless codecs, type registry, validation.

PHI-FREE RULE: annotator_id is an OPAQUE token (alnum/underscore/hyphen).
               It MUST NOT be a patient identifier, patient name, or MRN.
               Validators here enforce that invariant at write time.
"""
from .exc import AnnotationError
from .model import Annotation, AnnotationSet, ConsensusResult
from .codecs import RLECodec, JsonScalarCodec, get_codec
from .validate import (
    validate_annotator_id,
    validate_mask_shape,
    validate_mask_payload,
    validate_landmark_payload,
    validate_bbox_payload,
)
from .registry import register_type, get_type, list_types, TypeEntry

__all__ = [
    "AnnotationError",
    "Annotation",
    "AnnotationSet",
    "ConsensusResult",
    "RLECodec",
    "JsonScalarCodec",
    "get_codec",
    "validate_annotator_id",
    "validate_mask_shape",
    "validate_mask_payload",
    "validate_landmark_payload",
    "validate_bbox_payload",
    "register_type",
    "get_type",
    "list_types",
    "TypeEntry",
]
