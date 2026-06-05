"""
annotations.importers — tool-agnostic importers that normalize external outputs
into canonical Annotation objects.

Public API
----------
from src.annotations.importers.generic   import from_mask_array
from src.annotations.importers.mturk     import from_mturk_row
from src.annotations.importers.dicom_seg import from_dicom_seg, to_dicom_seg

All importers:
  - Return canonical Annotation (or list[Annotation]) with validated annotator_id.
  - Fail-soft: raise AnnotationError (wrapping FriendlyError) on bad/unknown input.
  - Never raise raw tracebacks to callers.
  - PHI-FREE: no patient data stored or logged.
"""
from src.annotations.importers.generic import from_mask_array
from src.annotations.importers.mturk import from_mturk_row
from src.annotations.importers.dicom_seg import from_dicom_seg, to_dicom_seg

__all__ = [
    "from_mask_array",
    "from_mturk_row",
    "from_dicom_seg",
    "to_dicom_seg",
]
