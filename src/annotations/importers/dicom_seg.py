"""
annotations.importers.dicom_seg — DICOM SEG import/export adapter.

Scope (segment-per-annotator subset):
  - Each DICOM SEG Segment = one annotator.
  - SegmentNumber / SegmentLabel is used as the annotator token.
  - Each segment's binary pixel data → one Annotation(binary_segmentation).
  - to_dicom_seg: one Annotation → one Segment in a new SEG Dataset.

PHI-FREE:
  - SegmentLabel must be an opaque token (validated via validate_annotator_id).
  - No patient-identifying data is read from or written to the SEG dataset here.

Unsupported corners (documented):
  - Fractional segmentation type (FRACTIONAL) — only BINARY supported.
  - Multi-frame SEG with 3-D frame geometry (only 2-D single-frame per segment).
  - Shared functional groups / per-frame functional groups — ignored.
  - Reading referenced SOP instances — ignored; caller supplies image_ref separately.
  - Segment Algorithm metadata — not propagated (not stored in Annotation model).
"""
from __future__ import annotations

from typing import List, Optional, Any

import numpy as np

from src.services.errors import FriendlyError
from src.annotations.exc import AnnotationError
from src.annotations.model import Annotation
from src.annotations.validate import validate_annotator_id


def _raise(fe: FriendlyError) -> None:
    raise AnnotationError(fe)


# ---------------------------------------------------------------------------
# Import: DICOM SEG → list[Annotation]
# ---------------------------------------------------------------------------

def from_dicom_seg(ds: Any) -> List[Annotation]:
    """
    Import a DICOM SEG dataset as a list of Annotations (one per segment).

    Each Segment in ds.SegmentSequence is treated as one annotator:
      - SegmentLabel → annotator_id (must be opaque token, validated).
      - Pixel data for that segment → 2-D uint8 binary mask.
      - annotation_type = 'binary_segmentation'.
      - tool = 'dicom_seg'.
      - created_at from ds.ContentDate + ds.ContentTime if present, else UTC now.
      - version = SegmentNumber (int).

    Parameters
    ----------
    ds : pydicom Dataset (already loaded, e.g. via pydicom.dcmread()).
         Must have SegmentSequence and PixelData.

    Returns
    -------
    List[Annotation] — one per segment, in SegmentNumber order.

    Raises
    ------
    AnnotationError — missing SegmentSequence, missing pixel data, bad
                      SegmentLabel (not a valid opaque token), unsupported type.

    Unsupported:
        - FRACTIONAL segmentation type.
        - Segments with no corresponding frame (mask returned as all-zeros).
    """
    import pydicom

    # ---- Validate dataset has SegmentSequence ----
    if not hasattr(ds, "SegmentSequence") or not ds.SegmentSequence:
        _raise(FriendlyError(
            title="DICOM SEG missing SegmentSequence",
            message="The supplied dataset has no SegmentSequence attribute or it is empty.",
            recourse=[
                "Verify this is a DICOM Segmentation IOD (SOP class 1.2.840.10008.5.1.4.1.1.66.4).",
                "Open the file with pydicom.dcmread() and check ds.SegmentSequence.",
            ],
        ))

    # ---- Dataset-level SegmentationType (default for segments that don't override) ----
    # Per the DICOM-SEG IOD, SegmentationType may vary per segment; the dataset-level
    # value is only the fallback. The actual FRACTIONAL guard runs per-segment below
    # (#55) so a per-segment FRACTIONAL type is not missed by a top-level-only read.
    ds_seg_type = getattr(ds, "SegmentationType", "BINARY")

    # ---- Extract pixel data ----
    if not hasattr(ds, "PixelData") or ds.PixelData is None:
        _raise(FriendlyError(
            title="DICOM SEG missing PixelData",
            message="The dataset has a SegmentSequence but no PixelData.",
            recourse=[
                "Ensure the DICOM SEG file is complete and not corrupted.",
                "PixelData must be present for mask extraction.",
            ],
        ))

    rows = getattr(ds, "Rows", None)
    cols = getattr(ds, "Columns", None)
    if rows is None or cols is None:
        _raise(FriendlyError(
            title="DICOM SEG missing Rows/Columns",
            message="Cannot determine mask dimensions: Rows or Columns tag missing.",
            recourse=["Supply a well-formed DICOM SEG with Rows and Columns tags set."],
        ))

    rows = int(rows)
    cols = int(cols)

    # ---- Decode pixel array ----
    # ds.pixel_array shape: (n_frames, rows, cols) or (rows, cols)
    try:
        pixel_array = ds.pixel_array
    except Exception as exc:
        _raise(FriendlyError(
            title="DICOM SEG pixel_array decode failed",
            message=f"pydicom could not decode the pixel data: {exc}",
            recourse=[
                "Verify the transfer syntax is supported by pydicom.",
                "Try pydicom.dcmread() with force=True.",
            ],
        ))

    if pixel_array.ndim == 2:
        # Single frame — wrap to (1, rows, cols)
        pixel_array = pixel_array[np.newaxis, ...]

    n_frames = pixel_array.shape[0]

    # ---- Build frame→segment mapping from PerFrameFunctionalGroupsSequence ----
    # Each frame item may have SegmentIdentificationSequence.ReferencedSegmentNumber.
    frame_segment_map: dict = {}  # frame_idx → segment_number
    pfgs = getattr(ds, "PerFrameFunctionalGroupsSequence", None)
    if pfgs is not None:
        for fi, frame_item in enumerate(pfgs):
            seg_id_seq = getattr(frame_item, "SegmentIdentificationSequence", None)
            if seg_id_seq:
                seg_num = getattr(seg_id_seq[0], "ReferencedSegmentNumber", None)
                if seg_num is not None:
                    frame_segment_map[fi] = int(seg_num)

    # If no per-frame mapping, fall back: frame index 0 → segment 1, etc.
    if not frame_segment_map:
        for fi in range(n_frames):
            frame_segment_map[fi] = fi + 1

    # ---- created_at from ContentDate + ContentTime ----
    content_date = getattr(ds, "ContentDate", None)
    content_time = getattr(ds, "ContentTime", None)
    if content_date and content_time:
        # Normalise to ISO-8601 (DICOM dates: YYYYMMDD, times: HHMMSS[.ffffff])
        date_str = str(content_date)
        time_str = str(content_time).split(".")[0]  # drop fractional seconds
        if len(date_str) == 8 and len(time_str) >= 6:
            created_at = (
                f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                f"T{time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}Z"
            )
        else:
            created_at = None
    else:
        created_at = None

    if created_at is None:
        from datetime import datetime, timezone
        created_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # ---- Build one Annotation per segment ----
    annotations: List[Annotation] = []

    for seg_item in ds.SegmentSequence:
        seg_number = int(getattr(seg_item, "SegmentNumber", 0))
        seg_label = str(getattr(seg_item, "SegmentLabel", "")).strip()

        # ---- Per-segment FRACTIONAL guard (#55) ----
        # SegmentationType may be overridden per segment; fall back to the
        # dataset-level value when the segment does not carry its own.
        seg_type = getattr(seg_item, "SegmentationType", ds_seg_type)
        if str(seg_type).upper() == "FRACTIONAL":
            _raise(FriendlyError(
                title="Unsupported DICOM SEG type: FRACTIONAL",
                message=(
                    "This importer supports BINARY segmentation type only. "
                    f"SegmentNumber {seg_number} has SegmentationType='{seg_type}'."
                ),
                recourse=[
                    "Convert the FRACTIONAL SEG to BINARY before importing.",
                    "Or use a specialised DICOM SEG library (e.g. highdicom) for FRACTIONAL support.",
                ],
            ))

        if not seg_label:
            _raise(FriendlyError(
                title="DICOM SEG Segment has empty SegmentLabel",
                message=(
                    f"SegmentNumber {seg_number} has an empty SegmentLabel. "
                    "SegmentLabel is used as annotator_id and must be a non-empty opaque token."
                ),
                recourse=[
                    "Set SegmentLabel to an opaque annotator token (e.g. 'worker_A1').",
                    "Do NOT use patient names or PHI as SegmentLabel.",
                ],
            ))

        # Validate as annotator_id — PHI guard
        try:
            validate_annotator_id(seg_label)
        except AnnotationError:
            raise

        # Collect all frames belonging to this segment
        mask = np.zeros((rows, cols), dtype=np.uint8)
        found_frame = False
        for fi, frame_seg_num in frame_segment_map.items():
            if frame_seg_num == seg_number and fi < n_frames:
                frame = pixel_array[fi].astype(np.uint8)
                # Binary OR accumulate (BINARY type — any non-zero = foreground)
                mask = np.bitwise_or(mask, frame)
                found_frame = True

        if not found_frame:
            # No frames for this segment — return all-zeros mask (valid, documented)
            pass

        # Binarise: any non-zero → 1
        mask = (mask > 0).astype(np.uint8)

        annotations.append(Annotation(
            annotator_id=seg_label,
            tool="dicom_seg",
            annotation_type="binary_segmentation",
            created_at=created_at,
            version=seg_number,
            payload=mask,
        ))

    return annotations


# ---------------------------------------------------------------------------
# Export: list[Annotation] → DICOM SEG Dataset
# ---------------------------------------------------------------------------

def to_dicom_seg(
    annotations: List[Annotation],
    reference: Any,
) -> Any:
    """
    Build a minimal DICOM SEG Dataset from a list of Annotations.

    One Segment per Annotation:
      - SegmentLabel  = annotation.annotator_id
      - SegmentNumber = index (1-based)
      - One frame per annotation (2-D mask).

    Parameters
    ----------
    annotations : List[Annotation] — must all have binary_segmentation type
                  and 2-D uint8 payload.
    reference   : pydicom Dataset used to copy spatial metadata (Rows, Columns,
                  SOPInstanceUID etc.).  May be a minimal Dataset with at least
                  Rows and Columns.

    Returns
    -------
    pydicom.Dataset — a DICOM SEG Dataset ready for save_as().

    Raises
    ------
    AnnotationError — empty annotation list, wrong annotation_type, missing payload,
                      payload shape mismatch, invalid annotator_id.

    Unsupported (documented):
        - Only binary_segmentation annotations accepted.
        - Only 2-D (single-slice) masks.
        - No per-frame functional groups written for geometry (position/orientation).
    """
    import pydicom
    from pydicom.dataset import Dataset, FileDataset, FileMetaDataset
    from pydicom.sequence import Sequence
    from pydicom.uid import (
        ExplicitVRLittleEndian,
        SegmentationStorage,
        generate_uid,
    )

    if not annotations:
        _raise(FriendlyError(
            title="to_dicom_seg: empty annotation list",
            message="Cannot build a DICOM SEG dataset from an empty annotation list.",
            recourse=["Provide at least one Annotation with a binary_segmentation payload."],
        ))

    # ---- Validate all annotations ----
    rows = int(getattr(reference, "Rows", 0))
    cols = int(getattr(reference, "Columns", 0))

    for i, ann in enumerate(annotations):
        if ann.annotation_type != "binary_segmentation":
            _raise(FriendlyError(
                title="to_dicom_seg: unsupported annotation_type",
                message=(
                    f"Annotation at index {i} has type '{ann.annotation_type}'. "
                    "to_dicom_seg only supports 'binary_segmentation'."
                ),
                recourse=["Filter or convert non-binary-segmentation annotations before exporting."],
            ))
        if ann.payload is None:
            _raise(FriendlyError(
                title="to_dicom_seg: annotation payload is None",
                message=f"Annotation at index {i} has no payload. Decode it first.",
                recourse=["Call annotation.decode() or provide the mask array as payload."],
            ))
        if not isinstance(ann.payload, np.ndarray) or ann.payload.ndim != 2:
            _raise(FriendlyError(
                title="to_dicom_seg: invalid payload shape",
                message=f"Annotation at index {i} payload must be a 2-D numpy array.",
                recourse=["Provide a 2-D uint8 mask as the annotation payload."],
            ))
        # Validate annotator_id — PHI guard
        try:
            validate_annotator_id(ann.annotator_id)
        except AnnotationError:
            raise
        # Infer rows/cols from first annotation if reference has none
        if rows == 0 or cols == 0:
            rows, cols = ann.payload.shape

    # ---- File meta ----
    file_meta = FileMetaDataset()
    file_meta.MediaStorageSOPClassUID = SegmentationStorage
    file_meta.MediaStorageSOPInstanceUID = generate_uid()
    file_meta.TransferSyntaxUID = ExplicitVRLittleEndian
    file_meta.ImplementationClassUID = generate_uid()

    seg_ds = FileDataset(None, {}, file_meta=file_meta, preamble=b"\0" * 128)
    # ---- Top-level tags ----
    seg_ds.SOPClassUID = SegmentationStorage
    seg_ds.SOPInstanceUID = file_meta.MediaStorageSOPInstanceUID
    seg_ds.Modality = "SEG"
    seg_ds.SegmentationType = "BINARY"

    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    seg_ds.ContentDate = now.strftime("%Y%m%d")
    seg_ds.ContentTime = now.strftime("%H%M%S")

    seg_ds.Rows = rows
    seg_ds.Columns = cols

    # Copy basic spatial metadata from reference if available
    for tag in ("FrameOfReferenceUID", "StudyInstanceUID", "SeriesInstanceUID"):
        val = getattr(reference, tag, None)
        if val is not None:
            setattr(seg_ds, tag, val)

    # ---- BitsAllocated / pixel representation ----
    seg_ds.SamplesPerPixel = 1
    seg_ds.PhotometricInterpretation = "MONOCHROME2"
    seg_ds.BitsAllocated = 1
    seg_ds.BitsStored = 1
    seg_ds.HighBit = 0
    seg_ds.PixelRepresentation = 0
    seg_ds.NumberOfFrames = len(annotations)

    # ---- SegmentSequence ----
    seg_items = []
    frames = []

    for seg_num, ann in enumerate(annotations, start=1):
        seg_item = Dataset()
        seg_item.SegmentNumber = seg_num
        seg_item.SegmentLabel = ann.annotator_id
        seg_item.SegmentAlgorithmType = "MANUAL"
        seg_item.SegmentedPropertyCategoryCodeSequence = Sequence()
        seg_item.SegmentedPropertyTypeCodeSequence = Sequence()
        seg_items.append(seg_item)

        # Binarise mask → uint8 (0 or 1)
        frame = (ann.payload > 0).astype(np.uint8)
        if frame.shape != (rows, cols):
            _raise(FriendlyError(
                title="to_dicom_seg: mask shape mismatch",
                message=(
                    f"Annotation at index {seg_num - 1} has mask shape {frame.shape}, "
                    f"expected ({rows}, {cols})."
                ),
                recourse=["Ensure all annotation masks share the same spatial grid."],
            ))
        frames.append(frame)

    seg_ds.SegmentSequence = Sequence(seg_items)

    # ---- PerFrameFunctionalGroupsSequence ----
    # Minimal: each frame item records its ReferencedSegmentNumber.
    pf_items = []
    for seg_num in range(1, len(annotations) + 1):
        pf_item = Dataset()
        seg_id_item = Dataset()
        seg_id_item.ReferencedSegmentNumber = seg_num
        pf_item.SegmentIdentificationSequence = Sequence([seg_id_item])
        pf_items.append(pf_item)
    seg_ds.PerFrameFunctionalGroupsSequence = Sequence(pf_items)

    # ---- Pack pixel data ----
    # Each frame is (rows*cols) bits packed into bytes (LSB first per DICOM).
    # For BitsAllocated=1 the frames are bit-packed.
    pixel_bytes = _pack_binary_frames(frames, rows, cols)
    seg_ds.PixelData = pixel_bytes

    return seg_ds


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _pack_binary_frames(
    frames: List[np.ndarray],
    rows: int,
    cols: int,
) -> bytes:
    """
    Pack a list of binary (0/1 uint8) 2-D frames into DICOM bit-packed pixel bytes.

    DICOM BINARY segmentation: BitsAllocated=1, each pixel is 1 bit,
    packed LSB-first per byte, frame-by-frame.
    Each frame is padded to a byte boundary independently.
    """
    all_bytes = bytearray()
    for frame in frames:
        flat = frame.ravel().astype(np.uint8)
        # Pad to byte boundary
        pad = (8 - len(flat) % 8) % 8
        if pad:
            flat = np.concatenate([flat, np.zeros(pad, dtype=np.uint8)])
        # Pack 8 bits per byte, LSB first
        packed = np.packbits(flat, bitorder="little")
        all_bytes.extend(packed.tobytes())
    # DICOM pixel data must be even-length
    if len(all_bytes) % 2:
        all_bytes.append(0)
    return bytes(all_bytes)


def _unpack_binary_frame(
    packed: bytes,
    rows: int,
    cols: int,
    frame_idx: int,
) -> np.ndarray:
    """
    Unpack a single frame from DICOM bit-packed pixel bytes.

    Used internally for round-trip verification (not part of from_dicom_seg,
    which uses pydicom's pixel_array).
    """
    n_pixels = rows * cols
    # Bits per frame, padded to byte boundary
    bits_per_frame_padded = ((n_pixels + 7) // 8) * 8
    bytes_per_frame = bits_per_frame_padded // 8

    offset = frame_idx * bytes_per_frame
    frame_bytes = packed[offset: offset + bytes_per_frame]

    arr = np.unpackbits(np.frombuffer(frame_bytes, dtype=np.uint8), bitorder="little")
    return arr[:n_pixels].reshape(rows, cols).astype(np.uint8)
