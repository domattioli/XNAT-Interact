"""
Lossless annotation codecs.

Each codec exposes:
    encode(payload) -> bytes
    decode(bytes)   -> payload

Codecs MUST be lossless (bit-for-bit exact round-trip).
Codecs MUST produce blobs smaller than raw array.nbytes for typical sparse masks.

PHI-FREE: no patient data touches these functions.
"""
from __future__ import annotations

import io
import json
import zlib
import struct
from typing import Any, Dict

import numpy as np

from src.services.errors import FriendlyError
from src.annotations.exc import AnnotationError


def _raise(fe: FriendlyError) -> None:
    """Raise AnnotationError wrapping a FriendlyError. Never returns."""
    raise AnnotationError(fe)


# ---------------------------------------------------------------------------
# RLE codec  (2-D integer masks — binary or multi-label)
# ---------------------------------------------------------------------------

class RLECodec:
    """
    Run-length encoding for 2-D integer masks (binary uint8 or multi-label int32).

    Wire format (all little-endian):
        [4B] rows  (uint32)
        [4B] cols  (uint32)
        [1B] dtype_len  (uint8 — length of numpy dtype string)
        [N B] dtype_str  (UTF-8 numpy dtype string, e.g. "uint8")
        [rest] zlib-compressed RLE payload

    RLE payload (before zlib):
        Flat sequence of (value, run_length) pairs, each as:
            value      — native dtype bytes (1, 2, or 4 bytes depending on dtype)
            run_length — uint32 little-endian (4 bytes)

    Round-trip is bit-for-bit exact. Blob is smaller than raw nbytes for sparse
    masks; worst case (alternating pixels) may be larger — the caller is
    responsible for choosing masks that benefit from RLE.
    """

    name = "rle"

    @staticmethod
    def encode(payload: np.ndarray) -> bytes:
        """Encode a 2-D integer ndarray to compressed RLE bytes."""
        if not isinstance(payload, np.ndarray):
            raise ValueError("RLECodec.encode: payload must be np.ndarray")
        if payload.ndim != 2:
            raise ValueError(f"RLECodec.encode: expected 2-D array, got {payload.ndim}-D")

        arr = payload
        rows, cols = arr.shape
        dtype_str = arr.dtype.str  # e.g. '<u1', '|u1', '<i4'
        dtype_str_bytes = dtype_str.encode("utf-8")

        # Build RLE runs on the flattened array
        flat = arr.ravel()
        rle_parts = []
        if flat.size > 0:
            current_val = flat[0]
            run_len = 1
            for v in flat[1:]:
                if v == current_val:
                    run_len += 1
                else:
                    rle_parts.append((current_val, run_len))
                    current_val = v
                    run_len = 1
            rle_parts.append((current_val, run_len))

        # Serialise RLE payload: pairs (value_bytes + uint32_run_length)
        value_dtype = np.dtype(arr.dtype)
        buf = io.BytesIO()
        for val, run in rle_parts:
            # Write value as its native dtype bytes (ensures exact reconstruction)
            buf.write(np.array(val, dtype=value_dtype).tobytes())
            # Write run length as uint32 LE
            buf.write(struct.pack("<I", run))
        rle_raw = buf.getvalue()

        # Compress RLE payload
        rle_compressed = zlib.compress(rle_raw, level=9)

        # Assemble header + compressed payload
        header = struct.pack("<II", rows, cols)
        header += struct.pack("<B", len(dtype_str_bytes))
        header += dtype_str_bytes

        return header + rle_compressed

    @staticmethod
    def decode(data: bytes) -> np.ndarray:
        """Decode compressed RLE bytes back to a 2-D ndarray (exact)."""
        try:
            offset = 0
            rows, cols = struct.unpack_from("<II", data, offset)
            offset += 8
            dtype_len = struct.unpack_from("<B", data, offset)[0]
            offset += 1
            dtype_str = data[offset:offset + dtype_len].decode("utf-8")
            offset += dtype_len

            rle_compressed = data[offset:]
            rle_raw = zlib.decompress(rle_compressed)

            value_dtype = np.dtype(dtype_str)
            value_size = value_dtype.itemsize
            step = value_size + 4  # value bytes + uint32 run_length

            flat_parts = []
            for i in range(0, len(rle_raw), step):
                val_bytes = rle_raw[i:i + value_size]
                run_bytes = rle_raw[i + value_size:i + value_size + 4]
                val = np.frombuffer(val_bytes, dtype=value_dtype)[0]
                run = struct.unpack("<I", run_bytes)[0]
                flat_parts.append(np.full(run, val, dtype=value_dtype))

            if flat_parts:
                flat = np.concatenate(flat_parts)
            else:
                flat = np.array([], dtype=value_dtype)

            return flat.reshape(rows, cols)
        except AnnotationError:
            raise
        except Exception as exc:
            _raise(FriendlyError(
                title="Annotation decode failed",
                message="Could not decode RLE mask blob. The data may be corrupt or from an incompatible version.",
                recourse=[
                    "Re-encode the mask from the original source.",
                    "Check the codec version matches the one used at encode time.",
                ],
                _original_exc=exc,
            ))


# ---------------------------------------------------------------------------
# JsonScalar codec  (landmarks, bboxes, scalar dicts)
# ---------------------------------------------------------------------------

class JsonScalarCodec:
    """
    Encodes landmark {x,y} / bbox {x,y,w,h} / scalar payloads to compact JSON bytes.
    Round-trip exact for int/float/str/list/dict payloads with no NaN/Inf.
    """

    name = "json_scalar"

    @staticmethod
    def encode(payload: Any) -> bytes:
        """Encode payload to UTF-8 JSON bytes (compact, sorted keys)."""
        try:
            return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        except (TypeError, ValueError) as exc:
            _raise(FriendlyError(
                title="Annotation encode failed",
                message=f"Could not JSON-encode annotation payload: {exc}",
                recourse=[
                    "Ensure the payload contains only JSON-serialisable types (str, int, float, list, dict).",
                    "Remove any NaN or Inf float values before encoding.",
                ],
                _original_exc=exc,
            ))

    @staticmethod
    def decode(data: bytes) -> Any:
        """Decode UTF-8 JSON bytes back to the original payload."""
        try:
            return json.loads(data.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as exc:
            _raise(FriendlyError(
                title="Annotation decode failed",
                message="Could not decode JSON scalar annotation blob.",
                recourse=[
                    "Ensure the blob was encoded with JsonScalarCodec.",
                    "Check for data corruption.",
                ],
                _original_exc=exc,
            ))


# ---------------------------------------------------------------------------
# Codec registry
# ---------------------------------------------------------------------------

_CODECS: Dict[str, Any] = {
    RLECodec.name: RLECodec,
    JsonScalarCodec.name: JsonScalarCodec,
}


def get_codec(name: str):
    """Return codec class by name. Raises AnnotationError if unknown."""
    if name not in _CODECS:
        _raise(FriendlyError(
            title="Unknown codec",
            message=f"No codec registered under the name '{name}'. Available: {list(_CODECS)}.",
            recourse=[
                "Use one of the built-in codec names: 'rle', 'json_scalar'.",
                "Register a custom codec in src/annotations/codecs.py if needed.",
            ],
        ))
    return _CODECS[name]
