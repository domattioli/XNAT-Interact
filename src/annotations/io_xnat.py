"""
annotations.io_xnat — upload / download AnnotationSet via XNAT file API.

Public API
----------
upload_annotation_set(server, image_ref, annotation_set, *, project_name, resource_label)
    -> UploadResult

download_annotation_set(server, image_ref, dest_dir, *, project_name, resource_label)
    -> DownloadResult

Design constraints
------------------
- Data-efficient: blobs encoded via each type's registered codec (RLE for masks).
- Keep-all versioning: blob filenames encode (annotator_id, type, version) so
  re-submits by the same annotator land as NEW files — nothing is overwritten.
  overwrite=False on every file.put call.
- Image NOT re-uploaded: only annotation artifacts (blobs + manifest) are written
  to the ANNOTATIONS resource.
- PHI-free: annotator_id validated as opaque token by model.py/.add().
- Fail-soft: connection errors → FriendlyError, no raw traceback.
- Offline-testable: all I/O injected via `server` parameter (FakeXNAT in tests).

Blob filename scheme
--------------------
  ann__<annotator_id>__<annotation_type>__v<version>.<codec_ext>

Examples:
  ann__worker_A1__binary_segmentation__v1.rle
  ann__worker_B2__landmark__v3.json

codec → extension: rle → .rle   |   json_scalar → .json   |   fallback → .bin

Manifest filename
-----------------
  manifest.json
  (always overwritten — it is the index of all blobs and is rewritten on every upload)
"""
from __future__ import annotations

import json
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.services.errors import FriendlyError, handle as _handle
from src.annotations.model import AnnotationSet
from src.annotations.registry import get_type
from src.annotations.codecs import get_codec
from src.services.xnat_conventions import ResourceLabel as _ResourceLabel


# ---------------------------------------------------------------------------
# Codec → file extension map
# ---------------------------------------------------------------------------

_CODEC_EXT: Dict[str, str] = {
    "rle": ".rle",
    "json_scalar": ".json",
}

MANIFEST_FILENAME = "manifest.json"


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

@dataclass
class UploadResult:
    """
    Return value of upload_annotation_set().

    Fields
    ------
    ok            : True → all blobs + manifest written without error.
    files_written : List of filenames uploaded to the XNAT resource.
    friendly      : None when ok; FriendlyError on failure.
    """
    ok: bool
    files_written: List[str] = field(default_factory=list)
    friendly: Optional[FriendlyError] = None


@dataclass
class DownloadResult:
    """
    Return value of download_annotation_set().

    Fields
    ------
    ok              : True → manifest + all blobs fetched and decoded.
    annotation_set  : Rebuilt AnnotationSet, or None on failure.
    files_written   : Paths of files written under dest_dir.
    friendly        : None when ok; FriendlyError on failure.
    """
    ok: bool
    annotation_set: Optional[AnnotationSet] = None
    files_written: List[Path] = field(default_factory=list)
    friendly: Optional[FriendlyError] = None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _blob_filename(annotator_id: str, annotation_type: str, version: int, codec: str) -> str:
    """
    Build a versioned blob filename that encodes identity + version.

    Format: ann__<annotator_id>__<annotation_type>__v<version><ext>

    Nothing is overwritten for a given (annotator_id, annotation_type, version)
    triple because the filename is deterministic per triple.  A re-submit with
    a higher version produces a NEW, distinct filename.
    """
    ext = _CODEC_EXT.get(codec, ".bin")
    return f"ann__{annotator_id}__{annotation_type}__v{version}{ext}"


def _image_qs(image_ref: str, project_name: Optional[str]) -> str:
    """
    Build the XNAT query string for an image scan resource.

    If project_name is supplied and image_ref does not already start with
    '/projects/', a qualified query string is built; otherwise image_ref
    is used verbatim.
    """
    if project_name and not image_ref.startswith("/projects/"):
        return f"/projects/{project_name}/{image_ref.lstrip('/')}"
    return image_ref


# ---------------------------------------------------------------------------
# Upload
# ---------------------------------------------------------------------------

def upload_annotation_set(
    server: Any,
    image_ref: str,
    annotation_set: AnnotationSet,
    *,
    project_name: Optional[str] = None,
    resource_label: str = "ANNOTATIONS",
) -> UploadResult:
    """
    Upload an AnnotationSet to XNAT as manifest + versioned RLE/JSON blobs.

    For each Annotation:
      1. Encode payload via its type's codec → bytes.
      2. Write blob to resource under a versioned filename (overwrite=False).

    Then write manifest.json (index of all blobs with metadata).

    The reference IMAGE is never uploaded here.  Only annotation artifacts
    land under the ANNOTATIONS resource.

    Parameters
    ----------
    server          : pyxnat.Interface or FakeXNAT — must support
                      server.select(qs).resource(label).file(fn).put(...).
    image_ref       : Opaque XNAT scan query string (not a patient ID).
    annotation_set  : AnnotationSet to upload.
    project_name    : Optional XNAT project name (used to build the QS).
    resource_label  : Resource label on the image (default: 'ANNOTATIONS').

    Returns
    -------
    UploadResult
        ok=True → blobs + manifest written.
        ok=False → FriendlyError with recourse; no traceback escapes.
    """
    files_written: List[str] = []

    try:
        qs = _image_qs(image_ref, project_name)
    except Exception as exc:
        fe = _handle(
            exc,
            title="Annotation upload failed — could not reach XNAT resource",
            message=(
                "Could not access the XNAT resource for this image. "
                "Check your server connection and image reference."
            ),
            recourse=[
                "Verify the XNAT server is reachable.",
                "Check that image_ref is a valid XNAT query string.",
            ],
            context=f"upload_annotation_set, image_ref={image_ref}",
        )
        return UploadResult(ok=False, files_written=[], friendly=fe)

    # Encode each annotation and write its blob
    manifest_entries: List[dict] = []

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)

        for i, ann in enumerate(annotation_set.annotations):
            # --- Encode ---
            try:
                type_entry = get_type(ann.annotation_type)
                codec_cls = get_codec(type_entry.codec)
                blob_bytes = codec_cls.encode(ann.payload)
                codec_name = type_entry.codec
            except Exception as exc:
                fe = _handle(
                    exc,
                    title="Annotation encode failed",
                    message=(
                        f"Could not encode annotation {i} "
                        f"(type={ann.annotation_type}, annotator={ann.annotator_id})."
                    ),
                    recourse=[
                        "Check that the payload matches the annotation type.",
                        "Use a numpy 2-D integer array for mask types.",
                    ],
                    context=(
                        f"upload_annotation_set, annotation_index={i}, "
                        f"type={ann.annotation_type}"
                    ),
                )
                return UploadResult(ok=False, files_written=files_written, friendly=fe)

            # --- Versioned blob filename ---
            blob_fn = _blob_filename(
                ann.annotator_id, ann.annotation_type, ann.version, codec_name
            )

            # --- Write blob to temp, then put to XNAT ---
            local_blob = tmp / blob_fn
            local_blob.write_bytes(blob_bytes)

            try:
                server.put_file(
                    qs,
                    resource_label,
                    blob_fn,
                    str(local_blob),
                    content="ANNOTATION",
                    format=codec_name.upper(),
                    tags=f"annotator={ann.annotator_id}",
                    overwrite=False,   # keep-all: never clobber an existing version
                )
            except Exception as exc:
                fe = _handle(
                    exc,
                    title="Annotation upload failed — blob write error",
                    message=(
                        f"Could not upload blob '{blob_fn}' to XNAT. "
                        "Check your connection and try again."
                    ),
                    recourse=[
                        "Verify the XNAT server is reachable.",
                        "Re-run the upload — previously-written versioned blobs are safe.",
                    ],
                    context=(
                        f"upload_annotation_set, blob_fn={blob_fn}, "
                        f"image_ref={image_ref}"
                    ),
                )
                return UploadResult(ok=False, files_written=files_written, friendly=fe)

            files_written.append(blob_fn)

            # Manifest entry: metadata + blob filename, no patient ID
            manifest_entries.append({
                "index": i,
                "blob_filename": blob_fn,
                "codec": codec_name,
                "annotator_id": ann.annotator_id,
                "tool": ann.tool,
                "annotation_type": ann.annotation_type,
                "created_at": ann.created_at,
                "version": ann.version,
                "derived": ann.derived,
            })

        # --- Build and upload manifest ---
        base_manifest = annotation_set.to_manifest()
        full_manifest = {
            "image_ref": base_manifest["image_ref"],
            "annotations": manifest_entries,
        }

        manifest_local = tmp / MANIFEST_FILENAME
        manifest_local.write_bytes(
            json.dumps(full_manifest, indent=2, sort_keys=True).encode("utf-8")
        )

        try:
            server.put_file(
                qs,
                resource_label,
                MANIFEST_FILENAME,
                str(manifest_local),
                content="ANNOTATION",
                format="JSON",
                tags="manifest",
                overwrite=True,   # manifest is the index — always update to latest
            )
        except Exception as exc:
            fe = _handle(
                exc,
                title="Annotation upload failed — manifest write error",
                message=(
                    "Could not upload manifest.json to XNAT. "
                    "Blobs were written; re-running will regenerate the manifest."
                ),
                recourse=[
                    "Verify the XNAT server is reachable.",
                    "Re-run the upload to regenerate the manifest.",
                ],
                context=f"upload_annotation_set, image_ref={image_ref}",
            )
            return UploadResult(ok=False, files_written=files_written, friendly=fe)

        files_written.append(MANIFEST_FILENAME)

    return UploadResult(ok=True, files_written=files_written, friendly=None)


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def download_annotation_set(
    server: Any,
    image_ref: str,
    dest_dir: Any,
    *,
    project_name: Optional[str] = None,
    resource_label: str = "ANNOTATIONS",
) -> DownloadResult:
    """
    Download annotation blobs from XNAT and rebuild an AnnotationSet.

    Steps:
      1. get_copy manifest.json → parse.
      2. For each entry, get_copy the blob → decode via its codec.
      3. Rebuild AnnotationSet from decoded payloads.
      4. Write decoded artifacts to dest_dir (cross-platform pathlib).

    Parameters
    ----------
    server          : pyxnat.Interface or FakeXNAT.
    image_ref       : Opaque XNAT scan query string.
    dest_dir        : Destination directory (created if absent). Pathlib.
    project_name    : Optional XNAT project name.
    resource_label  : Resource label (default: 'ANNOTATIONS').

    Returns
    -------
    DownloadResult
        ok=True  → AnnotationSet rebuilt; files_written populated.
        ok=False → FriendlyError; no traceback escapes.
    """
    dest_path = Path(dest_dir)
    dest_path.mkdir(parents=True, exist_ok=True)
    files_written: List[Path] = []

    # --- Build querystring ---
    try:
        qs = _image_qs(image_ref, project_name)
    except Exception as exc:
        fe = _handle(
            exc,
            title="Annotation download failed — could not reach XNAT resource",
            message=(
                "Could not access the XNAT resource for this image. "
                "Check your server connection and image reference."
            ),
            recourse=[
                "Verify the XNAT server is reachable.",
                "Check that image_ref is a valid XNAT query string.",
            ],
            context=f"download_annotation_set, image_ref={image_ref}",
        )
        return DownloadResult(ok=False, annotation_set=None, files_written=[], friendly=fe)

    # --- Fetch manifest ---
    manifest_dest = dest_path / MANIFEST_FILENAME
    try:
        server.get_file_copy(qs, resource_label, MANIFEST_FILENAME, manifest_dest)
    except Exception as exc:
        fe = _handle(
            exc,
            title="Annotation download failed — could not fetch manifest",
            message=(
                "Could not download manifest.json from the ANNOTATIONS resource. "
                "Ensure annotations have been uploaded for this image."
            ),
            recourse=[
                "Run upload_annotation_set() first.",
                "Check the XNAT server connection.",
            ],
            context=f"download_annotation_set, image_ref={image_ref}",
        )
        return DownloadResult(ok=False, annotation_set=None, files_written=[], friendly=fe)

    files_written.append(manifest_dest)

    try:
        manifest = json.loads(manifest_dest.read_bytes().decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        fe = _handle(
            exc,
            title="Annotation download failed — corrupt manifest",
            message="The manifest.json file could not be parsed. It may be corrupt.",
            recourse=[
                "Re-upload the annotation set to regenerate the manifest.",
                "Check for disk errors or network corruption.",
            ],
            context=f"download_annotation_set, image_ref={image_ref}",
        )
        return DownloadResult(ok=False, annotation_set=None, files_written=files_written, friendly=fe)

    # --- Fetch each blob and decode ---
    blobs: Dict[int, bytes] = {}

    for entry in manifest.get("annotations", []):
        idx = entry["index"]
        blob_fn = entry["blob_filename"]
        codec_name = entry["codec"]

        blob_dest = dest_path / blob_fn
        try:
            server.get_file_copy(qs, resource_label, blob_fn, blob_dest)
        except Exception as exc:
            fe = _handle(
                exc,
                title=f"Annotation download failed — blob '{blob_fn}' missing",
                message=(
                    f"Could not download blob '{blob_fn}' from XNAT. "
                    "The file may have been deleted or the connection dropped."
                ),
                recourse=[
                    "Re-upload the annotation set.",
                    "Check the XNAT server connection.",
                ],
                context=(
                    f"download_annotation_set, image_ref={image_ref}, "
                    f"blob_fn={blob_fn}"
                ),
            )
            return DownloadResult(
                ok=False, annotation_set=None,
                files_written=files_written, friendly=fe
            )

        files_written.append(blob_dest)
        blobs[idx] = blob_dest.read_bytes()

    # --- Rebuild AnnotationSet ---
    # Translate extended manifest entries back to the shape from_manifest() expects:
    # it needs {image_ref, annotations: [{index, annotator_id, tool,
    #           annotation_type, created_at, version, derived, codec}, ...]}
    from_manifest_doc = {
        "image_ref": manifest["image_ref"],
        "annotations": [
            {
                "index": e["index"],
                "annotator_id": e["annotator_id"],
                "tool": e["tool"],
                "annotation_type": e["annotation_type"],
                "created_at": e["created_at"],
                "version": e["version"],
                "derived": e.get("derived", False),
                "codec": e["codec"],
            }
            for e in manifest.get("annotations", [])
        ],
    }

    try:
        annotation_set = AnnotationSet.from_manifest(from_manifest_doc, blobs=blobs)
    except Exception as exc:
        fe = _handle(
            exc,
            title="Annotation download failed — decode error",
            message=(
                "Could not decode one or more annotation blobs. "
                "The blobs may be corrupt or from an incompatible codec version."
            ),
            recourse=[
                "Re-upload the annotation set from the original source.",
                "Check that codec versions match between upload and download.",
            ],
            context=f"download_annotation_set, image_ref={image_ref}",
        )
        return DownloadResult(
            ok=False, annotation_set=None,
            files_written=files_written, friendly=fe
        )

    return DownloadResult(
        ok=True,
        annotation_set=annotation_set,
        files_written=files_written,
        friendly=None,
    )
