"""
tests/test_annotations_io.py — Phase 5 Task D (T014-T016)

Offline, no network, no PHI.

Covers:
  - K-user upload → K versioned blobs + 1 manifest; IMAGE not re-uploaded (SC-003)
  - Keep-all versioning: same annotator re-submit → both versions present
  - Download → AnnotationSet rebuilt; decoded masks bit-for-bit exact; files pathlib
  - PHI-free manifest: no patient-identifier annotator_ids in manifest (SC-006)
  - Connection failure → FriendlyError, no raw traceback
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import List

import numpy as np
import pytest

from tests.fakes.fake_xnat import FakeXNAT
from src.annotations.model import Annotation, AnnotationSet
from src.annotations.io_xnat import (
    upload_annotation_set,
    download_annotation_set,
    UploadResult,
    DownloadResult,
    MANIFEST_FILENAME,
)
from src.services.errors import FriendlyError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_ANNOTATORS = ["worker_A1", "worker_B2", "worker_C3"]
_IMAGE_REF = "subjects/S001/experiments/E01/scans/scan1"
_PROJECT = "TEST_PROJ"


def _sparse_mask(seed: int = 0, rows: int = 32, cols: int = 32) -> np.ndarray:
    rng = np.random.default_rng(seed)
    mask = np.zeros((rows, cols), dtype=np.uint8)
    n_fg = max(1, int(rows * cols * 0.05))
    flat_idx = rng.choice(rows * cols, size=n_fg, replace=False)
    mask.ravel()[flat_idx] = 1
    return mask


def _make_annotation(annotator_id: str, version: int = 1, seed: int = 0) -> Annotation:
    return Annotation(
        annotator_id=annotator_id,
        tool="test_tool",
        annotation_type="binary_segmentation",
        created_at="2026-06-05T12:00:00Z",
        version=version,
        payload=_sparse_mask(seed=seed),
    )


def _make_annotation_set(annotators: List[str]) -> AnnotationSet:
    aset = AnnotationSet(image_ref=_IMAGE_REF)
    for i, aid in enumerate(annotators):
        aset.add(_make_annotation(aid, version=1, seed=i))
    return aset


# ---------------------------------------------------------------------------
# SC-003: image NOT re-uploaded; K blobs + 1 manifest written
# ---------------------------------------------------------------------------

class TestUploadBlobsAndManifest:
    def test_k_annotators_k_blobs_plus_manifest(self, tmp_path: Path):
        """K annotators → K versioned blobs + 1 manifest; no image upload."""
        fake = FakeXNAT(project_name=_PROJECT)
        aset = _make_annotation_set(_ANNOTATORS)

        result = upload_annotation_set(
            fake, _IMAGE_REF, aset, project_name=_PROJECT
        )

        assert result.ok
        # K blobs + 1 manifest
        assert len(result.files_written) == len(_ANNOTATORS) + 1
        assert MANIFEST_FILENAME in result.files_written

        # Each blob file has the versioned naming scheme
        blob_files = [f for f in result.files_written if f != MANIFEST_FILENAME]
        for blob_fn in blob_files:
            assert blob_fn.startswith("ann__")
            assert "__v1" in blob_fn

    def test_no_image_upload_among_recorded_calls(self, tmp_path: Path):
        """SC-003: resource.put_zip and no image blob appear in recorded calls."""
        fake = FakeXNAT(project_name=_PROJECT)
        aset = _make_annotation_set(_ANNOTATORS)

        upload_annotation_set(fake, _IMAGE_REF, aset, project_name=_PROJECT)

        # No put_zip call (image upload uses put_zip)
        put_zip_calls = [c for c in fake.calls if c["op"] == "resource.put_zip"]
        assert len(put_zip_calls) == 0, "Image must NOT be re-uploaded"

        # All writes are file.put with content=ANNOTATION (not IMAGE)
        put_calls = [c for c in fake.calls if c["op"] == "file.put"]
        image_puts = [c for c in put_calls if c["kwargs"].get("content") == "IMAGE"]
        assert len(image_puts) == 0, "No file.put with content=IMAGE should appear"

    def test_blob_filenames_encode_annotator_type_version(self):
        fake = FakeXNAT(project_name=_PROJECT)
        aset = _make_annotation_set(["worker_A1"])

        result = upload_annotation_set(fake, _IMAGE_REF, aset, project_name=_PROJECT)

        blob_files = [f for f in result.files_written if f != MANIFEST_FILENAME]
        assert len(blob_files) == 1
        fn = blob_files[0]
        # Pattern: ann__<annotator>__<type>__v<version>.<ext>
        assert re.match(r"ann__worker_A1__binary_segmentation__v1\.\w+", fn), fn

    def test_overwrite_false_on_blob_puts(self):
        """keep-all: every blob put call has overwrite=False."""
        fake = FakeXNAT(project_name=_PROJECT)
        aset = _make_annotation_set(_ANNOTATORS)

        upload_annotation_set(fake, _IMAGE_REF, aset, project_name=_PROJECT)

        blob_puts = [
            c for c in fake.calls
            if c["op"] == "file.put" and c["kwargs"].get("_filename") != MANIFEST_FILENAME
        ]
        for call in blob_puts:
            assert call["kwargs"]["overwrite"] is False, (
                f"Expected overwrite=False on blob put, got {call['kwargs']['overwrite']}"
            )


# ---------------------------------------------------------------------------
# Keep-all versioning: same annotator re-submits with version+1
# ---------------------------------------------------------------------------

class TestKeepAllVersioning:
    def test_resubmit_both_versions_present(self):
        """Re-submit by same annotator (version+1) → both v1 and v2 on server."""
        fake = FakeXNAT(project_name=_PROJECT)

        # Upload v1
        aset_v1 = AnnotationSet(image_ref=_IMAGE_REF)
        aset_v1.add(_make_annotation("worker_A1", version=1, seed=0))
        r1 = upload_annotation_set(fake, _IMAGE_REF, aset_v1, project_name=_PROJECT)
        assert r1.ok

        # Re-upload v2 (same annotator, new version)
        aset_v2 = AnnotationSet(image_ref=_IMAGE_REF)
        aset_v2.add(_make_annotation("worker_A1", version=2, seed=99))
        r2 = upload_annotation_set(fake, _IMAGE_REF, aset_v2, project_name=_PROJECT)
        assert r2.ok

        # Both versioned blobs should be among the written files
        all_files = r1.files_written + r2.files_written
        v1_blobs = [f for f in all_files if "__v1" in f and f != MANIFEST_FILENAME]
        v2_blobs = [f for f in all_files if "__v2" in f and f != MANIFEST_FILENAME]
        assert len(v1_blobs) >= 1, "v1 blob must be written"
        assert len(v2_blobs) >= 1, "v2 blob must be written"

    def test_resubmit_distinct_filenames(self):
        """v1 and v2 blob filenames are distinct — nothing overwritten."""
        fake = FakeXNAT(project_name=_PROJECT)

        aset_v1 = AnnotationSet(image_ref=_IMAGE_REF)
        aset_v1.add(_make_annotation("worker_A1", version=1, seed=0))
        r1 = upload_annotation_set(fake, _IMAGE_REF, aset_v1, project_name=_PROJECT)

        aset_v2 = AnnotationSet(image_ref=_IMAGE_REF)
        aset_v2.add(_make_annotation("worker_A1", version=2, seed=99))
        r2 = upload_annotation_set(fake, _IMAGE_REF, aset_v2, project_name=_PROJECT)

        blobs_v1 = {f for f in r1.files_written if f != MANIFEST_FILENAME}
        blobs_v2 = {f for f in r2.files_written if f != MANIFEST_FILENAME}
        overlap = blobs_v1 & blobs_v2
        assert len(overlap) == 0, f"Blob filenames must not overlap across versions: {overlap}"


# ---------------------------------------------------------------------------
# Round-trip: upload → download → decoded masks equal originals bit-for-bit
# ---------------------------------------------------------------------------

class TestRoundTrip:
    def test_decoded_masks_equal_originals(self, tmp_path: Path):
        """Upload K masks → download → rebuilt AnnotationSet payloads match bit-for-bit."""
        fake = FakeXNAT(project_name=_PROJECT)
        originals = [_sparse_mask(seed=i) for i in range(len(_ANNOTATORS))]

        aset = AnnotationSet(image_ref=_IMAGE_REF)
        for aid, mask in zip(_ANNOTATORS, originals):
            ann = Annotation(
                annotator_id=aid,
                tool="test_tool",
                annotation_type="binary_segmentation",
                created_at="2026-06-05T12:00:00Z",
                version=1,
                payload=mask,
            )
            aset.add(ann)

        up = upload_annotation_set(fake, _IMAGE_REF, aset, project_name=_PROJECT)
        assert up.ok

        dest = tmp_path / "downloaded"
        down = download_annotation_set(
            fake, _IMAGE_REF, dest, project_name=_PROJECT
        )
        assert down.ok, f"Download failed: {down.friendly}"
        rebuilt = down.annotation_set
        assert rebuilt is not None
        assert len(rebuilt.annotations) == len(_ANNOTATORS)

        for i, (ann, orig_mask) in enumerate(zip(rebuilt.annotations, originals)):
            np.testing.assert_array_equal(
                ann.payload, orig_mask,
                err_msg=f"Mask mismatch for annotation {i} (annotator={ann.annotator_id})"
            )

    def test_files_written_are_pathlib_paths(self, tmp_path: Path):
        """files_written must be pathlib.Path instances (cross-platform)."""
        fake = FakeXNAT(project_name=_PROJECT)
        aset = _make_annotation_set(["worker_A1"])

        upload_annotation_set(fake, _IMAGE_REF, aset, project_name=_PROJECT)
        dest = tmp_path / "dl"
        down = download_annotation_set(fake, _IMAGE_REF, dest, project_name=_PROJECT)

        assert down.ok
        for p in down.files_written:
            assert isinstance(p, Path), f"Expected Path, got {type(p)}: {p!r}"

    def test_files_written_under_dest_dir(self, tmp_path: Path):
        """All downloaded files live under dest_dir."""
        fake = FakeXNAT(project_name=_PROJECT)
        aset = _make_annotation_set(["worker_A1"])

        upload_annotation_set(fake, _IMAGE_REF, aset, project_name=_PROJECT)
        dest = tmp_path / "dl_dir"
        down = download_annotation_set(fake, _IMAGE_REF, dest, project_name=_PROJECT)

        assert down.ok
        for p in down.files_written:
            assert p.is_relative_to(dest), f"{p} is not under {dest}"

    def test_manifest_and_blob_files_exist_on_disk(self, tmp_path: Path):
        """All files in files_written actually exist on disk after download."""
        fake = FakeXNAT(project_name=_PROJECT)
        aset = _make_annotation_set(["worker_A1"])

        upload_annotation_set(fake, _IMAGE_REF, aset, project_name=_PROJECT)
        dest = tmp_path / "verify"
        down = download_annotation_set(fake, _IMAGE_REF, dest, project_name=_PROJECT)

        assert down.ok
        for p in down.files_written:
            assert p.exists(), f"Expected file on disk: {p}"
            assert p.stat().st_size > 0, f"File is empty: {p}"


# ---------------------------------------------------------------------------
# SC-006: PHI-free manifest — no patient-identifier annotator_ids
# ---------------------------------------------------------------------------

class TestPhiFreeManifest:
    def test_annotator_ids_are_opaque_tokens(self):
        """manifest.json annotator_id values must pass the opaque-token pattern."""
        fake = FakeXNAT(project_name=_PROJECT)
        aset = _make_annotation_set(_ANNOTATORS)

        upload_annotation_set(fake, _IMAGE_REF, aset, project_name=_PROJECT)

        # Find the manifest file.put call and recover the manifest bytes via FakeXNAT
        # (FakeXNAT stores bytes keyed by filename; use set_file_content trick: read
        #  the bytes that were stored during put, which the round-trip tests prove work)
        manifest_bytes = fake._file_contents.get(MANIFEST_FILENAME)
        assert manifest_bytes is not None, "manifest.json bytes not in FakeXNAT store"

        manifest = json.loads(manifest_bytes.decode("utf-8"))

        # Validate: every annotator_id is an opaque token (no spaces, not human name)
        human_name_re = re.compile(r"\s")
        opaque_token_re = re.compile(r"^[A-Za-z0-9_-]+$")
        for entry in manifest["annotations"]:
            aid = entry["annotator_id"]
            assert not human_name_re.search(aid), (
                f"SC-006 FAIL: annotator_id '{aid}' contains whitespace (possible PHI)"
            )
            assert opaque_token_re.match(aid), (
                f"SC-006 FAIL: annotator_id '{aid}' is not an opaque token"
            )

    def test_manifest_has_no_patient_fields(self):
        """manifest.json must not contain fields named after common PHI keys."""
        fake = FakeXNAT(project_name=_PROJECT)
        aset = _make_annotation_set(["worker_A1"])
        upload_annotation_set(fake, _IMAGE_REF, aset, project_name=_PROJECT)

        manifest_bytes = fake._file_contents.get(MANIFEST_FILENAME)
        manifest_text = manifest_bytes.decode("utf-8")

        phi_keys = ["patient_name", "patient_id", "mrn", "dob", "ssn", "date_of_birth"]
        for phi_key in phi_keys:
            assert phi_key not in manifest_text.lower(), (
                f"SC-006 FAIL: PHI key '{phi_key}' found in manifest"
            )


# ---------------------------------------------------------------------------
# Fail-soft: connection failure → FriendlyError, no raw traceback
# ---------------------------------------------------------------------------

class TestFailSoft:
    def test_upload_connection_failure_returns_friendly_error(self):
        """Connection error on blob put → ok=False, FriendlyError, no traceback leak."""
        fake = FakeXNAT(project_name=_PROJECT)
        aset = _make_annotation_set(["worker_A1"])

        fake.set_next_failure(ConnectionError("simulated upload failure"))
        result = upload_annotation_set(fake, _IMAGE_REF, aset, project_name=_PROJECT)

        assert not result.ok
        assert isinstance(result.friendly, FriendlyError)
        assert result.friendly.title  # non-empty title
        # No raw traceback: result.friendly is a structured FriendlyError, not Exception
        assert not isinstance(result.friendly, Exception)

    def test_download_connection_failure_returns_friendly_error(self, tmp_path: Path):
        """Connection error on manifest get_copy → ok=False, FriendlyError."""
        fake = FakeXNAT(project_name=_PROJECT)

        # Prime a valid upload first, then inject failure for download
        aset = _make_annotation_set(["worker_A1"])
        upload_annotation_set(fake, _IMAGE_REF, aset, project_name=_PROJECT)

        fake.set_next_failure(TimeoutError("simulated download timeout"))
        dest = tmp_path / "fail_dl"
        result = download_annotation_set(fake, _IMAGE_REF, dest, project_name=_PROJECT)

        assert not result.ok
        assert isinstance(result.friendly, FriendlyError)
        assert result.annotation_set is None

    def test_upload_failure_result_is_friendly_error_not_exception(self):
        """Caller must never see a raw exception bubble — FriendlyError is returned."""
        fake = FakeXNAT(project_name=_PROJECT)
        aset = _make_annotation_set(["worker_A1"])

        fake.set_next_failure(OSError("SSL cert failure"))

        try:
            result = upload_annotation_set(fake, _IMAGE_REF, aset, project_name=_PROJECT)
        except Exception as exc:  # noqa: BLE001
            pytest.fail(f"upload_annotation_set must not raise; got {exc!r}")

        assert not result.ok
        assert result.friendly is not None


# ---------------------------------------------------------------------------
# Upload result structure
# ---------------------------------------------------------------------------

class TestUploadResultStructure:
    def test_result_is_upload_result_instance(self):
        fake = FakeXNAT(project_name=_PROJECT)
        aset = _make_annotation_set(["worker_A1"])
        result = upload_annotation_set(fake, _IMAGE_REF, aset, project_name=_PROJECT)
        assert isinstance(result, UploadResult)

    def test_files_written_contains_strings(self):
        fake = FakeXNAT(project_name=_PROJECT)
        aset = _make_annotation_set(["worker_A1"])
        result = upload_annotation_set(fake, _IMAGE_REF, aset, project_name=_PROJECT)
        assert result.ok
        for fn in result.files_written:
            assert isinstance(fn, str), f"Expected str filename, got {type(fn)}: {fn!r}"

    def test_friendly_is_none_on_success(self):
        fake = FakeXNAT(project_name=_PROJECT)
        aset = _make_annotation_set(["worker_A1"])
        result = upload_annotation_set(fake, _IMAGE_REF, aset, project_name=_PROJECT)
        assert result.ok
        assert result.friendly is None


# ---------------------------------------------------------------------------
# Download result structure
# ---------------------------------------------------------------------------

class TestDownloadResultStructure:
    def test_result_is_download_result_instance(self, tmp_path: Path):
        fake = FakeXNAT(project_name=_PROJECT)
        aset = _make_annotation_set(["worker_A1"])
        upload_annotation_set(fake, _IMAGE_REF, aset, project_name=_PROJECT)

        result = download_annotation_set(
            fake, _IMAGE_REF, tmp_path / "dl", project_name=_PROJECT
        )
        assert isinstance(result, DownloadResult)

    def test_annotation_set_image_ref_preserved(self, tmp_path: Path):
        fake = FakeXNAT(project_name=_PROJECT)
        aset = _make_annotation_set(["worker_A1"])
        upload_annotation_set(fake, _IMAGE_REF, aset, project_name=_PROJECT)

        result = download_annotation_set(
            fake, _IMAGE_REF, tmp_path / "dl2", project_name=_PROJECT
        )
        assert result.ok
        assert result.annotation_set.image_ref == _IMAGE_REF

    def test_friendly_is_none_on_success(self, tmp_path: Path):
        fake = FakeXNAT(project_name=_PROJECT)
        aset = _make_annotation_set(["worker_A1"])
        upload_annotation_set(fake, _IMAGE_REF, aset, project_name=_PROJECT)

        result = download_annotation_set(
            fake, _IMAGE_REF, tmp_path / "dl3", project_name=_PROJECT
        )
        assert result.ok
        assert result.friendly is None
