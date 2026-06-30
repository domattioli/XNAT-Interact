"""
tests/test_annotation_manifest_merge_m6.py — Regression for M6 / issue #33.

Offline, synthetic data, FakeXNAT, no PHI, no live server.

Bug (pre-fix): upload_annotation_set rebuilt manifest.json from ONLY the
current upload's annotations, so a v2 upload overwrote the manifest and
orphaned all v1 blobs — download_annotation_set silently missed them.

Fix: before writing the new manifest, fetch any existing manifest from the
server and merge its entries with the new ones (dedup by blob_filename,
re-index sequentially).
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import List

import numpy as np
import pytest

from tests.fakes.fake_xnat import FakeXNAT
from src.annotations.model import Annotation, AnnotationSet
from src.annotations.io_xnat import (
    MANIFEST_FILENAME,
    download_annotation_set,
    upload_annotation_set,
)

# ---------------------------------------------------------------------------
# Shared fixtures / helpers
# ---------------------------------------------------------------------------

_IMAGE_REF = "subjects/S001/experiments/E01/scans/scan1"
_PROJECT = "M6_TEST_PROJ"


def _sparse_mask(seed: int = 0, rows: int = 16, cols: int = 16) -> np.ndarray:
    rng = np.random.default_rng(seed)
    mask = np.zeros((rows, cols), dtype=np.uint8)
    n_fg = max(1, int(rows * cols * 0.05))
    flat_idx = rng.choice(rows * cols, size=n_fg, replace=False)
    mask.ravel()[flat_idx] = 1
    return mask


def _make_ann(annotator_id: str, version: int, seed: int = 0) -> Annotation:
    return Annotation(
        annotator_id=annotator_id,
        tool="test_tool",
        annotation_type="binary_segmentation",
        created_at="2026-06-29T00:00:00Z",
        version=version,
        payload=_sparse_mask(seed=seed),
    )


def _manifest_from_fake(fake: FakeXNAT) -> dict:
    """Read and parse manifest.json bytes stored in the FakeXNAT."""
    raw = fake._file_contents.get(MANIFEST_FILENAME)
    assert raw is not None, "manifest.json not found in FakeXNAT store"
    return json.loads(raw.decode("utf-8"))


# ---------------------------------------------------------------------------
# Core regression: v1 blob survives v2 upload
# ---------------------------------------------------------------------------

class TestManifestMergeM6:
    def test_v1_blob_filename_survives_v2_upload(self):
        """
        After uploading v2, the manifest must still index the v1 blob_filename.
        Pre-fix: the v1 entry was overwritten → v1 blob orphaned.
        """
        fake = FakeXNAT(project_name=_PROJECT)

        # v1 upload
        aset_v1 = AnnotationSet(image_ref=_IMAGE_REF)
        aset_v1.add(_make_ann("worker_A1", version=1, seed=0))
        r1 = upload_annotation_set(fake, _IMAGE_REF, aset_v1, project_name=_PROJECT)
        assert r1.ok, f"v1 upload failed: {r1.friendly}"

        v1_blob_fn = next(f for f in r1.files_written if f != MANIFEST_FILENAME)

        # v2 upload (same annotator, new version → distinct blob filename)
        aset_v2 = AnnotationSet(image_ref=_IMAGE_REF)
        aset_v2.add(_make_ann("worker_A1", version=2, seed=99))
        r2 = upload_annotation_set(fake, _IMAGE_REF, aset_v2, project_name=_PROJECT)
        assert r2.ok, f"v2 upload failed: {r2.friendly}"

        manifest = _manifest_from_fake(fake)
        indexed_fns = {e["blob_filename"] for e in manifest["annotations"]}

        # The v1 blob must still be indexed — this is the M6 regression guard.
        assert v1_blob_fn in indexed_fns, (
            f"M6 regression: v1 blob '{v1_blob_fn}' was dropped from manifest "
            f"after v2 upload. Indexed: {indexed_fns}"
        )

    def test_manifest_indexes_both_versions(self):
        """manifest must contain exactly 2 entries — one per version."""
        fake = FakeXNAT(project_name=_PROJECT)

        aset_v1 = AnnotationSet(image_ref=_IMAGE_REF)
        aset_v1.add(_make_ann("worker_A1", version=1, seed=1))
        upload_annotation_set(fake, _IMAGE_REF, aset_v1, project_name=_PROJECT)

        aset_v2 = AnnotationSet(image_ref=_IMAGE_REF)
        aset_v2.add(_make_ann("worker_A1", version=2, seed=2))
        upload_annotation_set(fake, _IMAGE_REF, aset_v2, project_name=_PROJECT)

        manifest = _manifest_from_fake(fake)
        assert len(manifest["annotations"]) == 2, (
            f"Expected 2 manifest entries (v1 + v2), got {len(manifest['annotations'])}"
        )

    def test_manifest_index_values_unique_and_contiguous(self):
        """After 2-version upload, index values must be 0, 1 (unique + contiguous)."""
        fake = FakeXNAT(project_name=_PROJECT)

        for version in (1, 2):
            aset = AnnotationSet(image_ref=_IMAGE_REF)
            aset.add(_make_ann("worker_A1", version=version, seed=version))
            upload_annotation_set(fake, _IMAGE_REF, aset, project_name=_PROJECT)

        manifest = _manifest_from_fake(fake)
        indices = [e["index"] for e in manifest["annotations"]]
        assert sorted(indices) == list(range(len(indices))), (
            f"index values not contiguous 0..N-1: {indices}"
        )
        assert len(set(indices)) == len(indices), (
            f"index values not unique: {indices}"
        )

    def test_manifest_index_unique_after_three_versions(self):
        """Three sequential uploads → 3 entries, unique contiguous indices."""
        fake = FakeXNAT(project_name=_PROJECT)

        for version in (1, 2, 3):
            aset = AnnotationSet(image_ref=_IMAGE_REF)
            aset.add(_make_ann("worker_A1", version=version, seed=version * 10))
            upload_annotation_set(fake, _IMAGE_REF, aset, project_name=_PROJECT)

        manifest = _manifest_from_fake(fake)
        assert len(manifest["annotations"]) == 3
        indices = [e["index"] for e in manifest["annotations"]]
        assert sorted(indices) == [0, 1, 2]

    def test_idempotent_reupload_of_same_version_no_duplicate_entry(self):
        """Re-uploading the identical v1 (same blob_filename) is idempotent — no duplicate."""
        fake = FakeXNAT(project_name=_PROJECT)

        aset_v1 = AnnotationSet(image_ref=_IMAGE_REF)
        aset_v1.add(_make_ann("worker_A1", version=1, seed=7))
        upload_annotation_set(fake, _IMAGE_REF, aset_v1, project_name=_PROJECT)
        # Upload identical v1 again
        upload_annotation_set(fake, _IMAGE_REF, aset_v1, project_name=_PROJECT)

        manifest = _manifest_from_fake(fake)
        assert len(manifest["annotations"]) == 1, (
            f"Idempotent re-upload must not create duplicate entries; "
            f"got {len(manifest['annotations'])}"
        )


# ---------------------------------------------------------------------------
# Download history preserved after v2 upload
# ---------------------------------------------------------------------------

class TestDownloadHistoryM6:
    def test_download_after_v2_returns_both_versions(self, tmp_path: Path):
        """
        download_annotation_set after a v2 upload must rebuild an AnnotationSet
        containing BOTH v1 and v2 annotations.  Pre-fix: only v2 was returned.
        """
        fake = FakeXNAT(project_name=_PROJECT)

        aset_v1 = AnnotationSet(image_ref=_IMAGE_REF)
        aset_v1.add(_make_ann("worker_A1", version=1, seed=11))
        upload_annotation_set(fake, _IMAGE_REF, aset_v1, project_name=_PROJECT)

        aset_v2 = AnnotationSet(image_ref=_IMAGE_REF)
        aset_v2.add(_make_ann("worker_A1", version=2, seed=22))
        upload_annotation_set(fake, _IMAGE_REF, aset_v2, project_name=_PROJECT)

        dest = tmp_path / "dl"
        result = download_annotation_set(fake, _IMAGE_REF, dest, project_name=_PROJECT)
        assert result.ok, f"Download failed: {result.friendly}"

        versions = {ann.version for ann in result.annotation_set.annotations}
        assert 1 in versions, "v1 annotation missing from download after v2 upload (M6 regression)"
        assert 2 in versions, "v2 annotation missing from download"

    def test_download_files_written_include_both_blobs(self, tmp_path: Path):
        """Both v1 and v2 blob files must appear in download files_written."""
        fake = FakeXNAT(project_name=_PROJECT)

        r1 = None
        for version in (1, 2):
            aset = AnnotationSet(image_ref=_IMAGE_REF)
            aset.add(_make_ann("worker_A1", version=version, seed=version * 3))
            r = upload_annotation_set(fake, _IMAGE_REF, aset, project_name=_PROJECT)
            assert r.ok
            if version == 1:
                r1 = r

        v1_blob_fn = next(f for f in r1.files_written if f != MANIFEST_FILENAME)

        dest = tmp_path / "dl2"
        result = download_annotation_set(fake, _IMAGE_REF, dest, project_name=_PROJECT)
        assert result.ok

        written_names = {p.name for p in result.files_written}
        assert v1_blob_fn in written_names, (
            f"v1 blob '{v1_blob_fn}' not in download files_written: {written_names}"
        )

    def test_v1_mask_round_trips_correctly_after_v2_upload(self, tmp_path: Path):
        """v1 payload decoded from download must equal the original v1 mask."""
        fake = FakeXNAT(project_name=_PROJECT)

        v1_mask = _sparse_mask(seed=55)
        aset_v1 = AnnotationSet(image_ref=_IMAGE_REF)
        aset_v1.add(Annotation(
            annotator_id="worker_A1",
            tool="test_tool",
            annotation_type="binary_segmentation",
            created_at="2026-06-29T00:00:00Z",
            version=1,
            payload=v1_mask,
        ))
        upload_annotation_set(fake, _IMAGE_REF, aset_v1, project_name=_PROJECT)

        aset_v2 = AnnotationSet(image_ref=_IMAGE_REF)
        aset_v2.add(_make_ann("worker_A1", version=2, seed=66))
        upload_annotation_set(fake, _IMAGE_REF, aset_v2, project_name=_PROJECT)

        dest = tmp_path / "dl3"
        result = download_annotation_set(fake, _IMAGE_REF, dest, project_name=_PROJECT)
        assert result.ok

        ann_by_version = {ann.version: ann for ann in result.annotation_set.annotations}
        assert 1 in ann_by_version, "v1 annotation missing"
        np.testing.assert_array_equal(
            ann_by_version[1].payload, v1_mask,
            err_msg="v1 mask decoded incorrectly after v2 upload",
        )


# ---------------------------------------------------------------------------
# Sanity: first upload (no prior manifest) must not crash
# ---------------------------------------------------------------------------

class TestFirstUploadNoManifest:
    def test_first_upload_succeeds_with_no_prior_manifest(self, tmp_path: Path):
        """No existing manifest on server → upload must succeed without raising."""
        fake = FakeXNAT(project_name=_PROJECT)
        aset = AnnotationSet(image_ref=_IMAGE_REF)
        aset.add(_make_ann("worker_A1", version=1, seed=0))

        result = upload_annotation_set(fake, _IMAGE_REF, aset, project_name=_PROJECT)
        assert result.ok, f"First upload (no prior manifest) failed: {result.friendly}"

    def test_first_upload_manifest_has_correct_entry_count(self):
        """First upload with 2 annotations → manifest has exactly 2 entries."""
        fake = FakeXNAT(project_name=_PROJECT)
        aset = AnnotationSet(image_ref=_IMAGE_REF)
        aset.add(_make_ann("worker_A1", version=1, seed=0))
        aset.add(_make_ann("worker_B2", version=1, seed=1))

        upload_annotation_set(fake, _IMAGE_REF, aset, project_name=_PROJECT)

        manifest = _manifest_from_fake(fake)
        assert len(manifest["annotations"]) == 2

    def test_first_upload_then_download_round_trip(self, tmp_path: Path):
        """Sanity: first-upload → download still works after M6 fix."""
        fake = FakeXNAT(project_name=_PROJECT)
        mask = _sparse_mask(seed=42)
        aset = AnnotationSet(image_ref=_IMAGE_REF)
        aset.add(Annotation(
            annotator_id="worker_A1",
            tool="test_tool",
            annotation_type="binary_segmentation",
            created_at="2026-06-29T00:00:00Z",
            version=1,
            payload=mask,
        ))
        upload_annotation_set(fake, _IMAGE_REF, aset, project_name=_PROJECT)

        dest = tmp_path / "dl"
        result = download_annotation_set(fake, _IMAGE_REF, dest, project_name=_PROJECT)
        assert result.ok, f"Round-trip failed: {result.friendly}"
        assert len(result.annotation_set.annotations) == 1
        np.testing.assert_array_equal(result.annotation_set.annotations[0].payload, mask)
