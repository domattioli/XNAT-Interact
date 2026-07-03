from __future__ import annotations
import sys
from pathlib import Path
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.annotations.model import AnnotationSet
from src.annotations.exc import AnnotationError


def _manifest(annotator_id: str) -> dict:
    return {
        "image_ref": "subj/exp/scan/img",
        "annotations": [
            {
                "index": 0,
                "annotator_id": annotator_id,
                "tool": "test-tool",
                "annotation_type": "bbox",
                "created_at": "2026-01-01T00:00:00Z",
                "version": 1,
                "derived": False,
                "codec": None,
            }
        ],
    }


def test_from_manifest_accepts_valid_opaque_id():
    aset = AnnotationSet.from_manifest(_manifest("worker_A1"))
    assert len(aset.annotations) == 1
    assert aset.annotations[0].annotator_id == "worker_A1"


def test_from_manifest_rejects_human_name_with_spaces():
    # S3: a name-like annotator_id (whitespace => possible PHI) must be rejected on load.
    with pytest.raises(AnnotationError):
        AnnotationSet.from_manifest(_manifest("John Doe"))


def test_from_manifest_rejects_empty_id():
    with pytest.raises(AnnotationError):
        AnnotationSet.from_manifest(_manifest(""))


def test_from_manifest_rejects_special_chars():
    with pytest.raises(AnnotationError):
        AnnotationSet.from_manifest(_manifest("worker/../etc"))
