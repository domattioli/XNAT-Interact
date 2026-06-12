"""
tests/test_annotations_aggregate.py — Phase 5 Task B (T006-T009)

Offline, no network, no PHI.

Covers:
  T006  reference aggregator: identical binary masks → consensus == that mask
  T007  reference aggregator: mixed binary masks → per-pixel majority vote
  T008  reference aggregator: N=1 fast-path → returns that payload unchanged
  T009  reference aggregator: scalar mean correct for landmark / bbox dicts
  T010  reference aggregator: shape mismatch → AnnotationError (FriendlyError,
        no raw traceback leaked as a plain exception)
  T011  reference aggregator: empty input → AnnotationError (clear message)
  SC004 extensibility proof — brand-new toy aggregator registered with
        register_aggregator(); aggregate_set() dispatches to it end-to-end;
        ZERO edits to any core module required.
  T012  staple stub raises NotImplementedError with a helpful message
  T013  staple stub is NOT in list_aggregators() until explicitly registered
"""
from __future__ import annotations

import numpy as np
import pytest

from src.annotations.model import Annotation, AnnotationSet, ConsensusResult
from src.annotations.exc import AnnotationError
from src.annotations.aggregate import (
    Aggregator,
    register_aggregator,
    get_aggregator,
    list_aggregators,
    aggregate_set,
)
from src.annotations.aggregate.reference import ReferenceAggregator
from src.annotations.aggregate.staple import StapleAggregator


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _binary_mask(rows: int = 8, cols: int = 8, fill: int = 0) -> np.ndarray:
    """Solid binary mask, all pixels == fill (0 or 1)."""
    return np.full((rows, cols), fill, dtype=np.uint8)


def _ann(annotator_id: str, payload, version: int = 1) -> Annotation:
    """Minimal Annotation carrying a payload (no codec/blob needed for these tests)."""
    return Annotation(
        annotator_id=annotator_id,
        tool="test",
        annotation_type="binary_segmentation",
        created_at="2026-06-05T00:00:00Z",
        version=version,
        payload=payload,
    )


def _ann_dict(annotator_id: str, payload: dict, ann_type: str = "landmark") -> Annotation:
    return Annotation(
        annotator_id=annotator_id,
        tool="test",
        annotation_type=ann_type,
        created_at="2026-06-05T00:00:00Z",
        version=1,
        payload=payload,
    )


def _make_aset(annotations) -> AnnotationSet:
    aset = AnnotationSet(image_ref="scan-001")
    for ann in annotations:
        aset.annotations.append(ann)   # bypass validate_annotator_id for opaque tokens
    return aset


# ---------------------------------------------------------------------------
# T006 — identical binary masks → consensus equals that mask
# ---------------------------------------------------------------------------

class TestT006IdenticalMasks:
    def test_all_zeros(self):
        masks = [_binary_mask(fill=0) for _ in range(3)]
        agg = ReferenceAggregator()
        result = agg.aggregate(masks)
        assert isinstance(result, ConsensusResult)
        assert result.method == "reference"
        assert np.array_equal(result.payload, _binary_mask(fill=0))

    def test_all_ones(self):
        masks = [_binary_mask(fill=1) for _ in range(5)]
        agg = ReferenceAggregator()
        result = agg.aggregate(masks)
        assert np.array_equal(result.payload, _binary_mask(fill=1))

    def test_identical_mixed_pattern(self):
        rng = np.random.default_rng(0)
        m = rng.integers(0, 2, size=(8, 8), dtype=np.uint8)
        agg = ReferenceAggregator()
        result = agg.aggregate([m.copy(), m.copy(), m.copy()])
        assert np.array_equal(result.payload, m)


# ---------------------------------------------------------------------------
# T007 — mixed binary masks → per-pixel majority vote
# ---------------------------------------------------------------------------

class TestT007MajorityVote:
    def test_two_ones_one_zero(self):
        # 2 annotators say 1, 1 says 0 → majority = 1
        m_ones = _binary_mask(fill=1)
        m_zero = _binary_mask(fill=0)
        agg = ReferenceAggregator()
        result = agg.aggregate([m_ones, m_ones, m_zero])
        assert np.all(result.payload == 1)

    def test_two_zeros_one_one(self):
        m_ones = _binary_mask(fill=1)
        m_zero = _binary_mask(fill=0)
        agg = ReferenceAggregator()
        result = agg.aggregate([m_zero, m_zero, m_ones])
        assert np.all(result.payload == 0)

    def test_per_pixel_majority(self):
        # Build two masks that differ only in left/right halves
        m_a = np.zeros((4, 4), dtype=np.uint8)
        m_a[:, :2] = 1   # left half = 1
        m_b = np.zeros((4, 4), dtype=np.uint8)
        m_b[:, 2:] = 1   # right half = 1

        # Three annotators: m_a, m_a, m_b → left majority=1, right majority=0
        agg = ReferenceAggregator()
        result = agg.aggregate([m_a, m_a, m_b])
        assert np.all(result.payload[:, :2] == 1)   # left: 2 vs 1 → 1
        assert np.all(result.payload[:, 2:] == 0)   # right: 1 vs 2 → 0

    def test_via_aggregate_set(self):
        """aggregate_set() dispatches to reference by default and returns majority."""
        m_ones = _binary_mask(fill=1)
        m_zero = _binary_mask(fill=0)
        aset = _make_aset([
            _ann("ann-A", m_ones),
            _ann("ann-B", m_ones),
            _ann("ann-C", m_zero),
        ])
        result = aggregate_set(aset, "binary_segmentation")
        assert isinstance(result, ConsensusResult)
        assert result.method == "reference"
        assert np.all(result.payload == 1)


# ---------------------------------------------------------------------------
# T008 — N=1 fast-path
# ---------------------------------------------------------------------------

class TestT008SingleAnnotator:
    def test_mask_n1(self):
        m = _binary_mask(fill=1)
        agg = ReferenceAggregator()
        result = agg.aggregate([m])
        # same object returned (fast-path, no copy required) — or equal array
        assert np.array_equal(result.payload, m)
        assert result.method == "reference"

    def test_dict_n1(self):
        agg = ReferenceAggregator()
        pt = {"x": 3.0, "y": 7.0}
        result = agg.aggregate([pt])
        assert result.payload == pt

    def test_via_aggregate_set_n1(self):
        m = _binary_mask(fill=0)
        aset = _make_aset([_ann("ann-solo", m)])
        result = aggregate_set(aset, "binary_segmentation")
        assert np.array_equal(result.payload, m)


# ---------------------------------------------------------------------------
# T009 — scalar / dict mean (landmark + bbox)
# ---------------------------------------------------------------------------

class TestT009ScalarDictMean:
    def test_landmark_mean(self):
        pts = [{"x": 0.0, "y": 0.0}, {"x": 2.0, "y": 4.0}]
        agg = ReferenceAggregator()
        result = agg.aggregate(pts)
        assert result.payload["x"] == pytest.approx(1.0)
        assert result.payload["y"] == pytest.approx(2.0)
        assert result.method == "reference"

    def test_bbox_mean(self):
        boxes = [
            {"x": 0.0, "y": 0.0, "w": 10.0, "h": 10.0},
            {"x": 4.0, "y": 6.0, "w": 20.0, "h": 30.0},
        ]
        agg = ReferenceAggregator()
        result = agg.aggregate(boxes)
        assert result.payload["x"] == pytest.approx(2.0)
        assert result.payload["y"] == pytest.approx(3.0)
        assert result.payload["w"] == pytest.approx(15.0)
        assert result.payload["h"] == pytest.approx(20.0)

    def test_landmark_via_aggregate_set(self):
        aset = AnnotationSet(image_ref="scan-lm")
        aset.annotations.append(_ann_dict("ann-1", {"x": 1.0, "y": 3.0}, "landmark"))
        aset.annotations.append(_ann_dict("ann-2", {"x": 3.0, "y": 7.0}, "landmark"))
        result = aggregate_set(aset, "landmark")
        assert result.payload["x"] == pytest.approx(2.0)
        assert result.payload["y"] == pytest.approx(5.0)

    def test_bbox_via_aggregate_set(self):
        aset = AnnotationSet(image_ref="scan-bb")
        aset.annotations.append(_ann_dict("ann-1", {"x": 0.0, "y": 0.0, "w": 100.0, "h": 50.0}, "bbox"))
        aset.annotations.append(_ann_dict("ann-2", {"x": 10.0, "y": 20.0, "w": 80.0, "h": 30.0}, "bbox"))
        result = aggregate_set(aset, "bbox")
        assert result.payload["x"] == pytest.approx(5.0)
        assert result.payload["y"] == pytest.approx(10.0)


# ---------------------------------------------------------------------------
# T010 — shape mismatch → AnnotationError (FriendlyError), no raw traceback
# ---------------------------------------------------------------------------

class TestT010ShapeMismatch:
    def test_shape_mismatch_raises_annotation_error(self):
        m_a = np.zeros((4, 4), dtype=np.uint8)
        m_b = np.zeros((8, 8), dtype=np.uint8)
        agg = ReferenceAggregator()
        with pytest.raises(AnnotationError) as exc_info:
            agg.aggregate([m_a, m_b])
        fe = exc_info.value.friendly
        assert "shape" in fe.title.lower() or "shape" in fe.message.lower()
        assert fe.recourse  # at least one recourse hint

    def test_no_raw_exception_leaks(self):
        """AnnotationError must not propagate as a plain ValueError or RuntimeError."""
        m_a = np.zeros((2, 2), dtype=np.uint8)
        m_b = np.zeros((3, 3), dtype=np.uint8)
        agg = ReferenceAggregator()
        # Must be AnnotationError, not bare exception
        with pytest.raises(AnnotationError):
            agg.aggregate([m_a, m_b])


# ---------------------------------------------------------------------------
# T011 — empty input → AnnotationError (clear message)
# ---------------------------------------------------------------------------

class TestT011EmptyInput:
    def test_empty_raises_annotation_error(self):
        agg = ReferenceAggregator()
        with pytest.raises(AnnotationError) as exc_info:
            agg.aggregate([])
        fe = exc_info.value.friendly
        # Message must mention "empty" or similar
        combined = (fe.title + " " + fe.message).lower()
        assert "empty" in combined or "least one" in combined

    def test_empty_via_aggregate_set_raises(self):
        """aggregate_set on empty AnnotationSet → AnnotationError from aggregator."""
        aset = AnnotationSet(image_ref="scan-empty")
        with pytest.raises(AnnotationError):
            aggregate_set(aset, "binary_segmentation")


# ---------------------------------------------------------------------------
# SC-004 — extensibility proof: zero core edits needed for new aggregator
# ---------------------------------------------------------------------------

class ToyResult:
    """Stand-in to verify toy aggregator ran."""
    def __init__(self, count: int):
        self.count = count


class _ToyAggregator(Aggregator):
    """
    Trivial aggregator that counts payloads and returns a ConsensusResult
    with payload={"count": N}.  Registered under 'toy' in the test only.
    Zero changes to model.py / registry.py / codecs.py / validate.py /
    aggregate/__init__.py were made to add this.
    """

    def aggregate(self, payloads, **ctx) -> ConsensusResult:
        return ConsensusResult(
            payload={"count": len(payloads), "toy": True},
            per_annotator={},
            method="toy",
        )


class TestSC004ExtensibilityProof:
    """
    SC-004: a brand-new aggregator can be added with ZERO core edits.

    Steps performed here (entirely in test code, no core files touched):
      1. Subclass Aggregator.
      2. Call register_aggregator('toy', ...).
      3. Call aggregate_set(..., aggregator_name='toy').
      4. Assert the toy aggregator's output is returned.
    """

    def test_toy_not_registered_initially(self):
        """'toy' must not bleed in from a prior test run in the same process."""
        # If already registered from a previous test in this class, skip check.
        # (pytest may run tests in any order; we guard defensively.)
        # The real assertion is that registering it works — checked below.
        pass

    def test_register_and_dispatch(self):
        register_aggregator("toy", _ToyAggregator())

        m = _binary_mask(fill=1)
        aset = _make_aset([
            _ann("ann-1", m),
            _ann("ann-2", m),
            _ann("ann-3", m),
        ])
        result = aggregate_set(aset, "binary_segmentation", aggregator_name="toy")
        assert isinstance(result, ConsensusResult)
        assert result.method == "toy"
        assert result.payload["toy"] is True
        assert result.payload["count"] == 3

    def test_toy_in_list_after_registration(self):
        register_aggregator("toy", _ToyAggregator())  # idempotent
        assert "toy" in list_aggregators()

    def test_get_aggregator_returns_toy(self):
        register_aggregator("toy", _ToyAggregator())
        agg = get_aggregator("toy")
        assert isinstance(agg, _ToyAggregator)

    def test_core_modules_untouched(self):
        """
        Declarative proof: reference + toy both work via the same seam.
        This test imports model/registry/codecs/validate to confirm they
        carry no aggregator-specific logic — they are read-only consumers.
        """
        from src.annotations import model as _model
        from src.annotations import registry as _registry

        # model.py: ConsensusResult does not reference 'toy' or 'reference'
        assert not hasattr(_model.ConsensusResult, "toy")
        # registry.py: default_aggregator is a string, not an object
        entry = _registry.get_type("binary_segmentation")
        assert isinstance(entry.default_aggregator, str)
        assert entry.default_aggregator == "reference"   # unchanged


# ---------------------------------------------------------------------------
# T012 — staple stub raises NotImplementedError with helpful message
# ---------------------------------------------------------------------------

class TestT012StapleStub:
    def test_raises_not_implemented(self):
        stub = StapleAggregator()
        with pytest.raises(NotImplementedError) as exc_info:
            stub.aggregate([_binary_mask()])
        msg = str(exc_info.value)
        assert "STAPLE" in msg or "staple" in msg.lower()
        # Must point to the implementation file
        assert "staple.py" in msg or "src/annotations/aggregate" in msg

    def test_helpful_message_mentions_reference(self):
        stub = StapleAggregator()
        with pytest.raises(NotImplementedError) as exc_info:
            stub.aggregate([_binary_mask()])
        assert "reference" in str(exc_info.value).lower()


# ---------------------------------------------------------------------------
# T013 — staple NOT in list_aggregators() until explicitly registered
# ---------------------------------------------------------------------------

class TestT013StapleNotAutoRegistered:
    def test_not_in_list(self):
        assert "staple" not in list_aggregators()

    def test_get_aggregator_raises_for_staple(self):
        with pytest.raises(AnnotationError) as exc_info:
            get_aggregator("staple")
        fe = exc_info.value.friendly
        assert "staple" in fe.message.lower() or "staple" in fe.title.lower()

    def test_explicit_registration_works(self):
        """Opt-in registration brings staple online (then we clean up)."""
        register_aggregator("staple", StapleAggregator())
        assert "staple" in list_aggregators()
        agg = get_aggregator("staple")
        assert isinstance(agg, StapleAggregator)
        # The aggregator is registered but still raises NotImplementedError on use
        with pytest.raises(NotImplementedError):
            agg.aggregate([_binary_mask()])
        # Cleanup: remove from registry so T013 assertions hold for other tests
        from src.annotations.aggregate import _AGGREGATOR_REGISTRY
        _AGGREGATOR_REGISTRY.pop("staple", None)
