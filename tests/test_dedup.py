"""
T009 — seed-set dedup tests (Feature 009, Stage 3).

Fixtures build synthetic cases from seeded DICOMs; each seed → unique pixels →
unique content hash.  Tests cover all set-algebra relationships
(EXACT / SUBSET / SUPERSET / PARTIAL / DISJOINT) plus re-export simulation
(same pixels, different orig UIDs) and UID-corroboration semantics.

No live XNAT required.  No RUN_XNAT_DUAL gate.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Set

import pytest

from src.services.dedup import (
    CaseRelation,
    image_dedup,
    case_dedup,
)
from src.services.registry import Registry
from tests.synthetic_data import make_phi_dicom_dataset


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _content_hash_for_seed(seed: int) -> str:
    """Return the sha256 content hash for the synthetic DICOM at *seed*."""
    ds = make_phi_dicom_dataset(seed=seed)
    return hashlib.sha256(bytes(ds.PixelData)).hexdigest()


def _make_registry(tmp_path: Path) -> Registry:
    return Registry(tmp_path / "test_registry.db")


def _seed_registry(registry: Registry, case_key: str, seeds: list[int]) -> Set[str]:
    """Register a case (case_key) with images derived from *seeds*; return hash set."""
    registry.upsert_case(case_key)
    hashes: Set[str] = set()
    for i, seed in enumerate(seeds):
        h = _content_hash_for_seed(seed)
        registry.upsert_image_hash(h, case_key=case_key, instance_number=i)
        hashes.add(h)
    return hashes


# ---------------------------------------------------------------------------
# Fixture precondition: distinct seeds → distinct hashes
# ---------------------------------------------------------------------------

class TestSeedDistinctness:
    """Guard: every seed used in dedup tests yields a unique content hash."""

    def test_distinct_seeds_yield_distinct_hashes(self):
        seeds = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        hashes = [_content_hash_for_seed(s) for s in seeds]
        assert len(set(hashes)) == len(seeds), (
            "Seed collision detected — disjoint control is invalid."
        )


# ---------------------------------------------------------------------------
# Image-level dedup
# ---------------------------------------------------------------------------

class TestImageDedup:
    """image_dedup() — authoritative key is content_hash."""

    def test_new_image_not_duplicate(self, tmp_path):
        registry = _make_registry(tmp_path)
        h = _content_hash_for_seed(0)
        result = image_dedup(h, orig_sopuid="1.2.3", registry=registry)
        assert not result.is_duplicate
        assert result.confidence == "NONE"

    def test_existing_image_is_duplicate(self, tmp_path):
        registry = _make_registry(tmp_path)
        h = _content_hash_for_seed(0)
        registry.upsert_image_hash(h, orig_sopuid="1.2.3")
        result = image_dedup(h, orig_sopuid="1.2.3", registry=registry)
        assert result.is_duplicate
        assert result.confidence in ("HIGH", "CONTENT")

    def test_uid_match_raises_confidence_to_high(self, tmp_path):
        registry = _make_registry(tmp_path)
        h = _content_hash_for_seed(1)
        registry.upsert_image_hash(h, orig_sopuid="uid.same")
        result = image_dedup(h, orig_sopuid="uid.same", registry=registry)
        assert result.is_duplicate
        assert result.uid_corroborates
        assert result.confidence == "HIGH"

    def test_uid_mismatch_does_not_override_content_match(self, tmp_path):
        """Same content, different orig UID → still a duplicate (UID does not gate)."""
        registry = _make_registry(tmp_path)
        h = _content_hash_for_seed(2)
        registry.upsert_image_hash(h, orig_sopuid="uid.original")
        # Re-export: same pixels, regenerated UID
        result = image_dedup(h, orig_sopuid="uid.regenerated", registry=registry)
        assert result.is_duplicate, "Content match must survive UID disagreement"
        assert not result.uid_corroborates
        assert result.confidence == "CONTENT"

    def test_near_dup_flag_advisory_only(self, tmp_path):
        """near_dup_flag=True does NOT make is_duplicate True for an unseen hash."""
        registry = _make_registry(tmp_path)
        h = _content_hash_for_seed(3)
        result = image_dedup(h, orig_sopuid=None, registry=registry, near_dup_flag=True)
        assert not result.is_duplicate, "Advisory flag must not auto-detect as duplicate"
        assert result.near_dup_flag

    def test_content_hash_is_authoritative_field(self, tmp_path):
        """ImageDedupResult.content_hash echoes what was checked."""
        registry = _make_registry(tmp_path)
        h = _content_hash_for_seed(4)
        result = image_dedup(h, orig_sopuid=None, registry=registry)
        assert result.content_hash == h


# ---------------------------------------------------------------------------
# Case-level set algebra
# ---------------------------------------------------------------------------

class TestCaseDedup:
    """case_dedup() — set-algebra over content-hash sets."""

    def _registry_with_case_a(self, tmp_path: Path, seeds: list[int]) -> tuple:
        registry = _make_registry(tmp_path)
        hashes = _seed_registry(registry, "case_A", seeds)
        return registry, hashes

    # -- EXACT ----------------------------------------------------------------

    def test_exact_duplicate(self, tmp_path):
        seeds = [0, 1, 2]
        registry, existing_hashes = self._registry_with_case_a(tmp_path, seeds)
        incoming = set(existing_hashes)  # identical
        result = case_dedup(incoming, registry)
        assert result.classification == CaseRelation.EXACT
        assert "case_A" in result.matched_case_keys
        assert result.overlap_ratio == pytest.approx(1.0)

    # -- SUBSET ---------------------------------------------------------------

    def test_subset(self, tmp_path):
        seeds = [0, 1, 2, 3]
        registry, existing_hashes = self._registry_with_case_a(tmp_path, seeds)
        # incoming is a proper subset of case_A
        incoming = {_content_hash_for_seed(0), _content_hash_for_seed(1)}
        result = case_dedup(incoming, registry)
        assert result.classification == CaseRelation.SUBSET
        assert result.overlap_ratio > 0.0

    # -- SUPERSET -------------------------------------------------------------

    def test_superset(self, tmp_path):
        seeds = [0, 1]
        registry, existing_hashes = self._registry_with_case_a(tmp_path, seeds)
        # incoming has all of case_A plus new images
        incoming = existing_hashes | {_content_hash_for_seed(5), _content_hash_for_seed(6)}
        result = case_dedup(incoming, registry)
        assert result.classification == CaseRelation.SUPERSET

    # -- PARTIAL --------------------------------------------------------------

    def test_partial_overlap(self, tmp_path):
        seeds = [0, 1, 2]
        registry, existing_hashes = self._registry_with_case_a(tmp_path, seeds)
        # incoming shares seed 0 only; has unique seeds 7, 8
        incoming = {
            _content_hash_for_seed(0),
            _content_hash_for_seed(7),
            _content_hash_for_seed(8),
        }
        result = case_dedup(incoming, registry)
        assert result.classification == CaseRelation.PARTIAL
        assert len(result.overlap_hashes) >= 1

    # -- DISJOINT -------------------------------------------------------------

    def test_disjoint(self, tmp_path):
        seeds_existing = [0, 1, 2]
        registry, _ = self._registry_with_case_a(tmp_path, seeds_existing)
        # incoming uses completely different seeds
        incoming = {_content_hash_for_seed(5), _content_hash_for_seed(6)}
        result = case_dedup(incoming, registry)
        assert result.classification == CaseRelation.DISJOINT
        assert result.matched_case_keys == []
        assert result.overlap_ratio == 0.0
        assert result.overlap_hashes == set()

    def test_disjoint_empty_registry(self, tmp_path):
        """No cases in registry → always DISJOINT."""
        registry = _make_registry(tmp_path)
        incoming = {_content_hash_for_seed(0)}
        result = case_dedup(incoming, registry)
        assert result.classification == CaseRelation.DISJOINT

    def test_disjoint_does_not_create_registry_entries(self, tmp_path):
        """case_dedup() is read-only — disjoint result leaves registry unchanged."""
        registry = _make_registry(tmp_path)
        _seed_registry(registry, "case_A", [0, 1])
        incoming = {_content_hash_for_seed(5)}
        case_dedup(incoming, registry)
        # Registry still has exactly the original case
        cur = registry._conn.execute("SELECT count(*) FROM cases")
        assert cur.fetchone()[0] == 1

    # -- Re-export simulation -------------------------------------------------

    def test_reexport_same_pixels_different_uid_detected(self, tmp_path):
        """Re-export: same pixels, regenerated UIDs → content hash still detects dup."""
        registry = _make_registry(tmp_path)
        # Register original with one orig_sopuid
        h = _content_hash_for_seed(0)
        registry.upsert_case("case_orig")
        registry.upsert_image_hash(h, case_key="case_orig", orig_sopuid="uid.v1")

        # Re-export: same content hash, different UID
        result = image_dedup(h, orig_sopuid="uid.v2_regenerated", registry=registry)
        assert result.is_duplicate, "Content hash must detect re-export despite UID change"
        assert not result.uid_corroborates, "UID disagreement must be surfaced"
        assert result.confidence == "CONTENT"

    # -- UID-corroboration semantics ------------------------------------------

    def test_uid_corroboration_raises_confidence_not_required(self, tmp_path):
        """UID match → confidence=HIGH; absence does NOT prevent detection."""
        registry = _make_registry(tmp_path)
        h = _content_hash_for_seed(1)
        registry.upsert_image_hash(h, orig_sopuid="uid.x")

        # With matching UID
        r1 = image_dedup(h, orig_sopuid="uid.x", registry=registry)
        assert r1.confidence == "HIGH"

        # Without UID (still detected)
        r2 = image_dedup(h, orig_sopuid=None, registry=registry)
        assert r2.is_duplicate
        assert r2.confidence == "CONTENT"

    # -- overlap_hashes contents ----------------------------------------------

    def test_overlap_hashes_are_the_matching_shots(self, tmp_path):
        shared_seeds = [0, 1]
        exclusive_seeds = [2, 3]
        registry, _ = self._registry_with_case_a(tmp_path, shared_seeds + exclusive_seeds)
        incoming = {_content_hash_for_seed(s) for s in shared_seeds}
        incoming.add(_content_hash_for_seed(9))  # unique to incoming

        result = case_dedup(incoming, registry)
        expected_overlap = {_content_hash_for_seed(s) for s in shared_seeds}
        assert result.overlap_hashes == expected_overlap
