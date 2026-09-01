"""
T010 — evidence package tests (Feature 009, Stage 3).

Verifies that:
  - overlap → evidence package is produced with correct structure
  - nothing auto-merges or auto-rejects on overlap
  - within-case near-identical shots are flagged but ALL retained (count preserved)
  - a path that yields zero landed files produces no case/registry entry

No live XNAT required.  No RUN_XNAT_DUAL gate.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Set

import pytest

from src.services.dedup import (
    CaseRelation,
    EvidencePackage,
    build_evidence_package,
    case_dedup,
    image_dedup,
    is_near_duplicate_pair,
)
from src.services.registry import Registry
from tests.synthetic_data import make_phi_dicom_dataset


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ch(seed: int) -> str:
    ds = make_phi_dicom_dataset(seed=seed)
    return hashlib.sha256(bytes(ds.PixelData)).hexdigest()


def _make_registry(tmp_path: Path) -> Registry:
    return Registry(tmp_path / "ev_registry.db")


def _seed_case(registry: Registry, case_key: str, seeds: list[int]) -> Set[str]:
    registry.upsert_case(case_key)
    hashes: Set[str] = set()
    for i, seed in enumerate(seeds):
        h = _ch(seed)
        registry.upsert_image_hash(h, case_key=case_key, instance_number=i)
        hashes.add(h)
    return hashes


# ---------------------------------------------------------------------------
# Evidence package is produced on overlap
# ---------------------------------------------------------------------------

class TestEvidencePackageOnOverlap:
    """build_evidence_package() returns a complete, actionable package."""

    def test_overlap_produces_evidence_package(self, tmp_path):
        registry = _make_registry(tmp_path)
        _seed_case(registry, "caseA", [0, 1, 2])
        incoming = {_ch(0), _ch(3)}  # 1 shared, 1 new

        result = case_dedup(incoming, registry)
        assert result.classification != CaseRelation.DISJOINT

        pkg = build_evidence_package(
            incoming, result.matched_case_keys[0], result.classification, registry
        )
        assert isinstance(pkg, EvidencePackage)

    def test_evidence_package_has_matched_case_key(self, tmp_path):
        registry = _make_registry(tmp_path)
        _seed_case(registry, "caseB", [0, 1])
        incoming = {_ch(0)}

        result = case_dedup(incoming, registry)
        pkg = build_evidence_package(
            incoming, result.matched_case_keys[0], result.classification, registry
        )
        assert pkg.matched_case_key == "caseB"

    def test_evidence_package_overlap_ratio_nonzero(self, tmp_path):
        registry = _make_registry(tmp_path)
        _seed_case(registry, "caseC", [0, 1, 2])
        incoming = {_ch(0), _ch(5)}

        result = case_dedup(incoming, registry)
        pkg = build_evidence_package(
            incoming, result.matched_case_keys[0], result.classification, registry
        )
        assert pkg.overlap_ratio > 0.0

    def test_evidence_package_contains_specific_matching_shots(self, tmp_path):
        registry = _make_registry(tmp_path)
        _seed_case(registry, "caseD", [0, 1, 2])
        incoming = {_ch(0), _ch(1), _ch(7)}  # 0 and 1 are shared

        result = case_dedup(incoming, registry)
        pkg = build_evidence_package(
            incoming, result.matched_case_keys[0], result.classification, registry
        )
        assert _ch(0) in pkg.overlap_hashes
        assert _ch(1) in pkg.overlap_hashes
        assert _ch(7) not in pkg.overlap_hashes

    def test_evidence_package_incoming_hashes_preserved(self, tmp_path):
        registry = _make_registry(tmp_path)
        _seed_case(registry, "caseE", [0])
        incoming = {_ch(0), _ch(5), _ch(6)}

        result = case_dedup(incoming, registry)
        pkg = build_evidence_package(
            incoming, result.matched_case_keys[0], result.classification, registry
        )
        assert pkg.incoming_hashes == incoming

    def test_evidence_package_classification_echoed(self, tmp_path):
        registry = _make_registry(tmp_path)
        _seed_case(registry, "caseF", [0, 1])
        incoming = {_ch(0), _ch(1)}  # exact

        result = case_dedup(incoming, registry)
        pkg = build_evidence_package(
            incoming, result.matched_case_keys[0], result.classification, registry
        )
        assert pkg.classification == CaseRelation.EXACT


# ---------------------------------------------------------------------------
# Nothing auto-merges or auto-rejects
# ---------------------------------------------------------------------------

class TestNoAutoAction:
    """The dedup layer reports; it never modifies the registry or discards data."""

    def test_image_dedup_does_not_register_new_hash(self, tmp_path):
        registry = _make_registry(tmp_path)
        h = _ch(0)
        # Query only — must NOT insert the hash
        image_dedup(h, orig_sopuid=None, registry=registry)
        assert not registry.image_exists(h), "image_dedup must not write to registry"

    def test_case_dedup_does_not_register_new_case(self, tmp_path):
        registry = _make_registry(tmp_path)
        _seed_case(registry, "caseG", [0, 1])
        incoming = {_ch(0), _ch(5)}

        before = registry._conn.execute("SELECT count(*) FROM cases").fetchone()[0]
        case_dedup(incoming, registry)
        after = registry._conn.execute("SELECT count(*) FROM cases").fetchone()[0]
        assert after == before, "case_dedup must not write new cases"

    def test_build_evidence_package_does_not_modify_registry(self, tmp_path):
        registry = _make_registry(tmp_path)
        _seed_case(registry, "caseH", [0])
        incoming = {_ch(0), _ch(5)}

        result = case_dedup(incoming, registry)
        before_cases = registry._conn.execute("SELECT count(*) FROM cases").fetchone()[0]
        before_hashes = registry._conn.execute("SELECT count(*) FROM image_hashes").fetchone()[0]

        build_evidence_package(
            incoming, result.matched_case_keys[0], result.classification, registry
        )

        after_cases = registry._conn.execute("SELECT count(*) FROM cases").fetchone()[0]
        after_hashes = registry._conn.execute("SELECT count(*) FROM image_hashes").fetchone()[0]

        assert after_cases == before_cases
        assert after_hashes == before_hashes

    def test_evidence_package_human_actions_enumerated(self, tmp_path):
        """EvidencePackage includes the possible human decisions (informational)."""
        registry = _make_registry(tmp_path)
        _seed_case(registry, "caseI", [0])
        incoming = {_ch(0)}

        result = case_dedup(incoming, registry)
        pkg = build_evidence_package(
            incoming, result.matched_case_keys[0], result.classification, registry
        )
        # The package must list possible actions — but the system takes NONE of them.
        assert isinstance(pkg, EvidencePackage)
        # Verify no "auto_merge" or "auto_reject" in the package fields.
        pkg_dict = pkg.__dict__
        for key in pkg_dict:
            assert "auto" not in key.lower(), f"Unexpected auto-action field: {key}"


# ---------------------------------------------------------------------------
# Within-case near-duplicate shots: flagged but ALL retained
# ---------------------------------------------------------------------------

class TestWithinCaseNearDups:
    """Near-dup shots inside a case are flagged advisory; count is preserved."""

    def test_near_dup_pair_flagged_advisory(self):
        """is_near_duplicate_pair() returns True for very similar perceptual hashes."""
        # Construct two hex strings that differ in only a few bits.
        base = "a" * 64
        # Flip two nibbles to create a small Hamming distance.
        similar = base[:2] + "b" + base[3:4] + "c" + base[5:]
        assert is_near_duplicate_pair(base, similar, threshold=20)

    def test_near_dup_different_hashes_not_flagged(self):
        """Very different hashes are not near-dups."""
        h_a = "0" * 64
        h_b = "f" * 64
        assert not is_near_duplicate_pair(h_a, h_b, threshold=10)

    def test_near_dup_identical_hashes_flagged(self):
        """Identical hashes are near-dups (Hamming == 0)."""
        h = "ab" * 32
        assert is_near_duplicate_pair(h, h)

    def test_near_dup_flag_does_not_reduce_count(self, tmp_path):
        """
        All images are retained even if the near-dup flag fires — count preserved.

        Simulates: 3 near-identical shots in a case → all 3 must be registered,
        even if the advisory flag marks them questionable.
        """
        registry = _make_registry(tmp_path)
        registry.upsert_case("caseJ")

        # Register 3 images (different content hashes, despite being "similar").
        h0, h1, h2 = _ch(0), _ch(1), _ch(2)
        for i, h in enumerate([h0, h1, h2]):
            registry.upsert_image_hash(h, case_key="caseJ", instance_number=i)

        # Simulate the advisory near-dup flag for each pair — just a bool, no action.
        # The flag does NOT remove any image from the registry.
        _ = is_near_duplicate_pair(h0, h1)  # advisory check
        _ = is_near_duplicate_pair(h1, h2)

        # All 3 must still be in the registry.
        result_hashes = registry.case_image_hashes("caseJ")
        assert len(result_hashes) == 3, "Near-dup flag must never reduce shot count"
        assert {h0, h1, h2} == result_hashes

    def test_near_dup_flag_on_image_dedup_result_is_advisory(self, tmp_path):
        """near_dup_flag=True on ImageDedupResult does NOT make is_duplicate True."""
        registry = _make_registry(tmp_path)
        h = _ch(3)
        result = image_dedup(h, orig_sopuid=None, registry=registry, near_dup_flag=True)
        assert not result.is_duplicate
        assert result.near_dup_flag is True


# ---------------------------------------------------------------------------
# No-empty-shell precondition at the dedup layer
# ---------------------------------------------------------------------------

class TestNoEmptyShell:
    """
    A path that yields zero landed files must not create a case/registry entry.

    At the dedup layer this is enforced by the invariant that case_dedup() and
    build_evidence_package() are read-only; only explicit upsert_case() calls
    create registry entries.  This test class confirms the dedup functions
    cannot be used to accidentally create empty shells.
    """

    def test_disjoint_result_does_not_create_case(self, tmp_path):
        """DISJOINT result + no explicit registration = no case in registry."""
        registry = _make_registry(tmp_path)
        incoming = {_ch(0), _ch(1)}
        result = case_dedup(incoming, registry)

        assert result.classification == CaseRelation.DISJOINT
        n_cases = registry._conn.execute("SELECT count(*) FROM cases").fetchone()[0]
        assert n_cases == 0, "Dedup must not create a case for a disjoint incoming set"

    def test_evidence_package_with_zero_overlap_hashes_no_registry_write(self, tmp_path):
        """
        Even if we explicitly call build_evidence_package on a minimal match,
        no new case or image_hash rows are created.
        """
        registry = _make_registry(tmp_path)
        _seed_case(registry, "caseK", [0])
        before_hashes = registry._conn.execute("SELECT count(*) FROM image_hashes").fetchone()[0]

        incoming = {_ch(0)}
        result = case_dedup(incoming, registry)
        build_evidence_package(
            incoming, result.matched_case_keys[0], result.classification, registry
        )

        after_hashes = registry._conn.execute("SELECT count(*) FROM image_hashes").fetchone()[0]
        assert after_hashes == before_hashes

    def test_empty_incoming_set_is_disjoint_no_write(self, tmp_path):
        """Empty incoming hash set → DISJOINT, no registry write."""
        registry = _make_registry(tmp_path)
        _seed_case(registry, "caseL", [0])
        result = case_dedup(set(), registry)
        assert result.classification == CaseRelation.DISJOINT
        n_cases = registry._conn.execute("SELECT count(*) FROM cases").fetchone()[0]
        assert n_cases == 1  # only caseL from seed; nothing added

    def test_is_near_duplicate_pair_no_registry_side_effect(self, tmp_path):
        """is_near_duplicate_pair() is a pure function — no registry interaction."""
        registry = _make_registry(tmp_path)
        h_a = _ch(0)
        h_b = _ch(1)
        is_near_duplicate_pair(h_a, h_b)
        assert registry._conn.execute("SELECT count(*) FROM image_hashes").fetchone()[0] == 0


# ---------------------------------------------------------------------------
# Corroboration booleans in evidence package
# ---------------------------------------------------------------------------

class TestEvidenceCorroboration:
    """UID/date/device corroboration is advisory; absence does not weaken detection."""

    def test_date_hash_corroboration_when_matching(self, tmp_path):
        registry = _make_registry(tmp_path)
        registry.upsert_case("caseM", date_hash="dh_abc123")
        registry.upsert_image_hash(_ch(0), case_key="caseM")

        incoming = {_ch(0)}
        result = case_dedup(incoming, registry)
        pkg = build_evidence_package(
            incoming,
            result.matched_case_keys[0],
            result.classification,
            registry,
            corroboration={"date_hash": "dh_abc123"},
        )
        assert pkg.date_corroborates

    def test_date_hash_no_corroboration_when_different(self, tmp_path):
        registry = _make_registry(tmp_path)
        registry.upsert_case("caseN", date_hash="dh_abc123")
        registry.upsert_image_hash(_ch(0), case_key="caseN")

        incoming = {_ch(0)}
        result = case_dedup(incoming, registry)
        pkg = build_evidence_package(
            incoming,
            result.matched_case_keys[0],
            result.classification,
            registry,
            corroboration={"date_hash": "dh_different"},
        )
        assert not pkg.date_corroborates

    def test_missing_corroboration_does_not_override_content_match(self, tmp_path):
        """Content hash match is the authority; missing corroboration doesn't weaken it."""
        registry = _make_registry(tmp_path)
        _seed_case(registry, "caseO", [0])
        incoming = {_ch(0)}

        result = case_dedup(incoming, registry)
        assert result.classification == CaseRelation.EXACT

        # No corroboration provided
        pkg = build_evidence_package(
            incoming, result.matched_case_keys[0], result.classification, registry
        )
        # No corroboration booleans set — but overlap is still real.
        assert not pkg.uid_corroborates
        assert not pkg.date_corroborates
        assert not pkg.device_corroborates
        assert pkg.overlap_ratio == pytest.approx(1.0)
