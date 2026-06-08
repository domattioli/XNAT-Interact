"""
tests/test_009_stage4b_dedup_ingest.py — Offline tests for T013:
  - dedup check wired into publish_to_xnat (additive, behavior-preserving)
  - no-empty-shell invariant (FR-008)

RULES:
  - NO network calls; FakeXNAT replaces the real server.
  - NO real PHI; synthetic content hashes.
  - DISJOINT uploads proceed exactly as today.
"""
from __future__ import annotations

import sqlite3
import tempfile
import zipfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from src.services.registry import Registry
from src.xnat_experiment_data import (
    DedupReviewRequired,
    ExperimentData,
    ReviewDecision,
    UploadError,
)
from tests.fakes.fake_xnat import FakeXNAT


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_connection(fake_server: FakeXNAT, project_name: str = "TEST_PROJ") -> SimpleNamespace:
    return SimpleNamespace(server=fake_server, gateway=fake_server, xnat_project_name=project_name)


def _make_login(username: str = "testuser") -> SimpleNamespace:
    return SimpleNamespace(validated_username=username)


def _make_intake_form(tmp_path: Path, uid: str = "TEST_DEDUP_UID") -> SimpleNamespace:
    saved_ffn = tmp_path / "RECONSTRUCTED_OR_DATA_INTAKE_FORM.json"
    saved_ffn.write_text("{}", encoding="utf-8")

    form = SimpleNamespace(
        uid=uid,
        group="TEST_GROUP",
        acquisition_site="TEST_SITE",
        operation_date="2024-01-01",
        epic_start_time="120000",
        ortho_procedure_type="TEST_PROCEDURE",
        scan_quality="usable",
        datetime=SimpleNamespace(date="2024-01-01", time="120000"),
        relevant_folder=tmp_path,
        saved_ffn=saved_ffn,
        saved_ffn_str=str(saved_ffn),
    )
    form.push_to_xnat = lambda subj_inst=None, verbose=False, **kw: None
    return form


class _MinimalSession(ExperimentData):
    """Concrete ExperimentData stub — bypasses real __init__."""

    @classmethod
    def build(cls, intake_form: Any, schema_prefix: str = "rf") -> "_MinimalSession":
        obj = object.__new__(cls)
        obj._intake_form = intake_form
        obj._schema_prefix_str = schema_prefix
        obj._scan_type_label = "DICOM"
        obj._is_valid = True
        obj._df = None
        return obj

    def _populate_df(self, config: Any) -> None:
        pass

    def _check_session_validity(self, config: Any) -> None:
        pass

    def write(self, config: Any, zip_dest: Any = None, verbose: Any = True) -> Any:
        raise NotImplementedError


def _make_zip(tmp_path: Path) -> dict:
    zip_path = tmp_path / "dicom_files.zip"
    with zipfile.ZipFile(str(zip_path), "w") as zf:
        zf.writestr("placeholder.dcm", b"FAKE_DCM")
    return {str(zip_path): {"CONTENT": "IMAGE", "FORMAT": "DICOM", "TAG": "INTRA_OP"}}


def _confirmed_confirmer(ctx):
    return ReviewDecision.CONFIRMED, []


def _make_registry(tmp_path: Path) -> Registry:
    return Registry(tmp_path / "test_registry.db")


# ---------------------------------------------------------------------------
# 1. DISJOINT upload proceeds unchanged — no dedup_registry supplied
# ---------------------------------------------------------------------------

class TestDedupDisjointNoRegistry:
    """No registry supplied → identical behavior to today."""

    def test_upload_proceeds_without_registry(self, tmp_path):
        fake = FakeXNAT(project_name="TEST_PROJ")
        session = _MinimalSession.build(_make_intake_form(tmp_path))
        zipped = _make_zip(tmp_path)

        session.publish_to_xnat(
            xnat_connection=_make_connection(fake),
            validated_login=_make_login(),
            zipped_data=zipped,
            delete_zip=False,
            verbose=False,
            pixel_review_confirmer=_confirmed_confirmer,
            dedup_registry=None,
            incoming_content_hashes=None,
        )
        # At least one put_zip recorded
        put_zip_calls = [c for c in fake.calls if c["op"] == "resource.put_zip"]
        assert len(put_zip_calls) >= 1


# ---------------------------------------------------------------------------
# 2. DISJOINT upload with registry → proceeds, hashes registered
# ---------------------------------------------------------------------------

class TestDedupDisjointWithRegistry:
    """Fresh registry (no prior cases) → DISJOINT → upload proceeds; hashes stored."""

    def test_disjoint_upload_proceeds_and_registers(self, tmp_path):
        fake = FakeXNAT(project_name="TEST_PROJ")
        registry = _make_registry(tmp_path)
        incoming = {"aabbccdd" * 8, "11223344" * 8}  # two synthetic content hashes

        session = _MinimalSession.build(_make_intake_form(tmp_path))
        zipped = _make_zip(tmp_path)

        session.publish_to_xnat(
            xnat_connection=_make_connection(fake),
            validated_login=_make_login(),
            zipped_data=zipped,
            delete_zip=False,
            verbose=False,
            pixel_review_confirmer=_confirmed_confirmer,
            dedup_registry=registry,
            incoming_content_hashes=incoming,
        )

        _pz = [c for c in fake.calls if c["op"] == "resource.put_zip"]
        assert len(_pz) >= 1
        # Hashes registered in the registry
        for h in incoming:
            assert registry.image_exists(h)

    def test_disjoint_second_case_different_hashes(self, tmp_path):
        """Two separate cases with disjoint hash-sets both upload cleanly."""
        fake = FakeXNAT(project_name="TEST_PROJ")
        registry = _make_registry(tmp_path)

        hashes_a = {"aaaaaaaa" * 8}
        hashes_b = {"bbbbbbbb" * 8}

        for uid, hashes in [("CASE_A", hashes_a), ("CASE_B", hashes_b)]:
            session = _MinimalSession.build(_make_intake_form(tmp_path, uid=uid))
            zipped = _make_zip(tmp_path)
            session.publish_to_xnat(
                xnat_connection=_make_connection(fake),
                validated_login=_make_login(),
                zipped_data=zipped,
                delete_zip=False,
                verbose=False,
                pixel_review_confirmer=_confirmed_confirmer,
                dedup_registry=registry,
                incoming_content_hashes=hashes,
            )

        _pz = [c for c in fake.calls if c["op"] == "resource.put_zip"]
        assert len(_pz) >= 2


# ---------------------------------------------------------------------------
# 3. Overlap (EXACT/SUBSET/SUPERSET/PARTIAL) → DedupReviewRequired raised;
#    NO XNAT objects created, NO data uploaded
# ---------------------------------------------------------------------------

class TestDedupOverlapBlocksUpload:
    """Pre-seeded overlap → DedupReviewRequired; no put_zip, no Subject/Exp/Scan created."""

    def _seed_existing_case(self, registry: Registry, case_key: str, hashes: set) -> None:
        registry.upsert_case(case_key)
        for h in hashes:
            registry.upsert_image_hash(h, case_key=case_key)

    @pytest.mark.parametrize("scenario,existing,incoming", [
        ("EXACT",     {"h1" * 32, "h2" * 32},            {"h1" * 32, "h2" * 32}),
        ("SUBSET",    {"h1" * 32, "h2" * 32, "h3" * 32}, {"h1" * 32}),
        ("SUPERSET",  {"h1" * 32},                        {"h1" * 32, "h2" * 32}),
        ("PARTIAL",   {"h1" * 32, "h2" * 32},            {"h1" * 32, "h3" * 32}),
    ])
    def test_overlap_raises_dedup_review(self, tmp_path, scenario, existing, incoming):
        fake = FakeXNAT(project_name="TEST_PROJ")
        registry = _make_registry(tmp_path)

        # Seed an existing case under a different UID
        existing_uid = "EXISTING_CASE"
        self._seed_existing_case(registry, existing_uid, existing)

        # Incoming case has a DIFFERENT uid but overlapping hashes
        session = _MinimalSession.build(_make_intake_form(tmp_path, uid="INCOMING_CASE"))
        zipped = _make_zip(tmp_path)

        with pytest.raises(DedupReviewRequired) as exc_info:
            session.publish_to_xnat(
                xnat_connection=_make_connection(fake),
                validated_login=_make_login(),
                zipped_data=zipped,
                delete_zip=False,
                verbose=False,
                pixel_review_confirmer=_confirmed_confirmer,
                dedup_registry=registry,
                incoming_content_hashes=incoming,
            )

        # Evidence package populated
        ev = exc_info.value.evidence
        assert ev.matched_case_key == existing_uid
        assert ev.overlap_ratio > 0.0
        assert ev.overlap_hashes  # non-empty intersection

        # No upload occurred
        _pz = [c for c in fake.calls if c["op"] == "resource.put_zip"]
        assert len(_pz) == 0, f"[{scenario}] put_zip must not be called on overlap"

    def test_overlap_no_xnat_objects_created(self, tmp_path):
        """No Subject/Experiment/Scan objects are created when overlap is detected."""
        fake = FakeXNAT(project_name="TEST_PROJ")
        registry = _make_registry(tmp_path)
        shared_hash = "cc" * 32

        self._seed_existing_case(registry, "EXISTING", {shared_hash})

        session = _MinimalSession.build(_make_intake_form(tmp_path, uid="INCOMING"))
        zipped = _make_zip(tmp_path)

        with pytest.raises(DedupReviewRequired):
            session.publish_to_xnat(
                xnat_connection=_make_connection(fake),
                validated_login=_make_login(),
                zipped_data=zipped,
                delete_zip=False,
                verbose=False,
                pixel_review_confirmer=_confirmed_confirmer,
                dedup_registry=registry,
                incoming_content_hashes={shared_hash},
            )

        # FakeXNAT records create() calls; none should have happened
        _creates = [c for c in fake.calls if c["op"] == "selectable.create"]
        assert len(_creates) == 0, "No XNAT objects must be created before dedup raises"


# ---------------------------------------------------------------------------
# 4. FR-008 — no-empty-shell: empty zipped_data → no objects created
# ---------------------------------------------------------------------------

class TestNoEmptyShell:
    """Empty zipped_data → publish_to_xnat returns early; no Subject/Exp/Scan created."""

    def test_empty_zipped_data_no_objects_created(self, tmp_path):
        fake = FakeXNAT(project_name="TEST_PROJ")
        session = _MinimalSession.build(_make_intake_form(tmp_path))

        session.publish_to_xnat(
            xnat_connection=_make_connection(fake),
            validated_login=_make_login(),
            zipped_data={},  # nothing to upload
            delete_zip=False,
            verbose=False,
            pixel_review_confirmer=_confirmed_confirmer,
        )

        _pz = [c for c in fake.calls if c["op"] == "resource.put_zip"]
        _creates = [c for c in fake.calls if c["op"] == "selectable.create"]
        assert len(_pz) == 0
        assert len(_creates) == 0, "No XNAT objects must be created for empty upload"


# ---------------------------------------------------------------------------
# 5. Registry fault → fail-soft; upload still proceeds
# ---------------------------------------------------------------------------

class TestRegistryFaultFailSoft:
    """If the registry is broken, upload degrades to today's behavior (not blocked)."""

    def test_broken_registry_upload_still_proceeds(self, tmp_path):
        """A registry that raises on every call must not block a legitimate upload."""

        class _BrokenRegistry:
            def image_exists(self, _):    raise RuntimeError("db gone")
            def upsert_case(self, *a, **k): raise RuntimeError("db gone")
            def upsert_image_hash(self, *a, **k): raise RuntimeError("db gone")
            @property
            def _conn(self):
                # Simulate broken connection for case_dedup enumerate
                raise RuntimeError("db gone")

        fake = FakeXNAT(project_name="TEST_PROJ")
        session = _MinimalSession.build(_make_intake_form(tmp_path))
        zipped = _make_zip(tmp_path)

        # Must NOT raise; must complete the upload
        session.publish_to_xnat(
            xnat_connection=_make_connection(fake),
            validated_login=_make_login(),
            zipped_data=zipped,
            delete_zip=False,
            verbose=False,
            pixel_review_confirmer=_confirmed_confirmer,
            dedup_registry=_BrokenRegistry(),
            incoming_content_hashes={"dd" * 32},
        )

        _pz = [c for c in fake.calls if c["op"] == "resource.put_zip"]
        assert len(_pz) >= 1
