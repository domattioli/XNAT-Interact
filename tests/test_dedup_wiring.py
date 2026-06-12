"""
tests/test_dedup_wiring.py — Offline tests for #32 dedup-gate wiring.

Verifies that write_publish_catalog_subroutine feeds dedup_registry +
incoming_content_hashes to publish_to_xnat, covering three scenarios:

  (a) Second publish of identical content under a new uid raises DedupReviewRequired.
  (b) Disjoint content publishes cleanly.
  (c) Fresh/empty registry publishes cleanly.

Uses FakeXNAT (no real server) and synthetic content hashes (no real PHI).
"""
from __future__ import annotations

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
    _RegistryExcludingCase,
)
from tests.fakes.fake_xnat import FakeXNAT


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_fake_xnat(project: str = "WIRE_TEST") -> FakeXNAT:
    return FakeXNAT(project_name=project)


def _make_connection(fake: FakeXNAT, project: str = "WIRE_TEST") -> SimpleNamespace:
    return SimpleNamespace(server=fake, gateway=fake, xnat_project_name=project)


def _make_login(username: str = "testuser") -> SimpleNamespace:
    return SimpleNamespace(validated_username=username)


def _make_intake_form(tmp_path: Path, uid: str = "WIRE_TEST_UID") -> SimpleNamespace:
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


def _make_zip(tmp_path: Path, name: str = "dicom_files.zip") -> dict:
    zip_path = tmp_path / name
    with zipfile.ZipFile(str(zip_path), "w") as zf:
        zf.writestr("placeholder.dcm", b"FAKE_DCM")
    return {str(zip_path): {"CONTENT": "IMAGE", "FORMAT": "DICOM", "TAG": "INTRA_OP"}}


def _auto_confirmer(ctx):
    return ReviewDecision.CONFIRMED, []


def _make_registry(tmp_path: Path, name: str = "registry.db") -> Registry:
    return Registry(tmp_path / name)


# ---------------------------------------------------------------------------
# Fake object/image stubs for df rows
# ---------------------------------------------------------------------------

class _FakeImageHash:
    def __init__(self, hash_str: str):
        self.hash_str = hash_str


class _FakeObject:
    def __init__(self, hash_str: str):
        self.image = _FakeImageHash(hash_str)


# ---------------------------------------------------------------------------
# Minimal session stub that exposes a real df with fake objects
# ---------------------------------------------------------------------------

class _WireTestSession(ExperimentData):
    """
    Minimal concrete ExperimentData for wiring tests.

    write() simulates the production path:
      - Registers the subject and image hashes in the registry (via direct
        upsert, mirroring the write-through that the real write() does via
        config.add_new_item).
      - Returns pre-built zipped_data.

    This means incoming hashes ARE in the registry by the time
    publish_to_xnat is called — exactly matching the production ordering
    that requires self-exclusion.
    """

    @classmethod
    def build(
        cls,
        intake_form: Any,
        hashes: set,
        registry: Registry,
        zipped_data: dict,
    ) -> "_WireTestSession":
        obj = object.__new__(cls)
        obj._intake_form = intake_form
        obj._schema_prefix_str = "rf"
        obj._scan_type_label = "DICOM"
        obj._is_valid = True
        obj._hashes = hashes
        obj._registry = registry
        obj._zipped_data = zipped_data
        # Build a synthetic df so _RegistryExcludingCase can extract hashes.
        import pandas as pd
        rows = [{"IS_VALID": True, "OBJECT": _FakeObject(h)} for h in hashes]
        obj._df = pd.DataFrame(rows)
        return obj

    def _populate_df(self, config: Any) -> None:
        pass

    def _check_session_validity(self, config: Any) -> None:
        pass

    def write(self, config: Any, zip_dest: Any = None, verbose: Any = True):
        # Simulate write-through: register case + hashes in registry.
        uid = str(self._intake_form.uid)
        self._registry.upsert_case(uid)
        for h in self._hashes:
            self._registry.upsert_image_hash(h, case_key=uid)
        # Refresh config.tables so adapter reads updated data.
        if hasattr(config, "tables"):
            import pandas as pd
            rows = []
            conn = self._registry._conn
            cur = conn.execute("SELECT content_hash, case_key FROM image_hashes WHERE case_key IS NOT NULL")
            for content_hash, case_key in cur.fetchall():
                rows.append({"NAME": content_hash.upper(), "SUBJECT": case_key})
            config.tables = {"IMAGE_HASHES": pd.DataFrame(rows, columns=["NAME", "SUBJECT"])}
        return self._zipped_data, config


# ---------------------------------------------------------------------------
# Fake ConfigTables stub that exposes a _registry attribute
# ---------------------------------------------------------------------------

class _FakeConfig:
    def __init__(self, registry: Registry):
        self._registry = registry
        self.config_ffn = "/tmp/fake_config.json"
        # Build tables dict from registry for _ConfigTablesRegistryAdapter.
        # IMAGE_HASHES table needs columns: NAME, SUBJECT.
        import pandas as pd
        rows = []
        conn = registry._conn
        cur = conn.execute("SELECT content_hash, case_key FROM image_hashes WHERE case_key IS NOT NULL")
        for content_hash, case_key in cur.fetchall():
            rows.append({"NAME": content_hash.upper(), "SUBJECT": case_key})
        self.tables = {"IMAGE_HASHES": pd.DataFrame(rows, columns=["NAME", "SUBJECT"])}

    def push_to_xnat(self, verbose=False):
        pass


# ---------------------------------------------------------------------------
# Test (a): duplicate content under new uid raises DedupReviewRequired
# ---------------------------------------------------------------------------

class TestDedupWiringDuplicate:
    """
    (a) Second publish of identical content under a new uid must raise
    DedupReviewRequired BEFORE any XNAT objects are created.
    """

    def test_exact_duplicate_raises(self, tmp_path):
        fake = _make_fake_xnat()
        registry = _make_registry(tmp_path)

        shared_hashes = {"aa" * 32, "bb" * 32}

        # Seed an existing case manually (simulates a prior successful publish).
        existing_uid = "EXISTING_CASE_001"
        registry.upsert_case(existing_uid)
        for h in shared_hashes:
            registry.upsert_image_hash(h, case_key=existing_uid)

        # New session with identical hashes but different uid.
        form = _make_intake_form(tmp_path, uid="NEW_CASE_002")
        zipped = _make_zip(tmp_path)
        config = _FakeConfig(registry)
        session = _WireTestSession.build(form, shared_hashes, registry, zipped)

        with pytest.raises(DedupReviewRequired) as exc_info:
            session.write_publish_catalog_subroutine(
                config=config,
                xnat_connection=_make_connection(fake),
                validated_login=_make_login(),
                verbose=False,
                delete_zip=False,
                pixel_review_confirmer=_auto_confirmer,
            )

        ev = exc_info.value.evidence
        assert ev.matched_case_key == existing_uid
        assert ev.overlap_ratio > 0.0
        assert len(ev.overlap_hashes) > 0

    def test_exact_duplicate_no_xnat_objects_created(self, tmp_path):
        """DedupReviewRequired must fire before any XNAT Subject/Exp/Scan creation."""
        fake = _make_fake_xnat()
        registry = _make_registry(tmp_path)

        h = "cc" * 32
        existing_uid = "EXISTING_CASE"
        registry.upsert_case(existing_uid)
        registry.upsert_image_hash(h, case_key=existing_uid)

        form = _make_intake_form(tmp_path, uid="INCOMING_CASE")
        zipped = _make_zip(tmp_path)
        config = _FakeConfig(registry)
        session = _WireTestSession.build(form, {h}, registry, zipped)

        with pytest.raises(DedupReviewRequired):
            session.write_publish_catalog_subroutine(
                config=config,
                xnat_connection=_make_connection(fake),
                validated_login=_make_login(),
                verbose=False,
                delete_zip=False,
                pixel_review_confirmer=_auto_confirmer,
            )

        creates = [c for c in fake.calls if c["op"] == "selectable.create"]
        assert len(creates) == 0, "No XNAT objects must be created when dedup fires"

        uploads = [c for c in fake.calls if c["op"] == "resource.put_zip"]
        assert len(uploads) == 0, "No data must be uploaded when dedup fires"


# ---------------------------------------------------------------------------
# Test (b): disjoint content publishes cleanly
# ---------------------------------------------------------------------------

class TestDedupWiringDisjoint:
    """
    (b) Disjoint content must pass the dedup gate and upload to XNAT.
    """

    def test_disjoint_publishes_clean(self, tmp_path):
        fake = _make_fake_xnat()
        registry = _make_registry(tmp_path)

        existing_hashes = {"aa" * 32, "bb" * 32}
        existing_uid = "EXISTING_CASE"
        registry.upsert_case(existing_uid)
        for h in existing_hashes:
            registry.upsert_image_hash(h, case_key=existing_uid)

        # Completely different hashes — should be accepted.
        new_hashes = {"cc" * 32, "dd" * 32}
        form = _make_intake_form(tmp_path, uid="NEW_DISJOINT_CASE")
        zipped = _make_zip(tmp_path)
        config = _FakeConfig(registry)
        session = _WireTestSession.build(form, new_hashes, registry, zipped)

        # Must not raise.
        session.write_publish_catalog_subroutine(
            config=config,
            xnat_connection=_make_connection(fake),
            validated_login=_make_login(),
            verbose=False,
            delete_zip=False,
            pixel_review_confirmer=_auto_confirmer,
        )

        uploads = [c for c in fake.calls if c["op"] == "resource.put_zip"]
        assert len(uploads) >= 1, "Disjoint case must complete the upload"

    def test_self_exclusion_prevents_false_positive(self, tmp_path):
        """
        Re-publish of the same uid must NOT trigger DedupReviewRequired
        via self-match.  The _RegistryExcludingCase proxy must hide the
        current case from case_dedup enumeration.
        """
        fake = _make_fake_xnat()
        registry = _make_registry(tmp_path)

        hashes = {"ee" * 32, "ff" * 32}
        uid = "SELF_TEST_CASE"

        # Pre-seed the case as if a prior write already registered it.
        registry.upsert_case(uid)
        for h in hashes:
            registry.upsert_image_hash(h, case_key=uid)

        # Build a proxy excluding the current uid.
        proxy = _RegistryExcludingCase(registry, uid)

        # case_dedup with the proxy must return DISJOINT (no other cases).
        from src.services.dedup import case_dedup, CaseRelation
        result = case_dedup(hashes, proxy)
        assert result.classification == CaseRelation.DISJOINT, (
            f"Self-exclusion failed: proxy returned {result.classification} "
            f"instead of DISJOINT"
        )


# ---------------------------------------------------------------------------
# Test (c): fresh/empty registry publishes cleanly
# ---------------------------------------------------------------------------

class TestDedupWiringFreshRegistry:
    """
    (c) Fresh (empty) registry must not block any upload.
    """

    def test_fresh_registry_publishes_clean(self, tmp_path):
        fake = _make_fake_xnat()
        registry = _make_registry(tmp_path)  # empty — no prior cases

        hashes = {"11" * 32, "22" * 32}
        form = _make_intake_form(tmp_path, uid="FIRST_EVER_CASE")
        zipped = _make_zip(tmp_path)
        config = _FakeConfig(registry)
        session = _WireTestSession.build(form, hashes, registry, zipped)

        # Must not raise.
        session.write_publish_catalog_subroutine(
            config=config,
            xnat_connection=_make_connection(fake),
            validated_login=_make_login(),
            verbose=False,
            delete_zip=False,
            pixel_review_confirmer=_auto_confirmer,
        )

        uploads = [c for c in fake.calls if c["op"] == "resource.put_zip"]
        assert len(uploads) >= 1, "First-ever case on fresh registry must upload"

    def test_none_registry_publishes_clean(self, tmp_path):
        """
        config._registry = None (registry unavailable) must degrade gracefully
        — no exception, upload proceeds.
        """
        fake = _make_fake_xnat()

        class _NoRegistryConfig:
            _registry = None
            config_ffn = str(tmp_path / "fake_config.json")
            def push_to_xnat(self, verbose=False): pass

        hashes = {"33" * 32}
        form = _make_intake_form(tmp_path, uid="NO_REGISTRY_CASE")
        zipped = _make_zip(tmp_path)
        config = _NoRegistryConfig()

        class _NoRegSession(ExperimentData):
            @classmethod
            def build(cls, intake_form, hashes, zipped_data):
                obj = object.__new__(cls)
                obj._intake_form = intake_form
                obj._schema_prefix_str = "rf"
                obj._scan_type_label = "DICOM"
                obj._is_valid = True
                obj._hashes = hashes
                obj._zipped_data = zipped_data
                import pandas as pd
                rows = [{"IS_VALID": True, "OBJECT": _FakeObject(h)} for h in hashes]
                obj._df = pd.DataFrame(rows)
                return obj
            def _populate_df(self, c): pass
            def _check_session_validity(self, c): pass
            def write(self, config, zip_dest=None, verbose=True):
                return self._zipped_data, config

        session = _NoRegSession.build(form, hashes, zipped)

        session.write_publish_catalog_subroutine(
            config=config,
            xnat_connection=_make_connection(fake),
            validated_login=_make_login(),
            verbose=False,
            delete_zip=False,
            pixel_review_confirmer=_auto_confirmer,
        )

        uploads = [c for c in fake.calls if c["op"] == "resource.put_zip"]
        assert len(uploads) >= 1, "No-registry path must not block upload"
