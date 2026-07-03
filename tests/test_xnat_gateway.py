"""
tests/test_xnat_gateway.py — Phase 6 Stage 1 gateway tests (T004).

Three assertions:
  (a) ABC method-set parity: every abstract method on XnatGateway is
      implemented by both PyxnatGateway and FakeXNAT (FakeGateway).
  (b) Recorded-call equivalence: a scripted publish sequence through
      FakeGateway produces the expected call log.
  (c) create_assessor writes are recorded distinct from scan-resource writes.
"""
from __future__ import annotations

import inspect
from pathlib import Path
from typing import get_type_hints

import pytest

from src.services.errors import FriendlyError
from src.services.xnat_gateway import XnatGateway, PyxnatGateway, GatewayError
from tests.fakes.fake_xnat import FakeXNAT, FakeGateway


# ---------------------------------------------------------------------------
# (a) ABC method-set parity
# ---------------------------------------------------------------------------

def _abstract_methods(cls) -> set:
    """Return the set of abstract method names declared on *cls*."""
    return {
        name
        for name, val in inspect.getmembers(cls)
        if getattr(val, "__isabstractmethod__", False)
    }


def _concrete_methods(cls) -> set:
    """Return the set of non-abstract public method names on *cls*."""
    return {
        name
        for name, val in inspect.getmembers(cls, predicate=inspect.isfunction)
        if not name.startswith("_") and not getattr(val, "__isabstractmethod__", False)
    }


class TestAbstractMethodParity:
    """Every abstract method on XnatGateway must be implemented on both impls."""

    def test_fake_xnat_implements_all_abstract_methods(self):
        """FakeXNAT must have no remaining abstract methods (ABC is satisfied)."""
        # If any abstract method were unimplemented, instantiation would raise.
        fake = FakeXNAT()
        assert isinstance(fake, XnatGateway)

    def test_pyxnat_gateway_implements_all_abstract_methods(self):
        """PyxnatGateway class must have no remaining abstract methods."""
        abstract = _abstract_methods(XnatGateway)
        pyxnat_methods = {name for name, _ in inspect.getmembers(PyxnatGateway, predicate=inspect.isfunction)}
        missing = abstract - pyxnat_methods
        assert not missing, f"PyxnatGateway missing: {missing}"

    def test_fake_gateway_alias(self):
        """FakeGateway must be the same class as FakeXNAT."""
        assert FakeGateway is FakeXNAT

    def test_fake_and_pyxnat_method_sets_match(self):
        """
        The set of public non-abstract methods on PyxnatGateway that overlap
        with XnatGateway abstract methods must match FakeXNAT's impl set.
        """
        abstract = _abstract_methods(XnatGateway)
        fake_methods = {name for name, _ in inspect.getmembers(FakeXNAT, predicate=inspect.isfunction)}
        pyxnat_methods = {name for name, _ in inspect.getmembers(PyxnatGateway, predicate=inspect.isfunction)}
        # Both must implement every abstract method.
        assert abstract <= fake_methods, f"FakeXNAT missing abstract methods: {abstract - fake_methods}"
        assert abstract <= pyxnat_methods, f"PyxnatGateway missing abstract methods: {abstract - pyxnat_methods}"


# ---------------------------------------------------------------------------
# (b) Recorded-call equivalence on a scripted publish sequence
# ---------------------------------------------------------------------------

class TestPublishSequenceRecording:
    """
    Scripted publish sequence through FakeGateway records expected calls.

    Sequence mirrors the real publish_to_xnat flow (XNAT_MODEL.md §2):
      1. create subject
      2. create experiment
      3. create scan
      4. put_zip SRC resource
      5. insert_file INTAKE_FORM resource
    """

    def setup_method(self):
        self.fake = FakeGateway()
        self.proj = "FAKE_PROJECT"
        self.uid = "1.2.3.4"
        self.subj_qs = f"/project/{self.proj}/subject/{self.uid}"
        self.exp_qs = self.subj_qs + f"/experiment/SOURCE_DATA-{self.uid}"
        self.scan_qs = self.exp_qs + "/scan/0"

    def _run_publish_sequence(self, fake_zip: Path, fake_json: str):
        # 1. create subject
        self.fake.create(self.subj_qs)
        # 2. create experiment
        self.fake.create(self.exp_qs, xsiType="xnat:rfSessionData")
        # 3. create scan
        self.fake.create(self.scan_qs)
        # 4. put_zip on scan SRC resource
        self.fake.put_zip(self.scan_qs, "SRC", str(fake_zip), content="IMAGE", format="DICOM", tags="DATA")
        # 5. insert_file INTAKE_FORM on subject
        self.fake.insert_file(self.subj_qs, "INTAKE_FORM", "form.json", fake_json, content="FORM", format="JSON", tags="")

    def test_publish_sequence_records_expected_ops(self, tmp_path):
        fake_zip = tmp_path / "data.zip"
        fake_zip.write_bytes(b"PK fake zip")
        fake_json = '{"uid": "1.2.3.4"}'

        self._run_publish_sequence(fake_zip, fake_json)

        ops = [c["op"] for c in self.fake.calls]
        assert "selectable.create" in ops
        assert "resource.put_zip" in ops
        assert "file.insert" in ops

    def test_publish_sequence_call_count(self, tmp_path):
        fake_zip = tmp_path / "data.zip"
        fake_zip.write_bytes(b"PK fake zip")
        fake_json = '{"uid": "1.2.3.4"}'

        self._run_publish_sequence(fake_zip, fake_json)

        # 3 creates + 1 put_zip + 1 insert = 5
        assert len(self.fake.calls) == 5

    def test_subject_marked_existing_after_create(self):
        self.fake.create(self.subj_qs)
        assert self.fake.exists(self.subj_qs)

    def test_exists_false_before_create(self):
        assert not self.fake.exists(self.subj_qs)

    def test_project_label_returns_project_name(self):
        label = self.fake.project_label(self.proj)
        assert label == self.proj

    def test_project_users_returns_list(self):
        users = self.fake.project_users(self.proj)
        assert isinstance(users, list)
        assert len(users) >= 1

    def test_set_attrs_records_call(self):
        self.fake.create(self.exp_qs, xsiType="xnat:rfSessionData")
        self.fake.set_attrs(self.exp_qs, {"xnat:rfSessionData/date": "2026-01-01"})
        mset_calls = [c for c in self.fake.calls if c["op"] == "attrs.mset"]
        assert len(mset_calls) == 1
        assert "xnat:rfSessionData/date" in mset_calls[0]["args"][0]

    def test_get_file_copy_round_trips_bytes(self, tmp_path):
        payload = b"hello annotation"
        filename = "ann.json"
        self.fake.set_file_content(filename, payload)
        dest = tmp_path / filename
        result = self.fake.get_file_copy(self.scan_qs, "ANNOTATIONS", filename, dest)
        assert Path(result).read_bytes() == payload

    def test_delete_file_records_call(self):
        self.fake.delete_file(self.scan_qs, "ANNOTATIONS", "old.json")
        ops = [c["op"] for c in self.fake.calls]
        assert "file.delete" in ops

    def test_list_files_empty_by_default(self):
        files = self.fake.list_files(self.scan_qs, "SRC")
        assert files == []

    def test_list_files_returns_seeded_names(self):
        self.fake.create(self.scan_qs)
        sel = self.fake._selectables[self.scan_qs]
        resource = sel.resource("SRC")
        self.fake.seed_resource_files(resource, [("a.dcm", b"bytes1"), ("b.dcm", b"bytes2")])
        result = self.fake.list_files(self.scan_qs, "SRC")
        assert result == ["a.dcm", "b.dcm"]

    def test_download_resource_writes_files(self, tmp_path):
        self.fake.create(self.scan_qs)
        sel = self.fake._selectables[self.scan_qs]
        resource = sel.resource("SRC")
        self.fake.seed_resource_files(resource, [("img.dcm", b"\x00\x01\x02")])
        written = self.fake.download_resource(self.scan_qs, "SRC", tmp_path)
        assert len(written) == 1
        assert written[0].read_bytes() == b"\x00\x01\x02"

    # M7 regression: download_resource must return N individual files, not 1 zip.
    def test_download_resource_returns_individual_files_not_zip(self, tmp_path):
        """Regression for M7: upload N DICOMs, download_resource returns N files
        (not 1 zip), and byte content round-trips correctly."""
        payloads = [
            ("slice_001.dcm", b"\xd4\xd4\x00\x01"),
            ("slice_002.dcm", b"\xd4\xd4\x00\x02"),
            ("slice_003.dcm", b"\xd4\xd4\x00\x03"),
        ]
        self.fake.create(self.scan_qs)
        sel = self.fake._selectables[self.scan_qs]
        resource = sel.resource("DICOM")
        self.fake.seed_resource_files(resource, payloads)

        written = self.fake.download_resource(self.scan_qs, "DICOM", tmp_path)

        # Must return 3 individual files, not 1 zip archive.
        assert len(written) == 3, (
            f"Expected 3 individual files but got {len(written)}: {written}"
        )
        # No zip files in the result.
        zip_files = [p for p in written if p.suffix.lower() == ".zip"]
        assert zip_files == [], f"download_resource returned zip archive(s): {zip_files}"
        # Byte content must match originals (basename-keyed).
        by_name = {p.name: p.read_bytes() for p in written}
        for fname, expected_bytes in payloads:
            assert fname in by_name, f"Expected file {fname!r} not in result"
            assert by_name[fname] == expected_bytes, (
                f"Byte mismatch for {fname}: got {by_name[fname]!r}"
            )


# ---------------------------------------------------------------------------
# (c) create_assessor writes are distinct from scan-resource writes
# ---------------------------------------------------------------------------

class TestAssessorWritesDistinct:
    """
    Assessor-level writes must be recorded under a different op key than
    scan-resource writes so Stage 4 contract tests can assert the shape.
    """

    def setup_method(self):
        self.fake = FakeGateway()
        self.uid = "1.2.3.4"
        self.proj = "FAKE_PROJECT"
        self.exp_qs = f"/project/{self.proj}/subject/{self.uid}/experiment/SOURCE_DATA-{self.uid}"

    def test_create_assessor_records_assessor_create_op(self, tmp_path):
        self.fake.create(self.exp_qs, xsiType="xnat:rfSessionData")
        seg_file = tmp_path / "seg.nii"
        seg_file.write_bytes(b"\x00segdata")
        self.fake.create_assessor(
            self.exp_qs,
            "SEGMENTATION_CONSENSUS-1.2.3.4",
            xsi_type="xnat:assessorData",
            files=[("SEG", "seg.nii", seg_file)],
        )
        ops = [c["op"] for c in self.fake.calls]
        assert "assessor.create" in ops

    def test_assessor_file_put_op_distinct_from_scan_file_put(self, tmp_path):
        self.fake.create(self.exp_qs, xsiType="xnat:rfSessionData")
        seg_file = tmp_path / "seg.nii"
        seg_file.write_bytes(b"\x00segdata")
        # Scan-resource write
        scan_qs = self.exp_qs + "/scan/0"
        self.fake.put_zip(scan_qs, "SRC", str(tmp_path / "data.zip"), content="IMAGE", format="DICOM", tags="DATA")
        # Assessor write
        self.fake.create_assessor(
            self.exp_qs,
            "SEGMENTATION_CONSENSUS-1.2.3.4",
            files=[("SEG", "seg.nii", seg_file)],
        )
        ops = [c["op"] for c in self.fake.calls]
        assert "resource.put_zip" in ops        # scan-resource
        assert "assessor.create" in ops         # assessor creation
        assert "assessor.file.put" in ops       # assessor file attachment
        # No "file.put" op from assessor (distinct key)
        assessor_puts = [c for c in self.fake.calls if c["op"] == "assessor.file.put"]
        scan_puts = [c for c in self.fake.calls if c["op"] == "resource.put_zip"]
        assert len(assessor_puts) == 1
        assert len(scan_puts) == 1

    def test_create_assessor_idempotent(self, tmp_path):
        """Calling create_assessor twice does not create duplicate create records."""
        self.fake.create(self.exp_qs, xsiType="xnat:rfSessionData")
        self.fake.create_assessor(self.exp_qs, "ASSESS-001")
        self.fake.create_assessor(self.exp_qs, "ASSESS-001")
        creates = [c for c in self.fake.calls if c["op"] == "assessor.create"]
        assert len(creates) == 1

    def test_fidelity_mode_populates_datatype_cache(self):
        """With fidelity_mode=True, create_assessor sets xsiType on attrs._datatype."""
        fake = FakeGateway(fidelity_mode=True)
        fake.create(self.exp_qs, xsiType="xnat:rfSessionData")
        assessor_qs = self.exp_qs + "/assessor/ASSESS-002"
        fake.create_assessor(self.exp_qs, "ASSESS-002", xsi_type="xnat:assessorData")
        sel = fake._selectables.get(assessor_qs)
        assert sel is not None
        assert sel.attrs._datatype == "xnat:assessorData"


# ---------------------------------------------------------------------------
# H6 — Parent-experiment existence check + label sanitization
# ---------------------------------------------------------------------------

class TestCreateAssessorH6Guards:
    """
    H6: create_assessor must guard parent-experiment existence and sanitize labels.
    """

    def setup_method(self):
        self.fake = FakeGateway()
        self.uid = "1.2.3.4"
        self.proj = "FAKE_PROJECT"
        self.exp_qs = f"/project/{self.proj}/subject/{self.uid}/experiment/SOURCE_DATA-{self.uid}"

    def test_create_assessor_raises_on_nonexistent_parent(self):
        """create_assessor raises GatewayError if parent experiment does not exist."""
        nonexistent_exp_qs = f"/project/{self.proj}/subject/{self.uid}/experiment/NONEXISTENT"
        with pytest.raises(GatewayError) as exc_info:
            self.fake.create_assessor(nonexistent_exp_qs, "ASSESS-001")
        assert "Parent experiment not found" in exc_info.value.friendly.title
        assert "does not exist" in exc_info.value.friendly.message

    def test_create_assessor_succeeds_with_existent_parent(self, tmp_path):
        """create_assessor succeeds when parent experiment exists."""
        # Create the parent experiment first
        self.fake.create(self.exp_qs, xsiType="xnat:rfSessionData")
        # Now create the assessor
        seg_file = tmp_path / "seg.nii"
        seg_file.write_bytes(b"\x00segdata")
        self.fake.create_assessor(
            self.exp_qs,
            "ASSESS-001",
            files=[("SEG", "seg.nii", seg_file)],
        )
        creates = [c for c in self.fake.calls if c["op"] == "assessor.create"]
        assert len(creates) == 1

    def test_create_assessor_rejects_label_with_path_separator(self):
        """create_assessor raises GatewayError if assessor_label contains '/'."""
        self.fake.create(self.exp_qs, xsiType="xnat:rfSessionData")
        with pytest.raises(GatewayError) as exc_info:
            self.fake.create_assessor(self.exp_qs, "ASSESS/SUBDIR")
        assert "Invalid assessor label" in exc_info.value.friendly.title
        assert "path separators" in exc_info.value.friendly.message

    def test_create_assessor_rejects_label_with_parent_dir_reference(self):
        """create_assessor raises GatewayError if assessor_label contains '..'."""
        self.fake.create(self.exp_qs, xsiType="xnat:rfSessionData")
        with pytest.raises(GatewayError) as exc_info:
            self.fake.create_assessor(self.exp_qs, "../ASSESS")
        assert "Invalid assessor label" in exc_info.value.friendly.title
        assert "path separators" in exc_info.value.friendly.message

    def test_create_assessor_accepts_valid_labels(self):
        """create_assessor accepts labels with alphanumerics, hyphens, underscores, dots."""
        self.fake.create(self.exp_qs, xsiType="xnat:rfSessionData")
        # These should all succeed
        valid_labels = [
            "SEGMENTATION_CONSENSUS-1.2.3.4",
            "SEG_001",
            "seg-consensus",
            "ASSESS-v2.0",
        ]
        for label in valid_labels:
            self.fake.create_assessor(self.exp_qs, label)
        creates = [c for c in self.fake.calls if c["op"] == "assessor.create"]
        assert len(creates) == len(valid_labels)


# ---------------------------------------------------------------------------
# (d) disconnect() is a safe no-op on an unconnected gateway (#33 L4)
# ---------------------------------------------------------------------------

def test_disconnect_before_connect_is_noop():
    """PyxnatGateway.disconnect() must not raise when connect() never ran (#33 L4)."""
    gw = PyxnatGateway(url="http://example.invalid", user="u", password="p")
    assert gw.server is None
    gw.disconnect()  # must NOT raise AttributeError
    assert gw.server is None
