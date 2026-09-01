"""
tests/test_009_stage5_pseudonym_versioning.py — Stage 5 (T015 / T016).

T015: Surgeon pseudonymization at intake (FR-011/012).
T016: Keep-all monotonic versioning for assessor/derived data (FR-013).

All tests are offline — no XNAT server, no VPN, no PHI.
"""
from __future__ import annotations


from src.xnat_resource_data import pseudonymize_surgeon_ids
from src.services import xnat_conventions as conventions
from tests.fakes.fake_xnat import FakeXNAT


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_SALT = bytes.fromhex("deadbeef" * 8)  # 32-byte throwaway test salt


# ---------------------------------------------------------------------------
# T015-a: pseudonymize_surgeon_ids — field replacement
# ---------------------------------------------------------------------------

class TestPseudonymizeSurgeonIds:

    def _surgical_info(self, sup="jdoe", perf="jsmith", tasks=None):
        return {
            "INSTITUTION_NAME": "UIHC",
            "SUPERVISING_SURGEON_UID": sup,
            "SUPERVISING_SURGEON_PRESENCE": "PRESENT",
            "PERFORMING_SURGEON_UID": perf,
            "PERFORMER_YEAR_IN_RESIDENCY": "3",
            "PERFORMANCE_ENUMERATED_TASK_PER_PERFORMER": tasks or {"jdoe": "camera", "jsmith": "drill"},
        }

    def test_supervising_surgeon_replaced(self):
        info = self._surgical_info(sup="jdoe")
        result = pseudonymize_surgeon_ids(info, _SALT)
        assert result["SUPERVISING_SURGEON_UID"] != "jdoe"
        assert result["SUPERVISING_SURGEON_UID"].startswith("surgeon_")

    def test_performing_surgeon_replaced(self):
        info = self._surgical_info(perf="jsmith")
        result = pseudonymize_surgeon_ids(info, _SALT)
        assert result["PERFORMING_SURGEON_UID"] != "jsmith"
        assert result["PERFORMING_SURGEON_UID"].startswith("surgeon_")

    def test_same_hawkid_same_pseudonym(self):
        """Same HawkID + same salt → identical pseudonym across two calls."""
        info1 = self._surgical_info(sup="hawkA")
        info2 = self._surgical_info(sup="hawkA")
        r1 = pseudonymize_surgeon_ids(info1, _SALT)
        r2 = pseudonymize_surgeon_ids(info2, _SALT)
        assert r1["SUPERVISING_SURGEON_UID"] == r2["SUPERVISING_SURGEON_UID"]

    def test_different_hawkids_different_pseudonyms(self):
        info1 = self._surgical_info(sup="hawkA")
        info2 = self._surgical_info(sup="hawkB")
        r1 = pseudonymize_surgeon_ids(info1, _SALT)
        r2 = pseudonymize_surgeon_ids(info2, _SALT)
        assert r1["SUPERVISING_SURGEON_UID"] != r2["SUPERVISING_SURGEON_UID"]

    def test_task_dict_keys_pseudonymized(self):
        """PERFORMANCE_ENUMERATED_TASK_PER_PERFORMER keys become pseudonyms."""
        tasks = {"jdoe": "camera", "jsmith": "drill"}
        info = self._surgical_info(tasks=tasks)
        result = pseudonymize_surgeon_ids(info, _SALT)
        result_tasks = result["PERFORMANCE_ENUMERATED_TASK_PER_PERFORMER"]
        # No raw HawkID in keys
        assert "jdoe" not in result_tasks
        assert "jsmith" not in result_tasks
        # Values preserved
        values = list(result_tasks.values())
        assert "camera" in values
        assert "drill" in values

    def test_no_raw_hawkid_in_result(self):
        """Raw HawkID must not appear anywhere in the result dict."""
        info = self._surgical_info(sup="supersecretid", perf="performersecret")
        result = pseudonymize_surgeon_ids(info, _SALT)
        result_str = str(result)
        assert "supersecretid" not in result_str
        assert "performersecret" not in result_str

    def test_original_dict_not_mutated(self):
        """Input dict is not modified in-place."""
        info = self._surgical_info(sup="jdoe")
        original_sup = info["SUPERVISING_SURGEON_UID"]
        pseudonymize_surgeon_ids(info, _SALT)
        assert info["SUPERVISING_SURGEON_UID"] == original_sup

    def test_unknown_value_passes_through(self):
        """'UNKNOWN' sentinel values are not pseudonymized."""
        info = self._surgical_info(sup="UNKNOWN", perf="NONE")
        result = pseudonymize_surgeon_ids(info, _SALT)
        assert result["SUPERVISING_SURGEON_UID"] == "UNKNOWN"
        assert result["PERFORMING_SURGEON_UID"] == "NONE"

    def test_crosswalk_records_mapping(self, tmp_path):
        """CrosswalkStore receives pseudonym → hawkid mapping."""
        from src.services.registry import CrosswalkStore
        cw = CrosswalkStore(tmp_path / "crosswalk.json")
        info = self._surgical_info(sup="jdoe")
        result = pseudonymize_surgeon_ids(info, _SALT, crosswalk=cw)
        pseudo = result["SUPERVISING_SURGEON_UID"]
        assert cw.get(pseudo) == "jdoe"

    def test_crosswalk_does_not_receive_raw_name_in_registry(self, tmp_path):
        """Operational result has no real name; crosswalk holds the mapping."""
        from src.services.registry import CrosswalkStore, Registry
        cw = CrosswalkStore(tmp_path / "crosswalk.json")
        reg = Registry(tmp_path / "reg.db")
        info = self._surgical_info(sup="jdoe")
        result = pseudonymize_surgeon_ids(info, _SALT, crosswalk=cw)
        pseudo = result["SUPERVISING_SURGEON_UID"]
        reg.upsert_surgeon(pseudo)
        # Confirm operational registry has no real name
        import sqlite3
        conn = sqlite3.connect(str(tmp_path / "reg.db"))
        rows = conn.execute("SELECT pseudonym FROM surgeons").fetchall()
        all_pseudonyms = [r[0] for r in rows]
        assert "jdoe" not in all_pseudonyms
        assert pseudo in all_pseudonyms


# ---------------------------------------------------------------------------
# T015-b: FR-012 verify — PatientID redaction already in deidentify.py
# ---------------------------------------------------------------------------

class TestPatientIdRedaction:
    """FR-012: confirm existing deidentify.py handles PatientID (verify-first)."""

    def test_patient_id_redacted(self):
        from src.services.deidentify import deidentify_dataset
        import pydicom
        ds = pydicom.Dataset()
        ds.PatientName = "Doe^John"
        ds.PatientID = "12345678"
        ds.AccessionNumber = "ACC001"
        deidentify_dataset(ds, "REDACTED")
        assert ds.PatientID == "REDACTED 4 XNAT"

    def test_patient_name_pn_vr_redacted(self):
        from src.services.deidentify import deidentify_dataset
        import pydicom
        ds = pydicom.Dataset()
        ds.PatientName = "Doe^John"
        ds.PatientID = "12345678"
        deidentify_dataset(ds, "REDACTED_PN")
        assert str(ds.PatientName) == "REDACTED_PN"


# ---------------------------------------------------------------------------
# T016: keep-all monotonic versioning for assessor labels
# ---------------------------------------------------------------------------

class TestNextAssessorLabel:

    def _make_fake(self):
        return FakeXNAT(fidelity_mode=True)

    def test_first_upload_is_v1(self):
        """No existing assessors → first label is base__v1."""
        gw = self._make_fake()
        exp_qs = "/project/P/subject/S/experiment/E"
        gw.create(exp_qs)
        label = conventions.next_assessor_label(gw, exp_qs, "CONSENSUS-uid1")
        assert label == "CONSENSUS-uid1__v1"

    def test_second_upload_is_v2(self):
        """One existing v1 assessor → next label is base__v2."""
        gw = self._make_fake()
        exp_qs = "/project/P/subject/S/experiment/E"
        gw.create(exp_qs)
        # Simulate first upload
        gw.create_assessor(exp_qs, "CONSENSUS-uid1__v1", xsi_type="xnat:assessorData")
        label = conventions.next_assessor_label(gw, exp_qs, "CONSENSUS-uid1")
        assert label == "CONSENSUS-uid1__v2"

    def test_v3_after_v1_and_v2(self):
        gw = self._make_fake()
        exp_qs = "/project/P/subject/S/experiment/E"
        gw.create(exp_qs)
        gw.create_assessor(exp_qs, "MYBASE__v1", xsi_type="xnat:assessorData")
        gw.create_assessor(exp_qs, "MYBASE__v2", xsi_type="xnat:assessorData")
        label = conventions.next_assessor_label(gw, exp_qs, "MYBASE")
        assert label == "MYBASE__v3"

    def test_unrelated_assessors_do_not_affect_version(self):
        """Assessors with a different base label do not change the version count."""
        gw = self._make_fake()
        exp_qs = "/project/P/subject/S/experiment/E"
        gw.create(exp_qs)
        gw.create_assessor(exp_qs, "OTHER_BASE__v1", xsi_type="xnat:assessorData")
        gw.create_assessor(exp_qs, "OTHER_BASE__v2", xsi_type="xnat:assessorData")
        label = conventions.next_assessor_label(gw, exp_qs, "MYBASE")
        assert label == "MYBASE__v1"

    def test_v1_not_overwritten(self):
        """Creating v2 via create_assessor preserves v1 (separate qs)."""
        gw = self._make_fake()
        exp_qs = "/project/P/subject/S/experiment/E"
        gw.create(exp_qs)
        gw.create_assessor(exp_qs, "SEG__v1", xsi_type="xnat:assessorData", files=[])
        # v1 must still exist
        existing = gw.list_assessors(exp_qs)
        assert "SEG__v1" in existing
        # v2 creation
        gw.create_assessor(exp_qs, "SEG__v2", xsi_type="xnat:assessorData", files=[])
        existing2 = gw.list_assessors(exp_qs)
        assert "SEG__v1" in existing2
        assert "SEG__v2" in existing2

    def test_list_assessors_empty_for_new_experiment(self):
        gw = self._make_fake()
        exp_qs = "/project/P/subject/S/experiment/E"
        gw.create(exp_qs)
        assert gw.list_assessors(exp_qs) == []

    def test_list_assessors_returns_all_created(self):
        gw = self._make_fake()
        exp_qs = "/project/P/subject/S/experiment/E"
        gw.create(exp_qs)
        gw.create_assessor(exp_qs, "A__v1", xsi_type="xnat:assessorData")
        gw.create_assessor(exp_qs, "B__v1", xsi_type="xnat:assessorData")
        labels = gw.list_assessors(exp_qs)
        assert set(labels) == {"A__v1", "B__v1"}
