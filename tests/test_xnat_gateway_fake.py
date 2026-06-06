"""
Characterization tests for FakeXNAT — the offline test double for pyxnat.

Goal: prove every surface declared in xnat_gateway.XnatGateway
is faithfully faked and that tests can run with zero network access.

NO pyxnat.Interface constructed here. NO real server. NO PHI.
"""
from __future__ import annotations

import pytest
from pathlib import Path

from tests.fakes.fake_xnat import FakeXNAT, FakeSelectable, FakeResource, FakeFile, FakeProject


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def fake():
    """Fresh FakeXNAT for each test."""
    return FakeXNAT(project_name="TEST_PROJ", project_users=["testuser", "admin"])


# ---------------------------------------------------------------------------
# 1. server.select(querystring) surface
# ---------------------------------------------------------------------------

class TestSelect:
    def test_returns_fake_selectable(self, fake: FakeXNAT):
        result = fake.select("/project/TEST_PROJ/subject/S001")
        assert isinstance(result, FakeSelectable)

    def test_exists_default_false(self, fake: FakeXNAT):
        s = fake.select("/project/TEST_PROJ/subject/S001")
        assert s.exists() is False

    def test_create_sets_exists(self, fake: FakeXNAT):
        s = fake.select("/project/TEST_PROJ/subject/S001")
        s.create()
        assert s.exists() is True

    def test_create_recorded(self, fake: FakeXNAT):
        s = fake.select("/project/TEST_PROJ/subject/S001")
        s.create(experiments="xnat:mrSessionData")
        ops = [c["op"] for c in fake.calls]
        assert "selectable.create" in ops

    def test_attrs_mset_recorded(self, fake: FakeXNAT):
        s = fake.select("/project/TEST_PROJ/subject/S001")
        s.attrs.mset({"xnat:subjectData/GROUP": "CONTROL"})
        ops = [c["op"] for c in fake.calls]
        assert "attrs.mset" in ops

    def test_selectable_resource_returns_fake_resource(self, fake: FakeXNAT):
        s = fake.select("/project/TEST_PROJ/subject/S001")
        r = s.resource("SRC")
        assert isinstance(r, FakeResource)


# ---------------------------------------------------------------------------
# 2. server.select.project(name) surface
# ---------------------------------------------------------------------------

class TestSelectProject:
    def test_returns_fake_project(self, fake: FakeXNAT):
        p = fake.select.project("TEST_PROJ")
        assert isinstance(p, FakeProject)

    def test_label_matches_name(self, fake: FakeXNAT):
        p = fake.select.project("TEST_PROJ")
        assert p.label() == "TEST_PROJ"

    def test_exists_default_true(self, fake: FakeXNAT):
        p = fake.select.project("TEST_PROJ")
        assert p.exists() is True

    def test_users_list(self, fake: FakeXNAT):
        p = fake.select.project("TEST_PROJ")
        # FakeSelector.project() returns a FakeProject with default users ["testuser"]
        assert "testuser" in p.users()
        assert isinstance(p.users(), list)

    def test_users_list_custom(self):
        """FakeProject can be constructed with a custom user list."""
        from tests.fakes.fake_xnat import FakeProject
        root = FakeXNAT()
        p = FakeProject(root=root, name="P", users=["alice", "bob"])
        assert "alice" in p.users()
        assert "bob" in p.users()

    def test_project_resource_returns_fake_resource(self, fake: FakeXNAT):
        r = fake.select.project("TEST_PROJ").resource("CONFIG")
        assert isinstance(r, FakeResource)


# ---------------------------------------------------------------------------
# 3. resource.put_zip() — mirrors xnat_experiment_data publish_to_xnat
# ---------------------------------------------------------------------------

class TestPutZip:
    def test_put_zip_recorded(self, fake: FakeXNAT, tmp_path: Path):
        zip_path = tmp_path / "data.zip"
        zip_path.write_bytes(b"PK")  # minimal fake zip bytes
        scan = fake.select("/project/TEST_PROJ/subject/S001/experiment/E01/scan/0")
        scan.resource("SRC").put_zip(str(zip_path), content="IMAGE", format="DICOM", tags="DATA")
        assert fake.calls[-1]["op"] == "resource.put_zip"
        assert fake.calls[-1]["kwargs"]["content"] == "IMAGE"
        assert fake.calls[-1]["kwargs"]["format"] == "DICOM"
        assert fake.calls[-1]["kwargs"]["tags"] == "DATA"

    def test_put_zip_label_recorded(self, fake: FakeXNAT, tmp_path: Path):
        zip_path = tmp_path / "or_data.zip"
        zip_path.write_bytes(b"PK")
        s = fake.select("/project/TEST_PROJ/subject/S001/experiment/E01/scan/0")
        s.resource("SRC").put_zip(str(zip_path), content="OR_DATA", format="DICOM", tags="DATA")
        assert fake.calls[-1]["kwargs"]["_label"] == "SRC"


# ---------------------------------------------------------------------------
# 4. ConfigTables push/pull/delete flow
#    mirrors utilities.py push_to_xnat / pull_from_xnat / create_backup
# ---------------------------------------------------------------------------

class TestConfigTableFlow:
    """Simulate push → pull → delete on a config file, matching the
    server.select.project(name).resource(folder).file(fn).* calls
    used by ConfigTables methods."""

    def _project_resource_file(self, fake: FakeXNAT, folder: str, fn: str):
        return fake.select.project("TEST_PROJ").resource(folder).file(fn)

    def test_push_recorded(self, fake: FakeXNAT, tmp_path: Path):
        cfg = tmp_path / "config.json"
        cfg.write_text('{"version": 1}', encoding="utf-8")
        fh = self._project_resource_file(fake, "CONFIG", "config.json")
        fh.put(str(cfg), content="META_DATA", format="JSON", tags="DOC", overwrite=True)
        put_calls = [c for c in fake.calls if c["op"] == "file.put"]
        assert len(put_calls) == 1
        assert put_calls[0]["kwargs"]["content"] == "META_DATA"
        assert put_calls[0]["kwargs"]["overwrite"] is True

    def test_get_copy_writes_placeholder(self, fake: FakeXNAT, tmp_path: Path):
        dest = tmp_path / "downloaded_config.json"
        fh = self._project_resource_file(fake, "CONFIG", "config.json")
        returned = fh.get_copy(dest)
        assert returned == dest
        assert dest.exists()
        assert "[FakeXNAT placeholder" in dest.read_text(encoding="utf-8")

    def test_get_copy_recorded(self, fake: FakeXNAT, tmp_path: Path):
        dest = tmp_path / "cfg.json"
        fh = self._project_resource_file(fake, "CONFIG", "config.json")
        fh.get_copy(dest)
        get_calls = [c for c in fake.calls if c["op"] == "file.get_copy"]
        assert len(get_calls) == 1

    def test_delete_recorded(self, fake: FakeXNAT):
        fh = self._project_resource_file(fake, "BACKUPS", "config-backup-2024.json")
        fh.delete()
        del_calls = [c for c in fake.calls if c["op"] == "file.delete"]
        assert len(del_calls) == 1
        assert del_calls[0]["kwargs"]["_filename"] == "config-backup-2024.json"

    def test_full_push_pull_delete_sequence(self, fake: FakeXNAT, tmp_path: Path):
        """End-to-end: push backup → push config → pull config → delete backup."""
        cfg = tmp_path / "config.json"
        cfg.write_text('{}', encoding="utf-8")
        backup_fn = "config-backup-20240101.json"

        # push backup (create_backup path)
        fake.select.project("TEST_PROJ").resource("BACKUPS").file(backup_fn).put(
            str(cfg), content="META_DATA", format="JSON", tags="DOC", overwrite=True
        )
        # push config (push_to_xnat path)
        fake.select.project("TEST_PROJ").resource("CONFIG").file("config.json").put(
            str(cfg), content="META_DATA", format="JSON", tags="DOC", overwrite=True
        )
        # pull config
        dest = tmp_path / "pulled.json"
        fake.select.project("TEST_PROJ").resource("CONFIG").file("config.json").get_copy(dest)
        # delete backup (failure-rollback path)
        fake.select.project("TEST_PROJ").resource("BACKUPS").file(backup_fn).delete()

        ops = [c["op"] for c in fake.calls]
        assert ops == ["file.put", "file.put", "file.get_copy", "file.delete"]


# ---------------------------------------------------------------------------
# 5. file.insert() — mirrors xnat_resource_data push_to_xnat
# ---------------------------------------------------------------------------

class TestFileInsert:
    def test_insert_recorded(self, fake: FakeXNAT):
        subj = fake.select("/project/TEST_PROJ/subject/S001")
        data = b"<intake_form>fake</intake_form>"
        subj.resource("INTAKE_FORM").file("intake.xml").insert(
            data, content="FORM_DATA", format="XML", tags="INTAKE"
        )
        ins_calls = [c for c in fake.calls if c["op"] == "file.insert"]
        assert len(ins_calls) == 1
        assert ins_calls[0]["args"][0] == data
        assert ins_calls[0]["kwargs"]["content"] == "FORM_DATA"


# ---------------------------------------------------------------------------
# 6. Full publish_to_xnat mimic (scan subject→exp→scan→resource flow)
# ---------------------------------------------------------------------------

class TestPublishFlow:
    """Simulate the complete publish_to_xnat call sequence from
    xnat_experiment_data.py, exercising select → create → attrs.mset →
    resource → put_zip in sequence."""

    def test_full_publish_sequence(self, fake: FakeXNAT, tmp_path: Path):
        zip_path = tmp_path / "session.zip"
        zip_path.write_bytes(b"PK\x03\x04")

        subj_qs = "/project/TEST_PROJ/subject/S001"
        exp_qs = "/project/TEST_PROJ/subject/S001/experiment/SOURCE_DATA-001"
        scan_qs = "/project/TEST_PROJ/subject/S001/experiment/SOURCE_DATA-001/scan/0"

        subj = fake.select(subj_qs)
        exp = fake.select(exp_qs)
        scan = fake.select(scan_qs)

        # All must start non-existent (matches assertion in _select_objects)
        assert not subj.exists()
        assert not exp.exists()
        assert not scan.exists()

        # Create in order
        subj.create()
        subj.attrs.mset({"xnat:subjectData/GROUP": "ARTHR"})
        exp.create(experiments="xnat:mrSessionData")
        exp.attrs.mset({"xnat:experimentData/ACQUISITION_SITE": "UIOWA",
                        "xnat:experimentData/DATE": "2024-01-01"})
        scan.create(scans="xnat:mrScanData")
        scan.attrs.mset({"xnat:mrScanData/TYPE": "ARTHROSCOPY",
                         "xnat:mrScanData/QUALITY": "usable"})

        # Upload zip
        scan.resource("SRC").put_zip(str(zip_path), content="IMAGE", format="DICOM", tags="DATA")

        # Insert intake form on subject resource
        subj.resource("INTAKE_FORM").file("intake.txt").insert(
            b"fake intake form bytes", content="FORM_DATA", format="TXT", tags="INTAKE"
        )

        ops = [c["op"] for c in fake.calls]
        assert ops == [
            "selectable.create",
            "attrs.mset",
            "selectable.create",
            "attrs.mset",
            "selectable.create",
            "attrs.mset",
            "resource.put_zip",
            "file.insert",
        ]

    def test_call_log_reset(self, fake: FakeXNAT):
        fake.select("/project/X/subject/A").create()
        assert len(fake.calls) == 1
        fake.reset_calls()
        assert len(fake.calls) == 0


# ---------------------------------------------------------------------------
# 7. Failure injection
# ---------------------------------------------------------------------------

class TestFailureInjection:
    def test_timeout_on_put_zip(self, fake: FakeXNAT, tmp_path: Path):
        zip_path = tmp_path / "fail.zip"
        zip_path.write_bytes(b"PK")
        fake.set_next_failure(TimeoutError("simulated network timeout"))
        scan = fake.select("/project/TEST_PROJ/subject/S001/experiment/E01/scan/0")
        with pytest.raises(TimeoutError, match="simulated network timeout"):
            scan.resource("SRC").put_zip(str(zip_path), content="OR_DATA", format="DICOM", tags="DATA")

    def test_auth_error_on_file_put(self, fake: FakeXNAT, tmp_path: Path):
        cfg = tmp_path / "config.json"
        cfg.write_text("{}", encoding="utf-8")
        fake.set_next_failure(PermissionError("bad credentials"))
        with pytest.raises(PermissionError, match="bad credentials"):
            fake.select.project("TEST_PROJ").resource("CONFIG").file("config.json").put(
                str(cfg), content="META_DATA", format="JSON", tags="DOC", overwrite=True
            )

    def test_failure_cleared_after_one_use(self, fake: FakeXNAT, tmp_path: Path):
        """Second call succeeds — failure fires only once."""
        zip_path = tmp_path / "d.zip"
        zip_path.write_bytes(b"PK")
        fake.set_next_failure(ConnectionError("one-shot"))
        scan = fake.select("/project/X/subject/A/experiment/E/scan/0")
        with pytest.raises(ConnectionError):
            scan.resource("SRC").put_zip(str(zip_path))
        # second call must not raise
        scan2 = fake.select("/project/X/subject/A/experiment/E/scan/0")
        scan2.resource("SRC").put_zip(str(zip_path))  # should not raise
        put_zip_calls = [c for c in fake.calls if c["op"] == "resource.put_zip"]
        assert len(put_zip_calls) == 1  # first attempt failed before recording

    def test_ssl_error_on_get_copy(self, fake: FakeXNAT, tmp_path: Path):
        dest = tmp_path / "out.json"
        fake.set_next_failure(OSError("SSL certificate verification failed"))
        with pytest.raises(OSError, match="SSL"):
            fake.select.project("TEST_PROJ").resource("CONFIG").file("config.json").get_copy(dest)

    def test_error_on_delete(self, fake: FakeXNAT):
        fake.set_next_failure(RuntimeError("deletion refused"))
        with pytest.raises(RuntimeError, match="deletion refused"):
            fake.select.project("TEST_PROJ").resource("BACKUPS").file("old.json").delete()

    def test_error_on_insert(self, fake: FakeXNAT):
        fake.set_next_failure(ValueError("bad data"))
        subj = fake.select("/project/TEST_PROJ/subject/S001")
        with pytest.raises(ValueError, match="bad data"):
            subj.resource("INTAKE_FORM").file("intake.txt").insert(b"data")


# ---------------------------------------------------------------------------
# 8. server.disconnect() no-op
# ---------------------------------------------------------------------------

class TestDisconnect:
    def test_disconnect_does_not_raise(self, fake: FakeXNAT):
        fake.disconnect()  # must be a no-op


# ---------------------------------------------------------------------------
# 9. select is callable AND has .project attribute (dual-mode check)
# ---------------------------------------------------------------------------

class TestSelectDualMode:
    def test_select_callable(self, fake: FakeXNAT):
        s = fake.select("/project/TEST_PROJ/subject/S001")
        assert isinstance(s, FakeSelectable)

    def test_select_project_attribute(self, fake: FakeXNAT):
        p = fake.select.project("TEST_PROJ")
        assert isinstance(p, FakeProject)

    def test_both_modes_independent(self, fake: FakeXNAT):
        """Calling select() as a function does not interfere with select.project()."""
        fake.select("/project/TEST_PROJ/subject/S001").create()
        p = fake.select.project("TEST_PROJ")
        assert p.label() == "TEST_PROJ"
