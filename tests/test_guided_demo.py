"""
tests/test_guided_demo — Tests for app.guided.demo module.

Offline tests for demo mode: environment detection, config, server seeding,
and login flow. No real XNAT server; all operations against FakeXNAT.
"""
from __future__ import annotations

import os
from typing import List

import pytest

from app.guided import demo
from app.logic.browse import fetch_data_table


class TestDemoModeDetection:
    """Test is_demo_mode() environment detection."""

    def test_is_demo_mode_unset_returns_false(self, monkeypatch):
        """XNAT_DEMO_MODE unset → False."""
        monkeypatch.delenv("XNAT_DEMO_MODE", raising=False)
        assert demo.is_demo_mode() is False

    def test_is_demo_mode_zero_returns_false(self, monkeypatch):
        """XNAT_DEMO_MODE='0' → False."""
        monkeypatch.setenv("XNAT_DEMO_MODE", "0")
        assert demo.is_demo_mode() is False

    def test_is_demo_mode_one_returns_true(self, monkeypatch):
        """XNAT_DEMO_MODE='1' → True."""
        monkeypatch.setenv("XNAT_DEMO_MODE", "1")
        assert demo.is_demo_mode() is True

    def test_is_demo_mode_true_returns_true(self, monkeypatch):
        """XNAT_DEMO_MODE='true' → True."""
        monkeypatch.setenv("XNAT_DEMO_MODE", "true")
        assert demo.is_demo_mode() is True

    def test_is_demo_mode_yes_returns_true(self, monkeypatch):
        """XNAT_DEMO_MODE='yes' → True."""
        monkeypatch.setenv("XNAT_DEMO_MODE", "yes")
        assert demo.is_demo_mode() is True

    def test_is_demo_mode_on_returns_true(self, monkeypatch):
        """XNAT_DEMO_MODE='on' → True."""
        monkeypatch.setenv("XNAT_DEMO_MODE", "on")
        assert demo.is_demo_mode() is True

    def test_is_demo_mode_case_insensitive(self, monkeypatch):
        """Case-insensitive: 'TRUE', 'YES', 'ON' all return True."""
        for val in ("TRUE", "YES", "ON", "True", "Yes", "On"):
            monkeypatch.setenv("XNAT_DEMO_MODE", val)
            assert demo.is_demo_mode() is True


class TestDemoConfig:
    """Test DemoConfig.list_of_all_items_in_table()."""

    def test_demo_config_has_project_name(self):
        """DemoConfig.project_name == 'DEMO_PROJECT'."""
        cfg = demo.DemoConfig()
        assert cfg.project_name == "DEMO_PROJECT"

    def test_demo_config_surgeons(self):
        """list_of_all_items_in_table('Surgeons') returns demo surgeons."""
        cfg = demo.DemoConfig()
        result = cfg.list_of_all_items_in_table("Surgeons")
        assert result == ["dr_smith", "dr_jones", "dr_okafor"]

    def test_demo_config_surgeons_lowercase(self):
        """Case-insensitive: 'surgeons' works too."""
        cfg = demo.DemoConfig()
        result = cfg.list_of_all_items_in_table("surgeons")
        assert result == ["dr_smith", "dr_jones", "dr_okafor"]

    def test_demo_config_acquisition_sites(self):
        """list_of_all_items_in_table('ACQUISITION_SITES') returns demo sites."""
        cfg = demo.DemoConfig()
        result = cfg.list_of_all_items_in_table("ACQUISITION_SITES")
        assert result == [
            "UNIVERSITY_OF_IOWA_HOSPITALS_AND_CLINICS",
            "MERCY_HOSPITAL",
        ]

    def test_demo_config_acquisition_sites_lowercase(self):
        """Case-insensitive: 'acquisition_sites' works."""
        cfg = demo.DemoConfig()
        result = cfg.list_of_all_items_in_table("acquisition_sites")
        assert result == [
            "UNIVERSITY_OF_IOWA_HOSPITALS_AND_CLINICS",
            "MERCY_HOSPITAL",
        ]

    def test_demo_config_groups(self):
        """list_of_all_items_in_table('Groups') returns demo groups."""
        cfg = demo.DemoConfig()
        result = cfg.list_of_all_items_in_table("Groups")
        assert result == [
            "PEDIATRIC_SUPRACONDYLAR_HUMERUS_FRACTURE",
            "HIP_ARTHROSCOPY",
            "ANKLE_ORIF",
        ]

    def test_demo_config_groups_lowercase(self):
        """Case-insensitive: 'groups' works."""
        cfg = demo.DemoConfig()
        result = cfg.list_of_all_items_in_table("groups")
        assert result == [
            "PEDIATRIC_SUPRACONDYLAR_HUMERUS_FRACTURE",
            "HIP_ARTHROSCOPY",
            "ANKLE_ORIF",
        ]

    def test_demo_config_unknown_table(self):
        """Unknown table name returns empty list."""
        cfg = demo.DemoConfig()
        result = cfg.list_of_all_items_in_table("UnknownTable")
        assert result == []


class TestBuildDemoServer:
    """Test build_demo_server() construction and seeding."""

    def test_build_demo_server_returns_fake_xnat(self):
        """build_demo_server() returns a FakeXNAT instance."""
        server = demo.build_demo_server()
        assert server is not None
        assert hasattr(server, "get")
        assert callable(server.get)

    def test_build_demo_server_has_project_name(self):
        """Server has project_name == 'DEMO_PROJECT'."""
        server = demo.build_demo_server()
        assert server.project_name == "DEMO_PROJECT"

    def test_build_demo_server_get_does_not_raise(self):
        """server.get('/') (preflight) does not raise."""
        server = demo.build_demo_server()
        # Should not raise; FakeXNAT.get() is a no-op
        server.get("/")

    def test_build_demo_server_seeded_subjects(self):
        """Demo server has seeded subjects for browse."""
        server = demo.build_demo_server()
        # Should have 3 demo subjects
        assert len(server._subject_labels) == 3
        assert "DEMO_S0001" in server._subject_labels
        assert "DEMO_S0002" in server._subject_labels
        assert "DEMO_S0003" in server._subject_labels

    def test_build_demo_server_seeded_experiments(self):
        """Demo server has seeded experiments with xsi_type."""
        server = demo.build_demo_server()
        # Should have 3 demo experiments
        assert len(server._experiments) == 3
        for exp in server._experiments:
            assert "subject_label" in exp
            assert "experiment_label" in exp
            assert "xsi_type" in exp
            assert exp["xsi_type"] == "xnat:rfSessionData"

    def test_build_demo_server_seeded_resources(self):
        """Demo server can access seeded resources with files."""
        server = demo.build_demo_server()
        # Resources get registered when accessed via select().resource() with scan ID
        # Verify by accessing them and checking files are present
        resource1 = server.select("/project/DEMO_PROJECT/subject/DEMO_S0001/experiment/SOURCE_DATA-HIP_DEMO_001/scan/0").resource("SRC")
        assert resource1.list_files()
        assert len(resource1.list_files()) >= 1

        resource2 = server.select("/project/DEMO_PROJECT/subject/DEMO_S0002/experiment/SOURCE_DATA-HUMERUS_DEMO_001/scan/0").resource("SRC")
        assert resource2.list_files()
        assert len(resource2.list_files()) >= 1


class TestDemoConnectFactory:
    """Test demo_connect_factory()."""

    def test_demo_connect_factory_returns_server(self):
        """demo_connect_factory() returns a FakeXNAT regardless of args."""
        server = demo.demo_connect_factory(
            url="http://ignored.example.com",
            user="ignored_user",
            password="ignored_pass",
        )
        assert server is not None
        assert hasattr(server, "get")

    def test_demo_connect_factory_ignores_args(self):
        """Args are completely ignored; returns demo server."""
        server1 = demo.demo_connect_factory("url1", "user1", "pass1")
        server2 = demo.demo_connect_factory("url2", "user2", "pass2")
        # Both should be independent FakeXNAT instances (same structure, different objects)
        assert server1.project_name == server2.project_name == "DEMO_PROJECT"


class TestDemoLogin:
    """Test demo_login()."""

    def test_demo_login_returns_success(self):
        """demo_login() returns LoginResult with ok=True."""
        result = demo.demo_login()
        assert result.ok is True
        assert result.friendly is None
        assert result.username == "demo_user"
        assert result.server is not None

    def test_demo_login_server_is_fake_xnat(self):
        """Server returned by demo_login() is a FakeXNAT."""
        result = demo.demo_login()
        assert result.ok is True
        assert hasattr(result.server, "project_name")
        assert result.server.project_name == "DEMO_PROJECT"

    def test_demo_login_server_preflight_passes(self):
        """Server from demo_login() passes preflight checks."""
        result = demo.demo_login()
        assert result.ok is True
        # Preflight was run as part of attempt_login()
        # If we got here, it passed all checks (SSL, reachability, credentials)
        server = result.server
        # Can call get() without raising
        server.get("/")


class TestFetchDataTableAgainstDemoServer:
    """Integration: fetch_data_table() against demo server returns rows."""

    def test_fetch_data_table_demo_server_returns_rows(self):
        """fetch_data_table() on demo server returns list of rows."""
        server = demo.build_demo_server()
        result = fetch_data_table(server, "DEMO_PROJECT")
        # Should return a list, not an error
        assert isinstance(result, list)
        # Should have at least 3 rows (one per subject/experiment)
        assert len(result) >= 3

    def test_fetch_data_table_demo_server_row_structure(self):
        """Rows have expected columns."""
        server = demo.build_demo_server()
        result = fetch_data_table(server, "DEMO_PROJECT")
        assert isinstance(result, list)
        # Check first row has expected keys
        if result:
            row = result[0]
            assert "subject" in row
            assert "experiment" in row
            assert "date" in row
            assert "scan_type" in row
            assert "num_files" in row
            assert "scan_id" in row

    def test_fetch_data_table_demo_server_num_files_positive(self):
        """At least one row has num_files > 0 (files were seeded)."""
        server = demo.build_demo_server()
        result = fetch_data_table(server, "DEMO_PROJECT")
        assert isinstance(result, list)
        # At least one row should have files (we seeded them)
        num_files_list = [row.get("num_files", -1) for row in result]
        assert any(n > 0 for n in num_files_list), \
            f"Expected at least one row with num_files > 0, got: {num_files_list}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
