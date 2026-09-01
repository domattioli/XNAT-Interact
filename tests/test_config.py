"""
tests/test_config.py — Offline tests for src.services.config.AppConfig

Rules verified:
  1. Env vars (XNAT_SERVER_URL, XNAT_PROJECT_NAME) take priority over everything.
  2. Config file overrides defaults when env vars absent.
  3. Defaults equal the former hardcoded literals (unchanged behaviour
     when no env / file present).
  4. No credential field exists on AppConfig.
  5. Switching server/project requires NO code edit — only env or file.
"""
from __future__ import annotations

import json
import pytest
from pathlib import Path

from src.services.config import (
    AppConfig,
    _DEFAULT_SERVER_URL,
    _DEFAULT_PROJECT_NAME,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure both env vars are absent so tests start from a clean slate."""
    monkeypatch.delenv("XNAT_SERVER_URL",   raising=False)
    monkeypatch.delenv("XNAT_PROJECT_NAME", raising=False)


# ---------------------------------------------------------------------------
# 1. Defaults equal prior hardcoded values
# ---------------------------------------------------------------------------

class TestDefaults:
    def test_default_server_url(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clean_env(monkeypatch)
        cfg = AppConfig.load()
        assert cfg.server_url == "https://rpacs.iibi.uiowa.edu/xnat/"

    def test_default_project_name(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clean_env(monkeypatch)
        cfg = AppConfig.load()
        assert cfg.project_name == "GROK_AHRQ_Data"

    def test_defaults_match_module_constants(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clean_env(monkeypatch)
        cfg = AppConfig.load()
        assert cfg.server_url   == _DEFAULT_SERVER_URL
        assert cfg.project_name == _DEFAULT_PROJECT_NAME


# ---------------------------------------------------------------------------
# 2. Env vars override defaults
# ---------------------------------------------------------------------------

class TestEnvVarPriority:
    def test_env_server_url_overrides_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clean_env(monkeypatch)
        monkeypatch.setenv("XNAT_SERVER_URL", "https://env.example.com/xnat/")
        cfg = AppConfig.load()
        assert cfg.server_url == "https://env.example.com/xnat/"
        # project stays at default
        assert cfg.project_name == _DEFAULT_PROJECT_NAME

    def test_env_project_name_overrides_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clean_env(monkeypatch)
        monkeypatch.setenv("XNAT_PROJECT_NAME", "MY_TEST_PROJECT")
        cfg = AppConfig.load()
        assert cfg.project_name == "MY_TEST_PROJECT"
        assert cfg.server_url   == _DEFAULT_SERVER_URL

    def test_both_env_vars_override_defaults(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clean_env(monkeypatch)
        monkeypatch.setenv("XNAT_SERVER_URL",   "https://a.example.com/xnat/")
        monkeypatch.setenv("XNAT_PROJECT_NAME", "PROJECT_A")
        cfg = AppConfig.load()
        assert cfg.server_url   == "https://a.example.com/xnat/"
        assert cfg.project_name == "PROJECT_A"


# ---------------------------------------------------------------------------
# 3. Config file overrides defaults; env still beats file
# ---------------------------------------------------------------------------

class TestConfigFilePriority:
    def test_file_overrides_defaults(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        _clean_env(monkeypatch)
        cfg_file = tmp_path / "xnat_config.json"
        cfg_file.write_text(
            json.dumps({
                "server_url":   "https://file.example.com/xnat/",
                "project_name": "FILE_PROJECT",
            }),
            encoding="utf-8",
        )
        cfg = AppConfig.load(path=str(cfg_file))
        assert cfg.server_url   == "https://file.example.com/xnat/"
        assert cfg.project_name == "FILE_PROJECT"

    def test_env_beats_file(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        _clean_env(monkeypatch)
        monkeypatch.setenv("XNAT_SERVER_URL", "https://env-wins.example.com/xnat/")
        cfg_file = tmp_path / "xnat_config.json"
        cfg_file.write_text(
            json.dumps({
                "server_url":   "https://file.example.com/xnat/",
                "project_name": "FILE_PROJECT",
            }),
            encoding="utf-8",
        )
        cfg = AppConfig.load(path=str(cfg_file))
        # env wins for server_url
        assert cfg.server_url   == "https://env-wins.example.com/xnat/"
        # file supplies project_name (env not set)
        assert cfg.project_name == "FILE_PROJECT"

    def test_partial_file_falls_back_for_missing_keys(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        _clean_env(monkeypatch)
        cfg_file = tmp_path / "xnat_config.json"
        cfg_file.write_text(
            json.dumps({"server_url": "https://partial.example.com/xnat/"}),
            encoding="utf-8",
        )
        cfg = AppConfig.load(path=str(cfg_file))
        assert cfg.server_url   == "https://partial.example.com/xnat/"
        assert cfg.project_name == _DEFAULT_PROJECT_NAME  # not in file → default

    def test_malformed_file_falls_back_to_defaults(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        _clean_env(monkeypatch)
        bad_file = tmp_path / "xnat_config.json"
        bad_file.write_text("not valid json {{{", encoding="utf-8")
        cfg = AppConfig.load(path=str(bad_file))
        assert cfg.server_url   == _DEFAULT_SERVER_URL
        assert cfg.project_name == _DEFAULT_PROJECT_NAME


# ---------------------------------------------------------------------------
# 4. No credential fields on AppConfig
# ---------------------------------------------------------------------------

class TestNoCredentials:
    _FORBIDDEN_ATTRS = ("password", "username", "token", "secret", "api_key", "credential")

    def test_no_credential_attributes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clean_env(monkeypatch)
        cfg = AppConfig.load()
        for attr in self._FORBIDDEN_ATTRS:
            assert not hasattr(cfg, attr), (
                f"AppConfig must not have a '{attr}' attribute — credentials must never be persisted."
            )

    def test_only_expected_fields_present(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clean_env(monkeypatch)
        cfg = AppConfig.load()
        # dataclass fields should be exactly server_url and project_name
        import dataclasses
        field_names = {f.name for f in dataclasses.fields(cfg)}
        assert field_names == {"server_url", "project_name"}


# ---------------------------------------------------------------------------
# 5. Switching server/project needs NO code edit
# ---------------------------------------------------------------------------

class TestRuntimeSwitching:
    """Confirm config values can be changed purely at runtime (env/file),
    without modifying source code."""

    def test_switch_via_env_no_code_change(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clean_env(monkeypatch)
        monkeypatch.setenv("XNAT_SERVER_URL",   "https://new-server.example.com/xnat/")
        monkeypatch.setenv("XNAT_PROJECT_NAME", "NEW_PROJECT")
        cfg = AppConfig.load()
        assert cfg.server_url   == "https://new-server.example.com/xnat/"
        assert cfg.project_name == "NEW_PROJECT"

    def test_load_returns_appconfig_instance(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clean_env(monkeypatch)
        cfg = AppConfig.load()
        assert isinstance(cfg, AppConfig)

    def test_load_with_explicit_path_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clean_env(monkeypatch)
        cfg = AppConfig.load(path=None)
        assert isinstance(cfg, AppConfig)
