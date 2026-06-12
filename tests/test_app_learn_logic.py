"""
tests/test_app_learn_logic.py — Offline tests for app/logic/learn.py.

No streamlit import.  No network.  No PHI.  No credential embedding.
"""
from __future__ import annotations

import re
import sys

import pytest

# ---------------------------------------------------------------------------
# Guard: streamlit must NOT be importable in the test env
# (ARCHITECTURE RULE: app/logic/* never imports streamlit)
# ---------------------------------------------------------------------------

def test_learn_module_does_not_import_streamlit():
    """Importing app.logic.learn must not pull streamlit into sys.modules."""
    # Remove if somehow already loaded (shouldn't be)
    for key in list(sys.modules.keys()):
        if key == "streamlit" or key.startswith("streamlit."):
            del sys.modules[key]

    import app.logic.learn  # noqa: F401  (side-effect: registers submodules)
    assert "streamlit" not in sys.modules, (
        "app.logic.learn imported streamlit — violates architecture rule"
    )


# ---------------------------------------------------------------------------
# Imports (after guard)
# ---------------------------------------------------------------------------

from app.logic.learn import (  # noqa: E402
    cli_for_batch,
    cli_for_browse,
    cli_for_download,
    cli_for_upload,
    learn_snippets,
)

# ---------------------------------------------------------------------------
# Credential / PHI safety helpers
# ---------------------------------------------------------------------------

_CREDENTIAL_PATTERNS = [
    r"password",
    r"passwd",
    r"secret",
    r"token",
    r"api[_\-]?key",
    r"access[_\-]?key",
    r"auth[_\-]?header",
    r"bearer",
    r"--password",
    r"-p\s+\S",          # -p <value> style
]

_PHI_PLACEHOLDER_PATTERNS = [
    r"patient[_\-]?name",
    r"patient[_\-]?id",
    r"dob",
    r"date[_\-]?of[_\-]?birth",
    r"mrn",
    r"ssn",
    r"social[_\-]?security",
]


def _assert_no_credential(snippet: str) -> None:
    for pat in _CREDENTIAL_PATTERNS:
        assert not re.search(pat, snippet, re.IGNORECASE), (
            f"Snippet contains credential pattern '{pat}':\n{snippet}"
        )


def _assert_no_phi_placeholder(snippet: str) -> None:
    for pat in _PHI_PLACEHOLDER_PATTERNS:
        assert not re.search(pat, snippet, re.IGNORECASE), (
            f"Snippet contains PHI placeholder pattern '{pat}':\n{snippet}"
        )


# ---------------------------------------------------------------------------
# cli_for_upload
# ---------------------------------------------------------------------------

class TestCliForUpload:

    def test_contains_upload_subcommand(self):
        cmd = cli_for_upload({})
        assert "upload" in cmd

    def test_default_placeholders_present(self):
        cmd = cli_for_upload({})
        assert "<surgeon>" in cmd
        assert "<site>" in cmd
        assert "<procedure>" in cmd

    def test_custom_values_interpolated(self):
        cmd = cli_for_upload({
            "surgeon": "Dr_Smith",
            "site":    "Iowa_City",
            "procedure": "knee_arthroscopy",
            "image_dir": "/data/case001",
            "project": "KNEE_STUDY",
        })
        assert "Dr_Smith" in cmd
        assert "Iowa_City" in cmd
        assert "knee_arthroscopy" in cmd
        assert "/data/case001" in cmd
        assert "KNEE_STUDY" in cmd

    def test_no_credential_embedded(self):
        _assert_no_credential(cli_for_upload({
            "surgeon": "Dr_Smith", "project": "TEST"
        }))

    def test_no_phi_placeholder_embedded(self):
        _assert_no_phi_placeholder(cli_for_upload({}))

    def test_returns_string(self):
        assert isinstance(cli_for_upload({}), str)

    def test_non_empty(self):
        assert len(cli_for_upload({})) > 10


# ---------------------------------------------------------------------------
# cli_for_batch
# ---------------------------------------------------------------------------

class TestCliForBatch:

    def test_contains_batch_subcommand(self):
        cmd = cli_for_batch()
        assert "batch" in cmd.lower()

    def test_default_placeholder(self):
        cmd = cli_for_batch()
        assert "xlsx" in cmd

    def test_custom_path(self):
        cmd = cli_for_batch("/uploads/batch_jan.xlsx")
        assert "/uploads/batch_jan.xlsx" in cmd

    def test_continue_on_error_flag(self):
        cmd = cli_for_batch()
        assert "continue" in cmd.lower() or "error" in cmd.lower()

    def test_no_credential_embedded(self):
        _assert_no_credential(cli_for_batch())

    def test_no_phi_placeholder_embedded(self):
        _assert_no_phi_placeholder(cli_for_batch())

    def test_returns_string(self):
        assert isinstance(cli_for_batch(), str)


# ---------------------------------------------------------------------------
# cli_for_download
# ---------------------------------------------------------------------------

class TestCliForDownload:

    def test_contains_download_subcommand(self):
        cmd = cli_for_download()
        assert "download" in cmd.lower()

    def test_default_placeholders(self):
        cmd = cli_for_download()
        assert "subject" in cmd.lower()
        assert "dest" in cmd.lower() or "destination" in cmd.lower()

    def test_custom_selection(self):
        cmd = cli_for_download(
            selection={
                "subject": "SUBJ_001",
                "experiment": "EXP_001",
                "project": "KNEE_STUDY",
            },
            dest="/local/downloads",
        )
        assert "SUBJ_001" in cmd
        assert "EXP_001" in cmd
        assert "KNEE_STUDY" in cmd
        assert "/local/downloads" in cmd

    def test_no_credential_embedded(self):
        _assert_no_credential(cli_for_download())

    def test_no_phi_placeholder_embedded(self):
        _assert_no_phi_placeholder(cli_for_download())

    def test_returns_string(self):
        assert isinstance(cli_for_download(), str)

    def test_none_selection_is_safe(self):
        cmd = cli_for_download(selection=None)
        assert isinstance(cmd, str)
        assert len(cmd) > 0


# ---------------------------------------------------------------------------
# cli_for_browse
# ---------------------------------------------------------------------------

class TestCliForBrowse:

    def test_contains_list_or_browse(self):
        cmd = cli_for_browse()
        assert "list" in cmd.lower() or "browse" in cmd.lower()

    def test_custom_project(self):
        cmd = cli_for_browse(project="MY_PROJECT")
        assert "MY_PROJECT" in cmd

    def test_no_credential_embedded(self):
        _assert_no_credential(cli_for_browse())

    def test_no_phi_placeholder_embedded(self):
        _assert_no_phi_placeholder(cli_for_browse())

    def test_returns_string(self):
        assert isinstance(cli_for_browse(), str)


# ---------------------------------------------------------------------------
# learn_snippets index
# ---------------------------------------------------------------------------

class TestLearnSnippets:

    _REQUIRED_KEYS = {"browse", "upload", "batch_upload", "download"}

    def test_returns_dict(self):
        result = learn_snippets()
        assert isinstance(result, dict)

    def test_required_keys_present(self):
        result = learn_snippets()
        missing = self._REQUIRED_KEYS - result.keys()
        assert not missing, f"learn_snippets() missing keys: {missing}"

    def test_each_entry_has_label_command_description(self):
        for key, entry in learn_snippets().items():
            assert "label" in entry,       f"Key '{key}' missing 'label'"
            assert "command" in entry,     f"Key '{key}' missing 'command'"
            assert "description" in entry, f"Key '{key}' missing 'description'"

    def test_all_commands_are_non_empty_strings(self):
        for key, entry in learn_snippets().items():
            cmd = entry["command"]
            assert isinstance(cmd, str), f"Key '{key}' command is not a string"
            assert len(cmd.strip()) > 0,  f"Key '{key}' command is empty"

    def test_no_credentials_in_any_snippet(self):
        for key, entry in learn_snippets().items():
            _assert_no_credential(entry["command"])

    def test_no_phi_in_any_snippet(self):
        for key, entry in learn_snippets().items():
            _assert_no_phi_placeholder(entry["command"])

    def test_all_labels_are_non_empty_strings(self):
        for key, entry in learn_snippets().items():
            assert isinstance(entry["label"], str)
            assert len(entry["label"].strip()) > 0

    def test_all_descriptions_are_non_empty_strings(self):
        for key, entry in learn_snippets().items():
            assert isinstance(entry["description"], str)
            assert len(entry["description"].strip()) > 0

    def test_no_exec_call_in_module_source(self):
        """Extra safety: learn.py must not import subprocess or call os.exec/eval."""
        import inspect
        import app.logic.learn as _learn_mod
        src = inspect.getsource(_learn_mod)
        # Check for actual import/call patterns, not docstring mentions
        import_or_call_patterns = [
            r"^import subprocess",
            r"^from subprocess",
            r"subprocess\.",
            r"os\.exec\w*\(",      # actual call e.g. os.execvp(
            r"os\.system\s*\(",
            r"\beval\s*\(",
            r"\bexec\s*\(",
        ]
        for pat in import_or_call_patterns:
            assert not re.search(pat, src, re.MULTILINE), (
                f"app/logic/learn.py contains forbidden pattern '{pat}'"
            )
