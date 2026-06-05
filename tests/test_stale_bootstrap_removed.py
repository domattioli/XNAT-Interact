"""
tests/test_stale_bootstrap_removed.py — T018 smoke test (#30).

src/initialize_basic_metatable_items.py was removed because:
  1. It contained `from Utilities import MetaTables` (stale import; module
     no longer exists and MetaTables was renamed to ConfigTables).
  2. ConfigTables.__init__ already calls _initialize_tables() on first-run,
     so the standalone script was fully redundant.

This test confirms the file is absent (removal successful) and that the
self-initialization path in ConfigTables is the canonical replacement.

RULES: NO network, NO PHI, NO real XNAT server.
"""
from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


# ---------------------------------------------------------------------------
# T018 smoke: stale script removed
# ---------------------------------------------------------------------------

class TestStaleBootstrapRemoved:

    def test_stale_script_file_does_not_exist(self) -> None:
        """
        T018: src/initialize_basic_metatable_items.py is gone.

        The file contained `from Utilities import MetaTables` which would fail
        on import.  It was fully superseded by ConfigTables._initialize_tables().
        """
        stale_path = REPO_ROOT / "src" / "initialize_basic_metatable_items.py"
        assert not stale_path.exists(), (
            f"Stale script still present at {stale_path}. "
            "Expected it to be removed (T018 — ConfigTables self-initializes)."
        )

    def test_stale_module_not_importable(self) -> None:
        """
        T018: the stale module cannot be imported (file removed).
        """
        with pytest.raises((ImportError, ModuleNotFoundError)):
            importlib.import_module("src.initialize_basic_metatable_items")

    def test_configtables_has_initialize_tables_method(self) -> None:
        """
        Replacement check: ConfigTables still has _initialize_tables() which
        performs the self-initialization the stale script previously did.
        """
        from src.utilities import ConfigTables
        assert hasattr(ConfigTables, "_initialize_tables") and callable(
            ConfigTables._initialize_tables
        ), (
            "ConfigTables._initialize_tables() missing — "
            "self-initialization path broken."
        )
