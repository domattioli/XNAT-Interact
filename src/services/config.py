"""
AppConfig — loads server URL and project name from:
  1. Environment variables  (XNAT_SERVER_URL, XNAT_PROJECT_NAME)
  2. Config file            (xnat_config.json or .xnat-interact.json in cwd or home)
  3. Hardcoded defaults     (same values as the prior literals in utilities.py / main.py)

CREDENTIALS POLICY (HARD RULE)
-------------------------------
Credentials (username, password) are NEVER stored in this config.
Use interactive prompts (pwinput / getpass) at runtime only.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence

# ---------------------------------------------------------------------------
# Defaults — identical to the former hardcoded literals so behaviour is
# unchanged when no env var or config file is present.
# ---------------------------------------------------------------------------
_DEFAULT_SERVER_URL  = "https://rpacs.iibi.uiowa.edu/xnat/"
_DEFAULT_PROJECT_NAME = "GROK_AHRQ_Data"

# Config-file names, searched in order (cwd then home).
_CONFIG_FILENAMES: Sequence[str] = ("xnat_config.json", ".xnat-interact.json")


# ---------------------------------------------------------------------------
# AppConfig
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class AppConfig:
    """
    Immutable application configuration (server URL + project name only).

    Fields
    ------
    server_url   : Full XNAT server URL, e.g. https://rpacs.iibi.uiowa.edu/xnat/
    project_name : XNAT project identifier, e.g. GROK_AHRQ_Data

    NOTE: No credential fields exist here by design.  Credentials are
    prompted interactively at runtime and must never be persisted.
    """
    server_url:   str
    project_name: str

    # Intentionally no `password`, `username`, `token`, or similar fields.

    @classmethod
    def load(cls, path: Optional[str] = None) -> "AppConfig":
        """
        Build an AppConfig using priority order:
          env vars > config file > defaults.

        Parameters
        ----------
        path : Optional path to a specific JSON config file.  When None the
               standard search order (cwd / home) is used.

        Returns
        -------
        AppConfig with .server_url and .project_name resolved.
        """
        # --- Step 1: env vars (highest priority) ---
        server_url   = os.environ.get("XNAT_SERVER_URL")
        project_name = os.environ.get("XNAT_PROJECT_NAME")

        # --- Step 2: config file (if either value still missing) ---
        if server_url is None or project_name is None:
            cfg = _load_config_file(path)
            if cfg:
                if server_url is None:
                    server_url = cfg.get("server_url")
                if project_name is None:
                    project_name = cfg.get("project_name")

        # --- Step 3: fall back to hardcoded defaults ---
        if server_url is None:
            server_url = _DEFAULT_SERVER_URL
        if project_name is None:
            project_name = _DEFAULT_PROJECT_NAME

        return cls(server_url=server_url, project_name=project_name)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _candidate_config_paths(explicit: Optional[str]) -> list[Path]:
    """Return candidate paths to check, in priority order."""
    if explicit is not None:
        return [Path(explicit)]
    candidates: list[Path] = []
    for name in _CONFIG_FILENAMES:
        candidates.append(Path.cwd() / name)
        candidates.append(Path.home() / name)
    return candidates


def _load_config_file(explicit: Optional[str]) -> Optional[dict]:
    """Return parsed JSON from the first readable candidate, or None."""
    for p in _candidate_config_paths(explicit):
        if p.is_file():
            try:
                with p.open(encoding="utf-8") as fh:
                    data = json.load(fh)
                if isinstance(data, dict):
                    return data
            except (json.JSONDecodeError, OSError):
                pass
    return None
