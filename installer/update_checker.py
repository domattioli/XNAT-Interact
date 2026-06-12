"""
installer.update_checker — injectable, fail-soft version comparison.

Design
------
Network access is behind an INJECTABLE ``fetch_latest`` callable so tests
run fully offline — no real HTTP calls.

``check_for_update`` reads the running version from ``installer/VERSION`` by
default (path injectable for tests), compares against the result of
``fetch_latest()``, and returns an ``UpdateInfo``.  Network failures → error
field set, ``available=False``, never raises.

Public API
----------
UpdateInfo(available, latest, url, error)

check_for_update(
    local_version: str | None = None,
    *,
    fetch_latest,            # () -> dict{"version": str, "url": str} | raises
    version_file: str | None = None,  # override path to VERSION file
) -> UpdateInfo
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Callable, Optional

# ---------------------------------------------------------------------------
# VERSION file helper
# ---------------------------------------------------------------------------

# Default location: <this file's directory>/VERSION
_DEFAULT_VERSION_FILE = os.path.join(os.path.dirname(__file__), "VERSION")


def _read_version_file(path: str) -> str:
    """Read and strip the VERSION file.  Returns '0.0.0' on any read error."""
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return fh.read().strip()
    except OSError:
        return "0.0.0"


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class UpdateInfo:
    """
    Result of check_for_update().

    available : True if a newer version was found.
    latest    : The latest version string, or None on error / not-checked.
    url       : Download URL for the new version, or None.
    error     : None on clean success; plain-language message on network
                failure or parse error.  Never a traceback.
    """
    available: bool
    latest: Optional[str]
    url: Optional[str]
    error: Optional[str]


# ---------------------------------------------------------------------------
# Semver-ish comparison
# ---------------------------------------------------------------------------

_VERSION_RE = re.compile(r"(\d+)(?:\.(\d+)(?:\.(\d+))?)?")


def _parse_version(v: str) -> tuple[int, ...]:
    """
    Parse a semver-ish version string into a comparable tuple.

    Examples
    --------
    "1.2.3"  -> (1, 2, 3)
    "2.0"    -> (2, 0, 0)
    "3"      -> (3, 0, 0)

    Returns (0, 0, 0) on any parse failure.
    """
    m = _VERSION_RE.match(v.strip())
    if not m:
        return (0, 0, 0)
    parts = [int(g) if g is not None else 0 for g in m.groups()]
    return tuple(parts)  # type: ignore[return-value]


def _is_newer(remote: str, local: str) -> bool:
    """True if remote version is strictly greater than local."""
    return _parse_version(remote) > _parse_version(local)


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------


def check_for_update(
    local_version: Optional[str] = None,
    *,
    fetch_latest: Callable[[], dict],
    version_file: Optional[str] = None,
) -> UpdateInfo:
    """
    Compare the running version to the latest published version.

    Parameters
    ----------
    local_version
        The running version string (e.g. ``"0.1.0"``).  If None, read from
        the VERSION file at ``version_file`` (or the default installer/VERSION).
    fetch_latest
        Zero-arg callable returning ``{"version": str, "url": str}``.
        Inject a fake for tests; the real implementation might fetch a GitHub
        releases endpoint or a plain VERSION file over HTTPS.
        Any exception raised by this callable is caught and reported as
        ``error``; ``available`` is False.
    version_file
        Override the path to the VERSION file (for tests).

    Returns
    -------
    UpdateInfo
        ``available=True``  when a newer version exists.
        ``available=False`` otherwise (up-to-date, parse error, or network failure).
        ``error``           set to a plain-language message on any failure.
        Never raises.
    """
    # Resolve local version
    if local_version is None:
        vf = version_file if version_file is not None else _DEFAULT_VERSION_FILE
        local_version = _read_version_file(vf)

    # Fetch remote info (fail-soft)
    try:
        remote_info = fetch_latest()
    except Exception as exc:  # noqa: BLE001
        return UpdateInfo(
            available=False,
            latest=None,
            url=None,
            error=(
                f"Update check failed (network may be unavailable): {exc}. "
                "You can check for updates manually by visiting the project page."
            ),
        )

    # Parse remote response
    try:
        remote_version = str(remote_info["version"])
        remote_url = str(remote_info.get("url", "")) or None
    except (KeyError, TypeError, ValueError) as exc:
        return UpdateInfo(
            available=False,
            latest=None,
            url=None,
            error=f"Could not parse update information: {exc}.",
        )

    available = _is_newer(remote_version, local_version)
    return UpdateInfo(
        available=available,
        latest=remote_version,
        url=remote_url if available else None,
        error=None,
    )
