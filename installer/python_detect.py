"""
installer.python_detect — injectable Python interpreter detection.

Design
------
All environment probing (PATH scan, Windows registry, common install locations,
version check, venv-capability) is behind INJECTABLE providers/callables so
tests can feed fake inputs offline — no real subprocess, no real registry.

Public API
----------
detect_python(
    min_version=(3, 9),
    *,
    candidates_provider=None,   # () -> Iterable[str]   — list of exe paths to try
    version_probe=None,         # (path: str) -> tuple[int,...]|None
    venv_probe=None,            # (path: str) -> bool
) -> DetectResult

DetectResult(found, path, version, reason)
"""
from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass
from typing import Callable, Iterable, Optional, Tuple

# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DetectResult:
    """
    Outcome of detect_python().

    found   : True if a compatible interpreter was located.
    path    : Absolute path to the chosen interpreter, or None.
    version : Parsed version tuple, e.g. (3, 11, 2), or None.
    reason  : Human-readable explanation (never contains PHI).
    """
    found: bool
    path: Optional[str]
    version: Optional[Tuple[int, ...]]
    reason: str


# ---------------------------------------------------------------------------
# Default provider implementations
# (thin — real OS calls, only invoked by production code, never in tests)
# ---------------------------------------------------------------------------

def _default_candidates_provider() -> Iterable[str]:
    """
    Enumerate candidate interpreter paths from:
      - PATH entries (python3, python, python3.X)
      - Common install locations (Windows: Program Files, AppData; macOS: /usr/local, /opt)
      - Windows registry HKCU/HKLM Python core keys
    """
    seen: list[str] = []

    # --- PATH scan ---
    path_dirs = os.environ.get("PATH", "").split(os.pathsep)
    names = ["python3", "python", "python3.13", "python3.12", "python3.11",
             "python3.10", "python3.9"]
    for d in path_dirs:
        for name in names:
            exe = os.path.join(d, name)
            if sys.platform == "win32":
                exe = os.path.join(d, name + ".exe")
            if exe not in seen:
                seen.append(exe)

    # --- Common non-PATH locations (Windows) ---
    if sys.platform == "win32":
        import os.path
        local_app = os.environ.get("LOCALAPPDATA", "")
        program_files = os.environ.get("ProgramFiles", r"C:\Program Files")
        pf_x86 = os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)")
        for base in [local_app, program_files, pf_x86]:
            for ver in ["3.13", "3.12", "3.11", "3.10", "3.9"]:
                for sub in [
                    os.path.join("Python", f"Python{ver.replace('.','')}", "python.exe"),
                    os.path.join(f"Python{ver.replace('.','')}", "python.exe"),
                ]:
                    candidate = os.path.join(base, sub)
                    if candidate not in seen:
                        seen.append(candidate)

        # Windows registry
        try:
            import winreg  # type: ignore[import]
            for hive in (winreg.HKEY_CURRENT_USER, winreg.HKEY_LOCAL_MACHINE):
                for reg_path in (
                    r"SOFTWARE\Python\PythonCore",
                    r"SOFTWARE\WOW6432Node\Python\PythonCore",
                ):
                    try:
                        with winreg.OpenKey(hive, reg_path) as key:
                            i = 0
                            while True:
                                try:
                                    ver_name = winreg.EnumKey(key, i)
                                    try:
                                        with winreg.OpenKey(
                                            key, rf"{ver_name}\InstallPath"
                                        ) as ip_key:
                                            install_dir, _ = winreg.QueryValueEx(
                                                ip_key, "ExecutablePath"
                                            )
                                            if install_dir and install_dir not in seen:
                                                seen.append(install_dir)
                                    except OSError:
                                        pass
                                    i += 1
                                except OSError:
                                    break
                    except OSError:
                        pass
        except ImportError:
            pass  # not Windows or winreg unavailable

    # --- Common non-PATH locations (macOS / Linux) ---
    if sys.platform != "win32":
        for base in ["/usr/local/bin", "/opt/homebrew/bin", "/usr/bin", "/opt/local/bin"]:
            for name in ["python3", "python3.13", "python3.12", "python3.11",
                         "python3.10", "python3.9"]:
                candidate = os.path.join(base, name)
                if candidate not in seen:
                    seen.append(candidate)

    return seen


def _default_version_probe(path: str) -> Optional[Tuple[int, ...]]:
    """
    Run ``<path> -c "import sys; print(sys.version_info[:3])"`` and parse.
    Returns tuple or None on any failure.
    """
    try:
        result = subprocess.run(
            [path, "-c", "import sys; print(sys.version_info[:3])"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode != 0:
            return None
        # Output like "(3, 11, 2)"
        raw = result.stdout.strip()
        nums = [int(x) for x in raw.strip("()").split(",") if x.strip().isdigit()]
        return tuple(nums) if len(nums) >= 2 else None
    except Exception:
        return None


def _default_venv_probe(path: str) -> bool:
    """
    Verify the interpreter can create a venv (venv module importable).
    Returns False on any error.
    """
    try:
        result = subprocess.run(
            [path, "-c", "import venv; print('ok')"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return result.returncode == 0 and "ok" in result.stdout
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Core detection logic (pure — all env access via injected callables)
# ---------------------------------------------------------------------------

def detect_python(
    min_version: Tuple[int, int] = (3, 9),
    *,
    candidates_provider: Optional[Callable[[], Iterable[str]]] = None,
    version_probe: Optional[Callable[[str], Optional[Tuple[int, ...]]]] = None,
    venv_probe: Optional[Callable[[str], bool]] = None,
) -> DetectResult:
    """
    Find the first compatible Python interpreter.

    Parameters
    ----------
    min_version
        Minimum acceptable (major, minor).  Default (3, 9).
    candidates_provider
        Zero-arg callable returning an iterable of exe paths to evaluate.
        Defaults to the real PATH + registry + common-locations scanner.
        Inject a fake for tests.
    version_probe
        (path) -> version tuple or None.
        Defaults to subprocess ``sys.version_info``.
        Inject a fake for tests.
    venv_probe
        (path) -> bool — True if the interpreter can create a venv.
        Defaults to subprocess import-venv check.
        Inject a fake for tests.

    Returns
    -------
    DetectResult
        found=True  + path + version if a compatible interpreter was found.
        found=False + reason if none found or all rejected.
    """
    _candidates = candidates_provider if candidates_provider is not None else _default_candidates_provider
    _version = version_probe if version_probe is not None else _default_version_probe
    _venv = venv_probe if venv_probe is not None else _default_venv_probe

    candidates: Iterable[str] = _candidates()

    rejected: list[str] = []

    for path in candidates:
        # Skip obviously missing paths (real provider may emit non-existent paths;
        # fake provider paths are always "present" so this check is skipped for them
        # unless the fake explicitly yields non-existent real paths).
        if not path:
            continue

        version = _version(path)

        if version is None:
            rejected.append(f"{path!r}: version probe failed (not executable or crashed)")
            continue

        # Minimum version gate
        if version[:2] < min_version:
            rejected.append(
                f"{path!r}: version {version} < minimum {min_version}"
            )
            continue

        # Venv/import capability gate
        if not _venv(path):
            rejected.append(f"{path!r}: venv probe failed (cannot create venv or import deps)")
            continue

        # All checks passed — first winner
        return DetectResult(
            found=True,
            path=path,
            version=version,
            reason=f"Compatible interpreter found at {path!r} (version {version}).",
        )

    # Nothing passed
    if rejected:
        summary = "; ".join(rejected[:5])  # cap to avoid huge messages
        reason = f"No compatible Python >= {min_version} found. Checked candidates: {summary}"
    else:
        reason = (
            f"No Python interpreter candidates found on PATH, registry, or common locations. "
            f"Minimum required: {min_version}."
        )

    return DetectResult(found=False, path=None, version=None, reason=reason)
