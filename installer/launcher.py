"""
installer.launcher — runtime selection and launch orchestration.

All subprocess / venv / app execution is behind INJECTABLE callables so tests
run fully offline — no real exec in tests.

Public API
----------
RuntimeChoice(use_bundled, python_path, reason)
LaunchPlan(python_path, venv_dir, steps, error)

choose_runtime(detect_result, bundled_runtime_path) -> RuntimeChoice
    Pure.  Selects system Python or bundled runtime.

prepare_and_launch(
    runtime_choice,
    app_entry,
    venv_dir,
    *,
    venv_setup=None,    # (python_path, venv_dir) -> None
    app_runner=None,    # (python_path, app_entry) -> None
) -> LaunchPlan
    Orchestrates venv create + app run via injected callables.
    Fail-soft: returns LaunchPlan with error set rather than raising.
"""
from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass, field
from typing import Callable, List, Optional

from installer.python_detect import DetectResult


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RuntimeChoice:
    """
    Which Python runtime to use.

    use_bundled  : True  → use the bundled runtime at bundled_runtime_path.
                   False → use an existing system interpreter.
    python_path  : Absolute path to the chosen interpreter (system or bundled).
                   None if neither is available.
    reason       : PHI-free human-readable explanation for the launch log.
    """
    use_bundled: bool
    python_path: Optional[str]
    reason: str


@dataclass
class LaunchPlan:
    """
    Description of the steps taken (or attempted) during launch.

    python_path  : Interpreter used (system or bundled).
    venv_dir     : Venv directory created (may be None on failure or skip).
    steps        : Ordered list of step descriptions executed successfully.
    error        : None on success; plain-language message when something failed.
                   Never a raw traceback (Constitution II).
    """
    python_path: Optional[str]
    venv_dir: Optional[str]
    steps: List[str] = field(default_factory=list)
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# Default real implementations (never called from tests)
# ---------------------------------------------------------------------------


def _default_venv_setup(python_path: str, venv_dir: str) -> None:
    """Create a venv at venv_dir using python_path."""
    result = subprocess.run(
        [python_path, "-m", "venv", venv_dir],
        capture_output=True,
        text=True,
        timeout=60,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"venv creation failed (exit {result.returncode}): {result.stderr.strip()}"
        )


def _default_app_runner(python_path: str, app_entry: str) -> None:
    """
    Launch the app via python_path.
    Replaces the current process (os.execv) so this is fire-and-forget.
    """
    os.execv(python_path, [python_path, app_entry])


# ---------------------------------------------------------------------------
# choose_runtime — pure logic, no IO
# ---------------------------------------------------------------------------


def choose_runtime(
    detect_result: DetectResult,
    bundled_runtime_path: Optional[str],
) -> RuntimeChoice:
    """
    Decide which Python runtime to use.

    Logic:
      1. If detect_result.found → use the detected system interpreter.
      2. Else if bundled_runtime_path is provided and non-empty → use bundled.
      3. Else → neither available, return RuntimeChoice with python_path=None.

    Parameters
    ----------
    detect_result
        Output of detect_python().
    bundled_runtime_path
        Path to the bundled Python interpreter shipped with the package, or
        None/empty if not present.

    Returns
    -------
    RuntimeChoice
    """
    if detect_result.found and detect_result.path:
        return RuntimeChoice(
            use_bundled=False,
            python_path=detect_result.path,
            reason=(
                f"Reusing existing system Python at {detect_result.path!r} "
                f"(version {detect_result.version}). Bundled runtime not invoked."
            ),
        )

    if bundled_runtime_path:
        return RuntimeChoice(
            use_bundled=True,
            python_path=bundled_runtime_path,
            reason=(
                f"No compatible system Python found ({detect_result.reason}). "
                f"Using bundled runtime at {bundled_runtime_path!r}."
            ),
        )

    # Neither available
    return RuntimeChoice(
        use_bundled=False,
        python_path=None,
        reason=(
            "No compatible Python found and no bundled runtime available. "
            "Installation may be incomplete."
        ),
    )


# ---------------------------------------------------------------------------
# prepare_and_launch — orchestration, injectable callables
# ---------------------------------------------------------------------------


def prepare_and_launch(
    runtime_choice: RuntimeChoice,
    app_entry: str,
    venv_dir: str,
    *,
    venv_setup: Optional[Callable[[str, str], None]] = None,
    app_runner: Optional[Callable[[str, str], None]] = None,
) -> LaunchPlan:
    """
    Orchestrate venv creation and app launch using the chosen runtime.

    All real subprocess/exec calls happen through the injected callables.
    Tests supply spies/fakes; production code uses the real defaults.

    Parameters
    ----------
    runtime_choice
        Output of choose_runtime().
    app_entry
        Path to the app entry-point script (e.g. ``app/main.py``).
    venv_dir
        Directory where the launch venv should be created.
    venv_setup
        (python_path, venv_dir) -> None.  Raises on failure.
        Default: real subprocess venv creation.
    app_runner
        (python_path, app_entry) -> None.  Called after venv setup.
        Default: os.execv replacement.

    Returns
    -------
    LaunchPlan
        .error is None on full success; plain-language message on any failure.
        Never raises.
    """
    _venv_setup = venv_setup if venv_setup is not None else _default_venv_setup
    _app_runner = app_runner if app_runner is not None else _default_app_runner

    plan = LaunchPlan(python_path=runtime_choice.python_path, venv_dir=None)

    # Fail-soft: no runtime available
    if not runtime_choice.python_path:
        plan.error = (
            "Cannot launch: no compatible Python interpreter found and no bundled "
            "runtime is available. Please reinstall from ITS Software Center or "
            "contact your Data Librarian."
        )
        return plan

    # Step 1 — venv setup
    try:
        _venv_setup(runtime_choice.python_path, venv_dir)
        plan.venv_dir = venv_dir
        plan.steps.append(f"Created venv at {venv_dir!r} using {runtime_choice.python_path!r}.")
    except Exception as exc:  # noqa: BLE001
        plan.error = (
            f"Failed to create the application environment: {exc}. "
            "The installation may be incomplete. Please reinstall from ITS Software "
            "Center or contact your Data Librarian."
        )
        return plan

    # Step 2 — run app
    try:
        _app_runner(runtime_choice.python_path, app_entry)
        plan.steps.append(f"Launched app entry-point {app_entry!r}.")
    except Exception as exc:  # noqa: BLE001
        plan.error = (
            f"Failed to start the application: {exc}. "
            "If this persists, reinstall from ITS Software Center or contact your "
            "Data Librarian."
        )
        return plan

    return plan
