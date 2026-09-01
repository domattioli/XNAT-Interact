"""
tests/test_packaging_launcher.py
---------------------------------
Offline unit tests for installer.launcher.

Covers:
  - choose_runtime: detect-found → system Python; detect-not-found → bundled;
    neither available → python_path=None.
  - prepare_and_launch: calls injected venv_setup + app_runner in order
    (spy assertions); no real subprocess.
  - Fail-soft: no-runtime → friendly message in plan.error.
  - Fail-soft: venv_setup raises → friendly error, app_runner NOT called.
  - Fail-soft: app_runner raises → friendly error.
"""
from __future__ import annotations


from installer.python_detect import DetectResult
from installer.launcher import (
    LaunchPlan,
    RuntimeChoice,
    choose_runtime,
    prepare_and_launch,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _found_result(path: str = "/usr/bin/python3.11", version=(3, 11, 2)) -> DetectResult:
    return DetectResult(found=True, path=path, version=version, reason="ok")


def _not_found_result(reason: str = "none on PATH") -> DetectResult:
    return DetectResult(found=False, path=None, version=None, reason=reason)


def _noop_venv(python_path: str, venv_dir: str) -> None:
    """Spy-friendly no-op venv setup."""


def _noop_runner(python_path: str, app_entry: str) -> None:
    """Spy-friendly no-op app runner."""


# ---------------------------------------------------------------------------
# choose_runtime tests
# ---------------------------------------------------------------------------

class TestChooseRuntime:
    def test_detect_found_uses_system_python(self):
        result = _found_result("/usr/bin/python3.11")
        choice = choose_runtime(result, bundled_runtime_path="/bundle/python")
        assert choice.use_bundled is False
        assert choice.python_path == "/usr/bin/python3.11"

    def test_detect_found_bundled_not_invoked(self):
        """When system Python found, bundled path irrelevant."""
        choice = choose_runtime(_found_result(), "/bundle/python")
        assert choice.use_bundled is False
        # reason should mention system / reuse, not bundled invoked
        assert "bundled runtime not invoked" in choice.reason.lower()

    def test_detect_not_found_uses_bundled(self):
        choice = choose_runtime(_not_found_result(), "/bundle/python3.11")
        assert choice.use_bundled is True
        assert choice.python_path == "/bundle/python3.11"

    def test_detect_not_found_no_bundled_gives_none_path(self):
        choice = choose_runtime(_not_found_result(), bundled_runtime_path=None)
        assert choice.python_path is None
        assert choice.use_bundled is False

    def test_detect_not_found_empty_string_bundled_gives_none_path(self):
        choice = choose_runtime(_not_found_result(), bundled_runtime_path="")
        assert choice.python_path is None

    def test_reason_always_non_empty(self):
        for choice in [
            choose_runtime(_found_result(), "/bundle/python"),
            choose_runtime(_not_found_result(), "/bundle/python"),
            choose_runtime(_not_found_result(), None),
        ]:
            assert isinstance(choice.reason, str)
            assert len(choice.reason) > 0

    def test_returns_runtime_choice_type(self):
        choice = choose_runtime(_found_result(), "/bundle/python")
        assert isinstance(choice, RuntimeChoice)


# ---------------------------------------------------------------------------
# prepare_and_launch tests
# ---------------------------------------------------------------------------

class TestPrepareAndLaunch:

    # --- normal path: venv_setup + app_runner both called in order ---

    def test_calls_venv_setup_then_app_runner(self):
        """Injected callables are called in order; spy records calls."""
        call_log: list[str] = []

        def spy_venv(py: str, venv: str) -> None:
            call_log.append(f"venv:{py}:{venv}")

        def spy_runner(py: str, entry: str) -> None:
            call_log.append(f"runner:{py}:{entry}")

        choice = RuntimeChoice(
            use_bundled=False,
            python_path="/usr/bin/python3.11",
            reason="system",
        )
        plan = prepare_and_launch(
            choice,
            app_entry="app/main.py",
            venv_dir="/tmp/xnat_venv",
            venv_setup=spy_venv,
            app_runner=spy_runner,
        )

        assert plan.error is None
        assert len(call_log) == 2
        assert call_log[0].startswith("venv:/usr/bin/python3.11")
        assert call_log[1].startswith("runner:/usr/bin/python3.11")

    def test_steps_recorded_on_success(self):
        choice = RuntimeChoice(
            use_bundled=False,
            python_path="/usr/bin/python3.11",
            reason="system",
        )
        plan = prepare_and_launch(
            choice,
            app_entry="app/main.py",
            venv_dir="/tmp/v",
            venv_setup=_noop_venv,
            app_runner=_noop_runner,
        )
        assert plan.error is None
        assert len(plan.steps) == 2  # venv step + launch step

    def test_venv_dir_set_on_success(self):
        choice = RuntimeChoice(
            use_bundled=True,
            python_path="/bundle/python",
            reason="bundled",
        )
        plan = prepare_and_launch(
            choice,
            app_entry="main.py",
            venv_dir="/tmp/v",
            venv_setup=_noop_venv,
            app_runner=_noop_runner,
        )
        assert plan.venv_dir == "/tmp/v"

    def test_python_path_in_plan(self):
        choice = RuntimeChoice(
            use_bundled=False,
            python_path="/usr/bin/python3.11",
            reason="ok",
        )
        plan = prepare_and_launch(
            choice,
            app_entry="main.py",
            venv_dir="/tmp/v",
            venv_setup=_noop_venv,
            app_runner=_noop_runner,
        )
        assert plan.python_path == "/usr/bin/python3.11"

    # --- fail-soft: no runtime available ---

    def test_no_runtime_gives_friendly_error_not_raises(self):
        choice = RuntimeChoice(
            use_bundled=False,
            python_path=None,
            reason="nothing found",
        )
        plan = prepare_and_launch(
            choice,
            app_entry="main.py",
            venv_dir="/tmp/v",
            venv_setup=_noop_venv,
            app_runner=_noop_runner,
        )
        assert plan.error is not None
        assert "reinstall" in plan.error.lower() or "data librarian" in plan.error.lower()
        # no exception escaped — we're here

    def test_no_runtime_app_runner_not_called(self):
        called = []

        def spy_runner(py, entry):
            called.append(True)

        choice = RuntimeChoice(use_bundled=False, python_path=None, reason="none")
        prepare_and_launch(
            choice,
            app_entry="main.py",
            venv_dir="/tmp/v",
            venv_setup=_noop_venv,
            app_runner=spy_runner,
        )
        assert called == []  # runner must NOT have been called

    # --- fail-soft: venv_setup raises ---

    def test_venv_setup_raises_gives_friendly_error(self):
        def bad_venv(py: str, venv: str) -> None:
            raise OSError("disk full")

        choice = RuntimeChoice(
            use_bundled=False,
            python_path="/usr/bin/python3.11",
            reason="ok",
        )
        plan = prepare_and_launch(
            choice,
            app_entry="main.py",
            venv_dir="/tmp/v",
            venv_setup=bad_venv,
            app_runner=_noop_runner,
        )
        assert plan.error is not None
        assert "reinstall" in plan.error.lower() or "data librarian" in plan.error.lower()
        # should NOT contain a raw traceback (just a message)
        assert "Traceback" not in plan.error

    def test_venv_setup_raises_app_runner_not_called(self):
        called = []

        def bad_venv(py, venv):
            raise RuntimeError("fail")

        def spy_runner(py, entry):
            called.append(True)

        choice = RuntimeChoice(
            use_bundled=False,
            python_path="/usr/bin/python3.11",
            reason="ok",
        )
        prepare_and_launch(
            choice,
            app_entry="main.py",
            venv_dir="/tmp/v",
            venv_setup=bad_venv,
            app_runner=spy_runner,
        )
        assert called == []

    # --- fail-soft: app_runner raises ---

    def test_app_runner_raises_gives_friendly_error(self):
        def bad_runner(py: str, entry: str) -> None:
            raise FileNotFoundError("app not found")

        choice = RuntimeChoice(
            use_bundled=False,
            python_path="/usr/bin/python3.11",
            reason="ok",
        )
        plan = prepare_and_launch(
            choice,
            app_entry="main.py",
            venv_dir="/tmp/v",
            venv_setup=_noop_venv,
            app_runner=bad_runner,
        )
        assert plan.error is not None
        assert "Traceback" not in plan.error

    # --- returns LaunchPlan type ---

    def test_returns_launch_plan_type(self):
        choice = RuntimeChoice(
            use_bundled=False,
            python_path="/usr/bin/python3.11",
            reason="ok",
        )
        plan = prepare_and_launch(
            choice,
            app_entry="main.py",
            venv_dir="/tmp/v",
            venv_setup=_noop_venv,
            app_runner=_noop_runner,
        )
        assert isinstance(plan, LaunchPlan)

    # --- bundled runtime path flows through correctly ---

    def test_bundled_runtime_path_used_in_venv_call(self):
        received = {}

        def spy_venv(py: str, venv: str) -> None:
            received["py"] = py

        choice = RuntimeChoice(
            use_bundled=True,
            python_path="/bundle/python3.11",
            reason="bundled",
        )
        prepare_and_launch(
            choice,
            app_entry="main.py",
            venv_dir="/tmp/v",
            venv_setup=spy_venv,
            app_runner=_noop_runner,
        )
        assert received["py"] == "/bundle/python3.11"
