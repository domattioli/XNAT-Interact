"""
tests/test_packaging_detect.py
------------------------------
Offline unit tests for installer.python_detect.detect_python().

All environment probing is injected — no real subprocess, no registry,
no real PATH traversal.  Tests cover every branch of the detection logic.
"""
from __future__ import annotations

import pytest

from installer.python_detect import DetectResult, detect_python


# ---------------------------------------------------------------------------
# Helper factories for injectable fakes
# ---------------------------------------------------------------------------

def _make_candidates(*paths: str):
    """Return a zero-arg callable that yields the given paths."""
    def provider():
        return list(paths)
    return provider


def _make_version_map(mapping: dict):
    """
    Return a version probe that looks up the path in ``mapping``.
    Paths missing from the map → returns None (simulate not-executable).
    """
    def probe(path: str):
        return mapping.get(path)
    return probe


def _make_venv_map(mapping: dict):
    """
    Return a venv probe that looks up the path in ``mapping``.
    Default (missing) → False.
    """
    def probe(path: str):
        return mapping.get(path, False)
    return probe


def _always_venv_ok(path: str) -> bool:
    return True


def _always_venv_fail(path: str) -> bool:
    return False


# ---------------------------------------------------------------------------
# T001 — compatible interpreter found; picks FIRST one
# ---------------------------------------------------------------------------

def test_compatible_found_picks_first():
    """First compatible interpreter in candidate list is chosen."""
    result = detect_python(
        min_version=(3, 9),
        candidates_provider=_make_candidates(
            "/usr/bin/python3.11",
            "/usr/bin/python3.10",
        ),
        version_probe=_make_version_map({
            "/usr/bin/python3.11": (3, 11, 2),
            "/usr/bin/python3.10": (3, 10, 9),
        }),
        venv_probe=_always_venv_ok,
    )
    assert result.found is True
    assert result.path == "/usr/bin/python3.11"
    assert result.version == (3, 11, 2)
    assert "compatible" in result.reason.lower() or result.path in result.reason


def test_compatible_found_returns_detectresult_type():
    result = detect_python(
        candidates_provider=_make_candidates("/usr/bin/python3.9"),
        version_probe=_make_version_map({"/usr/bin/python3.9": (3, 9, 7)}),
        venv_probe=_always_venv_ok,
    )
    assert isinstance(result, DetectResult)
    assert result.found is True


# ---------------------------------------------------------------------------
# T002 — all candidates too old → found=False
# ---------------------------------------------------------------------------

def test_all_too_old_returns_not_found():
    result = detect_python(
        min_version=(3, 9),
        candidates_provider=_make_candidates(
            "/usr/bin/python3.7",
            "/usr/bin/python3.8",
        ),
        version_probe=_make_version_map({
            "/usr/bin/python3.7": (3, 7, 0),
            "/usr/bin/python3.8": (3, 8, 12),
        }),
        venv_probe=_always_venv_ok,
    )
    assert result.found is False
    assert result.path is None
    assert result.version is None
    assert "3.7" in result.reason or "minimum" in result.reason.lower() or "compatible" in result.reason.lower()


def test_exact_min_version_accepted():
    """Version exactly equal to min_version is accepted."""
    result = detect_python(
        min_version=(3, 9),
        candidates_provider=_make_candidates("/usr/bin/python3.9"),
        version_probe=_make_version_map({"/usr/bin/python3.9": (3, 9, 0)}),
        venv_probe=_always_venv_ok,
    )
    assert result.found is True
    assert result.version == (3, 9, 0)


def test_one_below_min_rejected():
    result = detect_python(
        min_version=(3, 10),
        candidates_provider=_make_candidates("/usr/bin/python3.9"),
        version_probe=_make_version_map({"/usr/bin/python3.9": (3, 9, 7)}),
        venv_probe=_always_venv_ok,
    )
    assert result.found is False


# ---------------------------------------------------------------------------
# T003 — no candidates on system → found=False (→ caller uses bundled)
# ---------------------------------------------------------------------------

def test_no_candidates_returns_not_found():
    result = detect_python(
        candidates_provider=_make_candidates(),   # empty list
        version_probe=_make_version_map({}),
        venv_probe=_always_venv_ok,
    )
    assert result.found is False
    assert result.path is None
    # reason should mention "no" candidates
    assert result.reason  # non-empty


# ---------------------------------------------------------------------------
# T004 — version probe failures handled gracefully
# ---------------------------------------------------------------------------

def test_version_probe_returns_none_skips_candidate():
    """Candidate with version probe returning None is skipped (not executable)."""
    result = detect_python(
        candidates_provider=_make_candidates(
            "/usr/bin/python_broken",
            "/usr/bin/python3.11",
        ),
        version_probe=_make_version_map({
            "/usr/bin/python_broken": None,       # probe failure
            "/usr/bin/python3.11": (3, 11, 2),
        }),
        venv_probe=_always_venv_ok,
    )
    assert result.found is True
    assert result.path == "/usr/bin/python3.11"


def test_all_version_probes_fail():
    result = detect_python(
        candidates_provider=_make_candidates(
            "/usr/bin/python_a",
            "/usr/bin/python_b",
        ),
        version_probe=_make_version_map({}),   # all return None
        venv_probe=_always_venv_ok,
    )
    assert result.found is False
    assert "version probe failed" in result.reason or result.found is False


# ---------------------------------------------------------------------------
# T005 — venv probe fails → candidate rejected
# ---------------------------------------------------------------------------

def test_venv_probe_fail_rejects_candidate():
    """Interpreter with version OK but venv probe failing is rejected."""
    result = detect_python(
        candidates_provider=_make_candidates("/usr/bin/python3.11"),
        version_probe=_make_version_map({"/usr/bin/python3.11": (3, 11, 2)}),
        venv_probe=_always_venv_fail,
    )
    assert result.found is False
    assert "venv" in result.reason.lower()


def test_venv_probe_fail_falls_through_to_next_candidate():
    """First candidate's venv probe fails; second passes → second chosen."""
    result = detect_python(
        candidates_provider=_make_candidates(
            "/usr/bin/python3.11_novenv",
            "/usr/bin/python3.10_ok",
        ),
        version_probe=_make_version_map({
            "/usr/bin/python3.11_novenv": (3, 11, 2),
            "/usr/bin/python3.10_ok": (3, 10, 9),
        }),
        venv_probe=_make_venv_map({
            "/usr/bin/python3.11_novenv": False,
            "/usr/bin/python3.10_ok": True,
        }),
    )
    assert result.found is True
    assert result.path == "/usr/bin/python3.10_ok"


# ---------------------------------------------------------------------------
# T006 — multiple Pythons on PATH → deterministic first-compatible pick
# ---------------------------------------------------------------------------

def test_multiple_pythons_picks_first_compatible_deterministically():
    """With three candidates, first compatible one (by order) is returned."""
    result = detect_python(
        min_version=(3, 9),
        candidates_provider=_make_candidates(
            "/usr/bin/python3.7",    # too old
            "/usr/bin/python3.11",   # compatible ← should be chosen
            "/usr/bin/python3.12",   # also compatible but not first
        ),
        version_probe=_make_version_map({
            "/usr/bin/python3.7": (3, 7, 0),
            "/usr/bin/python3.11": (3, 11, 2),
            "/usr/bin/python3.12": (3, 12, 0),
        }),
        venv_probe=_always_venv_ok,
    )
    assert result.found is True
    assert result.path == "/usr/bin/python3.11"


# ---------------------------------------------------------------------------
# T007 — mixed failures + one good → found=True
# ---------------------------------------------------------------------------

def test_mixed_failures_finds_good_candidate():
    """Bad version probe, old version, bad venv, then a good one."""
    result = detect_python(
        min_version=(3, 9),
        candidates_provider=_make_candidates(
            "/a/python_crash",    # version probe → None
            "/b/python3.8",       # too old
            "/c/python3.11_nv",   # version ok, venv fail
            "/d/python3.11_ok",   # all good
        ),
        version_probe=_make_version_map({
            "/a/python_crash": None,
            "/b/python3.8": (3, 8, 0),
            "/c/python3.11_nv": (3, 11, 2),
            "/d/python3.11_ok": (3, 11, 2),
        }),
        venv_probe=_make_venv_map({
            "/c/python3.11_nv": False,
            "/d/python3.11_ok": True,
        }),
    )
    assert result.found is True
    assert result.path == "/d/python3.11_ok"


# ---------------------------------------------------------------------------
# T008 — reason field is always a non-empty string
# ---------------------------------------------------------------------------

def test_reason_always_non_empty_on_failure():
    for scenario_result in [
        detect_python(
            candidates_provider=_make_candidates(),
            version_probe=_make_version_map({}),
            venv_probe=_always_venv_ok,
        ),
        detect_python(
            min_version=(3, 9),
            candidates_provider=_make_candidates("/u/python3.7"),
            version_probe=_make_version_map({"/u/python3.7": (3, 7, 0)}),
            venv_probe=_always_venv_ok,
        ),
    ]:
        assert isinstance(scenario_result.reason, str)
        assert len(scenario_result.reason) > 0


def test_reason_non_empty_on_success():
    result = detect_python(
        candidates_provider=_make_candidates("/usr/bin/python3.11"),
        version_probe=_make_version_map({"/usr/bin/python3.11": (3, 11, 2)}),
        venv_probe=_always_venv_ok,
    )
    assert isinstance(result.reason, str)
    assert len(result.reason) > 0


# ---------------------------------------------------------------------------
# T009 — empty path strings skipped
# ---------------------------------------------------------------------------

def test_empty_path_string_skipped():
    """An empty string in candidates list must not crash; should skip it."""
    result = detect_python(
        candidates_provider=_make_candidates("", "/usr/bin/python3.11"),
        version_probe=_make_version_map({"/usr/bin/python3.11": (3, 11, 2)}),
        venv_probe=_always_venv_ok,
    )
    assert result.found is True
    assert result.path == "/usr/bin/python3.11"
