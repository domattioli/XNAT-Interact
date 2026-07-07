"""
tests/regression_014/test_s4_project_prefix.py -- #33 S4 regression (spec 014).

S4 (still-open per the 2026-07-07 audit, ledger.md):
src/annotations/io_xnat.py's _image_qs() checked
`image_ref.startswith("/project")` (no trailing slash) to decide whether
image_ref was already a fully qualified query string. That substring check
would also match an unqualified image_ref that merely started with the bare
word "project" (e.g. a resource literally named "projectXYZ"), wrongly
treating it as already-qualified and using it verbatim instead of
qualifying it with the project name.

Baseline evidence (pre-fix, this branch before the S4 fix commit): the test
below FAILS -- see ledger.md for the recorded run.

Offline only -- pure string logic, no server, no PHI.
"""
from __future__ import annotations

from src.annotations.io_xnat import _image_qs


def test_unqualified_ref_starting_with_bare_word_project_still_gets_qualified():
    # "/projectXYZ/subject/S" is NOT a qualified "/project/..." path -- it's
    # an unqualified ref that happens to start with the 8-char substring
    # "/project" (no delimiter). Pre-fix, `startswith("/project")` matched
    # this and wrongly treated it as already-qualified, returning it
    # verbatim instead of qualifying it with the project name.
    result = _image_qs("/projectXYZ/subject/S", project_name="MYPROJ")
    assert result == "/project/MYPROJ/projectXYZ/subject/S", (
        f"Expected the ref to be qualified with the project name since it "
        f"is not a genuine '/project/...' path; got: {result!r}"
    )


def test_already_qualified_ref_is_used_verbatim():
    qualified = "/project/MYPROJ/subject/S/experiment/E/scan/0"
    assert _image_qs(qualified, project_name="MYPROJ") == qualified


def test_ref_with_no_project_name_is_used_verbatim():
    ref = "some/relative/ref"
    assert _image_qs(ref, project_name=None) == ref
