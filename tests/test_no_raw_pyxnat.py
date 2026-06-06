"""
test_no_raw_pyxnat.py — SC-001 + SC-003 grep-guard.

Fails if raw pyxnat surface or inline path f-strings appear in ``src/``
outside the allowed gateway/conventions modules.

SC-001: No raw pyxnat in src/ outside xnat_gateway*.py
  Patterns banned:
    import pyxnat
    Interface(
    .put_zip(
    .get_copy(

SC-003: No inline project path f-strings in src/ outside xnat_conventions.py
  Pattern banned (regex):
    /project[s]?/   appearing inside f-string context (f"..." or f'...')
"""
from __future__ import annotations

import re
from pathlib import Path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SRC_DIR = Path(__file__).parent.parent / "src"


def _src_files(exclude_stems: list[str]) -> list[Path]:
    """Return all .py files under src/ excluding modules by stem pattern."""
    files = []
    for p in SRC_DIR.rglob("*.py"):
        if any(p.name.startswith(stem) for stem in exclude_stems):
            continue
        files.append(p)
    return files


def _grep(files: list[Path], pattern: str) -> list[tuple[Path, int, str]]:
    """Return (file, lineno, line) for every match of *pattern*."""
    hits: list[tuple[Path, int, str]] = []
    rx = re.compile(pattern)
    for f in files:
        for lineno, line in enumerate(f.read_text(encoding="utf-8").splitlines(), 1):
            if rx.search(line):
                hits.append((f, lineno, line.strip()))
    return hits


def _fmt_hits(hits: list[tuple[Path, int, str]]) -> str:
    return "\n".join(f"  {f.relative_to(SRC_DIR.parent)}:{n}: {ln}" for f, n, ln in hits)


# ---------------------------------------------------------------------------
# SC-001: no raw pyxnat surface outside xnat_gateway*.py
# ---------------------------------------------------------------------------

_SC001_EXCLUDE = ["xnat_gateway"]  # allow xnat_gateway.py, xnat_gateway_*.py


class TestNoRawPyxnat:
    """SC-001: raw pyxnat patterns must not appear outside xnat_gateway*.py."""

    def _files(self) -> list[Path]:
        return _src_files(_SC001_EXCLUDE)

    def test_no_import_pyxnat(self) -> None:
        hits = _grep(self._files(), r"\bimport pyxnat\b")
        assert not hits, (
            "SC-001 violation: 'import pyxnat' found in src/ outside xnat_gateway*.py\n"
            + _fmt_hits(hits)
        )

    def test_no_interface_constructor(self) -> None:
        hits = _grep(self._files(), r"\bInterface\s*\(")
        assert not hits, (
            "SC-001 violation: 'Interface(' found in src/ outside xnat_gateway*.py\n"
            + _fmt_hits(hits)
        )

    def test_no_put_zip(self) -> None:
        # Ban raw resource.put_zip() calls; allow gateway.put_zip() (routed call).
        hits = _grep(self._files(), r"(?<!gateway)\.put_zip\s*\(")
        assert not hits, (
            "SC-001 violation: raw '.put_zip(' (not gateway.put_zip) found in src/ "
            "outside xnat_gateway*.py\n"
            + _fmt_hits(hits)
        )

    def test_no_get_copy(self) -> None:
        # Ban raw file.get_copy() calls; allow gateway.get_file_copy() (routed call).
        hits = _grep(self._files(), r"\.get_copy\s*\(")
        assert not hits, (
            "SC-001 violation: '.get_copy(' found in src/ outside xnat_gateway*.py\n"
            + _fmt_hits(hits)
        )


# ---------------------------------------------------------------------------
# SC-003: no inline /project[s]?/ path f-strings outside xnat_conventions.py
# ---------------------------------------------------------------------------

_SC003_EXCLUDE = ["xnat_conventions"]


class TestNoInlineProjectPaths:
    """SC-003: inline /project[s]?/ f-string paths must not appear outside xnat_conventions.py."""

    def _files(self) -> list[Path]:
        return _src_files(_SC003_EXCLUDE)

    def test_no_inline_project_path_fstrings(self) -> None:
        # Match f-string literals that embed /project/ or /projects/
        # Pattern: f"..." or f'...' containing /project[s]?/
        hits = _grep(self._files(), r'f["\'].*?/projects?/.*?["\']')
        assert not hits, (
            "SC-003 violation: inline '/project[s]?/' f-string found in src/ "
            "outside xnat_conventions.py\n"
            + _fmt_hits(hits)
        )
