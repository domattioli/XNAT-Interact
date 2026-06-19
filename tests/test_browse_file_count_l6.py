"""
tests/test_browse_file_count_l6.py — Offline regression test for #33 finding L6.

`_file_count` used to pin the `SRC` resource label, so a scan whose files live
under a different resource (e.g. DERIVED) was undercounted. The query must list
files across ALL of the scan's resources.

RULES: no streamlit, no network, no PHI.
"""
from __future__ import annotations

from typing import Any, List

from app.logic.browse import _file_count


class _Sel:
    def __init__(self, files: List[str]) -> None:
        self._files = files

    def get(self) -> List[str]:
        return self._files


class _ResourceAwareServer:
    """Reproduces real XNAT: a scan with no SRC resource returns nothing under
    /resources/SRC/files, but /scans/SCAN/files lists files across all resources."""

    def __init__(self, files: List[str]) -> None:
        self._files = files
        self.seen_qs: List[str] = []

    def select(self, querystring: str) -> Any:
        self.seen_qs.append(querystring)
        if "/resources/SRC/" in querystring:
            return _Sel([])  # SRC resource absent for this scan
        return _Sel(self._files)  # all-resources listing


def test_l6_counts_non_src_resource_files() -> None:
    server = _ResourceAwareServer(files=["0001.dcm", "0002.dcm", "0003.dcm"])
    n = _file_count(server, "P", "S001", "E001", "SCAN1")
    assert n == 3  # pre-fix queried /resources/SRC/files -> 0


def test_l6_query_does_not_pin_src_resource() -> None:
    server = _ResourceAwareServer(files=["a.dcm"])
    _file_count(server, "P", "S001", "E001", "SCAN1")
    assert server.seen_qs, "select() was never called"
    assert not any("/resources/SRC/" in q for q in server.seen_qs)
