"""
tests/fakes/scan_tree_fake — a FakeXNAT that knows which scans and resource
labels it holds (spec 015 tests).

The plain FakeXNAT stores seeded files per (subject, experiment, scan, label)
but cannot list scans or labels.  The download code asks for those through
``list_scans`` and ``list_resources``; this small subclass answers from the
seeded files.  Synthetic data only.
"""
from __future__ import annotations

from typing import List

from tests.fakes.fake_xnat import FakeXNAT


class ScanTreeFake(FakeXNAT):
    """FakeXNAT plus scan and resource-label listing for seeded files."""

    def __init__(self, project_name: str = "P015", **kwargs) -> None:
        super().__init__(project_name=project_name, **kwargs)
        self._tree: dict = {}  # (subject, experiment) -> {scan: [labels]}

    def add_files(self, subject: str, experiment: str, scan: str, label: str, files) -> None:
        """Seed ``files`` (list of (name, bytes)) into one scan resource; an empty list makes an empty resource."""
        qs = f"/projects/{self.project_name}/subjects/{subject}/experiments/{experiment}/scans/{scan}"
        resource = self.select(qs).resource(label)
        self.seed_resource_files(resource, list(files))
        labels = self._tree.setdefault((subject, experiment), {}).setdefault(scan, [])
        if label not in labels:
            labels.append(label)

    def list_scans(self, project_name: str, subject: str, experiment: str) -> List[str]:
        return list(self._tree.get((subject, experiment), {}).keys())

    def list_resources(self, project_name: str, subject: str, experiment: str, scan: str) -> List[str]:
        return list(self._tree.get((subject, experiment), {}).get(scan, []))
