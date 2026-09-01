"""
tests/contract/comparator.py — State comparator for dual-run parity validation.

Captures and normalizes XNAT server state (FakeXNAT or real XNAT) so that
fake-side and real-side outcomes can be structurally diffed after stripping
server-assigned fields (IDs, timestamps, URIs, auto-generated names).
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List


# ---------------------------------------------------------------------------
# Fields stripped during normalization
# ---------------------------------------------------------------------------

# Exact key matches (case-sensitive) stripped unconditionally.
_STRIP_KEYS: frozenset = frozenset({
    "ID",
    "id",
    "URI",
    "uri",
    "insert_date",
    "insert_user",
    "last_modified",
    "create_date",
    "timestamp",
    "activation_date",
    "deactivation_date",
})

# Key-suffix patterns stripped (any key ending with these substrings).
_STRIP_SUFFIXES: tuple = (
    "_date",
    "_time",
    "_id",
    "Data/id",
)

# Key-prefix patterns stripped (any key matching these prefixes, e.g. xnat_rfData/id).
_XNAT_DATATYPE_ID_RE = re.compile(r"^xnat_\w+Data/id$")


def _should_strip(key: str) -> bool:
    if key in _STRIP_KEYS:
        return True
    for suffix in _STRIP_SUFFIXES:
        if key.endswith(suffix):
            return True
    if _XNAT_DATATYPE_ID_RE.match(key):
        return True
    return False


# ---------------------------------------------------------------------------
# ComparisonResult
# ---------------------------------------------------------------------------

@dataclass
class ComparisonResult:
    """Outcome of comparing two normalized states."""

    equal: bool
    fake_extra: Dict[str, Any] = field(default_factory=dict)
    real_extra: Dict[str, Any] = field(default_factory=dict)
    diffs: List[str] = field(default_factory=list)

    def assert_equal(self) -> None:
        """Raise AssertionError with a diff report if states are not equal."""
        if self.equal:
            return
        lines = ["Dual-run parity mismatch (fake vs real XNAT state):"]
        for d in self.diffs:
            lines.append(f"  {d}")
        if self.fake_extra:
            lines.append(f"  Keys only in fake: {sorted(self.fake_extra)}")
        if self.real_extra:
            lines.append(f"  Keys only in real: {sorted(self.real_extra)}")
        raise AssertionError("\n".join(lines))


# ---------------------------------------------------------------------------
# XnatStateComparator
# ---------------------------------------------------------------------------

class XnatStateComparator:
    """
    Capture and compare XNAT server state for dual-run parity validation.

    Usage::

        cmp = XnatStateComparator()
        fake_state = cmp.capture(fake_xnat, project)
        real_state = cmp.capture(real_xnat, project)
        cmp.compare(fake_state, real_state).assert_equal()
    """

    # ------------------------------------------------------------------
    # capture
    # ------------------------------------------------------------------

    def capture(self, gateway: Any, project: str) -> Dict[str, Any]:
        """
        Capture full server state for *project*: subjects, experiments,
        scans, resources, files (label + bytes hash), assessors.

        Returns a canonicalized dict suitable for normalization + comparison.

        Parameters
        ----------
        gateway:
            An ``XnatGateway`` instance (FakeXNAT or PyxnatGateway).
        project:
            XNAT project name string.
        """
        # "project" is the per-run namespace (TEST_PROJECT vs ITEST_<hex>) and
        # ALWAYS differs legitimately between fake and real runs — exclude it.
        state: Dict[str, Any] = {
            "subjects": {},
        }

        # --- subjects ---
        try:
            subjects = _list_subjects(gateway, project)
        except Exception:
            subjects = []

        for subj_label in subjects:
            subj_state: Dict[str, Any] = {"experiments": {}}

            # --- experiments ---
            try:
                experiments = _list_experiments(gateway, project, subj_label)
            except Exception:
                experiments = []

            for exp_label in experiments:
                exp_state: Dict[str, Any] = {
                    "scans": {},
                    "assessors": {},
                }
                exp_qs = f"/project/{project}/subject/{subj_label}/experiment/{exp_label}"

                # --- scans ---
                try:
                    scans = _list_scans(gateway, exp_qs)
                except Exception:
                    scans = []

                for scan_id in scans:
                    scan_qs = f"{exp_qs}/scan/{scan_id}"
                    scan_state: Dict[str, Any] = {"resources": {}}

                    # --- resources ---
                    try:
                        resources = _list_resources(gateway, scan_qs)
                    except Exception:
                        resources = []

                    for res_label in resources:
                        files_state: Dict[str, str] = {}
                        try:
                            file_names = gateway.list_files(scan_qs, res_label)
                            for fname in file_names:
                                try:
                                    content = _get_file_bytes(gateway, scan_qs, res_label, fname)
                                    files_state[fname] = hashlib.sha256(content).hexdigest()
                                except Exception:
                                    files_state[fname] = "<unreadable>"
                        except Exception:
                            pass
                        scan_state["resources"][res_label] = {"files": files_state}

                    exp_state["scans"][scan_id] = scan_state

                # --- assessors ---
                try:
                    assessors = _list_assessors(gateway, exp_qs)
                except Exception:
                    assessors = []

                for assessor_label in assessors:
                    exp_state["assessors"][assessor_label] = {}

                subj_state["experiments"][exp_label] = exp_state

            state["subjects"][subj_label] = subj_state

        return state

    # ------------------------------------------------------------------
    # normalize
    # ------------------------------------------------------------------

    def normalize(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Strip server-assigned fields from *state* recursively.

        Removes: ``ID``, ``insert_date``, ``xnat_*Data/id``, ``URI``,
        any timestamp fields, server-side auto-generated IDs.
        """
        return _strip_recursive(state)

    # ------------------------------------------------------------------
    # compare
    # ------------------------------------------------------------------

    def compare(self, fake_state: Dict[str, Any], real_state: Dict[str, Any]) -> ComparisonResult:
        """
        Diff *fake_state* and *real_state* after normalization.

        Returns a ``ComparisonResult`` whose ``.assert_equal()`` raises on
        mismatch with a human-readable diff report.
        """
        norm_fake = self.normalize(fake_state)
        norm_real = self.normalize(real_state)
        diffs: List[str] = []
        fake_extra: Dict[str, Any] = {}
        real_extra: Dict[str, Any] = {}
        _diff_dicts(norm_fake, norm_real, path="", diffs=diffs, fake_extra=fake_extra, real_extra=real_extra)
        equal = len(diffs) == 0 and len(fake_extra) == 0 and len(real_extra) == 0
        return ComparisonResult(equal=equal, fake_extra=fake_extra, real_extra=real_extra, diffs=diffs)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _strip_recursive(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {
            k: _strip_recursive(v)
            for k, v in obj.items()
            if not _should_strip(k)
        }
    if isinstance(obj, list):
        return [_strip_recursive(item) for item in obj]
    return obj


def _diff_dicts(
    fake: Any,
    real: Any,
    path: str,
    diffs: List[str],
    fake_extra: Dict[str, Any],
    real_extra: Dict[str, Any],
) -> None:
    """Recursive structural diff — populates diffs / fake_extra / real_extra in place."""
    if type(fake) != type(real):
        diffs.append(f"  [{path}] type mismatch: fake={type(fake).__name__}, real={type(real).__name__}")
        return

    if isinstance(fake, dict):
        all_keys = set(fake) | set(real)
        for key in sorted(all_keys):
            child_path = f"{path}.{key}" if path else key
            if key not in real:
                fake_extra[child_path] = fake[key]
            elif key not in fake:
                real_extra[child_path] = real[key]
            else:
                _diff_dicts(fake[key], real[key], child_path, diffs, fake_extra, real_extra)
    elif isinstance(fake, list):
        if len(fake) != len(real):
            diffs.append(f"  [{path}] list length: fake={len(fake)}, real={len(real)}")
        else:
            for i, (fi, ri) in enumerate(zip(fake, real)):
                _diff_dicts(fi, ri, f"{path}[{i}]", diffs, fake_extra, real_extra)
    else:
        if fake != real:
            diffs.append(f"  [{path}] value: fake={fake!r}, real={real!r}")


# ---------------------------------------------------------------------------
# Gateway-agnostic enumeration helpers
# (work with both FakeXNAT and PyxnatGateway via duck-typing)
# ---------------------------------------------------------------------------

def _list_subjects(gateway: Any, project: str) -> List[str]:
    """Return subject labels for *project*.

    FakeXNAT stores objects in ``_selectables`` keyed by query-string
    (e.g. ``/project/P/subject/S``).  There is no ``_subjects`` attribute —
    enumerate ``_selectables`` by QS-prefix instead, mirroring ``_list_scans``.
    """
    if hasattr(gateway, "_selectables"):
        prefix = f"/project/{project}/subject/"
        labels = []
        for qs, sel in gateway._selectables.items():
            if qs.startswith(prefix) and sel._exists:
                tail = qs[len(prefix):]
                # Subject keys have exactly one segment (no further "/")
                if "/" not in tail:
                    labels.append(tail)
        return sorted(labels)
    # PyxnatGateway path — use raw server
    server = getattr(gateway, "server", None) or gateway
    subjects = server.select(f"/project/{project}").subjects()
    return [s.label() for s in subjects]


def _list_experiments(gateway: Any, project: str, subject: str) -> List[str]:
    """Return experiment labels under *subject*.

    Same QS-parse strategy as ``_list_subjects`` / ``_list_scans``.
    """
    if hasattr(gateway, "_selectables"):
        prefix = f"/project/{project}/subject/{subject}/experiment/"
        labels = []
        for qs, sel in gateway._selectables.items():
            if qs.startswith(prefix) and sel._exists:
                tail = qs[len(prefix):]
                # Experiment keys have exactly one segment (no further "/")
                if "/" not in tail:
                    labels.append(tail)
        return sorted(labels)
    server = getattr(gateway, "server", None) or gateway
    exps = server.select(f"/project/{project}/subject/{subject}").experiments()
    return [e.label() for e in exps]


def _list_scans(gateway: Any, exp_qs: str) -> List[str]:
    """Return scan IDs under *exp_qs*."""
    if hasattr(gateway, "_selectables"):
        prefix = exp_qs + "/scan/"
        ids = []
        for qs, sel in gateway._selectables.items():
            if qs.startswith(prefix) and sel._exists:
                tail = qs[len(prefix):]
                if "/" not in tail:
                    ids.append(tail)
        return sorted(ids)
    server = getattr(gateway, "server", None) or gateway
    scans = server.select(exp_qs).scans()
    return [s.id() for s in scans]


def _list_resources(gateway: Any, scan_qs: str) -> List[str]:
    """Return resource labels for *scan_qs*.

    FakeXNAT stores resources in ``_resources`` keyed by
    ``(subject, experiment, scan, resource_label)`` 4-tuples (NOT (qs, label)
    2-tuples).  Parse scan_qs to extract the three path components, then
    match on the first three elements of each key.
    """
    if hasattr(gateway, "_resources"):
        # Parse scan_qs: /project/{p}/subject/{s}/experiment/{e}/scan/{scan_id}
        parts = [p for p in scan_qs.split("/") if p]
        subj = exp = scan_id = None
        try:
            subj = parts[parts.index("subject") + 1]
            exp = parts[parts.index("experiment") + 1]
            scan_id = parts[parts.index("scan") + 1]
        except (ValueError, IndexError):
            pass
        if subj and exp and scan_id:
            labels = [
                key[3]
                for key in gateway._resources
                if len(key) == 4 and key[0] == subj and key[1] == exp and key[2] == scan_id
            ]
            return sorted(labels)
    server = getattr(gateway, "server", None) or gateway
    resources = server.select(scan_qs).resources()
    return [r.label() for r in resources]


def _list_assessors(gateway: Any, exp_qs: str) -> List[str]:
    """Return assessor labels under *exp_qs*."""
    if hasattr(gateway, "_selectables"):
        prefix = exp_qs + "/assessor/"
        labels = []
        for qs, sel in gateway._selectables.items():
            if qs.startswith(prefix) and sel._exists:
                tail = qs[len(prefix):]
                if "/" not in tail:
                    labels.append(tail)
        return sorted(labels)
    server = getattr(gateway, "server", None) or gateway
    assessors = server.select(exp_qs).assessors()
    return [a.label() for a in assessors]


def _get_file_bytes(gateway: Any, scan_qs: str, resource_label: str, filename: str) -> bytes:
    """Return bytes of *filename* from *resource_label* under *scan_qs*."""
    import tempfile
    import os
    with tempfile.TemporaryDirectory() as tmpdir:
        dest = os.path.join(tmpdir, filename)
        gateway.get_file_copy(scan_qs, resource_label, filename, dest)
        with open(dest, "rb") as fh:
            return fh.read()
