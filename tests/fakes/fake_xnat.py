"""
FakeXNAT — offline test double for the pyxnat surface used by XNAT-Interact.

FakeXNAT implements XnatGateway (ABC) so any code that accepts an XnatGateway
can receive a FakeXNAT in tests.  ``FakeGateway`` is an alias for FakeXNAT.

CONTRACT CHECKLIST  (mirrors the pyxnat methods this double must cover)
-----------------------------------------------------------------------
Server-level:
  [x] server.select(querystring)             → FakeSelectable
  [x] server.select.project(name)            → FakeProject
  [x] server.disconnect()

Resource-level  (reached via .select() or .select.project().resource()):
  [x] resource.put_zip(ffn, content=, format=, tags=, overwrite=None)
  [x] resource.file(fn)                      → FakeFile

File-level:
  [x] file.put(ffn, content=, format=, tags=, overwrite=None)
  [x] file.get_copy(dest)                    → dest  (writes placeholder)
  [x] file.delete()
  [x] file.insert(data, content=, format=, tags=)

Selectable-level (subject / experiment / scan returned by .select(qs)):
  [x] selectable.exists()                    → bool  (default False)
  [x] selectable.create(**kwargs)
  [x] selectable.attrs.mset(mapping)
  [x] selectable.resource(label)             → FakeResource

Project-level (returned by .select.project(name)):
  [x] project.label()                        → str
  [x] project.exists()                       → bool
  [x] project.users()                        → list[str]
  [x] project.resource(folder)              → FakeResource

XnatGateway gateway methods (T003 — Stage 1 ABC conformance):
  [x] connect() / disconnect() / liveness()
  [x] project_label() / project_users()
  [x] select() / exists()
  [x] create() / set_attrs()
  [x] put_zip() / put_file() / insert_file()
  [x] get_file_copy() / delete_file()
  [x] list_files() / download_resource() / create_assessor()

Failure injection:
  [x] fake.set_next_failure(exc)  — next call on any FakeFile or FakeResource
      operation raises exc.  Automatically cleared after one use.
-----------------------------------------------------------------------
"""
from __future__ import annotations

import shutil
from pathlib import Path, PurePosixPath
from typing import Any, Dict, List, Optional

from src.services.errors import FriendlyError
from src.services.xnat_gateway import XnatGateway, GatewayError


# ---------------------------------------------------------------------------
# Shared call-recording helper
# ---------------------------------------------------------------------------

class _CallLog:
    """Mixin that appends every operation to the root FakeXNAT.calls list."""

    def _record(self, fake_root: "FakeXNAT", op: str, args: tuple, kwargs: dict) -> None:
        fake_root.calls.append({"op": op, "args": args, "kwargs": kwargs})

    def _maybe_raise(self, fake_root: "FakeXNAT") -> None:
        if fake_root._next_failure is not None:
            exc = fake_root._next_failure
            fake_root._next_failure = None
            raise exc


# ---------------------------------------------------------------------------
# FakeFile
# ---------------------------------------------------------------------------

class FakeFile(_CallLog):
    def __init__(self, resource: "FakeResource", filename: str) -> None:
        self._resource = resource
        self._filename = filename

    @property
    def _root(self) -> "FakeXNAT":
        return self._resource._root

    def put(self, ffn: Any, *, content: str = "", format: str = "", tags: str = "", overwrite: Optional[bool] = None) -> None:
        self._maybe_raise(self._root)
        # Auto-store file bytes so get_copy can round-trip real content.
        # Only reads bytes when ffn is a path to an existing file; leaves
        # any manually-primed _file_contents entry untouched if no file at ffn.
        try:
            p = Path(ffn)
            if p.is_file():
                self._root._file_contents[self._filename] = p.read_bytes()
        except (TypeError, OSError):
            pass  # ffn is not a path — skip auto-store; backward-compatible
        self._record(self._root, "file.put", (ffn,), {"content": content, "format": format, "tags": tags, "overwrite": overwrite, "_filename": self._filename})

    def get_copy(self, dest: Any) -> Any:
        self._maybe_raise(self._root)
        dest = Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        # Priority 1: root-level canned content (set_file_content).
        # Priority 2: T003 staged bytes in the parent resource.
        # Priority 3: placeholder text.
        canned: Optional[bytes] = self._root._file_contents.get(self._filename)
        if canned is not None:
            dest.write_bytes(canned)
        else:
            staged = self._resource.get_file_bytes(self._filename)
            if staged is not None:
                dest.write_bytes(staged)
            else:
                dest.write_text(f"[FakeXNAT placeholder for {self._filename}]", encoding="utf-8")
        self._record(self._root, "file.get_copy", (str(dest),), {"_filename": self._filename})
        return dest

    def delete(self) -> None:
        self._maybe_raise(self._root)
        self._record(self._root, "file.delete", (), {"_filename": self._filename})

    def insert(self, data: Any, *, content: str = "", format: str = "", tags: str = "") -> None:
        self._maybe_raise(self._root)
        self._record(self._root, "file.insert", (data,), {"content": content, "format": format, "tags": tags, "_filename": self._filename})


# ---------------------------------------------------------------------------
# FakeResource
# ---------------------------------------------------------------------------

class FakeResource(_CallLog):
    def __init__(self, root: "FakeXNAT", label: str) -> None:
        self._root = root
        self._label = label
        # T003: ordered list of (filename, bytes) representing real staged files.
        # Populated by seed_resource_files(); list_files() enumerates them and
        # file(fn).get_copy() returns the real bytes.  Empty by default so all
        # existing tests that never call seed_resource_files() are unaffected.
        self._staged_files: List[tuple] = []  # List[tuple[str, bytes]]

    def file(self, fn: str) -> FakeFile:
        return FakeFile(resource=self, filename=fn)

    def put_zip(self, ffn: Any, *, content: str = "", format: str = "", tags: str = "", overwrite: Optional[bool] = None) -> None:
        self._maybe_raise(self._root)
        # Fidelity: real XNAT unpacks the zip server-side so list_files() returns
        # the individual files inside.  Unpack here so capture() can enumerate
        # them from _staged_files, matching real XNAT behaviour.
        # Real XNAT put_zip is ADDITIVE by default (does not delete existing files
        # unless an explicit delete-resource step is done first).  Mirror that: do
        # NOT clear _staged_files on overwrite — just append.  Tests that need a
        # clean slate should call seed_resource_files() on a fresh resource.
        try:
            import zipfile
            p = Path(ffn)
            if p.is_file() and zipfile.is_zipfile(p):
                with zipfile.ZipFile(p) as zf:
                    for name in zf.namelist():
                        # Only add regular files (skip directory entries)
                        if not name.endswith("/"):
                            data = zf.read(name)
                            # Use basename so filenames match real XNAT's flat listing
                            basename = name.split("/")[-1]
                            self._staged_files.append((basename, data))
        except Exception:
            pass  # Non-existent or non-zip ffn — backward-compatible, no files staged
        self._record(self._root, "resource.put_zip", (ffn,), {"content": content, "format": format, "tags": tags, "overwrite": overwrite, "_label": self._label})

    # --- T003: real-file enumeration ------------------------------------------

    def list_files(self) -> List[str]:
        """Return filenames of staged real files (T003 fidelity)."""
        return [fn for fn, _ in self._staged_files]

    def num_files(self) -> int:
        """Return the server-reported '# Files' count (T003)."""
        return len(self._staged_files)

    def get_file_bytes(self, fn: str) -> Optional[bytes]:
        """Return staged bytes for *fn*, or None if not found."""
        for name, data in self._staged_files:
            if name == fn:
                return data
        return None


# ---------------------------------------------------------------------------
# FakeAttrs  (supports .mset())
# ---------------------------------------------------------------------------

class FakeAttrs(_CallLog):
    def __init__(self, root: "FakeXNAT", owner_qs: str) -> None:
        self._root = root
        self._owner_qs = owner_qs
        self._store: Dict[str, Any] = {}
        # Fidelity mode: mirrors pyxnat's internal datatype cache.
        # When fidelity_mode=True on the root FakeXNAT, a freshly create()d
        # handle's _datatype starts as None (exactly like real pyxnat) and
        # mset() raises TypeError until _datatype is explicitly set.
        self._datatype: Optional[str] = None

    def _get_datatype(self) -> Optional[str]:
        """Return the cached xsiType, mirroring pyxnat's internal helper."""
        return self._datatype

    def mset(self, mapping: Dict[str, Any]) -> None:
        # Fidelity mode: reproduce pyxnat's quote_from_bytes() TypeError when
        # the datatype cache is empty (as happens on a freshly create()d handle
        # before the xsiType is set).  The real pyxnat raises:
        #   TypeError: quote_from_bytes() expected bytes-like object, got str
        # because it tries to URL-encode the xsiType for the REST path and the
        # None→str coercion path fails.  Gate behind root.fidelity_mode so the
        # existing 718 tests are completely unaffected.
        if self._root.fidelity_mode and self._datatype is None:
            raise TypeError(
                "quote_from_bytes() expected bytes-like object, got str"
                " — pyxnat datatype cache empty (FakeXNAT fidelity mode)"
            )
        self._store.update(mapping)
        self._record(self._root, "attrs.mset", (mapping,), {"_qs": self._owner_qs})


# ---------------------------------------------------------------------------
# FakeSelectable  (subject / experiment / scan)
# ---------------------------------------------------------------------------

class FakeSelectable(_CallLog):
    def __init__(self, root: "FakeXNAT", querystring: str, *, exists: bool = False) -> None:
        self._root = root
        self._qs = querystring
        self._exists = exists
        self.attrs = FakeAttrs(root=root, owner_qs=querystring)

    def exists(self) -> bool:  # noqa: A003
        return self._exists

    def create(self, **kwargs: Any) -> None:
        self._exists = True
        # If xsiType is passed, set it on attrs so datatype cache is populated
        # (matches real pyxnat: create(xsiType='xnat:rfSessionData') sets the type).
        if 'xsiType' in kwargs:
            self.attrs._datatype = kwargs['xsiType']
        self._record(self._root, "selectable.create", (), {"_qs": self._qs, **kwargs})

    def resource(self, label: str) -> FakeResource:
        # Registry lookup: if this resource was pre-seeded via seed_resource_files(),
        # return the same FakeResource instance so the seeded files are accessible.
        # Parse the QS to extract subject/experiment/scan.
        parsed = self._root.select._parse_resource_qs(self._qs)
        if parsed:
            subj, exp, scan, _ = parsed
            key = (subj, exp, scan, label)
            if key in self._root._resources:
                return self._root._resources[key]
            # No pre-seeded resource; create a new one and register it.
            resource = FakeResource(root=self._root, label=label)
            self._root._resources[key] = resource
            return resource

        # Fallback: create a new resource without registry (backward-compatible).
        return FakeResource(root=self._root, label=label)


# ---------------------------------------------------------------------------
# FakeProject
# ---------------------------------------------------------------------------

class FakeProject(_CallLog):
    def __init__(
        self,
        root: "FakeXNAT",
        name: str,
        *,
        exists: bool = True,
        users: Optional[List[str]] = None,
    ) -> None:
        self._root = root
        self._name = name
        self._exists = exists
        self._users: List[str] = users if users is not None else ["testuser"]

    def label(self) -> str:
        return self._name

    def exists(self) -> bool:  # noqa: A003
        return self._exists

    def users(self) -> List[str]:
        return self._users

    def resource(self, folder: str) -> FakeResource:
        return FakeResource(root=self._root, label=folder)


# ---------------------------------------------------------------------------
# FakeSelector  — the object returned at server.select
#               callable:  server.select(qs)  → FakeSelectable
#               attribute: server.select.project(name) → FakeProject
# ---------------------------------------------------------------------------

class FakeSelector:
    """Implements BOTH call and attribute access on server.select."""

    def __init__(self, root: "FakeXNAT") -> None:
        self._root = root

    def __call__(self, querystring: str) -> FakeSelectable:
        # Return the same FakeSelectable handle for a given querystring so that
        # seed_existing() and idempotent-upsert logic can share state.
        if querystring not in self._root._selectables:
            self._root._selectables[querystring] = FakeSelectable(
                root=self._root, querystring=querystring
            )
        return self._root._selectables[querystring]

    def _parse_resource_qs(self, qs: str) -> Optional[tuple]:
        """
        Parse a querystring like:
            /projects/P/subjects/S/experiments/E/scans/SCAN/resources/RES
        or: /project/P/subject/S/experiment/E/scan/SCAN
        and return (subject, experiment, scan, resource_label) or None.

        This enables resource registry lookup for both singular and plural
        XNAT path formats.
        """
        parts = [p for p in qs.split("/") if p]
        try:
            # Try plural format: projects/subjects/experiments/scans/resources
            if "subjects" in parts and "experiments" in parts and "scans" in parts:
                idx_sub = parts.index("subjects")
                idx_exp = parts.index("experiments")
                idx_scan = parts.index("scans")
                subj = parts[idx_sub + 1] if idx_sub + 1 < len(parts) else None
                exp = parts[idx_exp + 1] if idx_exp + 1 < len(parts) else None
                scan = parts[idx_scan + 1] if idx_scan + 1 < len(parts) else None
                # Resource label may be in the QS or provided separately
                resource_label = None
                if "resources" in parts:
                    idx_res = parts.index("resources")
                    resource_label = parts[idx_res + 1] if idx_res + 1 < len(parts) else None
                if subj and exp and scan:
                    return (subj, exp, scan, resource_label)
            # Try singular format: project/subject/experiment/scan (no resources)
            elif "subject" in parts and "experiment" in parts and "scan" in parts:
                idx_sub = parts.index("subject")
                idx_exp = parts.index("experiment")
                idx_scan = parts.index("scan")
                subj = parts[idx_sub + 1] if idx_sub + 1 < len(parts) else None
                exp = parts[idx_exp + 1] if idx_exp + 1 < len(parts) else None
                scan = parts[idx_scan + 1] if idx_scan + 1 < len(parts) else None
                if subj and exp and scan:
                    return (subj, exp, scan, None)
        except (ValueError, IndexError):
            pass
        return None

    def project(self, name: str) -> FakeProject:
        return FakeProject(root=self._root, name=name)


# ---------------------------------------------------------------------------
# FakeXNAT  — top-level server double
# ---------------------------------------------------------------------------

class FakeXNAT(XnatGateway):
    """
    Offline stand-in for a ``pyxnat.Interface`` / ``XnatGateway``.

    Implements ``XnatGateway`` so tests can inject a ``FakeXNAT`` wherever
    production code accepts the ABC.  Use ``FakeGateway`` as a shorter alias.

    Usage::

        fake = FakeXNAT()
        # use like the real server
        subj = fake.select("/project/MYPROJ/subject/S001")
        subj.create()
        res = subj.resource("SRC")
        res.put_zip("/tmp/data.zip", content="IMAGE", format="DICOM", tags="DATA")
        # inspect what happened
        assert fake.calls[0]["op"] == "selectable.create"
        assert fake.calls[1]["op"] == "resource.put_zip"

    Failure injection::

        fake.set_next_failure(TimeoutError("simulated timeout"))
        with pytest.raises(TimeoutError):
            fake.select("/project/X/subject/S").resource("R").put_zip("/f.zip")

    T002 — subject enumeration fidelity:
        Real pyxnat returns internal IDs (``PROJ_S00001``) from wildcard selects;
        human labels are separate attributes.  Use ``seed_subject_label`` to map an
        internal ID to a label, then subclass and override ``list_subjects`` to
        expose EITHER internal IDs (reproducing the bug) OR labels (post-fix).
        ``_subject_labels`` maps internal_id → label for downstream lookup.

    T003 — scan resource fidelity:
        Use ``seed_resource_files(resource, filenames_and_bytes)`` to stage N real
        files in a FakeResource so that ``resource.list_files()`` returns all names
        and ``resource.file(fn).get_copy(dest)`` writes real bytes.

    """

    def __init__(
        self,
        project_name: str = "FAKE_PROJECT",
        project_users: Optional[List[str]] = None,
        fidelity_mode: bool = False,
    ) -> None:
        self.project_name = project_name
        # Stored as _project_users to avoid shadowing the XnatGateway.project_users() method.
        self._project_users: List[str] = project_users if project_users is not None else ["testuser"]
        # Legacy alias kept for backward compat with tests that read fake.project_users directly.
        # (These tests read the list, not call it; the alias allows both access patterns.)
        self.project_users_list: List[str] = self._project_users
        # fidelity_mode=True reproduces pyxnat behaviours invisible to the plain
        # offline double — specifically the post-create() empty datatype cache
        # (#27).  Defaults to False so all 726 existing tests are unaffected.
        self.fidelity_mode: bool = fidelity_mode
        self.calls: List[Dict[str, Any]] = []
        self._next_failure: Optional[BaseException] = None
        self._file_contents: Dict[str, bytes] = {}
        # Registry of FakeSelectable handles keyed by querystring.  Enables
        # seed_existing() for idempotent-upsert tests (T005b) and the shared-
        # handle guarantee in FakeSelector.__call__.
        self._selectables: Dict[str, "FakeSelectable"] = {}
        self.select = FakeSelector(root=self)
        # T002: internal_id → label mapping for subject-label fidelity tests.
        # Populated by seed_subject_label(); looked up by label_for_subject().
        self._subject_labels: Dict[str, str] = {}
        # T002: RF experiment registry — list of dicts with keys:
        #   subject_label, experiment_label, xsi_type
        # Populated by seed_rf_experiment(); exposed by list_experiments_with_type().
        self._experiments: List[Dict[str, Any]] = []
        # T003: registry of FakeResource handles keyed by (subject, experiment, scan, resource_label)
        # so seed_resource_files() can prime the same FakeResource that select() returns.
        self._resources: Dict[tuple, "FakeResource"] = {}

    # ------------------------------------------------------------------
    # Failure injection
    # ------------------------------------------------------------------

    def set_next_failure(self, exc: BaseException) -> None:
        """Next file/resource operation will raise *exc* (cleared after one use)."""
        self._next_failure = exc

    def set_file_content(self, filename: str, content: bytes) -> None:
        """
        Prime the content that ``file.get_copy(dest)`` will write for *filename*.

        Call this before the ``get_copy`` that should return the canned bytes.
        Passing ``None`` removes a previously-set override.
        """
        if content is None:
            self._file_contents.pop(filename, None)
        else:
            self._file_contents[filename] = content

    # ------------------------------------------------------------------
    # Connection lifecycle (no-ops for offline use)
    # ------------------------------------------------------------------

    def disconnect(self) -> None:
        pass

    def get(self, path: str) -> None:  # noqa: A003
        """Stub for server.get('/') liveness check."""
        pass

    # ------------------------------------------------------------------
    # Convenience reset
    # ------------------------------------------------------------------

    def reset_calls(self) -> None:
        """Clear the recorded call log without rebuilding the object."""
        self.calls.clear()

    # ------------------------------------------------------------------
    # T002 helpers — subject label / experiment type fidelity
    # ------------------------------------------------------------------

    def seed_subject_label(self, internal_id: str, label: str) -> None:
        """
        Register a mapping from *internal_id* (e.g. ``PROJ_S00001``) to a
        human-readable *label* (e.g. ``ITEST_SUBJ_0001``).

        Used by tests that reproduce the bug where browse returns internal IDs
        instead of labels.  Gate: no effect unless the test code explicitly calls
        this helper, so all existing tests remain unaffected.
        """
        self._subject_labels[internal_id] = label

    def label_for_subject(self, internal_id: str) -> str:
        """Return the human label for *internal_id*, or *internal_id* if unmapped."""
        return self._subject_labels.get(internal_id, internal_id)

    def seed_rf_experiment(
        self,
        subject_label: str,
        experiment_label: str,
        xsi_type: str = "xnat:rfSessionData",
    ) -> None:
        """
        Stage an RF (or any-type) experiment so that ``list_experiments_with_type``
        returns it.  Also marks the corresponding subject and experiment selectables
        as existing so that capture() / _list_subjects() / _list_experiments() can
        enumerate them during dual-run parity checks.
        """
        self._experiments.append(
            {
                "subject_label": subject_label,
                "experiment_label": experiment_label,
                "xsi_type": xsi_type,
            }
        )
        # Fidelity: ensure the subject and experiment appear in _selectables
        # so capture() enumerates them just as real XNAT would after create().
        # Uses project_name from the FakeXNAT instance (mirrors the single-project
        # assumption in list_experiments_with_type).
        project = self.project_name
        subj_qs = f"/project/{project}/subject/{subject_label}"
        exp_qs = f"{subj_qs}/experiment/{experiment_label}"
        subj_sel = self._selectables.setdefault(
            subj_qs, FakeSelectable(root=self, querystring=subj_qs)
        )
        subj_sel._exists = True
        exp_sel = self._selectables.setdefault(
            exp_qs, FakeSelectable(root=self, querystring=exp_qs)
        )
        exp_sel._exists = True
        if exp_sel.attrs._datatype is None:
            exp_sel.attrs._datatype = xsi_type

    def list_experiments_with_type(self, project_name: str) -> List[Dict[str, Any]]:
        """
        Return a list of experiment dicts with keys:
          subject_label, experiment_label, xsi_type.

        Mirrors real pyxnat's project-level experiment listing which includes an
        xsiType column.  Used by the type-agnostic browse path (T012 fix).
        Returns only seeded experiments; empty list when nothing seeded.
        """
        return list(self._experiments)

    # ------------------------------------------------------------------
    # T003 helper — scan resource file seeding
    # ------------------------------------------------------------------

    def seed_resource_files(
        self,
        resource: "FakeResource",
        files: List[tuple],
    ) -> None:
        """
        Prime *resource* with N real files for byte round-trip tests (T003).

        Parameters
        ----------
        resource : FakeResource to populate.
        files    : List of (filename: str, content: bytes) tuples.
                   Filenames must be unique within the resource.
        """
        for fn, data in files:
            resource._staged_files.append((fn, data))

    def seed_existing(self, querystring: str) -> "FakeSelectable":
        """
        Mark a querystring as already-existing in the fake.

        Pre-seeds a FakeSelectable whose exists() returns True, mirroring
        an orphaned/partial object left behind by a prior failed push.
        Used by idempotent-upsert tests (T005b) to exercise the reuse path.
        """
        sel = self._selectables.setdefault(
            querystring, FakeSelectable(root=self, querystring=querystring)
        )
        sel._exists = True
        return sel

    # ------------------------------------------------------------------
    # XnatGateway ABC — gateway interface methods (T003 Stage 1)
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """No-op: FakeXNAT is always connected."""

    def liveness(self) -> None:
        """No-op: fake is always live."""

    def project_label(self, project_name: str) -> str:
        """Return the fake project label (== project_name)."""
        return self.select.project(project_name).label()

    def project_users(self, project_name: str) -> List[str]:
        """Return the fake project users list."""
        return self.select.project(project_name).users()

    def select(self, querystring: str) -> Any:  # type: ignore[override]
        """Return a FakeSelectable for *querystring*.

        Note: at runtime, ``self.select`` is a ``FakeSelector`` instance (set
        in ``__init__``), so calling ``fake.select(qs)`` invokes
        ``FakeSelector.__call__``.  This class-level method only exists to
        satisfy the ``XnatGateway`` ABC; it is never reached directly.
        """
        # self.select is the FakeSelector instance attribute at runtime.
        raise NotImplementedError("Unreachable: shadowed by instance attribute self.select")

    def exists(self, querystring: str) -> bool:
        """Return True if the object at *querystring* was created."""
        sel = self._selectables.get(querystring)
        return sel._exists if sel is not None else False

    def create(self, querystring: str, **kwargs: Any) -> None:
        """Create the object at *querystring* (marks it as existing)."""
        sel = self._selectables.setdefault(
            querystring, FakeSelectable(root=self, querystring=querystring)
        )
        sel.create(**kwargs)

    def set_attrs(self, querystring: str, mapping: Dict[str, Any]) -> None:
        """Set attrs on the object at *querystring*."""
        sel = self._selectables.setdefault(
            querystring, FakeSelectable(root=self, querystring=querystring)
        )
        sel.attrs.mset(mapping)

    def put_zip(
        self,
        querystring: str,
        resource_label: str,
        ffn: Any,
        *,
        content: str = "",
        format: str = "",
        tags: str = "",
    ) -> None:
        """Record a put_zip call on *resource_label* under *querystring*."""
        sel = self._selectables.setdefault(
            querystring, FakeSelectable(root=self, querystring=querystring)
        )
        sel.resource(resource_label).put_zip(ffn, content=content, format=format, tags=tags)

    def put_file(
        self,
        querystring: str,
        resource_label: str,
        filename: str,
        ffn: Any,
        *,
        content: str = "",
        format: str = "",
        tags: str = "",
        overwrite: Optional[bool] = None,
    ) -> None:
        """Record a file.put call."""
        sel = self._selectables.setdefault(
            querystring, FakeSelectable(root=self, querystring=querystring)
        )
        sel.resource(resource_label).file(filename).put(
            ffn, content=content, format=format, tags=tags, overwrite=overwrite
        )

    def insert_file(
        self,
        querystring: str,
        resource_label: str,
        filename: str,
        data: Any,
        *,
        content: str = "",
        format: str = "",
        tags: str = "",
    ) -> None:
        """Record a file.insert call."""
        sel = self._selectables.setdefault(
            querystring, FakeSelectable(root=self, querystring=querystring)
        )
        sel.resource(resource_label).file(filename).insert(
            data, content=content, format=format, tags=tags
        )

    def get_file_copy(
        self,
        querystring: str,
        resource_label: str,
        filename: str,
        dest: Any,
    ) -> Any:
        """Download *filename* from *resource_label* under *querystring* to *dest*."""
        sel = self._selectables.setdefault(
            querystring, FakeSelectable(root=self, querystring=querystring)
        )
        return sel.resource(resource_label).file(filename).get_copy(dest)

    def delete_file(
        self,
        querystring: str,
        resource_label: str,
        filename: str,
    ) -> None:
        """Record a file.delete call."""
        sel = self._selectables.setdefault(
            querystring, FakeSelectable(root=self, querystring=querystring)
        )
        sel.resource(resource_label).file(filename).delete()

    def list_files(
        self,
        querystring: str,
        resource_label: str,
    ) -> List[str]:
        """
        Return filenames staged in *resource_label* under *querystring*.

        Returns an empty list when no files have been staged (seed_resource_files).
        """
        sel = self._selectables.get(querystring)
        if sel is None:
            return []
        # Use resource registry lookup via FakeSelectable.resource()
        resource = sel.resource(resource_label)
        return resource.list_files()

    def download_resource(
        self,
        querystring: str,
        resource_label: str,
        dest_dir: Any,
    ) -> List[Path]:
        """
        Download all staged files in *resource_label* under *querystring* to *dest_dir*.

        Writes real bytes when seeded via seed_resource_files; placeholder text otherwise.
        Returns list of local paths written.
        """
        dest_dir = Path(dest_dir)
        dest_dir.mkdir(parents=True, exist_ok=True)
        filenames = self.list_files(querystring, resource_label)
        written: List[Path] = []
        for fn in filenames:
            dest = dest_dir / fn
            self.get_file_copy(querystring, resource_label, fn, dest)
            written.append(dest)
        return written

    def list_assessors(self, experiment_qs: str) -> List[str]:
        """
        Return labels of all assessors recorded under *experiment_qs*.

        Supports keep-all monotonic versioning (FR-013, T016):
        next_assessor_label() calls this to determine v(n+1).
        """
        prefix = str(PurePosixPath(experiment_qs) / "assessor") + "/"
        labels = []
        for qs, sel in self._selectables.items():
            if qs.startswith(prefix) and sel._exists:
                # qs is like "/.../assessor/<label>" — take the last segment
                label = qs[len(prefix):]
                if "/" not in label:
                    labels.append(label)
        return labels

    def create_assessor(
        self,
        experiment_qs: str,
        assessor_label: str,
        *,
        xsi_type: str = "xnat:assessorData",
        files: Optional[List[tuple]] = None,
    ) -> None:
        """
        Record an assessor creation under *experiment_qs*.

        Uses fidelity_mode pattern (Phase 7 #27): create with xsiType → empty
        datatype cache populated → set_attrs safe.  Files attached via put_file.

        Records calls under op="assessor.create" and "assessor.file.put"
        (distinct from scan-resource writes recorded as "file.put" / "resource.put_zip").

        H6: Guards parent-experiment existence + sanitizes label.
        Raises FriendlyError if parent experiment does not exist or label is invalid.
        """
        # H6: Guard parent-experiment existence
        if experiment_qs not in self._selectables or not self._selectables[experiment_qs]._exists:
            raise GatewayError(
                FriendlyError(
                    title="Parent experiment not found",
                    message=f"Cannot upload derived data: parent experiment {experiment_qs} does not exist. "
                            f"Please ensure the source experiment has been created before uploading derived data.",
                    recourse=[
                        "Check that the experiment query string is correct",
                        "Verify the source experiment exists in XNAT",
                        "Create the source experiment first, then retry the upload",
                    ],
                )
            )

        # Sanitize assessor_label: reject path separators and parent-directory references
        sanitized = PurePosixPath(assessor_label).name
        if sanitized != assessor_label:
            raise GatewayError(
                FriendlyError(
                    title="Invalid assessor label",
                    message=f"Assessor label contains path separators or invalid characters: {assessor_label}. "
                            f"Labels must be single path components without '/' or '..' references.",
                    recourse=[
                        "Use only alphanumeric characters, hyphens, and underscores in the label",
                        "Remove any '/' or '..' from the label",
                        "Example valid label: SEGMENTATION_CONSENSUS-1.2.3.4",
                    ],
                )
            )

        assessor_qs = str(PurePosixPath(experiment_qs) / "assessor" / assessor_label)
        sel = self._selectables.setdefault(
            assessor_qs, FakeSelectable(root=self, querystring=assessor_qs)
        )
        if not sel._exists:
            sel._exists = True
            if self.fidelity_mode and sel.attrs._datatype is None:
                # Populate datatype cache exactly as create(xsiType=...) does.
                sel.attrs._datatype = xsi_type
            self.calls.append({
                "op": "assessor.create",
                "args": (),
                "kwargs": {"_qs": assessor_qs, "xsiType": xsi_type},
            })
        if files:
            for resource_label, filename, local_path in files:
                self.calls.append({
                    "op": "assessor.file.put",
                    "args": (str(local_path),),
                    "kwargs": {
                        "_assessor_qs": assessor_qs,
                        "_resource": resource_label,
                        "_filename": filename,
                    },
                })


# ---------------------------------------------------------------------------
# FakeGateway alias (T003 export)
# ---------------------------------------------------------------------------

#: Alias for FakeXNAT; use when the callee type-hint is ``XnatGateway``.
FakeGateway = FakeXNAT
