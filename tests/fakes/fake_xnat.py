"""
FakeXNAT — offline test double for the pyxnat surface used by XNAT-Interact.

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

Failure injection:
  [x] fake.set_next_failure(exc)  — next call on any FakeFile or FakeResource
      operation raises exc.  Automatically cleared after one use.
-----------------------------------------------------------------------
"""
from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional


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
        self._record(self._root, "selectable.create", (), {"_qs": self._qs, **kwargs})

    def resource(self, label: str) -> FakeResource:
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

    def project(self, name: str) -> FakeProject:
        return FakeProject(root=self._root, name=name)


# ---------------------------------------------------------------------------
# FakeXNAT  — top-level server double
# ---------------------------------------------------------------------------

class FakeXNAT:
    """
    Offline stand-in for a ``pyxnat.Interface`` instance.

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
        self.project_users: List[str] = project_users if project_users is not None else ["testuser"]
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
        returns it.  Subject *subject_label* and experiment *experiment_label* must
        already be reachable via ``list_subjects`` / ``list_experiments`` in the
        subclass; this helper only adds the xsiType metadata.
        """
        self._experiments.append(
            {
                "subject_label": subject_label,
                "experiment_label": experiment_label,
                "xsi_type": xsi_type,
            }
        )

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
