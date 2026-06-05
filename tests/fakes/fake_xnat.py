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
        # Use canned content if set on the root fake, otherwise write a placeholder.
        canned: Optional[bytes] = self._root._file_contents.get(self._filename)
        if canned is not None:
            dest.write_bytes(canned)
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

    def file(self, fn: str) -> FakeFile:
        return FakeFile(resource=self, filename=fn)

    def put_zip(self, ffn: Any, *, content: str = "", format: str = "", tags: str = "", overwrite: Optional[bool] = None) -> None:
        self._maybe_raise(self._root)
        self._record(self._root, "resource.put_zip", (ffn,), {"content": content, "format": format, "tags": tags, "overwrite": overwrite, "_label": self._label})


# ---------------------------------------------------------------------------
# FakeAttrs  (supports .mset())
# ---------------------------------------------------------------------------

class FakeAttrs(_CallLog):
    def __init__(self, root: "FakeXNAT", owner_qs: str) -> None:
        self._root = root
        self._owner_qs = owner_qs
        self._store: Dict[str, Any] = {}

    def mset(self, mapping: Dict[str, Any]) -> None:
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
        return FakeSelectable(root=self._root, querystring=querystring)

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

    """

    def __init__(
        self,
        project_name: str = "FAKE_PROJECT",
        project_users: Optional[List[str]] = None,
    ) -> None:
        self.project_name = project_name
        self.project_users: List[str] = project_users if project_users is not None else ["testuser"]
        self.calls: List[Dict[str, Any]] = []
        self._next_failure: Optional[BaseException] = None
        self._file_contents: Dict[str, bytes] = {}
        self.select = FakeSelector(root=self)

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
