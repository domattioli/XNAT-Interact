"""
xnat_gateway — thin seam naming the pyxnat surface used by XNAT-Interact.

Purpose
-------
This module defines the ``XnatGateway`` ABC and the ``PyxnatGateway`` production
implementation.  All XNAT-Interact application code should obtain a connection
through ``build_gateway``; tests use ``FakeGateway`` (alias of FakeXNAT).

``build_server`` is a shim that returns the underlying ``pyxnat.Interface``
directly; it exists for not-yet-migrated call sites and will be deleted in
Stage 6 once all callers route through ``XnatGateway``.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Abstract base — the 16-call pyxnat surface + 3 extension methods
# ---------------------------------------------------------------------------

class XnatGateway(ABC):
    """
    Abstract gateway over the XNAT server connection.

    Declares exactly the methods this codebase needs; every new XNAT feature
    should add a method here before implementing it anywhere else.

    Method origins are cross-referenced to ``docs/XNAT_MODEL.md §3``.
    """

    # ------------------------------------------------------------------
    # Connection lifecycle (XNAT_MODEL §3: Interface / disconnect)
    # ------------------------------------------------------------------

    @abstractmethod
    def connect(self) -> None:
        """
        Establish the authenticated connection to the XNAT server.

        Origin: ``Interface(server, user, password)`` — §3 row 1.
        """

    @abstractmethod
    def disconnect(self) -> None:
        """
        Close the HTTP session / JSESSION cookie.

        Origin: ``server.disconnect()`` — §3 row 16.
        """

    @abstractmethod
    def liveness(self) -> None:
        """
        Probe the server to confirm connectivity (raises on failure).

        Origin: ``server.get('/')`` — §3 row 15.
        """

    # ------------------------------------------------------------------
    # Project identity (XNAT_MODEL §3: project.label / project.users)
    # ------------------------------------------------------------------

    @abstractmethod
    def project_label(self, project_name: str) -> str:
        """
        Return the project label string for *project_name*.

        Origin: ``server.select.project(name).label()`` — §3 row 14.
        """

    @abstractmethod
    def project_users(self, project_name: str) -> List[str]:
        """
        Return the list of usernames who have access to *project_name*.

        Origin: ``server.select.project(name).users()`` — §3 row 14.
        """

    # ------------------------------------------------------------------
    # Object selection / existence (XNAT_MODEL §3: select / exists)
    # ------------------------------------------------------------------

    @abstractmethod
    def select(self, querystring: str) -> Any:
        """
        Return a subject / experiment / scan handle for *querystring*.

        Origin: ``server.select(querystring)`` — §3 row 2.

        The returned object supports ``.exists()``, ``.create(**kwargs)``,
        ``.attrs.mset(mapping)``, and ``.resource(label)``.
        """

    @abstractmethod
    def exists(self, querystring: str) -> bool:
        """
        Return True if the object at *querystring* already exists on the server.

        Origin: ``obj.exists()`` — §3 row 4.
        """

    # ------------------------------------------------------------------
    # Object creation / attribute setting (XNAT_MODEL §3: create / attrs.mset)
    # ------------------------------------------------------------------

    @abstractmethod
    def create(self, querystring: str, **kwargs: Any) -> None:
        """
        Create the object at *querystring* with optional xsiType kwargs.

        Origin: ``obj.create(**{level: 'xnat:{schema}…Data'})`` — §3 row 5.
        """

    @abstractmethod
    def set_attrs(self, querystring: str, mapping: Dict[str, Any]) -> None:
        """
        Set schema attributes on the object at *querystring*.

        Origin: ``obj.attrs.mset({xpath: value})`` — §3 row 6.
        """

    # ------------------------------------------------------------------
    # File upload (XNAT_MODEL §3: put_zip / file.put / file.insert)
    # ------------------------------------------------------------------

    @abstractmethod
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
        """
        Upload a zip archive to the resource *resource_label* under *querystring*.

        Origin: ``resource.put_zip(ffn, content=, format=, tags=)`` — §3 row 8.
        """

    @abstractmethod
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
        """
        Upload a single file to the resource *resource_label* under *querystring*.

        Origin: ``resource.file(name).put(ffn, content=, format=, tags=, overwrite=)``
        — §3 row 9.
        """

    @abstractmethod
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
        """
        Insert raw bytes / text into *filename* on the resource *resource_label*
        under *querystring*.

        Origin: ``file.insert(data, content=, format=, tags=)`` — §3 row 12.
        """

    # ------------------------------------------------------------------
    # File download / delete (XNAT_MODEL §3: get_copy / delete)
    # ------------------------------------------------------------------

    @abstractmethod
    def get_file_copy(
        self,
        querystring: str,
        resource_label: str,
        filename: str,
        dest: Any,
    ) -> Any:
        """
        Download *filename* from resource *resource_label* under *querystring*
        to the local path *dest*.  Returns *dest*.

        Origin: ``file.get_copy(dest)`` — §3 row 10.
        """

    @abstractmethod
    def delete_file(
        self,
        querystring: str,
        resource_label: str,
        filename: str,
    ) -> None:
        """
        Delete *filename* from resource *resource_label* under *querystring*.

        Origin: ``file.delete()`` — §3 row 11.
        """

    # ------------------------------------------------------------------
    # File enumeration + bulk download (XNAT_MODEL §6 §3 extension)
    # ------------------------------------------------------------------

    @abstractmethod
    def list_files(
        self,
        querystring: str,
        resource_label: str,
    ) -> List[str]:
        """
        Return the filenames present in resource *resource_label* under
        *querystring*.

        Extension of §3 surface (§6 rec 3 — fix download against the API).
        Corresponds to ``Resource.get(dest_dir)`` enumeration or
        ``resource.files()`` listing.
        """

    @abstractmethod
    def download_resource(
        self,
        querystring: str,
        resource_label: str,
        dest_dir: Any,
    ) -> List[Path]:
        """
        Download all files in resource *resource_label* under *querystring*
        to *dest_dir*.  Returns list of local paths written.

        Extension of §3 surface (§6 rec 3 — ``Resource.get(dest_dir)``).
        """

    # ------------------------------------------------------------------
    # Assessor (XNAT_MODEL §1 — derived data slot)
    # ------------------------------------------------------------------

    @abstractmethod
    def create_assessor(
        self,
        experiment_qs: str,
        assessor_label: str,
        *,
        xsi_type: str = "xnat:assessorData",
        files: Optional[List[tuple]] = None,
    ) -> None:
        """
        Create an assessor under *experiment_qs* with the given *assessor_label*
        and *xsi_type*, then attach *files* as resource entries.

        *files* is a list of ``(resource_label, filename, local_path)`` tuples.
        Uses the Phase 7 #27 datatype-cache pattern: create with xsiType, then
        populate attributes via set_attrs.

        Extension method for derived / consensus data (§1 Assessor slot,
        §6 rec 2 — use assessors for new derived results).
        """


# ---------------------------------------------------------------------------
# PyxnatGateway — production implementation (raw pyxnat calls verbatim)
# ---------------------------------------------------------------------------

class PyxnatGateway(XnatGateway):
    """
    Production ``XnatGateway`` backed by pyxnat.

    ``build_gateway(url, user, password)`` is the preferred constructor.
    The underlying ``pyxnat.Interface`` is accessible via ``.server`` for
    the not-yet-migrated call sites that still use ``build_server()``.
    """

    def __init__(self, url: str, user: str, password: str) -> None:
        self._url = url
        self._user = user
        self._password = password
        self.server = None  # populated in connect()

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> None:
        import pyxnat  # local import keeps the module importable without pyxnat installed
        self.server = pyxnat.Interface(server=self._url, user=self._user, password=self._password)

    def disconnect(self) -> None:
        self.server.disconnect()

    def liveness(self) -> None:
        self.server.get('/')

    # ------------------------------------------------------------------
    # Project identity
    # ------------------------------------------------------------------

    def project_label(self, project_name: str) -> str:
        return self.server.select.project(project_name).label()

    def project_users(self, project_name: str) -> List[str]:
        return self.server.select.project(project_name).users()

    # ------------------------------------------------------------------
    # Object selection / existence
    # ------------------------------------------------------------------

    def select(self, querystring: str) -> Any:
        return self.server.select(querystring)

    def exists(self, querystring: str) -> bool:
        return self.server.select(querystring).exists()

    # ------------------------------------------------------------------
    # Object creation / attribute setting
    # ------------------------------------------------------------------

    def create(self, querystring: str, **kwargs: Any) -> None:
        self.server.select(querystring).create(**kwargs)

    def set_attrs(self, querystring: str, mapping: Dict[str, Any]) -> None:
        self.server.select(querystring).attrs.mset(mapping)

    # ------------------------------------------------------------------
    # File upload
    # ------------------------------------------------------------------

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
        self.server.select(querystring).resource(resource_label).put_zip(ffn, content=content, format=format, tags=tags)

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
        self.server.select(querystring).resource(resource_label).file(filename).put(ffn, content=content, format=format, tags=tags, overwrite=overwrite)

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
        self.server.select(querystring).resource(resource_label).file(filename).insert(data, content=content, format=format, tags=tags)

    # ------------------------------------------------------------------
    # File download / delete
    # ------------------------------------------------------------------

    def get_file_copy(
        self,
        querystring: str,
        resource_label: str,
        filename: str,
        dest: Any,
    ) -> Any:
        return self.server.select(querystring).resource(resource_label).file(filename).get_copy(dest)

    def delete_file(
        self,
        querystring: str,
        resource_label: str,
        filename: str,
    ) -> None:
        self.server.select(querystring).resource(resource_label).file(filename).delete()

    # ------------------------------------------------------------------
    # File enumeration + bulk download
    # ------------------------------------------------------------------

    def list_files(
        self,
        querystring: str,
        resource_label: str,
    ) -> List[str]:
        return list(self.server.select(querystring).resource(resource_label).files().get())

    def download_resource(
        self,
        querystring: str,
        resource_label: str,
        dest_dir: Any,
    ) -> List[Path]:
        dest_dir = Path(dest_dir)
        dest_dir.mkdir(parents=True, exist_ok=True)
        self.server.select(querystring).resource(resource_label).get(str(dest_dir), extract=False)
        return list(dest_dir.iterdir())

    # ------------------------------------------------------------------
    # Assessor
    # ------------------------------------------------------------------

    def create_assessor(
        self,
        experiment_qs: str,
        assessor_label: str,
        *,
        xsi_type: str = "xnat:assessorData",
        files: Optional[List[tuple]] = None,
    ) -> None:
        # Phase 7 #27 datatype-cache pattern:
        # 1. create with xsiType so the cache is populated immediately
        # 2. then set attrs (mset will not raise TypeError)
        assessor_qs = experiment_qs + "/assessor/" + assessor_label
        assessor = self.server.select(assessor_qs)
        if not assessor.exists():
            assessor.create(xsiType=xsi_type)
        if files:
            for resource_label, filename, local_path in files:
                assessor.resource(resource_label).file(filename).put(
                    str(local_path), content="DERIVED", format="NIFTI", tags="DATA"
                )


# ---------------------------------------------------------------------------
# Factory functions
# ---------------------------------------------------------------------------

def build_gateway(url: str, user: str, password: str) -> PyxnatGateway:
    """
    Build and return a ``PyxnatGateway`` for *url*.

    The gateway is NOT yet connected; call ``gateway.connect()`` (or use
    ``XNATConnection`` which calls it during ``_establish_connection``).

    Parameters
    ----------
    url:      Full XNAT server URL, e.g. ``https://rpacs.iibi.uiowa.edu/xnat/``
    user:     XNAT username
    password: XNAT password

    Returns
    -------
    PyxnatGateway
    """
    return PyxnatGateway(url=url, user=user, password=password)


def build_server(url: str, user: str, password: str):
    """
    Shim: build a ``PyxnatGateway``, connect it, and return the underlying
    ``pyxnat.Interface`` for not-yet-migrated call sites.

    .. deprecated::
        Stage 6 will delete this function once all callers route through
        ``XnatGateway``.  New code must use ``build_gateway`` instead.

    Parameters
    ----------
    url:      Full XNAT server URL
    user:     XNAT username
    password: XNAT password

    Returns
    -------
    pyxnat.Interface
    """
    gw = build_gateway(url=url, user=user, password=password)
    gw.connect()
    return gw.server
