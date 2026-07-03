"""
xnat_gateway — thin seam naming the pyxnat surface used by XNAT-Interact.

Purpose
-------
This module defines the ``XnatGateway`` ABC and the ``PyxnatGateway`` production
implementation.  All XNAT-Interact application code should obtain a connection
through ``build_gateway``; tests use ``FakeGateway`` (alias of FakeXNAT).
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path, PurePosixPath
from typing import Any, Dict, List, Optional

from src.services.errors import FriendlyError


# ---------------------------------------------------------------------------
# Exception — wraps FriendlyError for gateway-layer failures
# ---------------------------------------------------------------------------

class GatewayError(Exception):
    """
    Raised when a gateway operation fails.

    Carries a :class:`~src.services.errors.FriendlyError` as ``friendly``
    so callers can render a user-facing message via ``errors.render()``.
    """
    def __init__(self, friendly: FriendlyError) -> None:
        self.friendly = friendly
        super().__init__(friendly.message)


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
    def list_assessors(self, experiment_qs: str) -> List[str]:
        """
        Return the list of assessor labels that exist under *experiment_qs*.

        Used by keep-all monotonic versioning (FR-013, T016) to determine the
        next version number before calling create_assessor.

        Returns an empty list when the experiment has no assessors or does not
        exist; never raises on "not found".
        """

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
        if self.server is None:
            return
        self.server.disconnect()
        self.server = None

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
        # GAP-RF: pyxnat's resource.put_zip() resolves the resource to its numeric
        # abstractresource ID and PUTs to .../resources/<numeric>/files/<fn>.
        # On XNAT 1.9.3 that numeric-ID path returns 404 under rf-typed sessions.
        # Verified live: label-addressed PUT with extract=true works; numeric-ID does not.
        # Fix: build label-addressed REST URI from the querystring, PUT zip body with
        # extract=true so XNAT extracts files server-side (zip itself not kept).
        # Guard: only apply when server is a real pyxnat Interface (has _exec).
        if hasattr(self.server, '_exec'):
            rest_base = self._qs_to_rest_path(querystring)
            self._ensure_resource(rest_base, resource_label)
            uri = f"{rest_base}/resources/{resource_label}/files"
            with open(str(ffn), "rb") as fh:
                body = fh.read()
            params: dict = {"extract": "true", "inbody": "true"}
            if content:
                params["content"] = content
            if format:
                params["format"] = format
            if tags:
                params["tags"] = tags
            self.server._exec(
                uri,
                method="PUT",
                body=body,
                headers={"Content-Type": "application/zip"},
                params=params,
            )
        else:
            # Fallback for test doubles / mocks that lack _exec (e.g. FakeXNAT).
            self.server.select(querystring).resource(resource_label).put_zip(
                ffn, content=content, format=format, tags=tags
            )

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
        # GAP-RF: same numeric-ID issue as put_zip — pyxnat resolves resource to
        # numeric ID for file.put() too.  Use label-addressed REST URI directly.
        if hasattr(self.server, '_exec'):
            rest_base = self._qs_to_rest_path(querystring)
            self._ensure_resource(rest_base, resource_label)
            uri = f"{rest_base}/resources/{resource_label}/files/{filename}"
            with open(str(ffn), "rb") as fh:
                body = fh.read()
            params: dict = {"inbody": "true"}
            if content:
                params["content"] = content
            if format:
                params["format"] = format
            if tags:
                params["tags"] = tags
            if overwrite:
                params["overwrite"] = "true"
            self.server._exec(
                uri,
                method="PUT",
                body=body,
                headers={"Content-Type": "application/octet-stream"},
                params=params,
            )
        else:
            # Fallback for test doubles / mocks that lack _exec.
            self.server.select(querystring).resource(resource_label).file(filename).put(
                ffn, content=content, format=format, tags=tags, overwrite=overwrite
            )

    # Singular-to-plural segment mapping for XNAT REST API paths.
    _SINGULAR_TO_PLURAL = {
        "project": "projects",
        "subject": "subjects",
        "experiment": "experiments",
        "scan": "scans",
        "resource": "resources",
        "assessor": "assessors",
    }

    @staticmethod
    def _qs_to_rest_path(querystring: str) -> str:
        """
        Convert a singular pyxnat query string to a plural XNAT REST path.

        e.g. /project/P/subject/S/experiment/E/scan/0
          -> /data/projects/P/subjects/S/experiments/E/scans/0

        Leading /data prefix is added.  Unknown segments pass through unchanged.
        """
        parts = [p for p in querystring.split("/") if p]
        out: list = []
        for seg in parts:
            out.append(PyxnatGateway._SINGULAR_TO_PLURAL.get(seg, seg))
        return "/data/" + "/".join(out)

    def _ensure_resource(self, rest_base: str, resource_label: str) -> None:
        """
        Ensure the resource *resource_label* exists under *rest_base*.

        Issues a PUT to ``{rest_base}/resources/{resource_label}`` with no body,
        which is a no-op if the resource already exists (XNAT 1.9.x idempotent
        resource creation).  Failures are silenced — if the resource truly does
        not exist the subsequent file PUT will fail with a clear 404.
        """
        uri = f"{rest_base}/resources/{resource_label}"
        try:
            self.server._exec(uri, method="PUT")
        except Exception:  # noqa: BLE001 — creation failure is non-fatal here
            pass

    def _assessor_uri_rewrite(self, querystring: str) -> Optional[str]:
        """
        If *querystring* contains an ``/assessor/`` component, resolve the
        assessor's accession ID and return ``/data/experiments/<id>``.

        Returns None when the querystring is not assessor-addressed or when
        ID resolution fails (caller falls through to standard pyxnat path).
        """
        if "/assessor/" not in querystring:
            return None
        parts = [p for p in querystring.split("/") if p]
        try:
            ass_idx = parts.index("assessor")
            assessor_label = parts[ass_idx + 1] if ass_idx + 1 < len(parts) else None
            exp_parts = parts[:ass_idx]
            experiment_qs = "/" + "/".join(exp_parts)
            if assessor_label and experiment_qs:
                aid = self._assessor_id(experiment_qs, assessor_label)
                if aid:
                    return f"/data/experiments/{aid}"
        except (IndexError, ValueError):
            pass
        return None

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
        # GAP-001: assessor label-paths 404 on XNAT 1.9.3; use accession-ID URI.
        assessor_uri = self._assessor_uri_rewrite(querystring)
        if assessor_uri:
            uri = f"{assessor_uri}/resources/{resource_label}/files/{filename}"
            # data is polymorphic: file path (str/Path) OR raw content (bytes/str).
            # Apply the same length-guard as the non-assessor branch to avoid
            # OSError ENAMETOOLONG when data is a long JSON content string.
            if isinstance(data, bytes):
                body = data
            elif isinstance(data, (str, Path)):
                try:
                    p = Path(str(data))
                    is_file = p.is_file()
                except OSError:
                    is_file = False
                if is_file:
                    with open(str(data), "rb") as fh:
                        body = fh.read()
                else:
                    body = str(data).encode("utf-8")
            else:
                body = bytes(data)
            # _exec PUT: body→requests data=; **kwargs forwarded to requests.put().
            # Verified: params= kwarg reaches requests correctly (no collision for PUT).
            self.server._exec(
                uri,
                method="PUT",
                body=body,
                headers={"Content-Type": "application/octet-stream"},
                params={
                    "format": format or "JSON",
                    "content": content or "DERIVED",
                    "inbody": "true",
                },
            )
            return

        # Non-assessor path: use label-addressed REST PUT (same numeric-ID fix).
        if hasattr(self.server, '_exec'):
            rest_base = self._qs_to_rest_path(querystring)
            self._ensure_resource(rest_base, resource_label)
            uri = f"{rest_base}/resources/{resource_label}/files/{filename}"
            # data may be a local file path (Path/str) OR raw bytes/str content.
            # The original pyxnat file.insert(data) accepted raw content directly;
            # callers like push_to_xnat pass the file content string, not a path.
            if isinstance(data, bytes):
                body = data
            elif isinstance(data, (str, Path)):
                # Try treating as a file path; fall through to raw-string encoding
                # if the string is too long to be a path or the file doesn't exist.
                try:
                    p = Path(str(data))
                    is_file = p.is_file()
                except OSError:
                    is_file = False
                if is_file:
                    with open(str(data), "rb") as fh:
                        body = fh.read()
                else:
                    body = str(data).encode("utf-8")
            else:
                body = bytes(data)
            params: dict = {"inbody": "true"}
            if content:
                params["content"] = content
            if format:
                params["format"] = format
            if tags:
                params["tags"] = tags
            self.server._exec(
                uri,
                method="PUT",
                body=body,
                headers={"Content-Type": "application/octet-stream"},
                params=params,
            )
        else:
            self.server.select(querystring).resource(resource_label).file(filename).insert(
                data, content=content, format=format, tags=tags
            )

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
        # GAP-001: assessor label-paths 404 on XNAT 1.9.3; use accession-ID URI.
        assessor_uri = self._assessor_uri_rewrite(querystring)
        if assessor_uri:
            uri = f"{assessor_uri}/resources/{resource_label}/files/{filename}"
            # _exec GET returns response.content (bytes); write to dest.
            # NEVER pass out= to _exec — it flows to requests.get() which has no out= param.
            content_bytes = self.server._exec(uri, method="GET")
            dest = Path(dest)
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(content_bytes)
            return dest

        # Non-assessor path: use standard pyxnat method
        return self.server.select(querystring).resource(resource_label).file(filename).get_copy(dest)

    def delete_file(
        self,
        querystring: str,
        resource_label: str,
        filename: str,
    ) -> None:
        # GAP-001: assessor label-paths 404 on XNAT 1.9.3; use accession-ID URI.
        assessor_uri = self._assessor_uri_rewrite(querystring)
        if assessor_uri:
            uri = f"{assessor_uri}/resources/{resource_label}/files/{filename}"
            self.server._exec(uri, method="DELETE")
            return

        # Non-assessor path: use standard pyxnat method
        self.server.select(querystring).resource(resource_label).file(filename).delete()

    # ------------------------------------------------------------------
    # Assessor listing
    # ------------------------------------------------------------------

    def list_assessors(self, experiment_qs: str) -> List[str]:
        """
        Return assessor labels that exist under *experiment_qs*.

        Wraps pyxnat's ``experiment.assessors().get()`` call.
        Returns an empty list when the experiment has no assessors or does not
        exist (never raises on "not found").
        """
        try:
            experiment = self.server.select(experiment_qs)
            if not experiment.exists():
                return []
            labels = list(experiment.assessors().get())
            return labels
        except Exception:  # noqa: BLE001 — fail-soft; versioning can still proceed
            return []

    # ------------------------------------------------------------------
    # File enumeration + bulk download
    # ------------------------------------------------------------------

    def list_files(
        self,
        querystring: str,
        resource_label: str,
    ) -> List[str]:
        import json as _json
        # GAP-001: assessor label-paths 404 on XNAT 1.9.3; use accession-ID URI.
        assessor_uri = self._assessor_uri_rewrite(querystring)
        if assessor_uri:
            uri = f"{assessor_uri}/resources/{resource_label}/files"
            # _exec GET with body dict → passes body as params= to requests.get().
            # Returns response.content as bytes.
            raw = self.server._exec(uri, method="GET", body={"format": "json"})
            data = _json.loads(raw)
            # XNAT ResultSet uses capitalised "Name" key.
            return [f["Name"] for f in data.get("ResultSet", {}).get("Result", [])]

        # Non-assessor path: use standard pyxnat method
        return list(self.server.select(querystring).resource(resource_label).files().get())

    def download_resource(
        self,
        querystring: str,
        resource_label: str,
        dest_dir: Any,
    ) -> List[Path]:
        dest_dir = Path(dest_dir)
        # Download into a clean sub-directory to avoid mixing pre-existing
        # contents with newly downloaded files (audit note: avoid misleading
        # pre-existing dir contents).
        download_dir = dest_dir / "_xnat_download"
        download_dir.mkdir(parents=True, exist_ok=True)
        try:
            self.server.select(querystring).resource(resource_label).get(
                str(download_dir), extract=True
            )
        except Exception as exc:  # noqa: BLE001  # fail-soft: propagate as GatewayError
            raise GatewayError(
                FriendlyError(
                    title="Resource download failed",
                    message=f"Failed to download resource '{resource_label}': {exc}",
                    recourse=["Check XNAT connectivity and resource existence"],
                )
            ) from exc
        # pyxnat with extract=True unpacks into <download_dir>/<resource_label>/files/...
        # or a similar nested layout. Collect all real leaf files, skipping any
        # residual .zip archives left by pyxnat.
        files = [
            p for p in download_dir.rglob("*")
            if p.is_file() and p.suffix.lower() != ".zip"
        ]
        return files

    # ------------------------------------------------------------------
    # Assessor
    # ------------------------------------------------------------------

    def _assessor_id(self, experiment_qs: str, assessor_label: str) -> Optional[str]:
        """
        Resolve the accession ID of an assessor by label.

        Returns the assessor's ID (e.g. 'Xnat4Tests_E00123') or None if
        resolution fails. Tries assessor.id() first, then falls back to
        assessor.attrs.get('ID').

        Args:
            experiment_qs: parent experiment querystring
            assessor_label: label of the assessor

        Returns:
            assessor accession ID, or None if not found
        """
        assessor_qs = str(PurePosixPath(experiment_qs) / "assessor" / assessor_label)
        try:
            assessor = self.server.select(assessor_qs)
            # Try id() method first (preferred)
            try:
                aid = assessor.id()
                if aid:
                    return aid
            except Exception:  # noqa: BLE001
                pass
            # Fall back to attrs lookup
            aid = assessor.attrs.get("ID")
            if aid:
                return aid
        except Exception:  # noqa: BLE001
            pass
        return None

    def create_assessor(
        self,
        experiment_qs: str,
        assessor_label: str,
        *,
        xsi_type: str = "xnat:assessorData",
        files: Optional[List[tuple]] = None,
    ) -> None:
        # H6: Guard parent-experiment existence + sanitize label
        if not self.exists(experiment_qs):
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

        # Phase 7 #27 datatype-cache pattern:
        # 1. create with xsiType so the cache is populated immediately
        # 2. then set attrs (mset will not raise TypeError)
        assessor_qs = str(PurePosixPath(experiment_qs) / "assessor" / assessor_label)
        assessor = self.server.select(assessor_qs)
        if not assessor.exists():
            # Try create() with the assessors kwarg (pyxnat convention for assessors).
            # If that fails, fall back to xsiType (legacy/deprecated but may work).
            try:
                assessor.create(assessors=xsi_type)
            except TypeError:
                # Fall back to xsiType kwarg if assessors= not supported
                assessor.create(xsiType=xsi_type)

        if files:
            # GAP-001 fix: pyxnat's nested assessor-resource file path
            # (.../experiments/<exp>/assessors/<label>/resources/<N>/files/<f>)
            # returns HTTP 404 on XNAT 1.9.3.  Assessors are themselves stored
            # as experiments; PUT via the assessor's direct experiment URI works:
            # PUT /data/experiments/<aid>/resources/<label>/files/<name>
            # Resolve the assessor accession ID (e.g. Xnat4Tests_E#####).
            aid = self._assessor_id(experiment_qs, assessor_label)
            if not aid:
                raise GatewayError(
                    FriendlyError(
                        title="Assessor ID resolution failed",
                        message=(
                            f"Could not resolve accession ID for assessor '{assessor_label}' "
                            f"under {experiment_qs}. "
                            f"The assessor may not have been created successfully or "
                            f"the XNAT server may not have assigned an ID yet."
                        ),
                        recourse=[
                            "Check that the parent experiment exists",
                            "Verify the assessor label is correct",
                            "Check XNAT connectivity and server status",
                        ],
                    )
                )

            for resource_label, filename, local_path in files:
                uri = (
                    f"/data/experiments/{aid}/resources/{resource_label}"
                    f"/files/{filename}"
                )
                with open(str(local_path), "rb") as fh:
                    body = fh.read()
                resp = self.server._exec(
                    uri,
                    method="PUT",
                    body=body,
                    headers={"Content-Type": "application/octet-stream"},
                    params={
                        "format": "NIFTI",
                        "content": "DERIVED",
                        "inbody": "true",
                    },
                )
                # _exec raises on 4xx/5xx; but verify for extra safety
                if hasattr(resp, "status_code") and resp.status_code >= 400:
                    raise GatewayError(
                        FriendlyError(
                            title="Assessor file upload failed",
                            message=(
                                f"PUT {uri} returned HTTP {resp.status_code}. "
                                f"File: {filename}"
                            ),
                            recourse=["Check XNAT connectivity and assessor existence"],
                        )
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
    Build and return a connected pyxnat.Interface server handle.

    Used by app/logic/auth.py's default connect factory. Unlike build_gateway()
    which returns an XnatGateway ABC, this returns a raw pyxnat.Interface
    compatible with the preflight checks (server.get('/')).

    DECISION: Minimal path (sufficient for auth.py + browse.py/download.py).
    Returns the connected Interface directly. The pyxnat.Interface object
    already supports:
      - server.select(qs) — QueryString selection (used by browse, download)
      - server.get('/') — liveness check (used by auth)

    Optional hooks (guarded by hasattr in caller code):
      - server.list_subjects_with_labels(project) — not on Interface; caller guards
      - server.label_for_subject(id) — not on Interface; caller guards
      - server.file_count(...) — not on Interface; caller guards

    Parameters
    ----------
    url:      Full XNAT server URL, e.g. ``https://rpacs.iibi.uiowa.edu/xnat/``
    user:     XNAT username
    password: XNAT password

    Returns
    -------
    pyxnat.Interface — connected and authenticated to the server.

    Raises
    ------
    Any exception raised by pyxnat.Interface() during connection or auth.
    """
    import pyxnat  # local import keeps the module importable without pyxnat installed
    return pyxnat.Interface(server=url, user=user, password=password)
