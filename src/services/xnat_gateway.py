"""
xnat_gateway — thin seam naming the pyxnat surface used by XNAT-Interact.

Purpose
-------
This module is the **single named seam** between XNAT-Interact application code
and the pyxnat library.  Production code should obtain a server object through
``build_server``; tests swap in ``FakeXNAT`` (tests/fakes/fake_xnat.py) without
touching this module.

Used pyxnat surface (exhaustive list as of initial seam extraction)
-------------------------------------------------------------------
All paths go through the object returned by ``build_server`` (called ``server``):

1.  server.select(querystring)
        Returns a subject / experiment / scan instance.
        Methods used on the result:
            .exists() -> bool
            .create(**kwargs)
            .attrs.mset(mapping)
            .resource(label) -> resource

2.  server.select.project(name)
        Returns a project handle.
        Methods used:
            .label() -> str
            .exists() -> bool
            .users() -> list[str]
            .resource(folder) -> resource

3.  resource.put_zip(ffn, content=, format=, tags=)
        Uploads a zip archive to an XNAT resource.

4.  resource.file(fn) -> file
        Returns a file handle within a resource.

5.  file.put(ffn, content=, format=, tags=, overwrite=)
        Uploads a single file to XNAT.

6.  file.get_copy(dest) -> dest
        Downloads a file from XNAT to a local path.

7.  file.delete()
        Deletes a file from XNAT.

8.  file.insert(data, content=, format=, tags=)
        Inserts raw bytes / text into an XNAT resource file.

9.  server.disconnect()
        Closes the HTTP session.

Injection hook
--------------
``XNATConnection`` does not yet expose a constructor-level injection hook
(deferred — adding ``server=None`` param would be safe but is out of scope for
Phase 1 Task A).  Tests exercise the fake directly; wiring the fake into
``XNATConnection`` is Task B / future work.
"""
from __future__ import annotations


def build_server(url: str, user: str, password: str):
    """
    Build and return a real ``pyxnat.Interface`` for *url*.

    This is the production path.  In tests, do NOT call this function —
    construct ``tests.fakes.fake_xnat.FakeXNAT`` directly instead.

    Parameters
    ----------
    url:      Full XNAT server URL, e.g. ``https://rpacs.iibi.uiowa.edu/xnat/``
    user:     XNAT username
    password: XNAT password

    Returns
    -------
    pyxnat.Interface
        A connected (but not yet verified) Interface object.
    """
    import pyxnat  # local import keeps the module importable without pyxnat installed

    return pyxnat.Interface(server=url, user=user, password=password)
