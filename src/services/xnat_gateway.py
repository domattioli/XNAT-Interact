"""
xnat_gateway — thin seam naming the pyxnat surface used by XNAT-Interact.

Purpose
-------
This module defines the ``XnatGateway`` ABC and the ``PyxnatGateway`` production
implementation.  All XNAT-Interact application code should obtain a connection
through ``build_gateway``; tests use ``FakeGateway`` (alias of FakeXNAT).
"""
from __future__ import annotations