"""
tests/test_build_server.py — Offline tests for src/services/xnat_gateway.build_server.

Contract checks:
  1. build_server imports cleanly from xnat_gateway (no ImportError).
  2. auth.py's default factory can import and resolve without error.
  3. build_server returns an object supporting .select() and .get() (pyxnat.Interface).
  4. Delegates all pyxnat methods via __getattr__ (transparent passthrough).

NO network calls; uses FakeXNAT and mocks where needed.
"""
from __future__ import annotations

from unittest import mock
from typing import Any


# Import the function under test.
from src.services.xnat_gateway import build_server


class MockPyxnatInterface:
    """Mock object simulating a pyxnat.Interface for offline testing."""

    def __init__(self, server: str, user: str, password: str) -> None:
        self._server = server
        self._user = user
        self._password = password
        self._connected = True

    def get(self, path: str) -> str:
        """Mock server.get('/') for liveness check."""
        if not self._connected:
            raise RuntimeError("Not connected")
        return f"Mock response from {path}"

    def select(self, querystring: str) -> Any:
        """Mock server.select(qs) for QueryString selection."""
        if not self._connected:
            raise RuntimeError("Not connected")
        return MockSelectable(self, querystring)

    def disconnect(self) -> None:
        """Mock server.disconnect()."""
        self._connected = False


class MockSelectable:
    """Mock object returned by server.select(qs)."""

    def __init__(self, server: MockPyxnatInterface, qs: str) -> None:
        self._server = server
        self._qs = qs

    def exists(self) -> bool:
        """Mock selectable.exists()."""
        return True

    def get(self) -> list:
        """Mock selectable.get() for wildcard listing."""
        return ["item1", "item2"]


# ---------------------------------------------------------------------------
# Test 1: build_server imports cleanly
# ---------------------------------------------------------------------------

def test_build_server_imports_cleanly():
    """build_server should be importable without error."""
    # If this test passes, the import succeeded — no ImportError.
    assert callable(build_server)


# ---------------------------------------------------------------------------
# Test 2: auth.py's default factory can resolve build_server
# ---------------------------------------------------------------------------

def test_auth_default_factory_imports_build_server():
    """
    app/logic/auth._default_connect_factory should be able to import
    build_server without ImportError.
    """
    from app.logic.auth import _default_connect_factory

    # _default_connect_factory tries to import build_server inside its body.
    # If this succeeds, the import chain is unbroken.
    assert callable(_default_connect_factory)


# ---------------------------------------------------------------------------
# Test 3: build_server with mocked pyxnat.Interface (patched at import time)
# ---------------------------------------------------------------------------

@mock.patch("pyxnat.Interface", MockPyxnatInterface)
def test_build_server_returns_connected_interface():
    """
    build_server should return an object that supports .select() and .get().
    """
    server = build_server("http://test.com/xnat/", "testuser", "testpass")

    # Should support get('/') for liveness check (used by auth.py).
    result = server.get("/")
    assert "Mock response" in result

    # Should support select(qs) for QueryString selection (used by browse.py, download.py).
    selectable = server.select("/projects/DEMO/subjects/*")
    assert selectable is not None

    # Selectable should have exists() method.
    assert hasattr(selectable, "exists")
    assert selectable.exists() is True


# ---------------------------------------------------------------------------
# Test 4: Wrapper delegates attributes
# ---------------------------------------------------------------------------

@mock.patch("pyxnat.Interface", MockPyxnatInterface)
def test_build_server_delegates_all_attributes():
    """
    build_server should return an interface that delegates pyxnat methods.
    Code using the result should access pyxnat attributes transparently.
    """
    server = build_server("http://test.com/xnat/", "testuser", "testpass")

    # Core methods must exist and be callable.
    assert hasattr(server, "get") and callable(server.get)
    assert hasattr(server, "select") and callable(server.select)
    assert hasattr(server, "disconnect") and callable(server.disconnect)


# ---------------------------------------------------------------------------
# Test 5: hasattr guards work (optional hook methods)
# ---------------------------------------------------------------------------

@mock.patch("pyxnat.Interface", MockPyxnatInterface)
def test_optional_hooks_guarded_by_hasattr():
    """
    browse.py and download.py use hasattr() to check for optional hook methods
    like list_subjects_with_labels() and label_for_subject().
    These should return False gracefully (method not present on Interface).
    """
    server = build_server("http://test.com/xnat/", "testuser", "testpass")

    # These hooks should NOT exist on a raw Interface.
    # Callers guard with hasattr(), so this is safe.
    assert not hasattr(server, "list_subjects_with_labels")
    assert not hasattr(server, "label_for_subject")
    assert not hasattr(server, "file_count")


# ---------------------------------------------------------------------------
# Test 6: Offline roundtrip (no network)
# ---------------------------------------------------------------------------

@mock.patch("pyxnat.Interface", MockPyxnatInterface)
def test_build_server_offline_roundtrip():
    """
    Full offline roundtrip: factory → build_server → server.get('/') call.
    This simulates auth.py's preflight check without touching the network.
    """
    from app.logic.auth import _default_connect_factory

    # Use the real default factory (which will call build_server).
    # The factory will construct a MockPyxnatInterface instead of real pyxnat.
    server = _default_connect_factory("http://test.com/xnat/", "testuser", "testpass")
    assert server is not None

    # Simulate auth.py's liveness check: server.get("/")
    result = server.get("/")
    assert result is not None


# ---------------------------------------------------------------------------
# Test 7: Credentials are passed to Interface
# ---------------------------------------------------------------------------

@mock.patch("pyxnat.Interface", MockPyxnatInterface)
def test_build_server_credentials_passed_to_interface():
    """
    build_server should pass credentials to pyxnat.Interface correctly.
    """
    # Call build_server.
    server = build_server("http://test.com/xnat/", "admin", "secret")

    # Verify credentials were captured in the mock.
    assert server._server == "http://test.com/xnat/"
    assert server._user == "admin"
    assert server._password == "secret"
