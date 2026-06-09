"""
tests/test_app_onboarding_logic.py — Offline tests for app/logic/onboarding.

RULES enforced:
  - NO `import streamlit` anywhere.
  - NO network calls; all probes injected.
  - NO PHI, NO credentials in any assertion.

Covers:
  1. check_onboarding — all True probes → ready.
  2. check_onboarding — all False probes → not ready.
  3. check_onboarding — each probe independently controls its field.
  4. check_onboarding — raising probe treated as False (fail-soft), no exception.
  5. check_onboarding — multiple raising probes, still no exception.
  6. OnboardingStatus.ready property.
  7. OnboardingStatus.next_step() — VPN first, then account, then project, then None.
  8. build_access_request — output contains name, hawkid, project.
  9. build_access_request — output does NOT contain password/credential tokens.
 10. build_access_request — output does NOT contain PHI placeholders.
"""
from __future__ import annotations

import pytest

# Only import from app/logic (no streamlit) — enforced by architecture rule.
from app.logic.onboarding import (
    OnboardingStatus,
    check_onboarding,
    build_access_request,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _probe(value: bool):
    """Return a callable probe that returns *value*."""
    return lambda: value


def _raising_probe(exc: BaseException = None):
    """Return a callable probe that raises *exc* (defaults to RuntimeError)."""
    if exc is None:
        exc = RuntimeError("simulated probe failure")
    def _probe():
        raise exc
    return _probe


# ---------------------------------------------------------------------------
# 1. All True probes → all fields True, ready
# ---------------------------------------------------------------------------

def test_all_true_probes_returns_ready():
    status = check_onboarding(
        account_probe=_probe(True),
        project_probe=_probe(True),
        vpn_probe=_probe(True),
    )
    assert status.vpn_connected is True
    assert status.has_account is True
    assert status.added_to_project is True
    assert status.ready is True


# ---------------------------------------------------------------------------
# 2. All False probes → all fields False, not ready
# ---------------------------------------------------------------------------

def test_all_false_probes_returns_not_ready():
    status = check_onboarding(
        account_probe=_probe(False),
        project_probe=_probe(False),
        vpn_probe=_probe(False),
    )
    assert status.vpn_connected is False
    assert status.has_account is False
    assert status.added_to_project is False
    assert status.ready is False


# ---------------------------------------------------------------------------
# 3. Each probe independently controls its field
# ---------------------------------------------------------------------------

def test_vpn_false_only():
    status = check_onboarding(
        account_probe=_probe(True),
        project_probe=_probe(True),
        vpn_probe=_probe(False),
    )
    assert status.vpn_connected is False
    assert status.has_account is True
    assert status.added_to_project is True
    assert status.ready is False


def test_account_false_only():
    status = check_onboarding(
        account_probe=_probe(False),
        project_probe=_probe(True),
        vpn_probe=_probe(True),
    )
    assert status.vpn_connected is True
    assert status.has_account is False
    assert status.added_to_project is True
    assert status.ready is False


def test_project_false_only():
    status = check_onboarding(
        account_probe=_probe(True),
        project_probe=_probe(False),
        vpn_probe=_probe(True),
    )
    assert status.vpn_connected is True
    assert status.has_account is True
    assert status.added_to_project is False
    assert status.ready is False


# ---------------------------------------------------------------------------
# 4. Raising probe → treated as False, no exception propagates
# ---------------------------------------------------------------------------

def test_raising_vpn_probe_is_false_not_exception():
    try:
        status = check_onboarding(
            account_probe=_probe(True),
            project_probe=_probe(True),
            vpn_probe=_raising_probe(),
        )
    except Exception as exc:
        pytest.fail(f"check_onboarding raised unexpectedly: {exc}")
    assert status.vpn_connected is False


def test_raising_account_probe_is_false_not_exception():
    try:
        status = check_onboarding(
            account_probe=_raising_probe(ValueError("bad creds")),
            project_probe=_probe(True),
            vpn_probe=_probe(True),
        )
    except Exception as exc:
        pytest.fail(f"check_onboarding raised unexpectedly: {exc}")
    assert status.has_account is False


def test_raising_project_probe_is_false_not_exception():
    try:
        status = check_onboarding(
            account_probe=_probe(True),
            project_probe=_raising_probe(ConnectionError("no server")),
            vpn_probe=_probe(True),
        )
    except Exception as exc:
        pytest.fail(f"check_onboarding raised unexpectedly: {exc}")
    assert status.added_to_project is False


# ---------------------------------------------------------------------------
# 5. Multiple raising probes — all map to False, no exception
# ---------------------------------------------------------------------------

def test_all_raising_probes_returns_all_false():
    try:
        status = check_onboarding(
            account_probe=_raising_probe(RuntimeError("a")),
            project_probe=_raising_probe(OSError("b")),
            vpn_probe=_raising_probe(TimeoutError("c")),
        )
    except Exception as exc:
        pytest.fail(f"check_onboarding raised unexpectedly: {exc}")
    assert status.vpn_connected is False
    assert status.has_account is False
    assert status.added_to_project is False
    assert status.ready is False


# ---------------------------------------------------------------------------
# 6. OnboardingStatus.ready property — direct construction
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("vpn,acct,proj,expected", [
    (True,  True,  True,  True),
    (True,  True,  False, False),
    (True,  False, True,  False),
    (False, True,  True,  False),
    (False, False, False, False),
])
def test_ready_property(vpn, acct, proj, expected):
    s = OnboardingStatus(
        vpn_connected=vpn,
        has_account=acct,
        added_to_project=proj,
    )
    assert s.ready is expected


# ---------------------------------------------------------------------------
# 7. next_step() ordering: VPN → account → project → None
# ---------------------------------------------------------------------------

def test_next_step_vpn_first():
    """VPN unmet → VPN guidance returned regardless of other fields."""
    s = OnboardingStatus(vpn_connected=False, has_account=False, added_to_project=False)
    step = s.next_step()
    assert step is not None
    assert "vpn" in step.lower() or "cisco" in step.lower() or "connect" in step.lower()


def test_next_step_account_when_vpn_ok():
    """VPN met, account missing → account guidance."""
    s = OnboardingStatus(vpn_connected=True, has_account=False, added_to_project=False)
    step = s.next_step()
    assert step is not None
    # Must mention account / librarian / hawkid context
    lower = step.lower()
    assert "account" in lower or "librarian" in lower or "xnat" in lower


def test_next_step_project_when_vpn_and_account_ok():
    """VPN + account met, project missing → project guidance."""
    s = OnboardingStatus(vpn_connected=True, has_account=True, added_to_project=False)
    step = s.next_step()
    assert step is not None
    lower = step.lower()
    assert "project" in lower or "add" in lower or "librarian" in lower


def test_next_step_none_when_all_ready():
    """All met → next_step returns None."""
    s = OnboardingStatus(vpn_connected=True, has_account=True, added_to_project=True)
    assert s.next_step() is None


# ---------------------------------------------------------------------------
# 8. build_access_request — output contains name, hawkid, project
# ---------------------------------------------------------------------------

def test_access_request_contains_name():
    draft = build_access_request("Jane Smith", "jsmith", "NEURO_STUDY")
    assert "Jane Smith" in draft


def test_access_request_contains_hawkid():
    draft = build_access_request("Jane Smith", "jsmith", "NEURO_STUDY")
    assert "jsmith" in draft


def test_access_request_contains_project():
    draft = build_access_request("Jane Smith", "jsmith", "NEURO_STUDY")
    assert "NEURO_STUDY" in draft


def test_access_request_all_three_fields_present():
    name, hawkid, project = "Alex Brown", "abrown99", "CARDIO_2025"
    draft = build_access_request(name, hawkid, project)
    assert name in draft
    assert hawkid in draft
    assert project in draft


# ---------------------------------------------------------------------------
# 9. build_access_request — no password/credential tokens
# ---------------------------------------------------------------------------

_CREDENTIAL_TOKENS = [
    "password",
    "passwd",
    "secret",
    "token",
    "credential",
    "api_key",
    "apikey",
    "auth_key",
]


@pytest.mark.parametrize("token", _CREDENTIAL_TOKENS)
def test_access_request_no_credential_tokens(token):
    """Draft must not contain credential-related vocabulary."""
    draft = build_access_request("Test User", "tuser", "TEST_PROJECT")
    assert token not in draft.lower(), (
        f"Credential token '{token}' found in access-request draft — security violation!"
    )


# ---------------------------------------------------------------------------
# 10. build_access_request — no PHI placeholders
# ---------------------------------------------------------------------------

_PHI_TOKENS = [
    "mrn",
    "date of birth",
    "dob",
    "patient",
    "diagnosis",
    "ssn",
    "social security",
    "medical record",
    "health information",
    "phi",
]


@pytest.mark.parametrize("token", _PHI_TOKENS)
def test_access_request_no_phi_tokens(token):
    """Draft must not contain PHI-related vocabulary."""
    draft = build_access_request("Test User", "tuser", "TEST_PROJECT")
    assert token not in draft.lower(), (
        f"PHI token '{token}' found in access-request draft — PHI violation!"
    )
