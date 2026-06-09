"""
app/logic/onboarding — pure onboarding-status logic for XNAT-Interact.

NO streamlit imports here. Offline-testable.

Public API
----------
OnboardingStatus  — dataclass: has_account, added_to_project, vpn_connected
check_onboarding  — injectable probes → OnboardingStatus
build_access_request — plain-text email draft (PHI-free, cred-free)

PHI / CREDENTIAL RULE (HARD)
-----------------------------
build_access_request MUST contain ONLY:
  - Student display name
  - HawkID (institutional username, not a password)
  - Project name

It MUST NOT contain passwords, credentials, PHI, patient data, MRN, DOB, or
any clinical/health information. The HawkID is a non-sensitive institutional
identifier used solely to identify the user to the Data Librarian.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional


# ---------------------------------------------------------------------------
# Status dataclass
# ---------------------------------------------------------------------------

@dataclass
class OnboardingStatus:
    """
    Tracks whether the three prerequisites for XNAT access are met.

    Fields
    ------
    has_account       : Student has an XNAT account (credentials accepted).
    added_to_project  : Student has been added to the target XNAT project.
    vpn_connected     : Student's device can reach the XNAT server (VPN up).
    """
    has_account: bool
    added_to_project: bool
    vpn_connected: bool

    @property
    def ready(self) -> bool:
        """True when all three prerequisites are satisfied."""
        return self.has_account and self.added_to_project and self.vpn_connected

    def next_step(self) -> Optional[str]:
        """
        Return plain-language guidance for the first unmet prerequisite,
        or None if all prerequisites are satisfied.

        Steps are checked in dependency order:
          VPN → account → project membership.
        VPN comes first because without it the other checks cannot succeed.
        """
        if not self.vpn_connected:
            return (
                "Connect to the UIowa VPN using Cisco AnyConnect before proceeding. "
                "Without VPN the XNAT server is unreachable."
            )
        if not self.has_account:
            return (
                "Request an XNAT account from the Data Librarian (dmattioli / stelong). "
                "Use the 'Draft access-request email' button below to generate a ready-to-send message."
            )
        if not self.added_to_project:
            return (
                "Ask the Data Librarian to add your XNAT username to the project. "
                "Use the 'Draft access-request email' button below to generate a ready-to-send message."
            )
        return None


# ---------------------------------------------------------------------------
# Probe defaults (real implementations, no network hardcoded)
# ---------------------------------------------------------------------------

def _default_vpn_probe() -> bool:
    """
    Attempt a lightweight TCP connection to the XNAT server to verify VPN.

    Falls back to False on any exception (fail-soft).
    """
    import socket
    from src.services.config import AppConfig  # noqa: PLC0415
    try:
        cfg = AppConfig.load()
        # Strip scheme and path; extract host + port.
        from urllib.parse import urlparse  # noqa: PLC0415
        parsed = urlparse(cfg.server_url)
        host = parsed.hostname or "xnat.icts.uiowa.edu"
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        with socket.create_connection((host, port), timeout=3):
            pass
        return True
    except Exception:
        return False


def _default_account_probe() -> bool:
    """
    Cannot determine account status without credentials in this context.

    Returns False by default so the checklist conservatively shows the step
    as incomplete until the user successfully logs in (which calls attempt_login
    with actual credentials). Injected in tests and in the authenticated context.
    """
    return False


def _default_project_probe() -> bool:
    """
    Cannot determine project membership without a live server handle.

    Returns False by default (conservative). Injected in tests and in the
    authenticated context via check_onboarding(project_probe=...).
    """
    return False


# ---------------------------------------------------------------------------
# Public check function
# ---------------------------------------------------------------------------

def check_onboarding(
    *,
    account_probe: Optional[Callable[[], bool]] = None,
    project_probe: Optional[Callable[[], bool]] = None,
    vpn_probe: Optional[Callable[[], bool]] = None,
) -> OnboardingStatus:
    """
    Run the three prerequisite probes and return an OnboardingStatus.

    Each probe is an injectable callable returning bool.

    Defaults
    --------
    vpn_probe     : TCP connect to the XNAT server (real network check).
    account_probe : Returns False conservatively (requires live credentials).
    project_probe : Returns False conservatively (requires live server handle).

    Fail-soft contract
    ------------------
    A probe that raises ANY exception is treated as "not satisfied" (False).
    Exceptions never propagate out of check_onboarding.

    Parameters
    ----------
    account_probe : Callable[[], bool] — True if credentials are accepted.
    project_probe : Callable[[], bool] — True if user is in the project's user list.
    vpn_probe     : Callable[[], bool] — True if the XNAT server is reachable.

    Returns
    -------
    OnboardingStatus
    """
    _vpn_fn     = vpn_probe     if vpn_probe     is not None else _default_vpn_probe
    _account_fn = account_probe if account_probe is not None else _default_account_probe
    _project_fn = project_probe if project_probe is not None else _default_project_probe

    def _safe(fn: Callable[[], bool]) -> bool:
        try:
            return bool(fn())
        except Exception:
            return False

    return OnboardingStatus(
        vpn_connected=_safe(_vpn_fn),
        has_account=_safe(_account_fn),
        added_to_project=_safe(_project_fn),
    )


# ---------------------------------------------------------------------------
# Access-request email draft
# ---------------------------------------------------------------------------

def build_access_request(
    student_name: str,
    hawkid: str,
    project_name: str,
) -> str:
    """
    Return a plain-text email draft requesting XNAT account creation and
    project membership from the Data Librarian.

    PHI / CREDENTIAL SAFETY
    ------------------------
    This function MUST output ONLY the student's display name, HawkID
    (institutional username — NOT a password), and project name.

    It MUST NOT include:
      - Passwords or credentials of any kind
      - Patient identifiers, MRN, DOB, or any clinical/health data
      - Any PHI whatsoever

    The HawkID is a non-sensitive institutional identifier (like a net ID).
    It is included here solely to allow the Data Librarian to create/locate
    the user account. It is never a password.

    Parameters
    ----------
    student_name : Display name of the student (e.g. "Jane Smith").
    hawkid       : UIowa HawkID / institutional username (NOT a password).
    project_name : XNAT project the student needs to be added to.

    Returns
    -------
    str — ready-to-copy plain-text email body.
    """
    return (
        f"To: Data Librarian (dmattioli / stelong)\n"
        f"Subject: XNAT Access Request — {hawkid}\n"
        f"\n"
        f"Hello,\n"
        f"\n"
        f"I am writing to request access to the XNAT system for the following student:\n"
        f"\n"
        f"  Name   : {student_name}\n"
        f"  HawkID : {hawkid}\n"
        f"\n"
        f"Please could you:\n"
        f"  1. Create an XNAT account for this user (if one does not already exist).\n"
        f"  2. Add the user to the project: {project_name}\n"
        f"\n"
        f"Thank you for your help.\n"
        f"\n"
        f"Best regards,\n"
        f"{student_name}\n"
    )
