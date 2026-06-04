"""
app/pages/onboarding — Streamlit onboarding / access checklist page.

Renders the three-step prerequisite checklist (VPN, account, project membership)
with live status indicators and a "Draft access-request email" action.

Logic lives in app/logic/onboarding (no streamlit there).
"""
from __future__ import annotations

import streamlit as st

from app import state
from app.logic.onboarding import OnboardingStatus, check_onboarding, build_access_request


# ---------------------------------------------------------------------------
# Step metadata
# ---------------------------------------------------------------------------

_STEPS = [
    {
        "label": "UIowa VPN connected",
        "detail": (
            "You must be connected to the UIowa VPN (Cisco AnyConnect) for the app "
            "to reach the XNAT server. Connect before clicking **Connect** on the login page."
        ),
        "attr": "vpn_connected",
    },
    {
        "label": "XNAT account exists",
        "detail": (
            "You need an XNAT account (HawkID). Contact the Data Librarian "
            "(`dmattioli` / `stelong`) to request one, or use the email draft below."
        ),
        "attr": "has_account",
    },
    {
        "label": "Added to project",
        "detail": (
            "The Data Librarian must add your XNAT username to the target project. "
            "Even with a valid account you cannot browse or upload until this step is done. "
            "Use the email draft below to make this request."
        ),
        "attr": "added_to_project",
    },
]


# ---------------------------------------------------------------------------
# Page renderer
# ---------------------------------------------------------------------------

def render() -> None:
    """Render the Onboarding / Access checklist page."""
    st.title("Onboarding / Access Checklist")
    st.caption(
        "Complete all three steps before attempting to log in to XNAT-Interact."
    )
    st.markdown("---")

    # ------------------------------------------------------------------
    # Build probes: if authenticated, inject live server-backed probes.
    # Otherwise use defaults (vpn = real TCP; account/project = False).
    # ------------------------------------------------------------------
    account_probe = None
    project_probe = None
    vpn_probe = None

    if state.is_authenticated():
        server = state.get_server()
        username = state.get_username() or ""

        # account: if we're authenticated, the account clearly exists
        account_probe = lambda: True  # noqa: E731

        # project: check user list on the server
        if server is not None:
            from src.services.config import AppConfig  # noqa: PLC0415
            try:
                cfg = AppConfig.load()
                project_name_cfg = cfg.project_name
            except Exception:
                project_name_cfg = ""

            _pname = project_name_cfg
            _uname = username

            def _proj_probe() -> bool:
                try:
                    proj = server.select.project(_pname)
                    return _uname in proj.users()
                except Exception:
                    return False

            project_probe = _proj_probe

    status: OnboardingStatus = check_onboarding(
        account_probe=account_probe,
        project_probe=project_probe,
        vpn_probe=vpn_probe,
    )

    # ------------------------------------------------------------------
    # Checklist
    # ------------------------------------------------------------------
    st.subheader("Prerequisites")

    for i, step in enumerate(_STEPS, start=1):
        satisfied: bool = getattr(status, step["attr"])
        icon = "✓" if satisfied else "✗"
        colour = "green" if satisfied else "red"
        label = step["label"]

        col_icon, col_body = st.columns([1, 9])
        with col_icon:
            st.markdown(
                f"<span style='color:{colour}; font-size:1.4em; font-weight:bold;'>{icon}</span>",
                unsafe_allow_html=True,
            )
        with col_body:
            st.markdown(f"**{i}. {label}**")
            if not satisfied:
                st.caption(step["detail"])

    # ------------------------------------------------------------------
    # Overall readiness banner
    # ------------------------------------------------------------------
    st.markdown("---")
    if status.ready:
        st.success(
            "All prerequisites met — you are ready to log in to XNAT-Interact.",
            icon="✅",
        )
    else:
        next_guidance = status.next_step()
        if next_guidance:
            st.warning(f"**Next step:** {next_guidance}", icon="⚠️")

    # ------------------------------------------------------------------
    # Access-request email draft
    # ------------------------------------------------------------------
    st.markdown("---")
    st.subheader("Draft Access-Request Email")
    st.caption(
        "Fill in your details to generate a ready-to-copy email for the Data Librarian. "
        "The draft contains ONLY your name, HawkID, and the project name — "
        "no passwords or sensitive data."
    )

    with st.form(key="access_request_form"):
        student_name = st.text_input(
            "Your full name",
            placeholder="Jane Smith",
            help="Used in the email greeting only.",
        )
        hawkid = st.text_input(
            "HawkID (UIowa username)",
            placeholder="jsmith",
            help="Your institutional HawkID — NOT your password.",
        )

        # Project name from config if available, else let user enter it
        try:
            from src.services.config import AppConfig  # noqa: PLC0415
            cfg = AppConfig.load()
            default_project = cfg.project_name
        except Exception:
            default_project = ""

        project_name = st.text_input(
            "XNAT Project name",
            value=default_project,
            placeholder="MY_PROJECT",
            help="The XNAT project you need access to.",
        )

        submitted = st.form_submit_button("Generate Email Draft")

    if submitted:
        if not student_name.strip():
            st.warning("Please enter your full name.")
        elif not hawkid.strip():
            st.warning("Please enter your HawkID.")
        elif not project_name.strip():
            st.warning("Please enter the project name.")
        else:
            draft = build_access_request(
                student_name=student_name.strip(),
                hawkid=hawkid.strip(),
                project_name=project_name.strip(),
            )
            st.success("Email draft generated — copy the text below and send it yourself.")
            st.code(draft, language=None)
            st.caption(
                "This draft contains only your name, HawkID, and project — "
                "no credentials or patient data."
            )
