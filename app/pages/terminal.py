"""
app/pages/terminal — Learn mode page (Phase 2 US6, optional).

Shows CLI-equivalent command snippets for common GUI actions.
Hidden by default; reached only via explicit sidebar nav.

IMPORTANT: This panel is informational only.
- The GUI is the supported, recommended path.
- These commands are for curious users who want to learn the CLI.
- No command is executed by the app — snippets are read-only reference text.
"""
from __future__ import annotations

import streamlit as st

from app.logic.learn import learn_snippets


def render() -> None:
    """Render the Learn mode / CLI-equivalent panel."""
    st.title("Learn Mode — CLI Reference")

    st.info(
        "**The GUI is the supported path.** "
        "These commands are shown for learning purposes only — "
        "the app never runs them for you. "
        "Open a terminal and paste them if you want to explore the CLI.",
        icon="ℹ️",
    )

    st.markdown("---")
    st.markdown(
        "Each snippet below shows what the equivalent command-line call would "
        "look like for that GUI action. Replace `<angle-bracket>` placeholders "
        "with your actual values. Credentials and patient identifiers are never "
        "shown here."
    )

    snippets = learn_snippets()

    for _key, entry in snippets.items():
        st.subheader(entry["label"])
        st.caption(entry["description"])
        st.code(entry["command"], language="bash")
        st.markdown("")  # visual spacer

    st.markdown("---")
    st.caption(
        "CLI path: `python -m xnat_interact --help` for the full option list. "
        "Contact the Data Librarian if you have questions about the CLI."
    )
