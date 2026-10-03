"""
app/guided/wizard_share — the "Share a result" page of the guided app (spec 017).

A student picks the folder that holds a finished analysis result and the page
walks them through five steps: Pick folder, Review, PHI check, Confirm, Done.
All the real work is done by the spec 016 intake engine, ``run_intake``.  The
page calls it at most twice per attempt: once as a dry run on the PHI check
step, and once for real when the student presses "Share now".  It never puts
anything on the server by any other route.

The module has two halves:

* plain functions (no browser needed) that hold every rule of the page, so
  the offline tests can call them directly, and
* thin ``_render_*`` functions that only draw Streamlit widgets and call the
  plain functions.
"""
from __future__ import annotations

import copy
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from src.services.errors import FriendlyError
from src.services.analysis_intake import IntakeRefusal, load_types, run_intake
from src.services.analysis_intake.descriptor import (
    DESCRIPTOR_JSON,
    DESCRIPTOR_YAML,
    load_descriptor,
    write_descriptor,
    write_descriptor_template,
)


# ---------------------------------------------------------------------------
# Steps and session-state keys
# ---------------------------------------------------------------------------

STEPS = ["Pick folder", "Review", "PHI check", "Confirm", "Done"]

KEY_FOLDER = "guided_share_folder"        # str: the chosen folder path
KEY_RECENT = "guided_share_recent"        # list[str]: recent folders, newest first (kept on Cancel)
KEY_TYPE = "guided_share_type"            # str: the analysis type name
KEY_FORM = "guided_share_form"            # dict: the five editable run fields
KEY_PIXEL_OK = "guided_share_pixel_ok"    # bool: the image confirmation tick
KEY_DRY_RUN = "guided_share_dry_run"      # IntakeResult or None: last dry run
KEY_RESULT = "guided_share_result"        # IntakeResult or None: the real run

# Widget keys all start with this prefix so Cancel and Done can clear them.
WIDGET_PREFIX = "guided_share_w_"

RECENT_LIMIT = 10

# The 016 FR-014 warning: the image check is a person's look, not a proof.
PIXEL_WARNING = (
    "This check does not prove that the source frames themselves are free of "
    "burned-in patient details. The tool does not yet remove burned-in text from "
    "images (issue #63), so look at the images carefully before you tick the box."
)

PIXEL_CONFIRM_TEXT = (
    "I have looked at the images in this folder, and they show no burned-in "
    "patient details (names, dates, ID numbers or other text)."
)

# Plain words for each PHI policy of a type.
_POLICY_WORDS = {
    "no_pixels": "No images in the result",
    "pixels_from_source_only": "Images are unchanged input frames or plain masks",
    "text_scan": "No patient names or dates in the text",
}

_STATUS_WORDS = {
    "done": "Shared and confirmed",
    "not_verified": "Sent, but not confirmed",
    "catalog_failed": "Shared and confirmed, but the catalog was not updated",
    "refused": "Not shared",
    "dry_run_ok": "Checks passed (nothing shared yet)",
}


# ---------------------------------------------------------------------------
# Small data holders
# ---------------------------------------------------------------------------

@dataclass
class FolderCheck:
    """The result of checking a typed folder path."""
    ok: bool
    path: Optional[Path] = None
    problem: Optional[str] = None


@dataclass
class OutcomeCard:
    """Everything the Done step shows, worked out from an ``IntakeResult``."""
    status: str
    status_text: str
    label: Optional[str]
    placement_used: Optional[str]
    fallback_reason: Optional[str]
    verified: bool
    catalog_row: bool
    warnings: List[str] = field(default_factory=list)
    command_line: str = ""
    friendly: Optional[FriendlyError] = None
    note: Optional[str] = None


# ---------------------------------------------------------------------------
# Step 0: Pick folder
# ---------------------------------------------------------------------------

def clean_path(raw: str) -> str:
    """Strip spaces and surrounding quotes from a typed or pasted path."""
    text = (raw or "").strip()
    while len(text) >= 2 and text[0] == text[-1] and text[0] in ("'", '"'):
        text = text[1:-1].strip()
    return os.path.expanduser(text) if text else ""


def check_folder(raw: str) -> FolderCheck:
    """Check that *raw* names a folder on this computer that can be read."""
    text = clean_path(raw)
    if not text:
        return FolderCheck(False, None, "Type or paste the path of the folder that holds your result.")
    path = Path(text)
    if not path.exists():
        return FolderCheck(False, path, "There is no folder at that path on this computer. Check the spelling.")
    if not path.is_dir():
        return FolderCheck(False, path, "That path is a file, not a folder. Give the folder that holds the file.")
    try:
        os.listdir(path)
    except OSError:
        return FolderCheck(False, path, "That folder could not be opened. Check that your account may read it.")
    return FolderCheck(True, path, None)


def remember_folder(recent: List[str], path: str, limit: int = RECENT_LIMIT) -> List[str]:
    """Put *path* first in the recent list, without duplicates, at most *limit* long."""
    return ([path] + [p for p in (recent or []) if p != path])[:limit]


def shareable_types(types: Dict[str, Any]) -> List[Tuple[str, str]]:
    """The types the page offers: (type_name, description), only those published by the intake."""
    return sorted((name, t.description) for name, t in types.items() if t.publish_via_intake)


def has_descriptor(folder: Path) -> bool:
    """True when the folder already has an analysis.json (or analysis.yaml)."""
    folder = Path(folder)
    return (folder / DESCRIPTOR_JSON).is_file() or (folder / DESCRIPTOR_YAML).is_file()


def _unexpected(what: str) -> FriendlyError:
    return FriendlyError(
        title="Something went wrong",
        message=f"The page could not {what}. Nothing was shared.",
        recourse=["Try again.", "If it happens again, ask the Data Librarian for help."],
    )


def prepare_folder(folder: Path, type_name: Optional[str], *,
                   types: Optional[Dict[str, Any]] = None) -> Tuple[Optional[Dict[str, Any]], Optional[FriendlyError]]:
    """
    Make sure the folder has an analysis.json and read it.

    When the folder has none, the 016 template for *type_name* is written first.
    When it has one, the type it names is used.  Returns (descriptor, None) or
    (None, a plain-language problem).
    """
    folder = Path(folder)
    try:
        types = types if types is not None else load_types()
        if not has_descriptor(folder):
            if not type_name:
                return None, FriendlyError(
                    title="Pick the kind of result",
                    message="This folder has no analysis.json yet, so the page needs to know what kind of result it holds.",
                    recourse=["Choose the type from the list, then press Next again."])
            write_descriptor_template(type_name, folder, types=types)
        descriptor = load_descriptor(folder)
    except IntakeRefusal as exc:
        return None, exc.friendly
    except Exception:  # noqa: BLE001 — never a traceback on the page (Principle II)
        return None, _unexpected("read the folder")
    name = descriptor.get("type_name")
    offered = dict(shareable_types(types))
    if name not in offered:
        return None, FriendlyError(
            title="This kind of result is not shared here",
            message=f"analysis.json names the type '{name}', which this page does not share.",
            recourse=[f"Types shared here: {', '.join(sorted(offered)) or 'none'}.",
                      "Fix type_name in analysis.json, or ask the Data Librarian."])
    return descriptor, None


# ---------------------------------------------------------------------------
# Step 1: Review
# ---------------------------------------------------------------------------

def _value_text(value: Any) -> str:
    return value if isinstance(value, str) else json.dumps(value)


def _text_value(text: str) -> Any:
    """Turn a typed parameter value back into a number or true/false when it is one."""
    stripped = text.strip()
    try:
        value = json.loads(stripped)
    except (ValueError, TypeError):
        return text
    return value if isinstance(value, (int, float, bool)) else text


def form_from_descriptor(descriptor: Dict[str, Any]) -> Dict[str, Any]:
    """The five editable fields of ``analysis.json``, as the Review form holds them."""
    run = descriptor.get("run") or {}
    params = run.get("parameters") or {}
    return {
        "case_uid": run.get("case_uid") or "",
        "code_ref": run.get("code_ref") or "",
        "parameters": [{"key": str(k), "value": _value_text(v)} for k, v in params.items()],
        "notes": run.get("notes") or "",
        "supersedes": run.get("supersedes") or "",
    }


def form_blockers(form: Dict[str, Any]) -> List[str]:
    """Reasons the Review step cannot move on yet (empty list means it can)."""
    blockers: List[str] = []
    if not str(form.get("case_uid") or "").strip():
        blockers.append("Fill in the case ID (case_uid).")
    if not str(form.get("code_ref") or "").strip():
        blockers.append("Fill in the code reference (code_ref), for example the git commit of your analysis code.")
    seen = set()
    for row in form.get("parameters") or []:
        key = str(row.get("key") or "").strip()
        value = str(row.get("value") or "").strip()
        if not key and not value:
            continue
        if not key:
            blockers.append("Every parameter needs a name.")
        elif key in seen:
            blockers.append(f"The parameter name '{key}' is used twice.")
        seen.add(key)
    return blockers


def apply_form(descriptor: Dict[str, Any], form: Dict[str, Any]) -> Dict[str, Any]:
    """Return a copy of *descriptor* with the five form fields written into ``run``; nothing else changes."""
    desc = copy.deepcopy(descriptor)
    run = desc.setdefault("run", {})
    run["case_uid"] = str(form.get("case_uid") or "").strip()
    run["code_ref"] = str(form.get("code_ref") or "").strip()
    params: Dict[str, Any] = {}
    for row in form.get("parameters") or []:
        key = str(row.get("key") or "").strip()
        if key:
            params[key] = _text_value(str(row.get("value") or ""))
    run["parameters"] = params
    run["notes"] = str(form.get("notes") or "")
    sup = str(form.get("supersedes") or "").strip()
    run["supersedes"] = sup or None
    return desc


def save_form(folder: Path, form: Dict[str, Any]) -> Optional[FriendlyError]:
    """Write the form into the folder's analysis.json (016 ``write_descriptor``).  None means saved."""
    try:
        write_descriptor(apply_form(load_descriptor(Path(folder)), form), Path(folder))
    except IntakeRefusal as exc:
        return exc.friendly
    except Exception:  # noqa: BLE001 — never a traceback on the page
        return _unexpected("save analysis.json")
    return None


# ---------------------------------------------------------------------------
# Step 2 and 3: PHI check, Confirm (the only two calls of the engine)
# ---------------------------------------------------------------------------

def needs_pixel_confirmation(atype: Any) -> bool:
    """True when the type allows images, so a person must confirm them."""
    return "pixels_from_source_only" in tuple(getattr(atype, "phi_policy", ()) or ())


def make_confirmer(pixel_ok: bool) -> Callable[[str], str]:
    """The confirmer handed to the engine: the student's tick, nothing else."""
    answer = "confirmed" if pixel_ok else "declined"

    def confirmer(_context: str) -> str:
        return answer
    return confirmer


def gateway_for(server: Any) -> Any:
    """The gateway to share with, by the same rule the browse page uses."""
    from app.guided.browse_view import uploader_for
    return uploader_for(server)


def run_dry(folder: Path, *, username: Optional[str], classifier: Optional[Callable] = None,
            pixel_ok: bool = False):
    """Run every check without sharing anything (the PHI check step)."""
    return run_intake(Path(folder), dry_run=True, username=username, classifier=classifier,
                      confirmer=make_confirmer(pixel_ok))


def run_share(folder: Path, *, server: Any, config_tables: Any, username: Optional[str],
              classifier: Optional[Callable] = None, pixel_ok: bool = False):
    """Share for real (the Confirm step).  One call per press of "Share now"."""
    return run_intake(Path(folder), gateway=gateway_for(server), config_tables=config_tables,
                      username=username, classifier=classifier, confirmer=make_confirmer(pixel_ok))


def policy_rows(atype: Any, dry_run_result: Any) -> List[Tuple[str, str]]:
    """Each PHI policy of the type in plain words, with "pass" or "stop" (or "not checked yet")."""
    if dry_run_result is None:
        status = "not checked yet"
    else:
        status = "pass" if dry_run_result.status == "dry_run_ok" else "stop"
    return [(_POLICY_WORDS.get(p, p), status) for p in getattr(atype, "phi_policy", ())]


# ---------------------------------------------------------------------------
# Step 4: Done
# ---------------------------------------------------------------------------

def command_line_for(folder: Path) -> str:
    """The command line that does the same share, so the student can learn it."""
    text = str(folder)
    if " " in text:
        text = f'"{text}"'
    return f"main.py publish-analysis {text}"


def outcome_card(result: Any, folder: Path) -> OutcomeCard:
    """Work out what the Done step shows from the engine's result."""
    outcome = getattr(result, "outcome", None)
    descriptor = getattr(result, "descriptor", None) or {}
    run = descriptor.get("run") or {}
    status = result.status
    if outcome is not None:
        placement, fallback, verified = outcome.placement_used, outcome.fallback_reason, bool(outcome.verified)
    else:
        placement, fallback = run.get("placement_used"), run.get("fallback_reason")
        verified = status in ("done", "catalog_failed")
    note = None
    if status == "not_verified":
        note = "This result is NOT counted as published."
    elif status == "catalog_failed":
        note = "Sharing the same folder again adds the catalog row."
    return OutcomeCard(
        status=status, status_text=_STATUS_WORDS.get(status, status), label=result.label,
        placement_used=placement, fallback_reason=fallback, verified=verified,
        catalog_row=(status == "done"), warnings=list(result.warnings or []),
        command_line=command_line_for(folder), friendly=result.friendly, note=note)


# ---------------------------------------------------------------------------
# Session-state helpers (Streamlit imported only here and below)
# ---------------------------------------------------------------------------

def _st():
    import streamlit as st
    return st


def discard_dry_run() -> None:
    """Forget the last dry run (Back, any form change, or "Check again")."""
    _st().session_state[KEY_DRY_RUN] = None


def clear_share_state() -> None:
    """Cancel or Done: clear the share state and go home.  Files in the folder are not touched."""
    from app.guided import wizard_state
    st = _st()
    wizard_state.reset_wizard()
    for key in [k for k in st.session_state if str(k).startswith(WIDGET_PREFIX)]:
        del st.session_state[key]


def _current_type(types: Dict[str, Any]) -> Any:
    return types.get(_st().session_state.get(KEY_TYPE) or "")


# ---------------------------------------------------------------------------
# Render functions
# ---------------------------------------------------------------------------

def _render_pick_folder(types: Dict[str, Any]) -> None:
    from app.guided import components, wizard_state
    st = _st()
    ss = st.session_state
    st.markdown("### Which folder holds your result?")
    st.caption("This is a folder on the computer running this app, for example the output folder of your analysis.")

    recent = ss.get(KEY_RECENT) or []
    path_key = WIDGET_PREFIX + "path"
    if path_key not in ss:
        ss[path_key] = ss.get(KEY_FOLDER) or ""
    if recent:
        pick = st.selectbox("Folders you used earlier", ["(choose one)"] + recent, key=WIDGET_PREFIX + "recent")
        if pick != "(choose one)" and st.button("Use this folder", key=WIDGET_PREFIX + "use_recent"):
            ss[path_key] = pick
            st.rerun()
    raw = st.text_input("Folder path", key=path_key)
    check = check_folder(raw)
    type_name = None
    if raw and not check.ok:
        st.warning(check.problem)
    elif check.ok:
        if has_descriptor(check.path):
            st.info("This folder already has an analysis.json; the page uses the type it names.")
        else:
            options = shareable_types(types)
            labels = [f"{name} — {desc}" for name, desc in options]
            choice = st.selectbox("What kind of result is it?", ["(choose a type)"] + labels,
                                  key=WIDGET_PREFIX + "type")
            if choice in labels:
                type_name = options[labels.index(choice)][0]
            st.caption("The page will write a ready-made analysis.json for this type when you press Next.")

    _nav_row(back=False)
    col = st.columns([1, 4])[1]
    with col:
        if st.button("Next →", key=WIDGET_PREFIX + "next0", type="primary", disabled=not check.ok,
                     use_container_width=True):
            written = not has_descriptor(check.path)
            descriptor, problem = prepare_folder(check.path, type_name, types=types)
            if problem is not None:
                components.render_friendly_error(problem)
                return
            if written:
                st.success("analysis.json was written into the folder.")
            folder = str(check.path)
            ss[KEY_FOLDER] = folder
            ss[KEY_RECENT] = remember_folder(recent, folder)
            ss[KEY_TYPE] = descriptor.get("type_name")
            ss[KEY_FORM] = form_from_descriptor(descriptor)
            ss[KEY_PIXEL_OK] = False
            discard_dry_run()
            for key in [k for k in ss if str(k).startswith(WIDGET_PREFIX + "form_")]:
                del ss[key]
            wizard_state.set_step(1)
            st.rerun()


def _render_review(types: Dict[str, Any]) -> None:
    from app.guided import components, wizard_state
    st = _st()
    ss = st.session_state
    folder = Path(ss.get(KEY_FOLDER) or "")
    form = dict(ss.get(KEY_FORM) or {})
    atype = _current_type(types)
    st.markdown("### Check the description of your result")
    st.text_input("Type (from analysis.json)", value=ss.get(KEY_TYPE) or "", disabled=True,
                  key=WIDGET_PREFIX + "ro_type")
    st.text_input("Type version", value=str(getattr(atype, "type_version", "")), disabled=True,
                  key=WIDGET_PREFIX + "ro_version")

    for name in ("case_uid", "code_ref", "notes", "supersedes"):
        key = WIDGET_PREFIX + "form_" + name
        if key not in ss:
            ss[key] = form.get(name) or ""
    new = {
        "case_uid": st.text_input("Case ID (case_uid)", key=WIDGET_PREFIX + "form_case_uid"),
        "code_ref": st.text_input("Code reference (code_ref), e.g. git:abc1234", key=WIDGET_PREFIX + "form_code_ref"),
        "notes": st.text_area("Notes (optional; no patient details)", key=WIDGET_PREFIX + "form_notes"),
        "supersedes": st.text_input("Replaces an earlier result (optional label, e.g. knee_flexion_angle__v1)",
                                    key=WIDGET_PREFIX + "form_supersedes"),
    }
    seed_key = WIDGET_PREFIX + "form_params_seed"
    if seed_key not in ss:
        ss[seed_key] = [dict(r) for r in form.get("parameters") or []] or [{"key": "", "value": ""}]
    edited = st.data_editor(ss[seed_key], num_rows="dynamic", key=WIDGET_PREFIX + "form_params",
                            column_config={"key": "Parameter name", "value": "Value"})
    rows = edited.to_dict("records") if hasattr(edited, "to_dict") else list(edited or [])
    new["parameters"] = [{"key": str(r.get("key") or ""), "value": str(r.get("value") or "")} for r in rows
                         if str(r.get("key") or "").strip() or str(r.get("value") or "").strip()]
    if new != form:
        ss[KEY_FORM] = new
        discard_dry_run()

    blockers = form_blockers(new)
    _nav_row(back=True)
    col = st.columns([1, 4])[1]
    with col:
        if st.button("Next →", key=WIDGET_PREFIX + "next1", type="primary", disabled=bool(blockers),
                     use_container_width=True):
            problem = save_form(folder, new)
            if problem is not None:
                components.render_friendly_error(problem)
                return
            discard_dry_run()
            wizard_state.set_step(2)
            st.rerun()
        for blocker in blockers:
            st.markdown(f"- {blocker}")


def _render_phi_check(types: Dict[str, Any], username: Optional[str], classifier: Optional[Callable]) -> None:
    from app.guided import components, wizard_state
    st = _st()
    ss = st.session_state
    folder = Path(ss.get(KEY_FOLDER) or "")
    atype = _current_type(types)
    st.markdown("### Patient-information check")
    st.caption("The page runs every check now, without sharing anything.")

    waiting = False
    if needs_pixel_confirmation(atype):
        st.warning(PIXEL_WARNING)
        ticked = st.checkbox(PIXEL_CONFIRM_TEXT, value=bool(ss.get(KEY_PIXEL_OK)), key=WIDGET_PREFIX + "pixel")
        if ticked != bool(ss.get(KEY_PIXEL_OK)):
            ss[KEY_PIXEL_OK] = ticked
            discard_dry_run()
        waiting = not ticked
        if waiting:
            st.info("Tick the box after looking at the images; the check waits for it.")

    if not waiting and ss.get(KEY_DRY_RUN) is None:
        with st.spinner("Checking..."):
            ss[KEY_DRY_RUN] = run_dry(folder, username=username, classifier=classifier,
                                      pixel_ok=bool(ss.get(KEY_PIXEL_OK)))
    result = ss.get(KEY_DRY_RUN)

    for words, status in policy_rows(atype, result):
        mark = {"pass": "✅", "stop": "⛔"}.get(status, "…")
        st.markdown(f"{mark} {words}: **{status}**")
    if result is not None and result.status != "dry_run_ok" and result.friendly is not None:
        components.render_friendly_error(result.friendly)
    if result is not None:
        for warning in result.warnings or []:
            st.info(warning)

    if result is not None and result.status != "dry_run_ok":
        if st.button("Check again", key=WIDGET_PREFIX + "again"):
            discard_dry_run()
            st.rerun()

    ok = result is not None and result.status == "dry_run_ok"
    _nav_row(back=True)
    col = st.columns([1, 4])[1]
    with col:
        if st.button("Next →", key=WIDGET_PREFIX + "next2", type="primary", disabled=not ok,
                     use_container_width=True):
            wizard_state.set_step(3)
            st.rerun()


def _render_confirm(types: Dict[str, Any], server: Any, config_tables: Any, username: Optional[str],
                    classifier: Optional[Callable]) -> None:
    from app.guided import wizard_state
    st = _st()
    ss = st.session_state
    folder = Path(ss.get(KEY_FOLDER) or "")
    form = ss.get(KEY_FORM) or {}
    atype = _current_type(types)
    st.markdown("### Ready to share")
    base = atype.base_label(form.get("case_uid") or "") if atype is not None else ""
    st.markdown(f"- **Folder:** `{folder}`\n- **Type:** {ss.get(KEY_TYPE)}\n- **Case:** {form.get('case_uid')}\n"
                f"- **Label (before the version number):** {base}\n"
                f"- **Placement:** {getattr(atype, 'placement', '')}")
    st.caption("A new version is added each time; earlier versions stay on XNAT.")
    _nav_row(back=True)
    col = st.columns([1, 4])[1]
    with col:
        if st.button("✅ Share now", key=WIDGET_PREFIX + "share", type="primary", use_container_width=True):
            with st.spinner("Sharing and checking the server copy..."):
                ss[KEY_RESULT] = run_share(folder, server=server, config_tables=config_tables, username=username,
                                           classifier=classifier, pixel_ok=bool(ss.get(KEY_PIXEL_OK)))
            wizard_state.set_step(4)
            st.rerun()


def _render_done() -> None:
    from app.guided import components
    st = _st()
    ss = st.session_state
    result = ss.get(KEY_RESULT)
    if result is None:
        st.info("Nothing was shared yet.")
    else:
        card = outcome_card(result, Path(ss.get(KEY_FOLDER) or ""))
        (st.success if card.status == "done" else st.warning)(f"**{card.status_text}**")
        if card.label:
            st.markdown(f"- **Label:** {card.label}")
        if card.placement_used:
            st.markdown(f"- **Placement:** {card.placement_used}")
        if card.fallback_reason:
            st.markdown(f"- **Why that placement:** {card.fallback_reason}")
        st.markdown(f"- **Server copy checked:** {'yes' if card.verified else 'no'}")
        st.markdown(f"- **Catalog row added:** {'yes' if card.catalog_row else 'no'}")
        for warning in card.warnings:
            st.info(warning)
        if card.friendly is not None:
            components.render_friendly_error(card.friendly)
        if card.note:
            st.markdown(f"**{card.note}**")
        st.markdown("The same share from the command line:")
        st.code(card.command_line, language="bash")
    if st.button("Done", key=WIDGET_PREFIX + "done", type="primary"):
        clear_share_state()
        st.rerun()


def _nav_row(*, back: bool) -> None:
    """Back (when allowed) and Cancel.  Back always discards the last dry run."""
    from app.guided import wizard_state
    st = _st()
    ss = st.session_state
    st.markdown("---")
    col1, col2 = st.columns([1, 1])
    step = wizard_state.get_step()
    with col1:
        if back and st.button("← Back", key=WIDGET_PREFIX + f"back{step}", use_container_width=True):
            if step == 2:
                ss[KEY_PIXEL_OK] = False
                ss.pop(WIDGET_PREFIX + "pixel", None)
            discard_dry_run()
            wizard_state.set_step(step - 1)
            st.rerun()
    with col2:
        if st.button("Cancel", key=WIDGET_PREFIX + f"cancel{step}", use_container_width=True):
            clear_share_state()
            st.rerun()


def render_share_wizard(server: Any, *, classifier: Optional[Callable] = None,
                        config_tables_factory: Optional[Callable[[Any], Any]] = None) -> None:
    """
    Draw the "Share a result" page.

    ``classifier`` stays None in the app, so the engine uses its own PHI text
    check and refuses when that check cannot run.  ``config_tables_factory``
    (tests only) gives the catalog connection; otherwise the one built at login
    is used, or None when there is none.
    """
    from app import state
    from app.guided import components, wizard_state
    st = _st()
    step = wizard_state.get_step()
    components.render_step_rail(STEPS, step)
    try:
        types = load_types()
    except IntakeRefusal as exc:
        components.render_friendly_error(exc.friendly)
        return
    except Exception:  # noqa: BLE001
        components.render_friendly_error(_unexpected("read the list of result types"))
        return
    username = state.get_username()
    if step == 0:
        _render_pick_folder(types)
    elif step == 1:
        _render_review(types)
    elif step == 2:
        _render_phi_check(types, username, classifier)
    elif step == 3:
        try:
            config_tables = config_tables_factory(server) if config_tables_factory else state.get_config_tables()
        except Exception:  # noqa: BLE001 — no catalog connection is reported on the Done step
            config_tables = None
        _render_confirm(types, server, config_tables, username, classifier)
    elif step == 4:
        _render_done()
    else:
        st.error("Unknown step.")
        if st.button("← Home", key=WIDGET_PREFIX + "home"):
            clear_share_state()
            st.rerun()
