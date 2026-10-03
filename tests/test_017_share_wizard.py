"""
Spec 017: the guided app "Share a result" page.

Offline only: synthetic result folders from tests/fakes/analysis_folders.py,
``FakeXNAT`` as the server, a stub PHI classifier and a fake catalog.  The
page logic is tested as plain functions; one AppTest smoke test covers the
home card and the route.
"""
from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

import pytest

from app.guided import wizard_share as ws
from src.services.analysis_intake import load_types
from tests.fakes.analysis_folders import (
    EXPERIMENT, EXPERIMENT_QS, PROJECT, SUBJECT, SYNTHETIC_NAME, USERNAME, FakeConfigTables,
    clean_angles_folder, consensus_folder, dirty_copied_png, dirty_notes_name, fake_name_classifier,
)
from tests.fakes.fake_xnat import FakeXNAT

REPO = Path(__file__).resolve().parents[1]
WRITE_OPS = {"file.put", "file.insert", "resource.put_zip", "assessor.create", "assessor.file.put"}


# ---------------------------------------------------------------------------
# Fixtures (T002)
# ---------------------------------------------------------------------------

def _with_manifest(case):
    """The page passes no manifest, so the download record sits in the folder, as after a real download."""
    shutil.copy(case.manifest, case.output / "download_manifest.json")
    return case


@pytest.fixture
def angles(tmp_path):
    return _with_manifest(clean_angles_folder(tmp_path))


@pytest.fixture
def consensus(tmp_path):
    return _with_manifest(consensus_folder(tmp_path))


@pytest.fixture
def fake():
    fx = FakeXNAT(project_name=PROJECT)
    fx.seed_rf_experiment(SUBJECT, EXPERIMENT)
    return fx


def _writes(fx):
    return [c for c in fx.calls if c["op"] in WRITE_OPS]


def _share(case, fx, ct=None, pixel_ok=False, classifier=fake_name_classifier):
    return ws.run_share(case.output, server=fx, config_tables=ct, username=USERNAME,
                        classifier=classifier, pixel_ok=pixel_ok)


def _dry(case, pixel_ok=False, classifier=fake_name_classifier):
    return ws.run_dry(case.output, username=USERNAME, classifier=classifier, pixel_ok=pixel_ok)


@pytest.fixture
def session(monkeypatch):
    """A plain dict in place of Streamlit's session state."""
    import streamlit as st
    store = {}
    monkeypatch.setattr(st, "session_state", store, raising=False)
    return store


def _all_text(obj) -> str:
    """Every string the page could show for *obj* (dataclass, dict, list, result)."""
    if obj is None:
        return ""
    if isinstance(obj, str):
        return obj
    if isinstance(obj, dict):
        return " ".join(_all_text(k) + " " + _all_text(v) for k, v in obj.items())
    if isinstance(obj, (list, tuple)):
        return " ".join(_all_text(v) for v in obj)
    if hasattr(obj, "__dict__"):
        return _all_text(vars(obj))
    return str(obj)


# ---------------------------------------------------------------------------
# T007: state, gateway rule, catalog connection at login
# ---------------------------------------------------------------------------

def test_reset_wizard_clears_share_keys_keeps_recent(session):
    from app.guided import wizard_state
    for key in wizard_state.SHARE_KEYS:
        session[key] = "x"
    session[ws.KEY_RECENT] = ["/a"]
    session["guided_current_task"] = "share"
    wizard_state.reset_wizard()
    assert not any(k in session for k in wizard_state.SHARE_KEYS)
    assert session[ws.KEY_RECENT] == ["/a"] and "guided_current_task" not in session


def test_share_keys_match_module():
    from app.guided import wizard_state
    assert set(wizard_state.SHARE_KEYS) == {ws.KEY_FOLDER, ws.KEY_TYPE, ws.KEY_FORM, ws.KEY_PIXEL_OK,
                                            ws.KEY_DRY_RUN, ws.KEY_RESULT}


def test_gateway_for_keeps_fake_and_wraps_plain(fake):
    from app.guided import browse_view
    assert ws.gateway_for(fake) is fake
    assert browse_view._uploader_for is browse_view.uploader_for

    class PlainConnection:
        pass
    plain = PlainConnection()
    wrapped = ws.gateway_for(plain)
    assert wrapped is not plain and wrapped.server is plain
    assert ws.gateway_for(None) is None


def test_build_config_tables_returns_none_on_failed_login(monkeypatch):
    import src.utilities as utilities
    from app.logic.auth import build_config_tables

    def no_network(*args, **kwargs):
        raise AssertionError("must not try to connect")
    monkeypatch.setattr(utilities, "XNATConnection", no_network)
    # The address does not match the project address, so the login is refused before any connection.
    assert build_config_tables("https://fake-xnat.test/xnat/", "student_a", "synthetic-secret") is None


def test_set_authenticated_stores_object_not_password(session):
    from app import state
    marker = object()
    state.set_authenticated("student_a", "server", config_tables=marker)
    assert state.get_config_tables() is marker
    state.set_authenticated("student_a", "server")
    assert state.get_config_tables() is None
    state.clear_auth()
    assert state.get_config_tables() is None


def test_login_keeps_no_password_in_session(monkeypatch):
    from streamlit.testing.v1 import AppTest
    import app.guided.main as gmain
    from app.logic.auth import LoginResult

    monkeypatch.delenv("XNAT_DEMO_MODE", raising=False)
    monkeypatch.setattr(gmain, "attempt_login",
                        lambda u, p: LoginResult(ok=True, friendly=None, server=FakeXNAT(project_name=PROJECT), username=u))
    seen = {}

    def fake_build(url, username, secret):
        seen["called"] = True
        return "CONFIG_TABLES_OBJECT"
    monkeypatch.setattr(gmain, "build_config_tables", fake_build)

    def script():
        import app.guided.main as m
        m._render_login()

    at = AppTest.from_function(script, default_timeout=30)
    at.run()
    at.text_input(key="login_username").input("student_a")
    at.text_input(key="login_password").input("synthetic-secret")
    at.button[0].click()
    at.run()
    assert seen.get("called")
    assert at.session_state["xnat_config_tables"] == "CONFIG_TABLES_OBJECT"
    assert "synthetic-secret" not in repr(at.session_state)


# ---------------------------------------------------------------------------
# T008: folder checks and recent list
# ---------------------------------------------------------------------------

def test_clean_path_strips_quotes_and_spaces(tmp_path):
    assert ws.clean_path(f'  "{tmp_path}"  ') == str(tmp_path)
    assert ws.clean_path(f"'{tmp_path}'") == str(tmp_path)
    assert ws.clean_path("   ") == ""


def test_check_folder_cases(tmp_path):
    assert ws.check_folder(f'"{tmp_path}"').ok
    missing = ws.check_folder(str(tmp_path / "nope"))
    assert not missing.ok and "no folder" in missing.problem
    f = tmp_path / "a.txt"
    f.write_text("x")
    assert "file, not a folder" in ws.check_folder(str(f)).problem
    assert not ws.check_folder("").ok


@pytest.mark.skipif(os.name == "nt" or (hasattr(os, "geteuid") and os.geteuid() == 0),
                    reason="permission bits are not enforced here")
def test_check_folder_unreadable(tmp_path):
    locked = tmp_path / "locked"
    locked.mkdir()
    locked.chmod(0)
    try:
        check = ws.check_folder(str(locked))
        assert not check.ok and "could not be opened" in check.problem
    finally:
        locked.chmod(0o755)


def test_remember_folder_newest_first_limit_no_duplicates():
    recent = []
    for i in range(12):
        recent = ws.remember_folder(recent, f"/f{i}")
    assert len(recent) == 10 and recent[0] == "/f11"
    recent = ws.remember_folder(recent, "/f5")
    assert recent[0] == "/f5" and recent.count("/f5") == 1


# ---------------------------------------------------------------------------
# T009: types and template
# ---------------------------------------------------------------------------

def test_shareable_types_lists_intake_types_only():
    names = [n for n, _ in ws.shareable_types(load_types())]
    assert "knee_flexion_angle" in names and "segmentation_consensus" in names
    assert "annotations" not in names


def test_prepare_folder_writes_template_when_absent(tmp_path):
    desc, problem = ws.prepare_folder(tmp_path, "knee_flexion_angle")
    assert problem is None and desc["type_name"] == "knee_flexion_angle"
    assert (tmp_path / "analysis.json").is_file()


def test_prepare_folder_needs_a_type_when_absent(tmp_path):
    desc, problem = ws.prepare_folder(tmp_path, None)
    assert desc is None and problem.title == "Pick the kind of result"
    assert not (tmp_path / "analysis.json").exists()


def test_prepare_folder_keeps_existing_type(consensus):
    before = (consensus.output / "analysis.json").read_text()
    desc, problem = ws.prepare_folder(consensus.output, "knee_flexion_angle")
    assert problem is None and desc["type_name"] == "segmentation_consensus"
    assert (consensus.output / "analysis.json").read_text() == before


def test_prepare_folder_refuses_bad_json(tmp_path):
    (tmp_path / "analysis.json").write_text("{broken")
    desc, problem = ws.prepare_folder(tmp_path, None)
    assert desc is None and "not valid JSON" in problem.title


# ---------------------------------------------------------------------------
# T010: Review form
# ---------------------------------------------------------------------------

def test_form_blockers():
    good = {"case_uid": "CASEA", "code_ref": "git:1", "parameters": [{"key": "a", "value": "1"}]}
    assert ws.form_blockers(good) == []
    assert any("case ID" in b for b in ws.form_blockers({**good, "case_uid": " "}))
    assert any("code reference" in b for b in ws.form_blockers({**good, "code_ref": ""}))
    dup = {**good, "parameters": [{"key": "a", "value": "1"}, {"key": "a", "value": "2"}]}
    assert any("used twice" in b for b in ws.form_blockers(dup))
    assert any("needs a name" in b for b in ws.form_blockers({**good, "parameters": [{"key": "", "value": "2"}]}))


def test_apply_form_changes_only_run_fields(angles):
    desc = json.loads((angles.output / "analysis.json").read_text())
    desc["run"]["source_hashes"] = [{"sha256": "ab"}]
    desc["run"]["producer"] = "someone"
    form = ws.form_from_descriptor(desc)
    assert form["parameters"] == [{"key": "smoothing", "value": "3"}]
    form.update(case_uid="CASEB", notes="fine", supersedes="", code_ref="git:9")
    form["parameters"].append({"key": "side", "value": "left"})
    new = ws.apply_form(desc, form)
    assert new["run"]["case_uid"] == "CASEB" and new["run"]["parameters"] == {"smoothing": 3, "side": "left"}
    assert new["run"]["supersedes"] is None
    assert new["run"]["source_hashes"] == [{"sha256": "ab"}] and new["run"]["producer"] == "someone"
    assert {k: v for k, v in new.items() if k != "run"} == {k: v for k, v in desc.items() if k != "run"}
    assert desc["run"]["case_uid"] != "CASEB"


def test_save_form_writes_analysis_json(angles):
    desc = json.loads((angles.output / "analysis.json").read_text())
    form = ws.form_from_descriptor(desc)
    form["notes"] = "second try"
    assert ws.save_form(angles.output, form) is None
    assert json.loads((angles.output / "analysis.json").read_text())["run"]["notes"] == "second try"


# ---------------------------------------------------------------------------
# T011: full share
# ---------------------------------------------------------------------------

def test_full_share_knee_flexion_angle(angles, fake):
    dry = _dry(angles)
    assert dry.status == "dry_run_ok"
    assert not _writes(fake)
    ct = FakeConfigTables()
    result = _share(angles, fake, ct)
    assert result.status == "done"
    card = ws.outcome_card(result, angles.output)
    assert card.label == "knee_flexion_angle__v1" and card.placement_used == "assessor"
    assert card.verified and card.catalog_row and card.friendly is None
    assert card.command_line == f"main.py publish-analysis {angles.output}"
    assert "knee_flexion_angle__v1" in fake.list_assessors(EXPERIMENT_QS)


def test_command_line_quotes_spaces():
    assert ws.command_line_for(Path("/a b/c")) == 'main.py publish-analysis "/a b/c"'


def test_policy_rows(angles):
    atype = load_types()["knee_flexion_angle"]
    assert {s for _, s in ws.policy_rows(atype, None)} == {"not checked yet"}
    assert {s for _, s in ws.policy_rows(atype, _dry(angles))} == {"pass"}
    assert len(ws.policy_rows(atype, None)) == 2


# ---------------------------------------------------------------------------
# T020: home card and route (AppTest)
# ---------------------------------------------------------------------------

def test_home_has_share_card_and_route(monkeypatch):
    from streamlit.testing.v1 import AppTest
    monkeypatch.delenv("XNAT_DEMO_MODE", raising=False)

    def script():
        import app.guided.main as m
        m._render_authenticated()

    at = AppTest.from_function(script, default_timeout=30)
    at.session_state["xnat_authenticated"] = True
    at.session_state["xnat_username"] = "student_a"
    at.session_state["xnat_server_handle"] = FakeXNAT(project_name=PROJECT)
    at.run()
    assert not at.exception
    headings = " ".join(m.value for m in at.markdown)
    for card in ("Add a Surgery", "Find & Download", "Work with Annotations", "Share a result"):
        assert card in headings
    at.button(key="btn_share").click()
    at.run()
    assert not at.exception
    assert any("Step 1 of 5: Pick folder" in m.value for m in at.markdown)


# ---------------------------------------------------------------------------
# T021, T022: refusals keep the student on the page
# ---------------------------------------------------------------------------

def test_undeclared_png_refused_server_unchanged(tmp_path, fake):
    case = _with_manifest(dirty_copied_png(tmp_path))
    assert _dry(case).status == "refused"
    result = _share(case, fake, FakeConfigTables())
    assert result.status == "refused" and not _writes(fake)


def test_flagged_notes_name_never_shown(tmp_path, fake):
    case = _with_manifest(dirty_notes_name(tmp_path))
    dry = _dry(case)
    assert dry.status == "refused"
    assert "notes line 2" in dry.friendly.message and "PERSON" in dry.friendly.message
    atype = load_types()["knee_flexion_angle"]
    shown = _all_text([dry.friendly, ws.policy_rows(atype, dry), dry.warnings])
    real = _share(case, fake, FakeConfigTables())
    shown += _all_text(ws.outcome_card(real, case.output))
    assert SYNTHETIC_NAME not in shown and SYNTHETIC_NAME.split()[0] not in shown
    assert not _writes(fake)


def test_refusal_keeps_form_and_check_again_passes(tmp_path, session):
    case = _with_manifest(dirty_notes_name(tmp_path))
    desc = json.loads((case.output / "analysis.json").read_text())
    form = ws.form_from_descriptor(desc)
    session[ws.KEY_FORM] = form
    session[ws.KEY_DRY_RUN] = _dry(case)
    assert session[ws.KEY_DRY_RUN].status == "refused"
    assert session[ws.KEY_FORM] == form
    fixed = dict(form, notes="Ran fine.")
    session[ws.KEY_FORM] = fixed
    ws.discard_dry_run()
    assert session[ws.KEY_DRY_RUN] is None
    assert ws.save_form(case.output, fixed) is None
    assert _dry(case).status == "dry_run_ok"


# ---------------------------------------------------------------------------
# T024: not verified and catalog failed
# ---------------------------------------------------------------------------

def test_not_verified_card(angles, fake):
    fake.set_file_content("angles.csv", b"frame_index\n0\n")
    ct = FakeConfigTables()
    result = _share(angles, fake, ct)
    assert result.status == "not_verified"
    card = ws.outcome_card(result, angles.output)
    assert not card.verified and not card.catalog_row
    assert card.friendly.title == "Upload not confirmed" and "NOT counted" in card.note
    assert ct.server_rows == {}


def test_catalog_failed_card(angles, fake):
    result = _share(angles, fake, None)
    assert result.status == "catalog_failed"
    card = ws.outcome_card(result, angles.output)
    assert card.verified and not card.catalog_row
    assert card.friendly.title == "Catalog not updated" and "again" in card.note
    # Sharing the same folder again adds the row.
    again = _share(angles, fake, FakeConfigTables())
    assert again.status == "done" and ws.outcome_card(again, angles.output).catalog_row


# ---------------------------------------------------------------------------
# T025, T027: image confirmation
# ---------------------------------------------------------------------------

def test_needs_pixel_confirmation_and_confirmer():
    types = load_types()
    assert ws.needs_pixel_confirmation(types["segmentation_consensus"])
    assert not ws.needs_pixel_confirmation(types["knee_flexion_angle"])
    assert ws.make_confirmer(True)("ctx") == "confirmed"
    assert ws.make_confirmer(False)("ctx") == "declined"


def test_unticked_image_check_refused(consensus):
    dry = _dry(consensus, pixel_ok=False)
    assert dry.status == "refused" and dry.friendly.title == "Image check not confirmed"
    assert _dry(consensus, pixel_ok=True).status == "dry_run_ok"


def test_ticked_consensus_share_records_confirmation(consensus, fake, tmp_path):
    result = _share(consensus, fake, FakeConfigTables(), pixel_ok=True)
    assert result.status == "done"
    dest = tmp_path / "back"
    dest.mkdir()
    fake.download_resource(f"{EXPERIMENT_QS}/assessor/{result.label}", "SEGMENTATION_CONSENSUS", dest)
    remote = json.loads((dest / "analysis.json").read_text())
    assert remote["run"]["pixel_confirmation"] == "confirmed"


# ---------------------------------------------------------------------------
# T028, T029: PHI text check not available
# ---------------------------------------------------------------------------

def test_classifier_unavailable_fails_closed(angles, monkeypatch):
    from src.services.analysis_intake import gates

    def unavailable():
        raise gates.ClassifierUnavailable("ImportError")
    monkeypatch.setattr(gates, "default_classifier", unavailable)
    dry = _dry(angles, classifier=None)
    assert dry.status == "refused" and dry.friendly.title == "PHI text check is not available"
    assert any("Data Librarian" in step for step in dry.friendly.recourse)
    atype = load_types()["knee_flexion_angle"]
    assert {s for _, s in ws.policy_rows(atype, dry)} == {"stop"}   # Next stays locked


def test_page_passes_no_classifier_by_default():
    import inspect
    assert inspect.signature(ws.run_dry).parameters["classifier"].default is None
    assert inspect.signature(ws.run_share).parameters["classifier"].default is None
    assert inspect.signature(ws.render_share_wizard).parameters["classifier"].default is None


def test_consensus_needs_no_classifier(consensus, monkeypatch):
    from src.services.analysis_intake import gates

    def unavailable():
        raise gates.ClassifierUnavailable("ImportError")
    monkeypatch.setattr(gates, "default_classifier", unavailable)
    assert _dry(consensus, pixel_ok=True, classifier=None).status == "dry_run_ok"


# ---------------------------------------------------------------------------
# T030, T031: one publish path; Cancel and Done clear state
# ---------------------------------------------------------------------------

def test_page_has_no_second_publish_path():
    source = (REPO / "app" / "guided" / "wizard_share.py").read_text(encoding="utf-8")
    for name in ("publish_analysis", "verify_published", "record_in_catalog", "insert_file", "put_file",
                 "create_assessor"):
        assert name not in source, name


def test_cancel_and_done_clear_state_leave_folder(angles, session):
    before = (angles.output / "analysis.json").read_bytes()
    for _ in range(2):   # once for Cancel, once for Done: both use the same clear
        session.update({ws.KEY_FOLDER: str(angles.output), ws.KEY_TYPE: "knee_flexion_angle",
                        ws.KEY_FORM: {"case_uid": "x"}, ws.KEY_PIXEL_OK: True, ws.KEY_DRY_RUN: "r",
                        ws.KEY_RESULT: "r", ws.KEY_RECENT: [str(angles.output)],
                        ws.WIDGET_PREFIX + "path": str(angles.output), "guided_current_task": "share",
                        "guided_wizard_step": 3})
        ws.clear_share_state()
        assert set(session) == {ws.KEY_RECENT}
    assert (angles.output / "analysis.json").read_bytes() == before
