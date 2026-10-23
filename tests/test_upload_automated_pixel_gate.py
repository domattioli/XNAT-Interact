"""
Automated pixel review gates every upload UI (operator decision, 2026-10-22).

prepare_and_upload runs the automated pixel review on the case before any
publish call: CLEAN uploads, REDACT masks and uploads with no human approval,
QUARANTINE stops with a FriendlyError.  A human ABORT still stops first.

Offline: synthetic twins only, OCR tiers stubbed where determinism matters.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, List

import pytest

from app.logic import upload as logic
from app.logic.upload import automated_pixel_review, prepare_and_upload
from src.services.errors import FriendlyError
from src.xnat_experiment_data import ReviewDecision
from tests import synthetic_twins as tw

REPO = Path(__file__).resolve().parents[1]
TWIN_PROFILES = str(REPO / "tests" / "fixtures" / "device_profiles_twins")
SHIPPED_PROFILES = str(REPO / "data" / "device_profiles")

_FORM = {
    "surgeon_hawkid": "jdoe",
    "operation_date": "2024-03-15",
    "acquisition_site": "UIHC",
    "procedure_name": "Knee Arthroscopy",
}


@pytest.fixture(scope="module")
def twins(tmp_path_factory):
    return tw.make_all_twins(tmp_path_factory.mktemp("gate_twins"))


@pytest.fixture
def stub_detectors(monkeypatch):
    from src.services.pixel_deid import detect, verdict

    monkeypatch.setattr(detect, "multipass_detect", lambda *a, **k: [])
    monkeypatch.setattr(verdict, "_check_phi_outside_mask", lambda *a, **k: False)


@pytest.fixture
def valid_form(monkeypatch):
    monkeypatch.setattr(logic, "validate_intake", lambda form: [])
    return dict(_FORM)


class _Publish:
    def __init__(self) -> None:
        self.calls: List[Any] = []

    def __call__(self, server_connection, form_values, image_dir, pixel_review_confirmer):
        self.calls.append(pixel_review_confirmer("ctx"))


def _run(form, image_dir, review, *, decision=ReviewDecision.CONFIRMED, boxes=None):
    pub = _Publish()
    out = prepare_and_upload(form, image_dir, object(), review_decision=decision,
                             redaction_boxes=boxes, publish_fn=pub, pixel_review=review)
    return out, pub


def test_clean_uploads_with_caller_decision(valid_form, tmp_path):
    out, pub = _run(valid_form, tmp_path, lambda d: (ReviewDecision.CONFIRMED, []))
    assert out.ok and pub.calls == [(ReviewDecision.CONFIRMED, [])]


def test_redact_uploads_masked_without_human_approval(valid_form, tmp_path):
    auto = [(1, 2, 30, 40)]
    out, pub = _run(valid_form, tmp_path, lambda d: (ReviewDecision.REDACT, auto))
    assert out.ok
    assert pub.calls == [(ReviewDecision.REDACT, auto)]


def test_redact_keeps_human_boxes_too(valid_form, tmp_path):
    out, pub = _run(valid_form, tmp_path, lambda d: (ReviewDecision.REDACT, [(1, 1, 2, 2)]),
                    boxes=[(9, 9, 3, 3)])
    assert out.ok and set(pub.calls[0][1]) == {(1, 1, 2, 2), (9, 9, 3, 3)}


def test_quarantine_stops_with_friendly_next_step(valid_form, tmp_path):
    out, pub = _run(valid_form, tmp_path, lambda d: (ReviewDecision.QUARANTINE, []))
    assert not out.ok and pub.calls == []
    assert isinstance(out.friendly, FriendlyError)
    assert out.friendly.recourse and "Data Librarian" in " ".join(out.friendly.recourse)


def test_review_crash_fails_closed_softly(valid_form, tmp_path):
    def boom(d):
        raise RuntimeError("pixel decode failed")

    out, pub = _run(valid_form, tmp_path, boom)
    assert not out.ok and pub.calls == [] and isinstance(out.friendly, FriendlyError)


def test_human_abort_wins_before_review(valid_form, tmp_path):
    seen = []
    out, pub = _run(valid_form, tmp_path, lambda d: seen.append(d) or (ReviewDecision.CONFIRMED, []),
                    decision=ReviewDecision.ABORT)
    assert not out.ok and pub.calls == [] and seen == []


def test_default_gate_is_automated_review():
    import inspect

    assert inspect.signature(prepare_and_upload).parameters["pixel_review"].default is automated_pixel_review


@pytest.mark.parametrize("ui", ["app/pages/upload.py", "app/guided/wizard_upload.py"])
def test_both_uis_keep_the_gate(ui):
    src = (REPO / ui).read_text()
    assert "prepare_and_upload(" in src
    assert "pixel_review=" not in src, f"{ui} must not override or disable the automated pixel review"


def test_no_dicom_means_nothing_to_review(tmp_path):
    (tmp_path / "notes.txt").write_text("not an image")
    assert automated_pixel_review(tmp_path) is None


def test_real_review_on_profiled_twin_redacts(twins, stub_detectors):
    decision, boxes = automated_pixel_review(twins["089"]["folder"], profiles_dir=TWIN_PROFILES)
    assert decision == ReviewDecision.REDACT and boxes
    assert all(len(b) == 4 and b[2] > 0 and b[3] > 0 for b in boxes)  # (x, y, w, h)


def test_real_review_works_without_craft_model(twins, stub_detectors, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)  # models/craft absent: CRAFT is not required
    decision, _ = automated_pixel_review(twins["089"]["folder"], profiles_dir=TWIN_PROFILES)
    assert decision == ReviewDecision.REDACT


def test_unprofiled_twin_is_quarantined_end_to_end(twins, stub_detectors, valid_form, tmp_path):
    import shutil

    single = [f["path"] for f in twins["089"]["files"] if f["role"] == "dicom" and f["n_frames"] == 1][0]
    shutil.copy(Path(twins["089"]["folder"]) / single, tmp_path / single)
    pub = _Publish()
    out = prepare_and_upload(valid_form, tmp_path, object(),
                             review_decision=ReviewDecision.CONFIRMED, publish_fn=pub,
                             pixel_review=lambda d: automated_pixel_review(d, profiles_dir=SHIPPED_PROFILES))
    assert not out.ok and pub.calls == []
    assert "held" in out.friendly.title.lower()
