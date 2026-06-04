"""
tests/test_app_metrics_logic.py — Offline tests for app/logic/metrics.

CRITICAL RULES enforced here:
  - NO `import streamlit` anywhere.
  - NO network calls.
  - NO PHI: no real patient names, paths, usernames, identifiers.
  - All writes use pytest tmp_path; nothing touches the real filesystem.

Covers:
  1. enabled=False → no file written, returns None.
  2. enabled=True → event appended; file is valid JSON-lines.
  3. Disallowed event name → ValueError raised, nothing written.
  4. Disallowed field key → ValueError raised, nothing written.
  5. Obvious PHI-ish field key (e.g. patient_name) → rejected.
  6. summarize() counts + upload_success_rate + median TTFU over seeded data.
  7. No username/identifier required or stored; session_token is opaque UUID only.
  8. record_upload_attempt helper — ok=True maps to upload_success.
  9. record_first_upload helper — negative duration raises ValueError.
 10. record_app_opened helper — invalid screen value rejected.
 11. summarize() on missing file returns zero-state dict.
 12. summarize() distinct_session_count counts unique opaque tokens only.
 13. String value on numeric-only field (e.g. duration_seconds="patient/123") rejected.
 14. make_session_token() returns 32-char hex string, two calls produce different tokens.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

# Only import from app/logic — no streamlit.
from app.logic.metrics import (
    ALLOWED_EVENT_NAMES,
    ALLOWED_FIELD_KEYS,
    ALLOWED_SCREEN_VALUES,
    MetricEvent,
    make_session_token,
    record_app_opened,
    record_download_attempt,
    record_event,
    record_first_upload,
    record_upload_attempt,
    summarize,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _store(tmp_path: Path, name: str = "metrics.jsonl") -> Path:
    return tmp_path / name


def _read_lines(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


# ---------------------------------------------------------------------------
# 1. enabled=False → no-op
# ---------------------------------------------------------------------------

def test_disabled_returns_none_no_file(tmp_path):
    store = _store(tmp_path)
    result = record_event("app_opened", enabled=False, store_path=store)
    assert result is None
    assert not store.exists()


def test_disabled_upload_helper_no_file(tmp_path):
    store = _store(tmp_path)
    result = record_upload_attempt(ok=True, enabled=False, store_path=store)
    assert result is None
    assert not store.exists()


def test_disabled_first_upload_helper_no_file(tmp_path):
    store = _store(tmp_path)
    result = record_first_upload(10.0, enabled=False, store_path=store)
    assert result is None
    assert not store.exists()


# ---------------------------------------------------------------------------
# 2. enabled=True → valid JSON-lines appended
# ---------------------------------------------------------------------------

def test_enabled_writes_jsonl(tmp_path):
    store = _store(tmp_path)
    evt = record_event("login_success", enabled=True, store_path=store)
    assert isinstance(evt, MetricEvent)
    assert store.exists()
    lines = _read_lines(store)
    assert len(lines) == 1
    assert lines[0]["name"] == "login_success"
    assert "timestamp" in lines[0]
    assert isinstance(lines[0]["fields"], dict)


def test_multiple_events_appended(tmp_path):
    store = _store(tmp_path)
    for name in ("app_opened", "login_success", "upload_attempt"):
        record_event(name, enabled=True, store_path=store)
    lines = _read_lines(store)
    assert len(lines) == 3
    names = [l["name"] for l in lines]
    assert names == ["app_opened", "login_success", "upload_attempt"]


def test_fields_round_trip(tmp_path):
    store = _store(tmp_path)
    record_event("upload_success", {"ok": True, "row_count": 5}, enabled=True, store_path=store)
    lines = _read_lines(store)
    assert lines[0]["fields"]["ok"] is True
    assert lines[0]["fields"]["row_count"] == 5


# ---------------------------------------------------------------------------
# 3. Disallowed event name → ValueError, nothing written
# ---------------------------------------------------------------------------

def test_disallowed_event_name_raises(tmp_path):
    store = _store(tmp_path)
    with pytest.raises(ValueError, match="not in the allowlist"):
        record_event("delete_patient_record", enabled=True, store_path=store)
    assert not store.exists()


def test_arbitrary_string_event_name_raises(tmp_path):
    store = _store(tmp_path)
    with pytest.raises(ValueError):
        record_event("SOME_CUSTOM_FREEFORM_EVENT", enabled=True, store_path=store)


# ---------------------------------------------------------------------------
# 4. Disallowed field key → ValueError, nothing written
# ---------------------------------------------------------------------------

def test_disallowed_field_key_raises(tmp_path):
    store = _store(tmp_path)
    with pytest.raises(ValueError, match="not in the allowlist"):
        record_event("app_opened", {"bad_key": 1}, enabled=True, store_path=store)
    assert not store.exists()


# ---------------------------------------------------------------------------
# 5. PHI-ish field key → rejected
# ---------------------------------------------------------------------------

PHI_FIELD_NAMES = [
    "patient_name",
    "username",
    "user_id",
    "mrn",
    "date_of_birth",
    "ssn",
    "email",
    "first_name",
    "last_name",
    "file_path",
    "subject_label",
]


@pytest.mark.parametrize("phi_key", PHI_FIELD_NAMES)
def test_phi_field_key_rejected(tmp_path, phi_key):
    store = _store(tmp_path)
    with pytest.raises(ValueError):
        record_event("app_opened", {phi_key: "some_value"}, enabled=True, store_path=store)
    assert not store.exists()


# ---------------------------------------------------------------------------
# 6. summarize() — counts, success rate, median TTFU
# ---------------------------------------------------------------------------

def _seed_store(store: Path, events: list[tuple]) -> None:
    """Write (name, fields) tuples as JSONL using a fixed timestamp."""
    with store.open("a") as fh:
        for name, fields in events:
            fh.write(json.dumps({"name": name, "timestamp": "2026-06-04T00:00:00+00:00", "fields": fields}) + "\n")


def test_summarize_event_counts(tmp_path):
    store = _store(tmp_path)
    _seed_store(store, [
        ("app_opened", {}),
        ("login_success", {}),
        ("upload_attempt", {}),
        ("upload_success", {"ok": True}),
        ("upload_failure", {"ok": False}),
        ("upload_success", {"ok": True}),
    ])
    summary = summarize(store)
    counts = summary["event_counts"]
    assert counts["app_opened"] == 1
    assert counts["login_success"] == 1
    assert counts["upload_attempt"] == 1
    assert counts["upload_success"] == 2
    assert counts["upload_failure"] == 1


def test_summarize_upload_success_rate(tmp_path):
    store = _store(tmp_path)
    # 3 successes, 1 failure → rate = 0.75
    _seed_store(store, [
        ("upload_success", {"ok": True}),
        ("upload_success", {"ok": True}),
        ("upload_success", {"ok": True}),
        ("upload_failure", {"ok": False}),
    ])
    summary = summarize(store)
    assert summary["upload_attempts"] == 4
    assert abs(summary["upload_success_rate"] - 0.75) < 1e-9


def test_summarize_upload_success_rate_none_when_no_attempts(tmp_path):
    store = _store(tmp_path)
    _seed_store(store, [("app_opened", {})])
    summary = summarize(store)
    assert summary["upload_success_rate"] is None
    assert summary["upload_attempts"] == 0


def test_summarize_median_ttfu_single(tmp_path):
    store = _store(tmp_path)
    _seed_store(store, [
        ("first_upload_completed", {"duration_seconds": 42.0}),
    ])
    summary = summarize(store)
    assert summary["median_time_to_first_upload_seconds"] == 42.0


def test_summarize_median_ttfu_multiple(tmp_path):
    store = _store(tmp_path)
    # durations: 10, 20, 30 → median = 20
    _seed_store(store, [
        ("first_upload_completed", {"duration_seconds": 10.0}),
        ("first_upload_completed", {"duration_seconds": 30.0}),
        ("first_upload_completed", {"duration_seconds": 20.0}),
    ])
    summary = summarize(store)
    assert summary["median_time_to_first_upload_seconds"] == 20.0


def test_summarize_median_ttfu_none_when_no_events(tmp_path):
    store = _store(tmp_path)
    _seed_store(store, [("login_success", {})])
    summary = summarize(store)
    assert summary["median_time_to_first_upload_seconds"] is None


# ---------------------------------------------------------------------------
# 7. No username/identifier required or stored
# ---------------------------------------------------------------------------

def test_no_username_field_in_allowlist():
    """username is not in ALLOWED_FIELD_KEYS — PHI-free by construction."""
    assert "username" not in ALLOWED_FIELD_KEYS
    assert "user_id" not in ALLOWED_FIELD_KEYS
    assert "patient_id" not in ALLOWED_FIELD_KEYS


def test_session_token_is_opaque_no_real_identity(tmp_path):
    """session_token field accepts only short opaque strings, not usernames."""
    store = _store(tmp_path)
    token = make_session_token()
    # Opaque token is accepted
    record_event("app_opened", {"session_token": token}, enabled=True, store_path=store)
    lines = _read_lines(store)
    assert lines[0]["fields"]["session_token"] == token
    # Token must not look like a real username (no spaces, short)
    assert " " not in token
    assert len(token) <= 64


def test_session_token_too_long_rejected(tmp_path):
    store = _store(tmp_path)
    long_string = "x" * 65
    with pytest.raises(ValueError, match="session_token"):
        record_event("app_opened", {"session_token": long_string}, enabled=True, store_path=store)


def test_summarize_distinct_session_count(tmp_path):
    store = _store(tmp_path)
    tok_a = make_session_token()
    tok_b = make_session_token()
    _seed_store(store, [
        ("app_opened", {"session_token": tok_a}),
        ("login_success", {"session_token": tok_a}),
        ("app_opened", {"session_token": tok_b}),
    ])
    summary = summarize(store)
    assert summary["distinct_session_count"] == 2


# ---------------------------------------------------------------------------
# 8. record_upload_attempt helper
# ---------------------------------------------------------------------------

def test_record_upload_attempt_success(tmp_path):
    store = _store(tmp_path)
    evt = record_upload_attempt(ok=True, enabled=True, store_path=store)
    assert evt is not None
    assert evt.name == "upload_success"
    lines = _read_lines(store)
    assert lines[0]["name"] == "upload_success"


def test_record_upload_attempt_failure(tmp_path):
    store = _store(tmp_path)
    evt = record_upload_attempt(ok=False, enabled=True, store_path=store)
    assert evt is not None
    assert evt.name == "upload_failure"


# ---------------------------------------------------------------------------
# 9. record_first_upload helper — negative duration raises
# ---------------------------------------------------------------------------

def test_record_first_upload_writes_event(tmp_path):
    store = _store(tmp_path)
    evt = record_first_upload(15.5, enabled=True, store_path=store)
    assert evt is not None
    assert evt.name == "first_upload_completed"
    lines = _read_lines(store)
    assert lines[0]["fields"]["duration_seconds"] == 15.5


def test_record_first_upload_negative_raises(tmp_path):
    store = _store(tmp_path)
    with pytest.raises(ValueError, match="non-negative"):
        record_first_upload(-1.0, enabled=True, store_path=store)


def test_record_first_upload_zero_ok(tmp_path):
    store = _store(tmp_path)
    evt = record_first_upload(0.0, enabled=True, store_path=store)
    assert evt is not None


# ---------------------------------------------------------------------------
# 10. record_app_opened — invalid screen value rejected
# ---------------------------------------------------------------------------

def test_record_app_opened_valid_screen(tmp_path):
    store = _store(tmp_path)
    evt = record_app_opened("upload", enabled=True, store_path=store)
    assert evt is not None
    lines = _read_lines(store)
    assert lines[0]["fields"]["screen"] == "upload"


def test_record_app_opened_invalid_screen_raises(tmp_path):
    store = _store(tmp_path)
    with pytest.raises(ValueError, match="screen"):
        record_app_opened("patient_dashboard", enabled=True, store_path=store)


# ---------------------------------------------------------------------------
# 11. summarize() on missing file → zero-state
# ---------------------------------------------------------------------------

def test_summarize_missing_file(tmp_path):
    store = _store(tmp_path, "does_not_exist.jsonl")
    summary = summarize(store)
    assert summary["event_counts"] == {}
    assert summary["upload_attempts"] == 0
    assert summary["upload_success_rate"] is None
    assert summary["median_time_to_first_upload_seconds"] is None
    assert summary["distinct_session_count"] == 0


# ---------------------------------------------------------------------------
# 12. String value on numeric-only field → rejected (PHI via string smuggling)
# ---------------------------------------------------------------------------

def test_string_value_on_numeric_field_rejected(tmp_path):
    """duration_seconds must be numeric — a string like 'patient/123' is rejected."""
    store = _store(tmp_path)
    with pytest.raises(ValueError):
        record_event(
            "first_upload_completed",
            {"duration_seconds": "patient/name/123"},
            enabled=True,
            store_path=store,
        )
    assert not store.exists()


def test_string_value_on_row_count_rejected(tmp_path):
    store = _store(tmp_path)
    with pytest.raises(ValueError):
        record_event(
            "upload_success",
            {"row_count": "many"},
            enabled=True,
            store_path=store,
        )


# ---------------------------------------------------------------------------
# 13. make_session_token() opaqueness
# ---------------------------------------------------------------------------

def test_make_session_token_format():
    tok = make_session_token()
    assert len(tok) == 32
    assert tok.isalnum()  # UUID4 hex, no dashes


def test_make_session_token_unique():
    tokens = {make_session_token() for _ in range(20)}
    assert len(tokens) == 20  # all distinct


# ---------------------------------------------------------------------------
# 14. Allowlist integrity — spot-checks
# ---------------------------------------------------------------------------

def test_allowed_event_names_contains_required_signals():
    required = {
        "upload_attempt", "upload_success", "upload_failure",
        "first_upload_completed", "app_opened", "login_success",
    }
    assert required.issubset(ALLOWED_EVENT_NAMES)


def test_allowed_field_keys_no_phi_keys():
    phi_candidates = {"name", "username", "password", "mrn", "dob", "ssn", "email", "path"}
    overlap = phi_candidates & ALLOWED_FIELD_KEYS
    assert overlap == set(), f"PHI-risk keys found in allowlist: {overlap}"


def test_record_download_attempt(tmp_path):
    store = _store(tmp_path)
    evt = record_download_attempt(enabled=True, store_path=store)
    assert evt is not None
    assert evt.name == "download_attempt"
