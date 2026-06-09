"""
app/logic/metrics — PHI-free, opt-in, aggregate-only adoption metrics.

Contract
--------
PHI-FREE by construction
    Event names and field keys are validated against strict allowlists before
    any data is written.  Free-form strings (filenames, usernames, paths,
    patient names) are NEVER accepted.  Allowed field values are numbers,
    booleans, or short controlled-vocabulary strings drawn from a closed enum.

OPT-IN
    Every write path requires ``enabled=True`` to be passed explicitly.
    When ``enabled`` is False the function is a no-op and returns None.
    The default opt-in state in the calling application should be False
    (off by default; the user must affirmatively enable metrics).

AGGREGATE-ONLY
    ``summarize()`` returns only counts and derived aggregate statistics
    (rates, medians).  It never returns per-event rows, per-user rows, or
    any value that could identify an individual.

NO NETWORK
    All writes go to a local append-only JSONL file specified by the caller.
    No data ever leaves the machine via this module.

NO STREAMLIT
    This module has zero Streamlit imports; it is safe to test in environments
    where Streamlit is not installed.
"""
from __future__ import annotations

import json
import statistics
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

# ---------------------------------------------------------------------------
# Allowlists — PHI-free by construction
# ---------------------------------------------------------------------------

#: Closed set of event names this module will accept.
#: Any event name NOT in this set is rejected with ValueError.
ALLOWED_EVENT_NAMES: frozenset[str] = frozenset(
    {
        "app_opened",
        "login_success",
        "upload_attempt",
        "upload_success",
        "upload_failure",
        "first_upload_completed",
        "download_attempt",
    }
)

#: Closed set of field keys this module will accept in an event's ``fields``
#: dict.  Any key NOT in this set is rejected with ValueError.
#:
#: Rationale for each key:
#:   duration_seconds  — numeric elapsed time; no identity information.
#:   row_count         — integer count of rows/files; no identity information.
#:   ok                — boolean success/failure flag.
#:   screen            — short enum string (e.g. "upload", "browse"); no PHI.
#:   session_token     — opaque random UUID assigned at app startup; NOT a
#:                       username, patient id, or any human-readable identifier.
ALLOWED_FIELD_KEYS: frozenset[str] = frozenset(
    {
        "duration_seconds",
        "row_count",
        "ok",
        "screen",
        "session_token",
    }
)

#: Allowed types for field values.  Strings are permitted only for the
#: ``screen`` and ``session_token`` fields (see _validate_fields).
_ALLOWED_VALUE_TYPES = (int, float, bool)

#: ``screen`` values must be drawn from this closed enum.
ALLOWED_SCREEN_VALUES: frozenset[str] = frozenset(
    {
        "upload",
        "browse",
        "download",
        "login",
        "home",
        "settings",
    }
)


# ---------------------------------------------------------------------------
# Dataclass
# ---------------------------------------------------------------------------


@dataclass
class MetricEvent:
    """A single anonymised adoption metric event.

    Attributes
    ----------
    name:
        Event name; must be in ``ALLOWED_EVENT_NAMES``.
    timestamp:
        ISO-8601 UTC timestamp string, e.g. ``"2026-06-04T12:34:56.789012+00:00"``.
    fields:
        Optional key/value payload; all keys must be in ``ALLOWED_FIELD_KEYS``.
    """

    name: str
    timestamp: str
    fields: Dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _utc_now() -> str:
    """Return current UTC time as an ISO-8601 string."""
    return datetime.now(tz=timezone.utc).isoformat()


def _validate_event_name(name: str) -> None:
    """Raise ValueError if *name* is not in the allowlist."""
    if name not in ALLOWED_EVENT_NAMES:
        raise ValueError(
            f"Metric event name {name!r} is not in the allowlist. "
            f"Allowed: {sorted(ALLOWED_EVENT_NAMES)}"
        )


def _validate_fields(fields: Dict[str, Any]) -> None:
    """Raise ValueError if any field key or value is not permitted.

    Rules
    -----
    1. Every key must be in ``ALLOWED_FIELD_KEYS``.
    2. Every value must be int, float, bool, or — for ``screen`` and
       ``session_token`` only — a short string from a closed enum or an
       opaque UUID-shaped token.
    3. Free-form string values on any other key are rejected even if the
       key is in the allowlist, preventing accidental PHI leakage through
       e.g. ``{"duration_seconds": "patient_name/123"}``.
    """
    for key, value in fields.items():
        if key not in ALLOWED_FIELD_KEYS:
            raise ValueError(
                f"Field key {key!r} is not in the allowlist. "
                f"Allowed keys: {sorted(ALLOWED_FIELD_KEYS)}"
            )
        if key == "screen":
            if not isinstance(value, str) or value not in ALLOWED_SCREEN_VALUES:
                raise ValueError(
                    f"Field 'screen' value {value!r} must be one of "
                    f"{sorted(ALLOWED_SCREEN_VALUES)}"
                )
        elif key == "session_token":
            # Must be a non-empty string; we do not validate UUID format strictly
            # so that callers can pass any opaque token, but we cap length to
            # prevent embedding long strings.
            if not isinstance(value, str) or len(value) > 64:
                raise ValueError(
                    "Field 'session_token' must be a non-empty string of at most "
                    "64 characters (opaque token, not a username or identifier)."
                )
        elif not isinstance(value, _ALLOWED_VALUE_TYPES):
            raise ValueError(
                f"Field {key!r} value {value!r} has type {type(value).__name__!r}; "
                f"only int, float, bool are accepted for this field."
            )


# ---------------------------------------------------------------------------
# Core API
# ---------------------------------------------------------------------------


def record_event(
    name: str,
    fields: Optional[Dict[str, Any]] = None,
    *,
    enabled: bool,
    store_path: str | Path,
    clock=None,
) -> Optional[MetricEvent]:
    """Record a single metric event to the append-only JSONL store.

    Parameters
    ----------
    name:
        Event name; must be in ``ALLOWED_EVENT_NAMES``.
    fields:
        Optional dict of non-PHI key/value pairs; all keys must be in
        ``ALLOWED_FIELD_KEYS``.
    enabled:
        **Opt-in gate.**  When False, the function is a no-op and returns
        None without touching the filesystem.  Pass ``enabled=True`` only
        after the user has explicitly opted in.
    store_path:
        Path to the append-only JSONL log file.  Created on first write.
    clock:
        Optional callable returning an ISO timestamp string.  Defaults to
        ``_utc_now()``.  Injectable for deterministic tests.

    Returns
    -------
    MetricEvent or None
        The event that was written, or None when disabled.

    Raises
    ------
    ValueError
        If *name* is not in ``ALLOWED_EVENT_NAMES``, or if any field key /
        value violates the allowlist rules.
    """
    if not enabled:
        return None

    _validate_event_name(name)
    safe_fields: Dict[str, Any] = fields if fields is not None else {}
    _validate_fields(safe_fields)

    ts = clock() if clock is not None else _utc_now()
    event = MetricEvent(name=name, timestamp=ts, fields=safe_fields)

    store = Path(store_path)
    store.parent.mkdir(parents=True, exist_ok=True)
    with store.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps({"name": event.name, "timestamp": event.timestamp, "fields": event.fields}) + "\n")

    return event


# ---------------------------------------------------------------------------
# Typed helpers
# ---------------------------------------------------------------------------


def record_upload_attempt(
    ok: bool,
    *,
    enabled: bool,
    store_path: str | Path,
) -> Optional[MetricEvent]:
    """Record one upload attempt.

    Parameters
    ----------
    ok:
        True on success, False on failure.
    """
    name = "upload_success" if ok else "upload_failure"
    return record_event(name, {"ok": ok}, enabled=enabled, store_path=store_path)


def record_first_upload(
    duration_seconds: float,
    *,
    enabled: bool,
    store_path: str | Path,
) -> Optional[MetricEvent]:
    """Record the elapsed time to the user's first successful upload.

    Parameters
    ----------
    duration_seconds:
        Wall-clock seconds from app_opened (or login_success) to the
        moment the first upload completes.  Must be a non-negative number.
    """
    if duration_seconds < 0:
        raise ValueError(f"duration_seconds must be non-negative; got {duration_seconds!r}")
    return record_event(
        "first_upload_completed",
        {"duration_seconds": float(duration_seconds)},
        enabled=enabled,
        store_path=store_path,
    )


def record_app_opened(
    screen: str = "home",
    *,
    enabled: bool,
    store_path: str | Path,
) -> Optional[MetricEvent]:
    """Record the app_opened event with the landing screen."""
    return record_event("app_opened", {"screen": screen}, enabled=enabled, store_path=store_path)


def record_download_attempt(
    *,
    enabled: bool,
    store_path: str | Path,
) -> Optional[MetricEvent]:
    """Record a download attempt."""
    return record_event("download_attempt", enabled=enabled, store_path=store_path)


def make_session_token() -> str:
    """Return a new opaque random session token (UUID4 hex, no dashes).

    This is NOT a username, patient id, or any human-readable identifier.
    It is suitable for counting distinct active sessions in an aggregate
    summary WITHOUT linking events to a real-world identity.
    """
    return uuid.uuid4().hex  # 32 hex chars; no PHI


# ---------------------------------------------------------------------------
# Aggregate summary
# ---------------------------------------------------------------------------


def summarize(store_path: str | Path) -> dict:
    """Read the JSONL log and return aggregate-only statistics.

    The returned dict contains **only** aggregate values: counts and derived
    statistics.  It never contains per-event rows, per-user rows, or any
    value that could identify an individual.

    Keys in the returned dict
    -------------------------
    event_counts : dict[str, int]
        Total count of each event name seen in the log.
    upload_attempts : int
        upload_success + upload_failure events.
    upload_success_rate : float or None
        upload_success / upload_attempts; None when attempts == 0.
    median_time_to_first_upload_seconds : float or None
        Median of ``duration_seconds`` from all ``first_upload_completed``
        events; None when no such events exist.
    distinct_session_count : int
        Count of distinct opaque session_token values across all events.
        Sessions are identified by random UUID tokens only — never by
        username or patient identifier.

    Returns
    -------
    dict
    """
    store = Path(store_path)
    if not store.exists():
        return {
            "event_counts": {},
            "upload_attempts": 0,
            "upload_success_rate": None,
            "median_time_to_first_upload_seconds": None,
            "distinct_session_count": 0,
        }

    event_counts: Dict[str, int] = {}
    ttfu_durations: list[float] = []
    session_tokens: set[str] = set()

    with store.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue

            name = obj.get("name", "")
            event_counts[name] = event_counts.get(name, 0) + 1

            fields = obj.get("fields", {})

            if name == "first_upload_completed":
                dur = fields.get("duration_seconds")
                if isinstance(dur, (int, float)):
                    ttfu_durations.append(float(dur))

            tok = fields.get("session_token")
            if isinstance(tok, str) and tok:
                session_tokens.add(tok)

    successes = event_counts.get("upload_success", 0)
    failures = event_counts.get("upload_failure", 0)
    attempts = successes + failures

    success_rate: Optional[float] = None
    if attempts > 0:
        success_rate = successes / attempts

    median_ttfu: Optional[float] = None
    if ttfu_durations:
        median_ttfu = statistics.median(ttfu_durations)

    return {
        "event_counts": event_counts,
        "upload_attempts": attempts,
        "upload_success_rate": success_rate,
        "median_time_to_first_upload_seconds": median_ttfu,
        "distinct_session_count": len(session_tokens),
    }
