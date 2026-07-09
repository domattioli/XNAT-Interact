# Contract: Run Report (016) — the machine-readable verdict a future agent parses

Files (both under `tests/stress/results/`, existing lane convention):

1. `sim016_<run_id>.json` — the report. Atomically rewritten (`tempfile` + `os.replace`)
   after every snapshot and at finalization: at any kill point a valid prior version exists.
2. `sim016_<run_id>.events.jsonl` — append-only event log, one JSON object per line,
   written as each event completes.
3. `sim016_<run_id>.pid` — coordinator pid, written at startup, removed at clean exit.

An agent's minimal read: `json.load(report)["verdict"]["overall"]` → `"PASS" | "FAIL"`.

## §1 Report JSON Schema (draft-07; binding for the future implementation)

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "SIM016 Run Report",
  "type": "object",
  "required": ["schema_version", "run", "counts", "failures", "snapshots", "verdict", "timeline_ref"],
  "properties": {
    "schema_version": { "const": "016.1" },
    "run": {
      "type": "object",
      "required": ["run_id", "project", "mode", "seed", "started_at", "ended_at", "config", "preamble"],
      "properties": {
        "run_id":     { "type": "string", "pattern": "^SIM016_[0-9]{8}T[0-9]{6}Z_[0-9a-f]{4}$" },
        "project":    { "type": "string" },
        "mode":       { "enum": ["smoke", "sustained"] },
        "seed":       { "type": "integer" },
        "started_at": { "type": "string", "format": "date-time" },
        "ended_at":   { "type": ["string", "null"], "format": "date-time" },
        "config":     { "type": "object" },
        "preamble": {
          "type": "object",
          "required": ["xnat_version", "preexisting_projects"],
          "properties": {
            "xnat_version": { "type": ["string", "null"] },
            "preexisting_projects": { "type": "array", "items": { "type": "string" } }
          }
        }
      }
    },
    "counts": {
      "type": "object",
      "required": ["cases_scheduled", "completed_cases", "write_attempt_total", "retry_total", "by_kind", "by_category"],
      "properties": {
        "cases_scheduled":     { "type": "integer" },
        "completed_cases":     { "type": "integer" },
        "write_attempt_total": { "type": "integer" },
        "retry_total":         { "type": "integer" },
        "by_kind":     { "type": "object", "additionalProperties": { "type": "integer" } },
        "by_category": { "type": "object",
          "properties": { "normal": {"type":"integer"}, "malformed": {"type":"integer"}, "dedup_probe": {"type":"integer"} } }
      }
    },
    "failures": {
      "type": "object",
      "required": ["expected_friendly", "unexpected_terminal", "tri_state", "unexpected_detail"],
      "properties": {
        "expected_friendly":   { "type": "integer" },
        "unexpected_terminal": { "type": "integer" },
        "tri_state": {
          "type": "object",
          "properties": { "FRIENDLY": {"type":"integer"}, "ACCEPTED": {"type":"integer"}, "CRASH": {"type":"integer"} }
        },
        "unexpected_detail": {
          "type": "array",
          "items": {
            "type": "object",
            "required": ["case_id", "classification", "error"],
            "properties": {
              "case_id": { "type": "string" },
              "classification": { "enum": ["FRIENDLY", "ACCEPTED", "CRASH", "TRANSIENT_EXHAUSTED"] },
              "error": { "type": "string", "maxLength": 2000 }
            }
          }
        }
      }
    },
    "snapshots": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["snapshot_id", "at", "sampled", "pass", "resources", "resource_ok"],
        "properties": {
          "snapshot_id": { "type": "string" },
          "at":          { "type": "string", "format": "date-time" },
          "sampled": {
            "type": "array",
            "items": {
              "type": "object",
              "required": ["case_id", "checksum_ok", "links_ok", "present_ok"],
              "properties": {
                "case_id": {"type":"string"}, "checksum_ok": {"type":"boolean"},
                "links_ok": {"type":"boolean"}, "present_ok": {"type":"boolean"},
                "detail": {"type":["string","null"]}
              }
            }
          },
          "pass": { "type": "boolean" },
          "resources": {
            "type": "object",
            "required": ["scratch_bytes", "scratch_leak_bytes", "open_connections", "scratch_monotonic_run_len"],
            "properties": {
              "scratch_bytes": {"type":"integer"}, "scratch_leak_bytes": {"type":"integer"},
              "open_connections": {"type":"integer"},
              "scratch_monotonic_run_len": {"type":"integer"}
            }
          },
          "resource_ok": { "type": "boolean" }
        }
      }
    },
    "verdict": {
      "type": "object",
      "required": ["ratio_check", "integrity_check", "resource_check", "overall", "reasons", "partial", "stopped_early"],
      "properties": {
        "ratio_check":     { "$ref": "#/definitions/check" },
        "integrity_check": { "$ref": "#/definitions/check" },
        "resource_check":  { "$ref": "#/definitions/check" },
        "overall":         { "enum": ["PASS", "FAIL"] },
        "reasons":         { "type": "array", "items": { "type": "string" } },
        "partial":         { "type": "boolean" },
        "stopped_early":   { "type": "boolean" }
      }
    },
    "timeline_ref": { "type": "string" }
  },
  "definitions": {
    "check": {
      "type": "object",
      "required": ["pass"],
      "properties": { "pass": { "type": "boolean" }, "threshold": {}, "observed": {} }
    }
  }
}
```

## §2 Verdict computation rules (normative)

1. `ratio_check.pass` ⇔ `failures.unexpected_terminal ≤ max(1, ceil(0.02 × counts.write_attempt_total))`.
   **`write_attempt_total` counts case-write EVENTS (one per scheduled case that reached
   `UPLOADING` at least once), NOT individual retry attempts** — retries are tallied
   separately in `counts.retry_total` and never inflate the denominator (a flaky run must
   not loosen its own gate). `threshold` records the computed bound; `observed` the count.
   Expected FRIENDLY rejections of injected malformed cases and `DedupReviewRequired` on
   dedup probes are EXCLUDED from the numerator and tallied in `failures.expected_friendly`.
   An injected malformed case that is silently ACCEPTED, or that CRASHes, and a dedup probe
   that publishes cleanly, are INCLUDED as unexpected (SC-008; clarify session answer 1).
2. `integrity_check.pass` ⇔ every element of `snapshots[]` has `pass = true`. One failing
   snapshot at any point ⇒ overall FAIL, independent of the ratio (zero-tolerance clause).
   A snapshot whose `sampled[]` is empty carries no evidential weight (it is vacuously
   `pass = true` but proves nothing); vacuous-PASS abuse is prevented by rule 4's
   completed-cases precondition, which guarantees the mandatory final snapshot samples at
   least one completed case on any run eligible to PASS.
3. `resource_check.pass` ⇔ every element of `snapshots[]` has `resource_ok = true`, where
   `resource_ok` ⇔ `scratch_bytes < 1_073_741_824` (gross-runaway backstop on RAW scratch,
   in-flight included) AND `scratch_monotonic_run_len < 3` AND
   `open_connections ≤ config.max_connections` (default 10).
   **`scratch_monotonic_run_len` is computed over `scratch_leak_bytes`** — the run scratch
   root's byte count EXCLUDING the scratch directories of cases currently in a non-terminal
   state — so legitimately concurrent in-flight uploads never register as growth; only
   bytes surviving past their case's terminal state (a genuine leak) do. The snapshot
   sampler's own verification connection IS included in `open_connections` and the budget.
4. `overall = "PASS"` ⇔ all three checks pass AND `counts.completed_cases ≥ 1`; otherwise
   `"FAIL"`. A run in which no case ever reached `COMPLETED` cannot PASS regardless of the
   three checks (reason string: `no_completed_writes`) — this closes the vacuous-PASS
   loophole where a run whose only write failed terminally would otherwise satisfy the
   `max(1, …)` floor and empty-snapshot integrity vacuity simultaneously.
   `partial` and `stopped_early` are ORTHOGONAL informational flags and never enter this
   formula: an early-stopped run's verdict is computed from the evidence it accumulated,
   exactly as a full run's is, and consuming agents MUST treat `partial = true` as "verdict
   covers the truncated run only". (This single formula is binding; `data-model.md` §5 and
   tasks.md T013 state the identical rule.)
5. Every FAIL appends exactly one human-readable sentence per failed check (and one for a
   failed completed-cases precondition) to `reasons[]`.

## §3 JSONL event-line schema (one object per line)

Required keys: `event_id`, `kind`, `scheduled_at`, `started_at`, `completed_at`, `outcome`,
`case_ref`, `retries`, `error` — semantics per `data-model.md` §3. Lines are appended after
event completion only; consumers MUST tolerate a truncated final line (kill -9 worst case)
by discarding it.
