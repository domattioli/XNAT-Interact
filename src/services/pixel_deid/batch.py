"""
Batch assessment driver for burned-in PHI pixel de-identification (Feature 010, Stage 5).

Throughput strategy
-------------------
``assess_batch`` runs **serially by default**.  Serial is correct, has no
process-pool failure modes, and comfortably meets the SC-005 budget (200
image-sized cases in <=15 min) once the per-case OCR cost is bounded
(see ``detect.tesseract_boxes`` ``--psm 3`` + timeout).

Parallelism is **opt-in** (``parallel=True``) and defensive: it uses a
``spawn`` multiprocessing context (NOT the default ``fork`` — forking after the
spaCy/Presidio NLP stack has loaded deadlocks the children) and a per-future
timeout so a hung or slow worker degrades to a serial recompute instead of
blocking the batch forever.  If pool setup or any worker fails, the whole batch
falls back to serial; correctness is never sacrificed for speed.

Public API
----------
assess_batch(cases, *, max_workers=None, parallel=False, worker_timeout=120, **assess_kw)
    -> list[CaseAssessment]
    Assess (case_id, frames, dataset) tuples, preserving input order.

batch_summary(assessments) -> dict
    Counts per verdict (clean / redacted / quarantine / total / failed).

``import src.services.pixel_deid.batch`` is cheap — heavy deps load lazily.
"""
from __future__ import annotations

import logging
from typing import Iterable, Optional

logger = logging.getLogger(__name__)


def assess_batch(
    cases: Iterable[tuple],
    *,
    max_workers: Optional[int] = None,
    parallel: bool = False,
    worker_timeout: float = 120.0,
    **assess_kw,
) -> list:
    """
    Assess a batch of cases, preserving input order.

    Parameters
    ----------
    cases:
        Iterable of ``(case_id, frames, dataset)`` tuples:
        - case_id: str / identifier
        - frames:  list[np.ndarray] of pixel data
        - dataset: pydicom Dataset (device identity lookup)
    max_workers:
        Worker process count when ``parallel`` is True (default: CPU count).
    parallel:
        Opt-in process-pool fan-out.  Default False (serial) — serial is the
        reliable path and meets the throughput budget.  When True, uses a
        ``spawn`` context + per-future timeout and degrades to serial on any
        failure (pickling, spawn, worker error, or *worker_timeout* expiry).
    worker_timeout:
        Per-case ceiling (seconds) for a parallel worker before the batch
        abandons the pool and recomputes serially.
    **assess_kw:
        Forwarded to ``assess_case`` (registry, model_dir, profiles_dir, margin).

    Returns
    -------
    list[CaseAssessment]
        One assessment per input case, in input order.  Correct regardless of
        whether the parallel path was used or degraded to serial.
    """
    case_list = list(cases)
    if not case_list:
        return []

    if parallel:
        result = _assess_batch_parallel(
            case_list,
            max_workers=max_workers,
            worker_timeout=worker_timeout,
            **assess_kw,
        )
        if result is not None:
            return result
        logger.warning(
            "Parallel assessment unavailable or timed out; "
            "falling back to serial. Correctness preserved."
        )

    return _assess_batch_serial(case_list, **assess_kw)


def _assess_batch_serial(case_list: list, **assess_kw) -> list:
    """Assess all cases serially, in order.  Per-case errors yield None."""
    from src.services.pixel_deid.verdict import assess_case

    assessments: list = []
    for i, (case_id, frames, dataset) in enumerate(case_list):
        try:
            assessments.append(assess_case(frames, dataset, **assess_kw))
        except Exception as exc:
            logger.error("Serial assessment failed for case %s (#%d): %s", case_id, i, exc)
            assessments.append(None)
    return assessments


def _assess_batch_parallel(
    case_list: list,
    *,
    max_workers: Optional[int] = None,
    worker_timeout: float = 120.0,
    **assess_kw,
) -> Optional[list]:
    """
    Best-effort parallel assessment.

    Returns the ordered list on success, or ``None`` to signal the caller to
    fall back to serial (on pool-setup failure, worker error, or timeout).
    Uses a ``spawn`` context to avoid fork-after-NLP-load deadlocks.
    """
    try:
        import multiprocessing as mp
        from concurrent.futures import ProcessPoolExecutor, TimeoutError as FutureTimeout

        ctx = mp.get_context("spawn")
        ordered: dict[int, object] = {}
        with ProcessPoolExecutor(max_workers=max_workers, mp_context=ctx) as executor:
            future_to_idx = {
                executor.submit(_assess_case_worker, case, assess_kw): idx
                for idx, case in enumerate(case_list)
            }
            for future, idx in future_to_idx.items():
                try:
                    ordered[idx] = future.result(timeout=worker_timeout)
                except FutureTimeout:
                    logger.error("Worker timed out for case #%d after %.0fs", idx, worker_timeout)
                    return None
                except Exception as exc:
                    logger.error("Worker failed for case #%d: %s", idx, exc)
                    return None
        return [ordered[i] for i in range(len(case_list))]
    except Exception as exc:
        logger.debug("ProcessPool setup failed (%s); degrade to serial.", type(exc).__name__)
        return None


def _assess_case_worker(case: tuple, assess_kw: dict) -> object:
    """ProcessPool worker: (case_id, frames, dataset) -> CaseAssessment."""
    from src.services.pixel_deid.verdict import assess_case

    _case_id, frames, dataset = case
    return assess_case(frames, dataset, **assess_kw)


def batch_summary(assessments: list) -> dict:
    """
    Summarize a batch by verdict count.

    Returns a dict with keys: 'clean', 'redacted', 'quarantine', 'total',
    'failed'.  ``total`` counts assessed (non-None) cases; ``failed`` counts
    None entries (per-case errors).
    """
    from src.services.pixel_deid.verdict import Verdict  # noqa: F401  (kept for symmetry/lazy load)

    summary = {"clean": 0, "redacted": 0, "quarantine": 0, "total": 0, "failed": 0}
    for assessment in assessments:
        if assessment is None:
            summary["failed"] += 1
            continue
        summary["total"] += 1
        v = assessment.verdict.value
        if v in summary:
            summary[v] += 1
    return summary
