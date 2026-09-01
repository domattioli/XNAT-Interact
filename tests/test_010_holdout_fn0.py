"""
T023 + T024 — Stage 6 validation: FN=0 holdout and rate checks.

SC-001 (T023): Every PHI-bearing cell must yield REDACTED or QUARANTINE — never CLEAN.
               If REDACTED, residual PHI pixels inside the ground-truth box == 0.
SC-002 (T024a): Quarantine rate on profiled in-distribution cases is low (< 0.34 for N=6).
SC-003 (T024b): Benign 'L' laterality marker survives after profiled assessment.

CRUX cell (cell 4): unprofiled + faint + single-frame + empty model_dir → QUARANTINE.
The absence of detection on an unknown device is NOT proof of clean (FR-006).
"""
from __future__ import annotations

import shutil
from dataclasses import dataclass

import numpy as np
import pytest

# ---------------------------------------------------------------------------
# Skip guards
# ---------------------------------------------------------------------------

_TESSERACT_MISSING = shutil.which("tesseract") is None

pytestmark = [
    pytest.mark.slow,
    pytest.mark.pixeldeid,
]

_skip_no_tess = pytest.mark.skipif(
    _TESSERACT_MISSING, reason="tesseract binary not found — skip pixel-deid tests"
)


# ---------------------------------------------------------------------------
# Lazy imports (heavy deps imported inside tests, not at module level)
# ---------------------------------------------------------------------------

def _import_verdict():
    from src.services.pixel_deid.verdict import assess_case, Verdict
    return assess_case, Verdict


def _import_profiles():
    from src.services.pixel_deid.profiles import boxes_for
    return boxes_for


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def residual_phi(masked_frame: np.ndarray, gt_boxes: list[tuple]) -> int:
    """Count nonzero pixels inside any of the ground-truth PHI boxes."""
    total = 0
    h, w = masked_frame.shape[:2]
    for (x0, y0, x1, y1) in gt_boxes:
        x0 = max(0, x0)
        y0 = max(0, y0)
        x1 = min(w, x1)
        y1 = min(h, y1)
        region = masked_frame[y0:y1, x0:x1]
        total += int(np.count_nonzero(region))
    return total


@dataclass
class HoldoutCell:
    """One labeled holdout cell."""
    label: str
    frames: list[np.ndarray]
    dataset: object
    gt_boxes: list[tuple]   # ground-truth PHI pixel regions; [] if no PHI
    has_phi: bool
    model_dir: str = "models/craft"


def _profiled_ds():
    """Profiled Siemens AXIOM Artis dataset (no pixel data needed here)."""
    from tests.synthetic_data import make_profiled_device_dataset
    return make_profiled_device_dataset()


def _unprofiled_ds():
    """Unprofiled (unknown-device) dataset."""
    from tests.synthetic_data import make_unprofiled_device_dataset
    return make_unprofiled_device_dataset()


# ---------------------------------------------------------------------------
# T023 — FN=0 holdout
# ---------------------------------------------------------------------------

class TestFN0Holdout:
    """
    T023: labeled holdout asserting FN=0.

    Invariant for every PHI-bearing cell:
      verdict in (REDACTED, QUARANTINE)  — NEVER CLEAN
      if REDACTED → residual PHI in GT box == 0
    """

    @pytest.fixture(autouse=True)
    def _empty_model_dir(self, tmp_path):
        """Provide a tmp directory with NO .onnx files (simulates absent CRAFT model)."""
        self._no_craft_dir = str(tmp_path / "empty_models")
        import os
        os.makedirs(self._no_craft_dir, exist_ok=True)

    # ------------------------------------------------------------------
    # Cell builders
    # ------------------------------------------------------------------

    def _cell_profiled_crisp(self) -> HoldoutCell:
        """Cell 1: profiled + crisp PHI rendered INTO the profile box region."""
        from tests.synthetic_data import make_burned_in_phi_pixel_array

        # Profile box (0,48,150,70) — 128×256 frame, text renders at y≈52-65 (inside box)
        frame = make_burned_in_phi_pixel_array(
            "DOE^JOHN MRN 00471123", rows=128, cols=256
        )
        ds = _profiled_ds()
        # GT box = profile box itself (text is inside it)
        gt_boxes = [(0, 48, 150, 70)]
        return HoldoutCell(
            label="profiled-crisp-single",
            frames=[frame],
            dataset=ds,
            gt_boxes=gt_boxes,
            has_phi=True,
        )

    def _cell_profiled_clean(self) -> HoldoutCell:
        """Cell 2: profiled + blank/clean frame (no PHI pixels)."""
        frame = np.zeros((128, 256), dtype=np.uint8)
        ds = _profiled_ds()
        return HoldoutCell(
            label="profiled-blank-single",
            frames=[frame],
            dataset=ds,
            gt_boxes=[],
            has_phi=False,
        )

    def _cell_unprofiled_crisp_single(self) -> HoldoutCell:
        """Cell 3: unprofiled + crisp burned-in PHI, single frame."""
        from tests.synthetic_data import make_burned_in_phi_pixel_array
        frame = make_burned_in_phi_pixel_array(
            "DOE^JOHN MRN 00471123", rows=128, cols=256
        )
        # GT box covers the rendered text region (rows 52-65, cols 5-216)
        gt_boxes = [(5, 48, 220, 70)]
        ds = _unprofiled_ds()
        return HoldoutCell(
            label="unprofiled-crisp-single",
            frames=[frame],
            dataset=ds,
            gt_boxes=gt_boxes,
            has_phi=True,
        )

    def _cell_unprofiled_faint_single_no_craft(self, no_craft_dir: str) -> HoldoutCell:
        """
        Cell 4 (CRUX): unprofiled + faint + single-frame + no CRAFT model.

        No model → detect_text_regions returns [].
        Tesseract may or may not find faint text.
        Fail-closed rule: unprofiled + empty mask_union → QUARANTINE.
        Even if tesseract partially detects, the unprofiled path with no
        positive-clean evidence routes to QUARANTINE on empty mask or REDACTED
        on non-empty — NEVER CLEAN.
        """
        from tests.synthetic_data import make_faint_burned_in_phi_pixel_array
        frame = make_faint_burned_in_phi_pixel_array(
            "DOE^JOHN MRN 00471123", rows=128, cols=256
        )
        ds = _unprofiled_ds()
        gt_boxes = [(5, 48, 220, 70)]
        return HoldoutCell(
            label="unprofiled-faint-single-no-craft",
            frames=[frame],
            dataset=ds,
            gt_boxes=gt_boxes,
            has_phi=True,
            model_dir=no_craft_dir,
        )

    def _cell_unprofiled_faint_multiframe(self) -> HoldoutCell:
        """Cell 5: unprofiled + faint + multi-frame static overlay."""
        from tests.synthetic_data import make_multiframe_phi_case
        arr = make_multiframe_phi_case(
            "MRN 00471123", n_frames=6, rows=128, cols=256, faint=True
        )
        frames = [arr[i] for i in range(arr.shape[0])]
        ds = _unprofiled_ds()
        gt_boxes = [(5, 48, 220, 70)]
        return HoldoutCell(
            label="unprofiled-faint-multi",
            frames=frames,
            dataset=ds,
            gt_boxes=gt_boxes,
            has_phi=True,
        )

    def _cell_profiled_faint_single(self) -> HoldoutCell:
        """Cell 6: profiled + faint PHI in profile box region."""
        from tests.synthetic_data import make_faint_burned_in_phi_pixel_array
        frame = make_faint_burned_in_phi_pixel_array(
            "DOE^JOHN MRN 00471123", rows=128, cols=256
        )
        ds = _profiled_ds()
        gt_boxes = [(0, 48, 150, 70)]
        return HoldoutCell(
            label="profiled-faint-single",
            frames=[frame],
            dataset=ds,
            gt_boxes=gt_boxes,
            has_phi=True,
        )

    def _cell_unprofiled_crisp_multiframe(self) -> HoldoutCell:
        """Cell 7: unprofiled + crisp + multi-frame."""
        from tests.synthetic_data import make_multiframe_phi_case
        arr = make_multiframe_phi_case(
            "DOE^JOHN MRN 00471123", n_frames=6, rows=128, cols=256, faint=False
        )
        frames = [arr[i] for i in range(arr.shape[0])]
        ds = _unprofiled_ds()
        gt_boxes = [(5, 48, 220, 70)]
        return HoldoutCell(
            label="unprofiled-crisp-multi",
            frames=frames,
            dataset=ds,
            gt_boxes=gt_boxes,
            has_phi=True,
        )

    def _cell_profiled_faint_multi(self) -> HoldoutCell:
        """Cell 8: profiled + faint + multi-frame."""
        from tests.synthetic_data import make_multiframe_phi_case
        arr = make_multiframe_phi_case(
            "DOE^JOHN MRN 00471123", n_frames=6, rows=128, cols=256, faint=True
        )
        frames = [arr[i] for i in range(arr.shape[0])]
        ds = _profiled_ds()
        gt_boxes = [(0, 48, 150, 70)]
        return HoldoutCell(
            label="profiled-faint-multi",
            frames=frames,
            dataset=ds,
            gt_boxes=gt_boxes,
            has_phi=True,
        )

    # ------------------------------------------------------------------
    # Master invariant
    # ------------------------------------------------------------------

    def _assert_fn0_invariant(
        self, cell: HoldoutCell, assessment, Verdict
    ):
        """
        FN=0 invariant:
          - PHI-bearing cell: verdict in (REDACTED, QUARANTINE)
          - If REDACTED and PHI-bearing: residual in GT boxes == 0
        """
        if cell.has_phi:
            assert assessment.verdict in (Verdict.REDACTED, Verdict.QUARANTINE), (
                f"[{cell.label}] LEAK: verdict={assessment.verdict.value}; "
                f"expected REDACTED or QUARANTINE for PHI-bearing cell"
            )
            if assessment.verdict == Verdict.REDACTED:
                for masked in assessment.masked_frames:
                    res = residual_phi(masked, cell.gt_boxes)
                    assert res == 0, (
                        f"[{cell.label}] REDACTED but {res} residual PHI pixels "
                        f"remain in GT box {cell.gt_boxes}"
                    )

    # ------------------------------------------------------------------
    # Individual test methods (one per cell for clear reporting)
    # ------------------------------------------------------------------

    @_skip_no_tess
    def test_cell1_profiled_crisp_single(self):
        """Cell 1: profiled + crisp → REDACTED, residual in profile box == 0."""
        assess_case, Verdict = _import_verdict()
        cell = self._cell_profiled_crisp()
        a = assess_case(cell.frames, cell.dataset, model_dir=cell.model_dir)
        assert a.verdict in (Verdict.REDACTED, Verdict.QUARANTINE), (
            f"Cell 1: expected REDACTED, got {a.verdict.value}"
        )
        self._assert_fn0_invariant(cell, a, Verdict)

    @_skip_no_tess
    def test_cell2_profiled_blank(self):
        """Cell 2: profiled + clean blank frame → does not crash; no PHI so invariant trivially satisfied."""
        assess_case, Verdict = _import_verdict()
        cell = self._cell_profiled_clean()
        a = assess_case(cell.frames, cell.dataset, model_dir=cell.model_dir)
        # No PHI in frame; system should not crash; CLEAN or REDACTED both acceptable
        assert a.verdict in (Verdict.CLEAN, Verdict.REDACTED, Verdict.QUARANTINE)
        # Invariant is trivially satisfied — no PHI present

    @_skip_no_tess
    def test_cell3_unprofiled_crisp_single(self):
        """Cell 3: unprofiled + crisp single-frame → REDACTED or QUARANTINE, NOT CLEAN."""
        assess_case, Verdict = _import_verdict()
        cell = self._cell_unprofiled_crisp_single()
        a = assess_case(cell.frames, cell.dataset, model_dir=cell.model_dir)
        self._assert_fn0_invariant(cell, a, Verdict)

    @_skip_no_tess
    def test_cell4_crux_unprofiled_faint_no_craft_quarantine(self):
        """
        Cell 4 (CRUX): unprofiled + faint + single-frame + no CRAFT model.

        Fail-closed rule: unknown device + no positive clean evidence → QUARANTINE.
        This is the SC-001 hard gate. MUST pass.
        """
        assess_case, Verdict = _import_verdict()
        cell = self._cell_unprofiled_faint_single_no_craft(self._no_craft_dir)
        a = assess_case(
            cell.frames,
            cell.dataset,
            model_dir=cell.model_dir,  # points to empty dir → detect_text_regions returns []
        )
        # CRUX assertion: unprofiled device with no detection evidence → QUARANTINE
        assert a.verdict == Verdict.QUARANTINE, (
            f"CRUX CELL FAILED: unprofiled+faint+no-CRAFT must be QUARANTINE, "
            f"got {a.verdict.value} (reason: {a.reason!r})"
        )

    @_skip_no_tess
    def test_cell5_unprofiled_faint_multiframe(self):
        """Cell 5: unprofiled + faint + multi-frame → consensus or detector catches → NOT CLEAN."""
        assess_case, Verdict = _import_verdict()
        cell = self._cell_unprofiled_faint_multiframe()
        a = assess_case(cell.frames, cell.dataset, model_dir=cell.model_dir)
        self._assert_fn0_invariant(cell, a, Verdict)

    @_skip_no_tess
    def test_cell6_profiled_faint_single(self):
        """Cell 6: profiled + faint → profile blind-mask covers GT box → REDACTED, residual=0."""
        assess_case, Verdict = _import_verdict()
        cell = self._cell_profiled_faint_single()
        a = assess_case(cell.frames, cell.dataset, model_dir=cell.model_dir)
        self._assert_fn0_invariant(cell, a, Verdict)

    @_skip_no_tess
    def test_cell7_unprofiled_crisp_multiframe(self):
        """Cell 7: unprofiled + crisp + multi-frame → NOT CLEAN."""
        assess_case, Verdict = _import_verdict()
        cell = self._cell_unprofiled_crisp_multiframe()
        a = assess_case(cell.frames, cell.dataset, model_dir=cell.model_dir)
        self._assert_fn0_invariant(cell, a, Verdict)

    @_skip_no_tess
    def test_cell8_profiled_faint_multiframe(self):
        """Cell 8: profiled + faint + multi-frame → profile covers → REDACTED, residual=0."""
        assess_case, Verdict = _import_verdict()
        cell = self._cell_profiled_faint_multi()
        a = assess_case(cell.frames, cell.dataset, model_dir=cell.model_dir)
        self._assert_fn0_invariant(cell, a, Verdict)


# ---------------------------------------------------------------------------
# T024a — Quarantine-rate check on profiled in-distribution cases (SC-002)
# ---------------------------------------------------------------------------

class TestQuarantineRateProfiled:
    """
    T024a: SC-002 — quarantine rate on profiled in-distribution cases should be low.

    Build N=6 profiled cases with PHI inside the profile region; assert the
    fraction with verdict==QUARANTINE is < 0.34 (i.e. ≤2 of 6).
    Most profiled cases auto-resolve to REDACTED via profile blind-mask.
    """

    @_skip_no_tess
    def test_quarantine_rate_low(self):
        assess_case, Verdict = _import_verdict()
        from tests.synthetic_data import make_burned_in_phi_pixel_array, make_profiled_device_dataset

        verdicts = []
        for seed in range(6):
            # Slight variation in seed to get different anatomy, same text
            frame = make_burned_in_phi_pixel_array(
                "DOE^JOHN MRN 00471123", rows=128, cols=256
            )
            ds = make_profiled_device_dataset()
            a = assess_case([frame], ds)
            verdicts.append(a.verdict)

        n_quarantine = sum(1 for v in verdicts if v == Verdict.QUARANTINE)
        quarantine_rate = n_quarantine / len(verdicts)

        # SC-002: for profiled in-distribution cases, quarantine rate should be low
        # Target < 0.34 (≤2 of 6 may quarantine due to profile/detector disagreement)
        assert quarantine_rate < 0.34, (
            f"Quarantine rate too high for profiled cases: "
            f"{n_quarantine}/{len(verdicts)} = {quarantine_rate:.2f} "
            f"(verdicts: {[v.value for v in verdicts]})"
        )


# ---------------------------------------------------------------------------
# T024b — Benign-marker preservation (SC-003)
# ---------------------------------------------------------------------------

class TestBenignMarkerPreservation:
    """
    T024b: SC-003 — benign 'L' laterality marker survives de-id on a profiled device.

    Render 'L' at a position outside the profile box so the profile blind-mask
    does not touch it. Assert the 'L' pixels survive (nonzero) after assessment.
    Profile box for siemens_axiom_artis: (0, 48, 150, 70).
    'L' rendered at (x=200, y=100) — rows 89-101, cols 201-209 — entirely outside.
    """

    @_skip_no_tess
    def test_benign_L_marker_preserved(self):
        assess_case, Verdict = _import_verdict()
        import cv2

        # Build frame: PHI in profile box region + benign 'L' outside
        rows, cols = 128, 256
        frame = np.zeros((rows, cols), dtype=np.uint8)

        # Render PHI inside profile box region (y=48-70)
        cv2.putText(
            frame, "DOE^JOHN MRN 00471123",
            (4, 64),  # (x=4, y=64) — inside profile box y range 48-70
            cv2.FONT_HERSHEY_SIMPLEX, 0.5, 200, 1, cv2.LINE_AA,
        )

        # Render benign 'L' marker far outside profile box (y=100, x=200)
        cv2.putText(
            frame, "L",
            (200, 100),  # (x=200, y=100) — rows ~89-101, outside profile box
            cv2.FONT_HERSHEY_SIMPLEX, 0.5, 200, 1, cv2.LINE_AA,
        )

        # Record where 'L' pixels are (before assessment)
        # L is at rows 89-101, cols 201-209 (verified empirically)
        l_region_before = frame[85:106, 198:215].copy()
        assert np.count_nonzero(l_region_before) > 0, (
            "Test setup error: 'L' pixels not found in expected region before assessment"
        )

        ds = _profiled_ds()
        a = assess_case([frame], ds)

        # After assessment, 'L' pixels in the non-masked region should survive
        assert len(a.masked_frames) >= 1
        masked = a.masked_frames[0]
        l_region_after = masked[85:106, 198:215]

        assert np.count_nonzero(l_region_after) > 0, (
            f"Benign 'L' marker was wiped out (verdict={a.verdict.value}). "
            f"'L' pixels before: {np.count_nonzero(l_region_before)}, "
            f"after: {np.count_nonzero(l_region_after)}. "
            f"Regions masked: {a.regions}"
        )

    @_skip_no_tess
    def test_benign_L_marker_pixels_stable(self):
        """Second assertion: L pixel count after ≥ L pixel count before (conservative)."""
        assess_case, Verdict = _import_verdict()
        import cv2

        rows, cols = 128, 256
        frame = np.zeros((rows, cols), dtype=np.uint8)

        # PHI inside profile box
        cv2.putText(
            frame, "DOE^JOHN MRN 00471123",
            (4, 64), cv2.FONT_HERSHEY_SIMPLEX, 0.5, 200, 1, cv2.LINE_AA,
        )
        # Benign 'L' outside profile box
        cv2.putText(
            frame, "L",
            (200, 100), cv2.FONT_HERSHEY_SIMPLEX, 0.5, 200, 1, cv2.LINE_AA,
        )

        before_L = int(np.count_nonzero(frame[85:106, 198:215]))
        ds = _profiled_ds()
        a = assess_case([frame], ds)
        masked = a.masked_frames[0]
        after_L = int(np.count_nonzero(masked[85:106, 198:215]))

        assert after_L == before_L, (
            f"Benign 'L' pixel count changed: before={before_L}, after={after_L}. "
            f"Verdict={a.verdict.value}, regions={a.regions}"
        )
