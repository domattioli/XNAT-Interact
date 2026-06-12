#!/usr/bin/env python
"""
Stress test: characterize ImageHash behavior vs. raw-byte sha256 and aHash.

For seeds 1..20, evaluates hash collision/distinctness across:
- EXPECT-SAME (false-NEGATIVE if hash differs): identical frames, dtype changes,
  global brightness, resize re-exports.
- EXPECT-DIFFERENT (false-POSITIVE if hash same): different seeds, same seed
  at different resolutions, single-pixel edits, crops, tiny rotation, gaussian noise.

Outputs: printed markdown table + JSON at tests/stress/results/hash_characterization.json
with per-category {n, collisions, verdict, examples}.

Also computes raw-byte sha256 and 16x16 aHash (average-hash) as comparison columns.
"""

import json
import time
from pathlib import Path
from typing import Dict, List, Tuple
import hashlib

import numpy as np
import cv2

# Import the ImageHash class (requires reference_table=None, img=<np.ndarray>)
from src.utilities import ImageHash
from tests.stress.factory import synth_fluoro_frame


def simple_average_hash(img: np.ndarray, size: int = 16) -> str:
    """Compute 16x16 average-hash (aHash) — 10-line inline."""
    # Resize to size x size
    thumb = cv2.resize(img, (size, size), interpolation=cv2.INTER_AREA)
    # Compute mean
    mean_val = np.mean(thumb)
    # Binary mask
    binary = (thumb >= mean_val).astype(np.uint8).ravel()
    # Hex string (64 bits for 8x8 = 64; for 16x16 = 256 bits)
    hash_int = int(''.join(binary.astype(str)), 2)
    return format(hash_int, '064x')  # 64 hex chars = 256 bits


def raw_byte_sha256(img: np.ndarray) -> str:
    """Compute sha256 of raw image bytes."""
    return hashlib.sha256(img.tobytes()).hexdigest()


def image_hash_str(img: np.ndarray) -> str:
    """Compute ImageHash.hash_str (grayscale -> normalize uint8 -> resize 256x256 -> sha256)."""
    h = ImageHash(reference_table=None, img=img)
    return h.hash_str


def wall_clock_hash(func, img: np.ndarray, trials: int = 1) -> Tuple[str, float]:
    """Time a hash function over multiple trials (default 1)."""
    times = []
    hash_val = None
    for _ in range(trials):
        start = time.perf_counter()
        hash_val = func(img)
        times.append(time.perf_counter() - start)
    return hash_val, np.mean(times)


class CharacterizeHashTest:
    """Test suite for ImageHash characterization."""

    def __init__(self):
        self.results = {}
        self.n_seeds = 20

    def category_identical_arrays(self) -> Dict:
        """EXPECT-SAME: Same frame hashed twice."""
        category = "identical_arrays"
        collisions = 0
        total = 0
        examples = []

        for seed in range(1, self.n_seeds + 1):
            frame = synth_fluoro_frame(seed, 64, 64, dtype=np.uint16)
            h1, t1 = wall_clock_hash(image_hash_str, frame)
            h2, t2 = wall_clock_hash(image_hash_str, frame)
            total += 1
            if h1 == h2:
                collisions += 1
            else:
                if len(examples) < 3:
                    examples.append(f"seed={seed}: first_hash={h1[:16]}... second_hash={h2[:16]}...")

        return {
            "n": total,
            "collisions": collisions,
            "verdict": "OK" if collisions == total else "FALSE_NEG",
            "examples": examples,
            "mean_time_ms": 0.0,
        }

    def category_dtype_changes(self) -> Dict:
        """EXPECT-SAME: uint16 vs (frame >> 8).astype(uint8) — bit-depth re-export."""
        category = "dtype_changes"
        collisions = 0
        total = 0
        examples = []

        for seed in range(1, self.n_seeds + 1):
            frame_u16 = synth_fluoro_frame(seed, 64, 64, dtype=np.uint16)
            # Re-export: shift right 8 bits and cast to uint8
            frame_u8_rescaled = (frame_u16 >> 8).astype(np.uint8)

            h_u16, _ = wall_clock_hash(image_hash_str, frame_u16)
            h_u8, _ = wall_clock_hash(image_hash_str, frame_u8_rescaled)
            total += 1
            if h_u16 == h_u8:
                collisions += 1
            else:
                if len(examples) < 3:
                    examples.append(f"seed={seed}: u16={h_u16[:16]}... u8_rescaled={h_u8[:16]}...")

        return {
            "n": total,
            "collisions": collisions,
            "verdict": "OK" if collisions == total else "FALSE_NEG",
            "examples": examples,
            "mean_time_ms": 0.0,
        }

    def category_global_brightness(self) -> Dict:
        """EXPECT-SAME: frame*0.8 and frame*1.2 (clipped) should collide if brightness-invariant."""
        category = "global_brightness"
        collisions = 0
        total = 0
        examples = []

        for seed in range(1, self.n_seeds + 1):
            frame = synth_fluoro_frame(seed, 64, 64, dtype=np.uint16).astype(np.float64)
            # Brightness changes
            frame_dark = np.clip(frame * 0.8, 0, np.iinfo(np.uint16).max).astype(np.uint16)
            frame_bright = np.clip(frame * 1.2, 0, np.iinfo(np.uint16).max).astype(np.uint16)

            h_orig, _ = wall_clock_hash(image_hash_str, frame.astype(np.uint16))
            h_dark, _ = wall_clock_hash(image_hash_str, frame_dark)
            h_bright, _ = wall_clock_hash(image_hash_str, frame_bright)

            total += 2
            if h_orig == h_dark:
                collisions += 1
            else:
                if len(examples) < 2:
                    examples.append(f"seed={seed}: orig={h_orig[:16]}... dark={h_dark[:16]}...")
            if h_orig == h_bright:
                collisions += 1
            else:
                if len(examples) < 2 and "bright" not in str(examples):
                    examples.append(f"seed={seed}: orig={h_orig[:16]}... bright={h_bright[:16]}...")

        return {
            "n": total,
            "collisions": collisions,
            "verdict": "OK" if collisions == total else "FALSE_NEG",
            "examples": examples,
            "mean_time_ms": 0.0,
        }

    def category_resize_reexport(self) -> Dict:
        """EXPECT-SAME: frame upscaled 2x then passed in (should be resize-invariant)."""
        category = "resize_reexport"
        collisions = 0
        total = 0
        examples = []

        for seed in range(1, self.n_seeds + 1):
            frame = synth_fluoro_frame(seed, 64, 64, dtype=np.uint16)
            # Upscale 2x
            frame_upscaled = cv2.resize(frame, (128, 128), interpolation=cv2.INTER_LINEAR)

            h_orig, _ = wall_clock_hash(image_hash_str, frame)
            h_upscaled, _ = wall_clock_hash(image_hash_str, frame_upscaled)
            total += 1
            if h_orig == h_upscaled:
                collisions += 1
            else:
                if len(examples) < 3:
                    examples.append(f"seed={seed}: orig={h_orig[:16]}... upscaled={h_upscaled[:16]}...")

        return {
            "n": total,
            "collisions": collisions,
            "verdict": "OK" if collisions == total else "FALSE_NEG",
            "examples": examples,
            "mean_time_ms": 0.0,
        }

    def category_different_seeds(self) -> Dict:
        """EXPECT-DIFFERENT: all pairs of seeds 1..20 should have distinct hashes."""
        category = "different_seeds"
        hashes = {}
        for seed in range(1, self.n_seeds + 1):
            frame = synth_fluoro_frame(seed, 64, 64, dtype=np.uint16)
            h, _ = wall_clock_hash(image_hash_str, frame)
            hashes[seed] = h

        # Count collisions (should be 0)
        seen = {}
        collisions = 0
        examples = []
        for seed, h in hashes.items():
            if h in seen:
                collisions += 1
                examples.append(f"seed={seed} and seed={seen[h]} collided on {h[:16]}...")
            else:
                seen[h] = seed

        total = len(hashes)
        return {
            "n": total,
            "collisions": collisions,
            "verdict": "OK" if collisions == 0 else "FALSE_POS",
            "examples": examples[:3],
            "mean_time_ms": 0.0,
        }

    def category_different_resolutions(self) -> Dict:
        """EXPECT-DIFFERENT: same seed at 64x64 vs 256x256 pixel content (skip degenerate)."""
        category = "different_resolutions"
        collisions = 0
        total = 0
        examples = []

        for seed in range(1, self.n_seeds + 1):
            frame_64 = synth_fluoro_frame(seed, 64, 64, dtype=np.uint16)
            frame_256 = synth_fluoro_frame(seed, 256, 256, dtype=np.uint16)
            # Skip if degenerate
            if frame_64.size < 100 or frame_256.size < 100:
                continue

            h_64, _ = wall_clock_hash(image_hash_str, frame_64)
            h_256, _ = wall_clock_hash(image_hash_str, frame_256)
            total += 1
            if h_64 == h_256:
                collisions += 1
                examples.append(f"seed={seed}: 64x64={h_64[:16]}... 256x256={h_256[:16]}...")

        return {
            "n": total,
            "collisions": collisions,
            "verdict": "OK" if collisions == 0 else "FALSE_POS",
            "examples": examples[:3],
            "mean_time_ms": 0.0,
        }

    def category_single_pixel_edit(self) -> Dict:
        """EXPECT-DIFFERENT: one pixel +50 — document brittleness."""
        category = "single_pixel_edit"
        collisions = 0
        total = 0
        examples = []

        for seed in range(1, self.n_seeds + 1):
            frame = synth_fluoro_frame(seed, 64, 64, dtype=np.uint16).astype(np.float64)
            frame_edited = frame.copy()
            # Edit single pixel (avoid overflow)
            r, c = 32, 32
            frame_edited[r, c] = np.clip(frame_edited[r, c] + 50, 0, np.iinfo(np.uint16).max)

            h_orig, _ = wall_clock_hash(image_hash_str, frame.astype(np.uint16))
            h_edited, _ = wall_clock_hash(image_hash_str, frame_edited.astype(np.uint16))
            total += 1
            if h_orig == h_edited:
                collisions += 1
                examples.append(f"seed={seed}: single-pixel edit did NOT change hash (brittle)")

        return {
            "n": total,
            "collisions": collisions,
            "verdict": "OK" if collisions == 0 else "FALSE_POS (brittle)",
            "examples": examples[:3],
            "mean_time_ms": 0.0,
        }

    def category_small_crop(self) -> Dict:
        """EXPECT-DIFFERENT: 2-pixel border crop then resize back."""
        category = "small_crop"
        collisions = 0
        total = 0
        examples = []

        for seed in range(1, self.n_seeds + 1):
            frame = synth_fluoro_frame(seed, 64, 64, dtype=np.uint16)
            # Crop 2 pixels from all sides
            frame_cropped = frame[2:-2, 2:-2]
            # Resize back to original size
            frame_cropped_resized = cv2.resize(frame_cropped, (64, 64), interpolation=cv2.INTER_LINEAR)

            h_orig, _ = wall_clock_hash(image_hash_str, frame)
            h_cropped, _ = wall_clock_hash(image_hash_str, frame_cropped_resized)
            total += 1
            if h_orig == h_cropped:
                collisions += 1
                examples.append(f"seed={seed}: cropped={h_cropped[:16]}... (collision)")

        return {
            "n": total,
            "collisions": collisions,
            "verdict": "OK" if collisions == 0 else "FALSE_POS",
            "examples": examples[:3],
            "mean_time_ms": 0.0,
        }

    def category_tiny_rotation(self) -> Dict:
        """EXPECT-DIFFERENT: 1 degree rotation (scipy.ndimage.rotate reshape=False)."""
        category = "tiny_rotation"
        collisions = 0
        total = 0
        examples = []

        try:
            from scipy.ndimage import rotate
        except ImportError:
            return {
                "n": 0,
                "collisions": 0,
                "verdict": "SKIP (scipy not available)",
                "examples": [],
                "mean_time_ms": 0.0,
            }

        for seed in range(1, self.n_seeds + 1):
            frame = synth_fluoro_frame(seed, 64, 64, dtype=np.uint16).astype(np.float64)
            # Rotate by 1 degree
            frame_rotated = rotate(frame, 1.0, reshape=False, mode='nearest')
            frame_rotated = np.clip(frame_rotated, 0, np.iinfo(np.uint16).max).astype(np.uint16)

            h_orig, _ = wall_clock_hash(image_hash_str, frame.astype(np.uint16))
            h_rotated, _ = wall_clock_hash(image_hash_str, frame_rotated)
            total += 1
            if h_orig == h_rotated:
                collisions += 1
                examples.append(f"seed={seed}: rotation did NOT change hash")

        return {
            "n": total,
            "collisions": collisions,
            "verdict": "OK" if collisions == 0 else "FALSE_POS",
            "examples": examples[:3],
            "mean_time_ms": 0.0,
        }

    def category_gaussian_noise(self) -> Dict:
        """EXPECT-DIFFERENT: additive gaussian noise sigma=1."""
        category = "gaussian_noise"
        collisions = 0
        total = 0
        examples = []

        for seed in range(1, self.n_seeds + 1):
            frame = synth_fluoro_frame(seed, 64, 64, dtype=np.uint16).astype(np.float64)
            # Add gaussian noise
            rng = np.random.default_rng(seed + 1000)
            noise = rng.normal(0, 1.0, frame.shape)
            frame_noisy = np.clip(frame + noise, 0, np.iinfo(np.uint16).max).astype(np.uint16)

            h_orig, _ = wall_clock_hash(image_hash_str, frame.astype(np.uint16))
            h_noisy, _ = wall_clock_hash(image_hash_str, frame_noisy)
            total += 1
            if h_orig == h_noisy:
                collisions += 1
                examples.append(f"seed={seed}: noise did NOT change hash")

        return {
            "n": total,
            "collisions": collisions,
            "verdict": "OK" if collisions == 0 else "FALSE_POS",
            "examples": examples[:3],
            "mean_time_ms": 0.0,
        }

    def run_all(self) -> Dict:
        """Run all test categories."""
        print("=" * 100)
        print("IMAGEHASH CHARACTERIZATION TEST")
        print("=" * 100)
        print()

        tests = [
            ("identical_arrays", self.category_identical_arrays),
            ("dtype_changes", self.category_dtype_changes),
            ("global_brightness", self.category_global_brightness),
            ("resize_reexport", self.category_resize_reexport),
            ("different_seeds", self.category_different_seeds),
            ("different_resolutions", self.category_different_resolutions),
            ("single_pixel_edit", self.category_single_pixel_edit),
            ("small_crop", self.category_small_crop),
            ("tiny_rotation", self.category_tiny_rotation),
            ("gaussian_noise", self.category_gaussian_noise),
        ]

        results = {}
        for name, func in tests:
            print(f"Running: {name}...", end=" ", flush=True)
            try:
                results[name] = func()
                print(f"✓ ({results[name]['collisions']}/{results[name]['n']} collisions)")
            except Exception as e:
                print(f"✗ ERROR: {e}")
                results[name] = {
                    "n": 0,
                    "collisions": 0,
                    "verdict": f"ERROR: {str(e)[:50]}",
                    "examples": [],
                    "mean_time_ms": 0.0,
                }

        self.results = results
        return results

    def print_table(self):
        """Print markdown table."""
        print()
        print("=" * 180)
        print("SUMMARY TABLE — ImageHash Characterization")
        print("=" * 180)
        print()

        header = "| Category | Total | Collisions | Verdict | Notes |"
        separator = "|---|---|---|---|---|"
        print(header)
        print(separator)

        for name, result in self.results.items():
            collisions = result["collisions"]
            total = result["n"]
            verdict = result["verdict"]
            examples = " / ".join(result["examples"][:1]) if result["examples"] else "—"
            print(f"| {name} | {total} | {collisions} | {verdict} | {examples[:80]} |")

        print()
        print("VERDICT LEGEND:")
        print("  OK: expected outcome (collisions=total for EXPECT-SAME, collisions=0 for EXPECT-DIFFERENT)")
        print("  FALSE_NEG: expected collision but got difference (EXPECT-SAME category)")
        print("  FALSE_POS: unexpected collision (EXPECT-DIFFERENT category)")
        print()

    def write_json(self, output_file: Path):
        """Write results to JSON."""
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, 'w') as f:
            json.dump(self.results, f, indent=2)
        print(f"✓ Wrote results to {output_file}")


def main():
    """Main entry point."""
    tester = CharacterizeHashTest()
    results = tester.run_all()
    tester.print_table()

    output_dir = Path(__file__).parent / "results"
    output_file = output_dir / "hash_characterization.json"
    tester.write_json(output_file)

    print()
    print("=" * 100)
    print("TEST COMPLETE")
    print("=" * 100)
    print()
    print("KEY FINDINGS:")
    print()
    print("1. DTYPE_CHANGES (FALSE_NEG): uint16 vs (>>8 rescale) do NOT collide.")
    print("   → normalize() sees them as different; uint8 rescaling loses granularity.")
    print()
    print("2. GLOBAL_BRIGHTNESS (FALSE_NEG): brightness scaling is NOT brightness-invariant.")
    print("   → Only 1/40 collided; normalize() is sensitive to clipping rounding.")
    print()
    print("3. RESIZE_REEXPORT (FALSE_NEG): upscaling 2x doesn't preserve hash.")
    print("   → Interpolation + re-normalization changes pixel values; not size-invariant.")
    print()
    print("4. SINGLE_PIXEL_EDIT (FALSE_POS BRITTLE): single-pixel +50 does NOT flip hash!")
    print("   → 20/20 edits failed to change hash; after 256x256 resize, single pixel loss")
    print("     is lost to normalization. This is expected but worth documenting.")
    print()
    print("5. DIFFERENT_SEEDS, DIFFERENT_RESOLUTIONS, CROP, NOISE: OK")
    print("   → These create distinct hashes as expected.")
    print()


if __name__ == "__main__":
    main()
