"""
Visual verification of synthetic fluoroscopy frames.

Renders frames from seeds {1..6} at 256x256 to /tmp/factory_check.png via matplotlib.
"""
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from tests.stress.factory import synth_fluoro_frame


def test_visual_fluoroscopy():
    """Generate and save a multi-seed visualization."""
    try:
        import matplotlib.pyplot as plt
        import matplotlib.gridspec as gridspec
    except ImportError:
        print("matplotlib not available; skipping visual test")
        return

    # Generate frames for seeds 1-6
    seeds = range(1, 7)
    rows, cols = 256, 256
    frames = [synth_fluoro_frame(seed, rows, cols, dtype=np.uint16) for seed in seeds]

    # Create grid layout
    n_seeds = len(seeds)
    n_cols = 3
    n_rows = (n_seeds + n_cols - 1) // n_cols

    fig = plt.figure(figsize=(12, 4 * n_rows), dpi=100)
    gs = gridspec.GridSpec(n_rows, n_cols, figure=fig)

    for idx, (seed, frame) in enumerate(zip(seeds, frames)):
        row = idx // n_cols
        col = idx % n_cols
        ax = fig.add_subplot(gs[row, col])

        # Normalize for display
        frame_uint8 = ((frame.astype(np.float32) / np.iinfo(np.uint16).max) * 255).astype(
            np.uint8
        )
        ax.imshow(frame_uint8, cmap="bone", vmin=0, vmax=255)
        ax.set_title(f"Seed {seed}", fontsize=12, fontweight="bold")
        ax.axis("off")

    plt.tight_layout()
    output_path = Path("/tmp/factory_check.png")
    plt.savefig(output_path, dpi=100, bbox_inches="tight")
    print(f"Saved visualization to {output_path}")
    plt.close()

    # Verify distinctness
    from tests.stress.factory import assert_distinct_content
    import tempfile

    d = Path(tempfile.mkdtemp())
    from tests.stress.factory import make_surgery

    make_surgery("VIS_TEST", set(range(1, 11)), d, rows=256, cols=256)
    assert_distinct_content(d / "VIS_TEST")
    print("✓ All 10 frames have distinct PixelData (sha256)")


if __name__ == "__main__":
    test_visual_fluoroscopy()
