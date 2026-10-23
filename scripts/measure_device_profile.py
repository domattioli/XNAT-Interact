#!/usr/bin/env python3
"""
Measure a device profile: draw the boxes where an imaging machine burns text
into its images, and save them as data/device_profiles/<device key>.json.

Usage (interactive, run by the Data Librarian on their own machine):
    python scripts/measure_device_profile.py <file or folder> [...]

    A window shows each image. Drag a rectangle over burned-in text, then press
      a  to add the rectangle as a box
      u  to undo the last box
      n  for the next image
      q  to finish and save
    Boxes from every image are kept, so cover each text area once.

Usage (non-interactive, for tests and scripted use):
    python scripts/measure_device_profile.py <file or folder> --boxes "x0,y0,x1,y1;x0,y0,x1,y1"

What it writes: ONLY the device key, the box coordinates, a fixed description
and the tolerance, in the schema of data/device_profiles/README.md.
What it never writes or prints: header values, file names, folder names or
pixels. The images are shown on screen only.

Exit status: 0 saved, 2 stopped (message says what to do next).
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

Box = Tuple[int, int, int, int]
DEFAULT_OUT = REPO / "data" / "device_profiles"
DEFAULT_TOLERANCE = 8


class Stop(Exception):
    """Foreseeable problem; message is safe to print (no header values, no paths)."""


def _candidates(inputs: Sequence[str]) -> List[Path]:
    out: List[Path] = []
    for raw in inputs:
        p = Path(raw)
        if p.is_dir():
            out.extend(sorted(f for f in p.rglob("*") if f.is_file() and (f.suffix.lower() == ".dcm" or f.suffix == "")))
        elif p.is_file():
            out.append(p)
    return out


def _read_images(inputs: Sequence[str]):
    """Return [(dataset, first_frame_2d)] for every readable DICOM with pixels."""
    import numpy as np
    import pydicom

    images = []
    for f in _candidates(inputs):
        try:
            ds = pydicom.dcmread(f)
            if "PixelData" not in ds:
                continue
            arr = ds.pixel_array
        except Exception:  # noqa: BLE001 — not an image; skipped silently (no file name printed)
            continue
        n = int(getattr(ds, "NumberOfFrames", 1) or 1)
        frame = arr[0] if n > 1 else arr
        if frame.ndim == 3:  # colour
            frame = np.asarray(frame)[..., :3].mean(axis=-1)
        images.append((ds, frame))
    return images


def _device_key(images) -> str:
    from src.services.pixel_deid.profiles import device_id_for

    keys = {device_id_for(ds) for ds, _ in images}
    if None in keys:
        raise Stop("Some images do not say which machine made them (no Manufacturer tag). "
                   "Next step: measure only images from one known machine.")
    if len(keys) != 1:
        raise Stop(f"The images come from {len(keys)} different machines. "
                   "Next step: run the tool once per machine, with only that machine's images.")
    return keys.pop()


def parse_boxes(text: str) -> List[Box]:
    boxes: List[Box] = []
    for part in (p.strip() for p in text.split(";")):
        if not part:
            continue
        try:
            vals = [int(v.strip()) for v in part.split(",")]
        except ValueError:
            raise Stop(f"Box '{part}' is not four whole numbers. Use x0,y0,x1,y1 separated by ';'.")
        if len(vals) != 4:
            raise Stop(f"Box '{part}' needs four numbers x0,y0,x1,y1.")
        boxes.append(tuple(vals))  # type: ignore[arg-type]
    return boxes


def clean_boxes(boxes: Sequence[Sequence[float]], shape: Tuple[int, int]) -> List[Box]:
    """Order corners, clip to the image, drop empty boxes and duplicates."""
    h, w = shape
    out: List[Box] = []
    for b in boxes:
        x0, y0, x1, y1 = (int(round(v)) for v in b)
        x0, x1 = sorted((x0, x1))
        y0, y1 = sorted((y0, y1))
        x0, x1 = max(0, x0), min(w, x1)
        y0, y1 = max(0, y0), min(h, y1)
        if x1 > x0 and y1 > y0 and (x0, y0, x1, y1) not in out:
            out.append((x0, y0, x1, y1))
    return out


def _draw_boxes(images) -> List[Box]:  # pragma: no cover — needs a screen and a person
    import matplotlib.pyplot as plt
    from matplotlib.patches import Rectangle
    from matplotlib.widgets import RectangleSelector

    boxes: List[Box] = []
    state = {"current": None, "done": False}

    for idx, (_ds, frame) in enumerate(images):
        if state["done"]:
            break
        fig, ax = plt.subplots(figsize=(9, 9))
        ax.imshow(frame, cmap="gray")
        ax.set_title(f"Image {idx + 1} of {len(images)}  -  drag, then a=add  u=undo  n=next  q=finish")
        ax.set_xticks([]), ax.set_yticks([])
        patches = []
        for b in boxes:
            patches.append(ax.add_patch(Rectangle((b[0], b[1]), b[2] - b[0], b[3] - b[1], fill=False, ec="red")))

        def on_select(press, release):
            state["current"] = (press.xdata, press.ydata, release.xdata, release.ydata)

        def on_key(event):
            if event.key == "a" and state["current"]:
                x0, y0, x1, y1 = state["current"]
                new = clean_boxes([(x0, y0, x1, y1)], frame.shape[:2])
                for b in new:
                    boxes.append(b)
                    patches.append(ax.add_patch(Rectangle((b[0], b[1]), b[2] - b[0], b[3] - b[1], fill=False, ec="red")))
                state["current"] = None
                fig.canvas.draw_idle()
            elif event.key == "u" and boxes:
                boxes.pop()
                patches.pop().remove()
                fig.canvas.draw_idle()
            elif event.key == "n":
                plt.close(fig)
            elif event.key == "q":
                state["done"] = True
                plt.close(fig)

        selector = RectangleSelector(ax, on_select, useblit=True, interactive=True)  # noqa: F841 — keep alive
        fig.canvas.mpl_connect("key_press_event", on_key)
        plt.show()
    return boxes


def build_profile(key: str, boxes: List[Box], n_images: int, tolerance: int) -> dict:
    return {
        "device_id": key,
        "description": f"Measured with scripts/measure_device_profile.py on {n_images} image(s).",
        "boxes": [list(b) for b in boxes],
        "tolerance": tolerance,
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Draw burned-in text boxes and save a device profile.")
    parser.add_argument("inputs", nargs="+", help="DICOM files or folders from ONE imaging machine")
    parser.add_argument("--boxes", help='Skip the window: "x0,y0,x1,y1;x0,y0,x1,y1"')
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT), help="Where to save <device key>.json")
    parser.add_argument("--tolerance", type=int, default=DEFAULT_TOLERANCE)
    parser.add_argument("--force", action="store_true", help="Replace an existing profile for this machine")
    args = parser.parse_args(argv)

    try:
        images = _read_images(args.inputs)
        if not images:
            raise Stop("No readable DICOM images were found. Next step: point the tool at the scanner's DICOM files.")
        key = _device_key(images)
        shape = images[0][1].shape[:2]
        if any(fr.shape[:2] != shape for _, fr in images):
            raise Stop("The images are not all the same size. Next step: measure one image size at a time.")
        raw = parse_boxes(args.boxes) if args.boxes is not None else _draw_boxes(images)
        boxes = clean_boxes(raw, shape)
        if not boxes:
            raise Stop("No boxes were drawn, so nothing was saved. Next step: run again and press 'a' after each rectangle.")
        out_dir = Path(args.out_dir)
        out = out_dir / f"{key}.json"
        if out.exists() and not args.force:
            raise Stop(f"A profile for this machine already exists ({out.name}). "
                       "Next step: re-run with --force to replace it.")
        out_dir.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(build_profile(key, boxes, len(images), args.tolerance), indent=2) + "\n",
                       encoding="utf-8")

        from src.services.pixel_deid.profiles import load_profiles
        if key not in load_profiles(str(out_dir)):
            raise Stop("The saved profile could not be read back. Next step: contact the maintainer.")
    except Stop as exc:
        print(f"Stopped: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:  # noqa: BLE001 — never echo exception text (it can quote header values or paths)
        print(f"Stopped: unexpected {type(exc).__name__}. Next step: contact the maintainer; nothing was saved.",
              file=sys.stderr)
        return 2
    print(f"Saved {len(boxes)} box(es) for device {key} from {len(images)} image(s) to {out.name}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
