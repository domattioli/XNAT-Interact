"""
Pixel de-identification package for burned-in PHI detection and redaction.

Tiers (cheapest → heaviest):
  0.  Device-profile blind mask (contrast-independent, see profiles.py)
  0'. Cross-frame variance consensus mask (see consensus.py)
  1.  Multipass text detector — Tesseract × {orig, invert, stretch, CLAHE}
      + learned ONNX-CPU text-region detector (detect.py)
  2.  Presidio PHI-NER over OCR text → PHI-vs-benign (verdict.py)

Stage 1 public entry-point: ``detect.multipass_detect(img) -> list[tuple]``
"""
from __future__ import annotations
