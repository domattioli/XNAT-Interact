"""
PHI-safe probing of real imaging folders.

Everything this package emits passes through :mod:`gate`, an allowlist that
accepts only known keys, integers, booleans, fixed enum values, pixel boxes and
short hex digests. Free text has no path to the output, so a patient name,
a date, a UID or a file name cannot leak into a terminal, a log or an agent's
context, even through an exception message or a library warning.
"""
from .gate import GateError, SafeReport
from .probes import run_all

__all__ = ["GateError", "SafeReport", "run_all"]
