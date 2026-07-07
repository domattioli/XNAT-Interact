"""
tests/regression_014/test_l1_implementation_uid.py -- #33 L1 regression (spec 014).

L1 (still-open per the 2026-07-07 audit, ledger.md):
ArthroDiagnosticImage._create_dicom_representation set
file_meta.ImplementationClassUID from parent_uid -- a per-session value.
DICOM PS3.10 SS7.1 requires ImplementationClassUID to identify the software
implementation itself, a value that must be constant for a given
implementation, not vary per file/session.

Baseline evidence (pre-fix, this branch before the L1 fix commit): the test
below FAILS (two different parent_uid values produced two different
ImplementationClassUID values) -- see ledger.md for the recorded run.

Drives the real bound method against a minimal stand-in, the same
technique tests/test_session_df_index_m3.py uses -- avoids the heavy
img_ffn/ConfigTables/video-file setup __init__ would otherwise require.

Offline only. No real server, no real PHI, no video file I/O.
"""
from __future__ import annotations

import types
from types import SimpleNamespace

import numpy as np
import pytest

import src.xnat_scan_data as _mod


class _MinimalArthroImage:
    """Drives the real _create_dicom_representation with no file/video I/O."""

    def __init__(self, uid: str) -> None:
        self._uid = uid
        self._still_num = "1"
        self._datetime = SimpleNamespace(date="20240101", time="120000")
        self._redacted_string = "REDACTED"
        self._ffn_str = "/tmp/fake_still.jpg"
        self.intake_form = SimpleNamespace(acquisition_site="TEST_SITE")
        self.image = SimpleNamespace(
            gray_img=np.zeros((4, 5), dtype=np.uint8),
        )

    def generate_uid(self) -> str:
        return "9_9_9_9"

    @property
    def uid(self) -> str:
        return self._uid

    @property
    def still_num(self) -> str:
        return self._still_num

    @property
    def datetime(self):
        return self._datetime

    @property
    def redacted_string(self) -> str:
        return self._redacted_string

    @property
    def ffn_str(self) -> str:
        return self._ffn_str

    @property
    def new_ffn(self) -> str:
        return "/tmp/fake_new.dcm"

    def run(self, parent_uid: str) -> None:
        types.MethodType(
            _mod.ArthroDiagnosticImage._create_dicom_representation, self
        )(parent_uid=parent_uid)


def _implementation_class_uid(parent_uid: str) -> str:
    session = _MinimalArthroImage(uid="1_2_3_4")
    session.run(parent_uid=parent_uid)
    return str(session._metadata.file_meta.ImplementationClassUID)


def test_implementation_class_uid_is_constant_across_different_parent_uids():
    uid_a = _implementation_class_uid("1_2_3_100")
    uid_b = _implementation_class_uid("9_8_7_200")

    # Pre-fix: ImplementationClassUID was derived from parent_uid, so two
    # different parent_uid values produced two different
    # ImplementationClassUID values -- this assertion is what fails against
    # the pre-fix code.
    assert uid_a == uid_b, (
        f"ImplementationClassUID must be a fixed per-implementation value "
        f"(DICOM PS3.10 §7.1), not vary with parent_uid; got {uid_a!r} vs {uid_b!r}"
    )


def test_implementation_class_uid_matches_declared_constant():
    uid = _implementation_class_uid("1_2_3_100")
    assert uid == _mod.XNAT_INTERACT_IMPLEMENTATION_CLASS_UID
