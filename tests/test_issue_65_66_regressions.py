"""Offline regression tests for GitHub issues #65 and #66.

#65: every video in a SourceESVSession must get a unique NEW_FN.
#66: _ConfigTablesRegistryAdapter must support image_dedup (image_exists + fetchone).

Synthetic data only; no server, no PHI.
"""
from __future__ import annotations

import types
from types import SimpleNamespace

import pandas as pd

import src.xnat_experiment_data as mod
from src.services.dedup import image_dedup


class _SessionStub:
    """Holds only the attributes _mine_session_metadata reads."""

    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df
        self.intake_form = SimpleNamespace(uid="SYNTHUID01")

    @property
    def df(self) -> pd.DataFrame:
        return self._df


class _BareVideo(mod.ArthroVideo):
    """ArthroVideo built without a file; skips the release-on-delete hook."""

    def __del__(self) -> None:
        pass


def test_issue_65_all_video_names_unique():
    videos = [object.__new__(_BareVideo) for _ in range(4)]
    df = pd.DataFrame({"IS_VALID": [True] * 4, "OBJECT": videos, "NEW_FN": [""] * 4})
    stub = _SessionStub(df)
    types.MethodType(mod.SourceESVSession._mine_session_metadata, stub)()
    names = list(stub.df["NEW_FN"])
    assert len(set(names)) == len(names), names
    assert names[0].startswith("0000-")


def _adapter(rows):
    cfg = SimpleNamespace(tables={"IMAGE_HASHES": pd.DataFrame(rows, columns=["NAME", "SUBJECT"])})
    return mod._ConfigTablesRegistryAdapter(cfg)


def test_issue_66_image_dedup_with_config_adapter():
    stored = "ab" * 32
    adapter = _adapter([{"NAME": stored.upper(), "SUBJECT": "CASE_A"}])
    assert image_dedup(stored, "1.2.3", adapter).is_duplicate is True
    assert image_dedup(stored.upper(), None, adapter).is_duplicate is True
    assert image_dedup("cd" * 32, "1.2.3", adapter).is_duplicate is False
    assert adapter.image_exists("") is False


def test_issue_66_static_cursor_fetchone():
    assert mod._StaticCursor([(1,), (2,)]).fetchone() == (1,)
    assert mod._StaticCursor([]).fetchone() is None
