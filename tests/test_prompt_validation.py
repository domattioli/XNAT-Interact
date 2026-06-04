"""
Tests for ORDataIntakeForm.prompt_until_valid_answer_given — the single most
reused piece of the terminal UX. Every menu choice flows through it.

We feed fake keyboard input by monkeypatching builtins.input, so no human is
needed. This is exactly the seam a future GUI / test harness drives instead of
a person typing at a terminal.
"""
from __future__ import annotations

import builtins

import pytest

from src.xnat_resource_data import ORDataIntakeForm, InvalidInputError


def _feed(monkeypatch, answers):
    """Make input() return successive values from `answers`."""
    it = iter(answers)
    monkeypatch.setattr(builtins, "input", lambda *a, **k: next(it))


def test_accepts_valid_answer_first_try(monkeypatch):
    _feed(monkeypatch, ["1"])
    out = ORDataIntakeForm.prompt_until_valid_answer_given("Task", ["1", "2", "3"])
    assert out == "1"


def test_answer_is_uppercased(monkeypatch):
    _feed(monkeypatch, ["a"])
    out = ORDataIntakeForm.prompt_until_valid_answer_given("Letter", ["A", "B"])
    assert out == "A"


def test_recovers_after_one_invalid_answer(monkeypatch):
    _feed(monkeypatch, ["bogus", "2"])
    out = ORDataIntakeForm.prompt_until_valid_answer_given(
        "Choice", ["1", "2"], max_num_attempts=3
    )
    assert out == "2"


def test_raises_after_max_attempts(monkeypatch):
    _feed(monkeypatch, ["x", "y"])
    with pytest.raises(InvalidInputError):
        ORDataIntakeForm.prompt_until_valid_answer_given(
            "Choice", ["1", "2"], max_num_attempts=2
        )


@pytest.mark.known_issue
def test_exhausting_attempts_raises_instead_of_reprompting(monkeypatch):
    """
    Current behavior: after `max_num_attempts` the call raises and (upstream)
    the whole task aborts. The improvement plan replaces this hard failure with
    a "let's try that step again / go back / cancel cleanly" interaction so a
    student never loses their place over a typo.
    """
    _feed(monkeypatch, ["nope", "still-nope"])
    with pytest.raises(InvalidInputError):
        ORDataIntakeForm.prompt_until_valid_answer_given(
            "Choice", ["1", "2"], max_num_attempts=2
        )
