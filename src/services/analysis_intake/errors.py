"""
Plain-language refusals for the analysis intake (spec 016, FR-022).

Every problem the intake can foresee is raised as an ``IntakeRefusal``.  It
carries a ``FriendlyError`` (title, message, next steps) so the command line,
and later the guided app, can show it without a traceback.
"""
from __future__ import annotations

from typing import List, Optional

from src.services.errors import FriendlyError


class IntakeRefusal(Exception):
    """The intake stopped on purpose.  ``friendly`` says why and what to do next."""

    def __init__(self, friendly: FriendlyError) -> None:
        super().__init__(friendly.message)
        self.friendly = friendly


def refuse(title: str, message: str, recourse: Optional[List[str]] = None) -> IntakeRefusal:
    """Build an ``IntakeRefusal``.  Always gives at least one next step."""
    steps = list(recourse or [])
    if not steps:
        steps = ["Fix the problem named above and run the command again."]
    return IntakeRefusal(FriendlyError(title=title, message=message, recourse=steps))
