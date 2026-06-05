"""
annotations.exc — Exception type for annotation errors.

AnnotationError is a proper BaseException subclass so it can be raised and
caught via pytest.raises / try-except. It carries a user-facing title and
message (the same fields as FriendlyError) without requiring FriendlyError
to be a BaseException subclass itself.

PHI-FREE: never embed patient identifiers in error messages.
"""
from __future__ import annotations

from src.services.errors import FriendlyError


class AnnotationError(Exception):
    """
    Raised by annotation codecs, validators, and registry on bad input.

    Attributes
    ----------
    friendly : FriendlyError
        User-facing title, message, and recourse steps. Safe to display
        to a researcher; MUST NOT contain PHI.
    """

    def __init__(self, friendly: FriendlyError) -> None:
        super().__init__(friendly.message)
        self.friendly = friendly

    @property
    def title(self) -> str:
        return self.friendly.title

    @property
    def message(self) -> str:
        return self.friendly.message

    @property
    def recourse(self):
        return self.friendly.recourse
