import re
from typing import Literal

Lane = Literal["hot", "cold", "skip"]

_HOT_PATTERNS = [
    r"\brebook(ed|ing)?\b",
    r"\breschedul(e|ed|ing)\b",
    r"\bconfirm(ed|s)?\b",
    r"\bbooked\s+(a|the|your|my|it|for|slot|demo|appointment|meeting)\b",
    r"\bbook\s+(a|the|your|my|slot|demo|appointment|meeting)\b",
    r"\bdemo\s+is\s+booked\b",
    r"\bescalat(e|ed|ing|ion)\b",
    r"\bspeak\s+to\s+a\s+manager\b",
    r"\bfile\s+a\s+complaint\b",
    r"\bunacceptable\b",
    r"\btomorrow\s+at\s+\d",
    r"\b\d+:\d+\s*(am|pm)\b",
    r"\bthursday\b",
    r"\bsee\s+you\s+then\b",
]

_COLD_OVERRIDES = [
    r"\bnot\s+interested\b",
    r"\bdon'?t\s+call\b",
    r"\balready\s+(booked|purchased|done|completed)\b",
    r"\bcall\s+(me\s+)?back\b",
    r"\bcallback\b",
]

_HOT_RE = [re.compile(p, re.IGNORECASE) for p in _HOT_PATTERNS]
_COLD_RE = [re.compile(p, re.IGNORECASE) for p in _COLD_OVERRIDES]


def classify_lane(transcript_text: str, turn_count: int) -> Lane:
    """Return processing lane for a completed call.

    skip: fewer than 4 turns — no LLM job created.
    hot:  outcome needs immediate downstream action.
    cold: deferrable, batch processing is fine.

    Cold overrides are checked first so phrases like 'already booked'
    or 'call back later' don't trigger hot classification.
    """
    if turn_count < 4:
        return "skip"

    for pattern in _COLD_RE:
        if pattern.search(transcript_text):
            return "cold"

    for pattern in _HOT_RE:
        if pattern.search(transcript_text):
            return "hot"

    return "cold"
