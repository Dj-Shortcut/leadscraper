"""Data-validatie voor Lead Radar records."""

from __future__ import annotations

import re
from typing import Any, Mapping

POSTAL_CODE_PATTERN = re.compile(r"^\d{4}$")
SCORE_MIN = 0
SCORE_MAX = 100


def validate_record(record: Mapping[str, Any]) -> None:
    """Valideer vereiste velden voor exporteerbare records."""
    enterprise_number = str(record.get("enterprise_number", "")).strip()
    assert enterprise_number, "enterprise_number mag niet leeg zijn"

    postal_code = str(record.get("postal_code", "")).strip()
    assert POSTAL_CODE_PATTERN.fullmatch(postal_code), "postal_code moet exact 4 cijfers bevatten"

    score_total = int(record.get("score_total", -1))
    assert (
        SCORE_MIN <= score_total <= SCORE_MAX
    ), f"score_total ({score_total}) valt buiten verwacht bereik {SCORE_MIN}-{SCORE_MAX}"
