"""Transformation helpers for lead bucketing."""

from __future__ import annotations

from typing import Optional

# Rule-based NACE prefix mapping.
# More specific prefixes must come before broader prefixes.
_NACE_PREFIX_BUCKETS: tuple[tuple[str, str], ...] = (
    ("96.02", "beauty"),  # hair and beauty treatment
    ("56", "horeca"),
    ("86", "health"),
    ("47", "retail"),
    ("43", "service_trades"),
    ("81", "service_trades"),
    ("95", "service_trades"),
)

_ALLOWED_BUCKETS = {
    "beauty",
    "horeca",
    "health",
    "retail",
    "service_trades",
    "other",
}


def normalize_nace_code(nace_code: Optional[str]) -> str:
    """Normalize a raw NACE code into comparable dotted-string form."""
    if nace_code is None:
        return ""
    return str(nace_code).strip().upper().replace(",", ".")


def bucket_from_nace(nace_code: Optional[str]) -> str:
    """Return one of: beauty, horeca, health, retail, service_trades, other."""
    normalized = normalize_nace_code(nace_code)
    if not normalized:
        return "other"

    for prefix, bucket in _NACE_PREFIX_BUCKETS:
        if normalized.startswith(prefix):
            return bucket

    return "other"


def ensure_bucket(bucket: Optional[str]) -> str:
    """Ensure external bucket values remain in the supported list."""
    value = (bucket or "").strip().lower()
    if value in _ALLOWED_BUCKETS:
        return value
    return "other"
