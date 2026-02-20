"""Lead scoring helpers."""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from transform import bucket_from_nace, ensure_bucket

_SECTOR_BONUS_BUCKETS = {"beauty", "horeca", "health"}


def _is_missing_value(value: object) -> bool:
    if value is None:
        return True
    if pd.isna(value):
        return True
    return str(value).strip() == ""


def score_leads(
    df: pd.DataFrame,
    *,
    months_recent: int = 18,
    today: datetime | None = None,
) -> pd.DataFrame:
    """Apply rule-based scoring and return dataframe sorted by score descending.

    Rules:
    - start_date within latest ``months_recent`` months: +30
    - sector in {beauty, horeca, health}: +15
    - missing phone/email (if columns exist): +5 each
    - missing nace: -5

    Adds ``score_total`` and ``score_reasons`` columns.
    """
    scored = df.copy()
    now = pd.Timestamp(today or datetime.utcnow())
    recent_threshold = now - pd.DateOffset(months=months_recent)

    # Ensure a normalized sector column exists.
    if "sector" not in scored.columns:
        if "nace" in scored.columns:
            scored["sector"] = scored["nace"].apply(bucket_from_nace)
        else:
            scored["sector"] = "other"
    else:
        scored["sector"] = scored["sector"].apply(ensure_bucket)

    totals: list[int] = []
    reasons_col: list[str] = []

    has_phone = "phone" in scored.columns
    has_email = "email" in scored.columns
    has_nace = "nace" in scored.columns

    parsed_start_dates = (
        pd.to_datetime(scored["start_date"], errors="coerce")
        if "start_date" in scored.columns
        else pd.Series(pd.NaT, index=scored.index)
    )

    for idx, row in scored.iterrows():
        total = 0
        reasons: list[str] = []

        start_date = parsed_start_dates.loc[idx]
        if pd.notna(start_date) and start_date >= recent_threshold:
            total += 30
            reasons.append(f"new<{months_recent}m;+30")

        sector = row.get("sector", "other")
        if sector in _SECTOR_BONUS_BUCKETS:
            total += 15
            reasons.append("sector;+15")

        if has_phone and _is_missing_value(row.get("phone")):
            total += 5
            reasons.append("missing_phone;+5")

        if has_email and _is_missing_value(row.get("email")):
            total += 5
            reasons.append("missing_email;+5")

        if has_nace and _is_missing_value(row.get("nace")):
            total -= 5
            reasons.append("missing_nace;-5")

        totals.append(total)
        reasons_col.append("|".join(reasons))

    scored["score_total"] = totals
    scored["score_reasons"] = reasons_col

    return scored.sort_values(by="score_total", ascending=False, kind="stable")
