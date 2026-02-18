"""Data transformation helpers for unified lead dataset."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from .config import (
    ACTIVE_STATUS_VALUES,
    TARGET_CITIES_FALLBACK,
    TARGET_POSTCODES,
    UNIFIED_OUTPUT_PATH,
)


def trim_string_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Trim leading/trailing spaces for all string/object columns."""
    out = df.copy()
    for col in out.select_dtypes(include=["object", "string"]).columns:
        out[col] = out[col].astype("string").str.strip()
    return out


def normalize_postcode(series: pd.Series) -> pd.Series:
    """Normalize postcodes to exactly four digits, else NA."""
    extracted = (
        series.astype("string")
        .str.extract(r"(\d{4})", expand=False)
        .str.zfill(4)
    )
    return extracted.where(extracted.str.fullmatch(r"\d{4}"), pd.NA)


def parse_dates(df: pd.DataFrame, date_columns: Iterable[str]) -> pd.DataFrame:
    """Parse date columns using pandas coercion semantics."""
    out = df.copy()
    for col in date_columns:
        if col in out.columns:
            out[col] = pd.to_datetime(out[col], errors="coerce")
    return out


def _resolve_activity_join_key(activities: pd.DataFrame) -> str:
    """Select enterprise key if present, fallback to establishment key."""
    if "enterprise_number" in activities.columns:
        return "enterprise_number"
    if "establishment_number" in activities.columns:
        return "establishment_number"
    raise KeyError(
        "activities dataframe must contain enterprise_number or establishment_number"
    )


def build_unified_dataframe(
    enterprises: pd.DataFrame,
    establishments: pd.DataFrame,
    activities: pd.DataFrame,
    date_columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Join/normalize/filter source data into one lead-targeting dataframe."""
    date_columns = tuple(date_columns or ())

    enterprises = parse_dates(trim_string_columns(enterprises), date_columns)
    establishments = parse_dates(trim_string_columns(establishments), date_columns)
    activities = parse_dates(trim_string_columns(activities), date_columns)

    if "postcode" in establishments.columns:
        establishments["postcode"] = normalize_postcode(establishments["postcode"])

    # enterprises + establishments on enterprise_number
    unified = enterprises.merge(
        establishments,
        on="enterprise_number",
        how="left",
        suffixes=("_enterprise", "_establishment"),
    )

    # activities/NACE on enterprise-level; fallback to establishment-level when needed
    activity_key = _resolve_activity_join_key(activities)
    unified_key = (
        "enterprise_number"
        if activity_key == "enterprise_number"
        else "establishment_number"
    )
    if unified_key in unified.columns:
        unified = unified.merge(
            activities,
            left_on=unified_key,
            right_on=activity_key,
            how="left",
            suffixes=("", "_activity"),
        )

    # Primary address source for local targeting: establishment
    postcode_col = "postcode"
    if postcode_col not in unified.columns and "postcode_establishment" in unified.columns:
        postcode_col = "postcode_establishment"

    city_col = "city"
    if city_col not in unified.columns and "city_establishment" in unified.columns:
        city_col = "city_establishment"

    status_candidates = [
        c
        for c in ("status", "status_enterprise", "enterprise_status", "legal_status")
        if c in unified.columns
    ]
    if status_candidates:
        status_series = unified[status_candidates[0]].astype("string").str.upper()
        active_mask = status_series.isin({s.upper() for s in ACTIVE_STATUS_VALUES})
    else:
        active_mask = pd.Series(True, index=unified.index)

    postcode_mask = pd.Series(False, index=unified.index)
    if postcode_col in unified.columns:
        unified[postcode_col] = normalize_postcode(unified[postcode_col])
        postcode_mask = unified[postcode_col].isin(TARGET_POSTCODES)

    city_mask = pd.Series(False, index=unified.index)
    if city_col in unified.columns:
        city_mask = (
            unified[city_col]
            .astype("string")
            .str.strip()
            .str.lower()
            .isin(TARGET_CITIES_FALLBACK)
        )

    filtered = unified[active_mask & (postcode_mask | city_mask)].copy()
    return filtered


def write_unified_csv(df: pd.DataFrame, output_path: str = UNIFIED_OUTPUT_PATH) -> Path:
    """Persist unified dataframe to configured processed location."""
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    return out
