"""Central configuration helpers for Leadscraper."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

TARGET_POSTCODES = {
    "9400",
    "9402",
    "9406",
    "9300",
    "1770",
    "1760",
    "1750",
    "9500",
    "9620",
    "1700",
    "1540",
}

# Fallback cities when postcode quality is poor in source dumps.
TARGET_CITIES_FALLBACK = {
    "ninove",
    "aalst",
    "roosdaal",
    "lierde",
    "geraardsbergen",
    "sint-lievens-houtem",
    "herzele",
    "denderleeuw",
    "galmaarden",
    "gooik",
    "lennik",
    "affligem",
}

ACTIVE_STATUS_VALUES = {
    "ACTIVE",
    "ACTIEF",
    "IN BUSINESS",
}

UNIFIED_OUTPUT_PATH = "data/processed/unified.csv"
SUPPORTED_COUNTRIES = {"BE"}


@dataclass(slots=True)
class RuntimeConfig:
    input_dir: Path
    output: Path
    country: str
    city: str
    query: str
    postcodes: str
    months: int
    min_score: int
    limit: int
    dry_run: bool


def build_runtime_config(args: object) -> RuntimeConfig:
    country = str(getattr(args, "country", "BE") or "BE").upper()
    if country not in SUPPORTED_COUNTRIES:
        supported = ", ".join(sorted(SUPPORTED_COUNTRIES))
        raise ValueError(f"Unsupported --country '{country}'. Supported countries: {supported}.")

    months = int(getattr(args, "months", 18))
    if months < 1:
        raise ValueError("--months must be >= 1")

    limit = int(getattr(args, "limit", 200))
    if limit < 0:
        raise ValueError("--limit must be >= 0")

    min_score = int(getattr(args, "min_score", 40))
    if min_score < 0 or min_score > 100:
        raise ValueError("--min-score must be between 0 and 100")

    input_dir = Path(str(getattr(args, "input", "") or ""))
    if not input_dir.exists():
        raise ValueError(f"Input directory does not exist: {input_dir}")

    output = Path(str(getattr(args, "output", "") or ""))
    if not output.name:
        raise ValueError("--output must include a filename, e.g. data/processed/leads.csv")

    return RuntimeConfig(
        input_dir=input_dir,
        output=output,
        country=country,
        city=str(getattr(args, "city", "") or "").strip(),
        query=str(getattr(args, "query", "") or "").strip().lower(),
        postcodes=str(getattr(args, "postcodes", "") or "").strip(),
        months=months,
        min_score=min_score,
        limit=limit,
        dry_run=bool(getattr(args, "dry_run", False)),
    )
