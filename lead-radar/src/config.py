"""Configuration constants for lead radar transformations."""

from __future__ import annotations

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

UNIFIED_OUTPUT_PATH = "lead-radar/data/processed/unified.csv"
