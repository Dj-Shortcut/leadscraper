from __future__ import annotations

import re
import unicodedata
from pathlib import Path
from typing import Iterable

import pandas as pd

DEFAULT_CANDIDATES: dict[str, list[str]] = {
    "enterprise_number": [
        "enterprise_number",
        "enterprise",
        "enterprise_no",
        "enterprise_nr",
        "ondernemingsnummer",
        "ondernemingsnummer",
        "num_entreprise",
        "numero_entreprise",
        "company_number",
    ],
    "status": ["status", "legal_status", "enterprise_status", "statuut", "etat"],
    "start_date": [
        "start_date",
        "startdate",
        "date_start",
        "activity_start_date",
        "begin_date",
        "begindatum",
        "datum_start",
    ],
    "postal_code": [
        "postal_code",
        "zip",
        "zipcode",
        "postcode",
        "code_postal",
    ],
    "city": ["city", "municipality", "gemeente", "locality", "ville", "stad"],
    "street": ["street", "street_name", "straat", "rue", "road"],
    "number": ["number", "house_number", "box_number", "nr", "num", "bus"],
    "nace_code": ["nace_code", "nace", "nacebel", "activity_code", "code_nace"],
}


DATASET_PATTERNS: dict[str, tuple[str, ...]] = {
    "enterprises": ("enterprise", "ondernem", "entreprise"),
    "establishments": ("establishment", "vestiging", "unite_etablissement", "etablissement"),
    "activities": ("activit", "nace"),
}


def _normalize_name(name: str) -> str:
    value = unicodedata.normalize("NFKD", str(name))
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value)
    return value.strip("_")


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_normalize_name(col) for col in df.columns]
    return df


def detect_columns(df: pd.DataFrame, candidates: dict[str, Iterable[str]]) -> dict[str, str]:
    """Detect canonical columns in *df* using candidate variants.

    Returns a mapping compatible with ``DataFrame.rename(columns=...)``.
    """
    normalized_candidates: dict[str, list[str]] = {
        canonical: [_normalize_name(option) for option in options] for canonical, options in candidates.items()
    }

    rename_map: dict[str, str] = {}
    available = set(df.columns)

    for canonical, options in normalized_candidates.items():
        for option in options:
            if option in available and option not in rename_map:
                rename_map[option] = canonical
                break

    return rename_map


def _resolve_input_path(path: str | Path, dataset_kind: str) -> Path:
    path_obj = Path(path)

    if path_obj.is_file():
        return path_obj

    if not path_obj.exists() or not path_obj.is_dir():
        raise FileNotFoundError(f"Input path does not exist: {path_obj}")

    patterns = DATASET_PATTERNS[dataset_kind]
    files = sorted(file for file in path_obj.iterdir() if file.is_file() and file.suffix.lower() == ".csv")
    for file in files:
        filename = _normalize_name(file.name)
        if any(pattern in filename for pattern in patterns):
            return file

    raise FileNotFoundError(f"No matching CSV found for '{dataset_kind}' under directory: {path_obj}")


def _load_dataset(path: str | Path, dataset_kind: str) -> pd.DataFrame:
    input_file = _resolve_input_path(path, dataset_kind)
    df = pd.read_csv(input_file, sep=";", dtype=str, low_memory=False)
    df = _normalize_columns(df)

    rename_map = detect_columns(df, DEFAULT_CANDIDATES)
    if rename_map:
        df = df.rename(columns=rename_map)

    return df


def load_enterprises(path: str | Path) -> pd.DataFrame:
    return _load_dataset(path, "enterprises")


def load_establishments(path: str | Path) -> pd.DataFrame:
    return _load_dataset(path, "establishments")


def load_activities(path: str | Path) -> pd.DataFrame:
    return _load_dataset(path, "activities")
