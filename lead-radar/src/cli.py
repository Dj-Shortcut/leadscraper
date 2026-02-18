"""CLI entrypoint voor Lead Radar verwerking."""

from __future__ import annotations

import argparse
import csv
import logging
import re
import unicodedata
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterator

from .config import TARGET_POSTCODES
from .export import export_leads
from .transform import bucket_from_nace
from .validate import validate_record


LOGGER = logging.getLogger(__name__)
LARGE_CSV_WARNING_BYTES = 1_000_000_000

INPUT_FILE_CANDIDATES: dict[str, list[str]] = {
    "enterprise": ["enterprises.csv", "enterprise.csv"],
    "establishment": ["establishments.csv", "establishment.csv"],
    "activity": ["activities.csv", "activity.csv"],
    "contact": ["contacts.csv", "contact.csv"],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lead Radar CSV pipeline")
    parser.add_argument("--input", required=True, help="Input map met bron-CSV's")
    parser.add_argument("--output", required=True, help="Output CSV pad")
    parser.add_argument("--postcodes", default="", help="Komma-gescheiden lijst postcodes")
    parser.add_argument("--months", type=int, default=18, help="Maximale leeftijd in maanden")
    parser.add_argument("--min-score", type=int, default=40, help="Minimale score voor output")
    parser.add_argument("--limit", type=int, default=200, help="Maximum records in output")
    parser.add_argument("--verbose", action="store_true", help="Toon detectie-info over inputbestanden")
    return parser.parse_args()


def detect_delimiter(path: Path, fallback: str = ";") -> str:
    with path.open("r", encoding="utf-8-sig", errors="ignore", newline="") as handle:
        sample = handle.read(4_096)

    if not sample:
        return fallback

    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=";,\t")
        return str(dialect.delimiter)
    except csv.Error:
        return fallback


def iter_csv_rows(path: Path) -> Iterator[dict[str, str]]:
    try:
        delimiter = detect_delimiter(path)
    except (csv.Error, OSError):
        delimiter = ";"

    try:
        file_size = path.stat().st_size
        if file_size >= LARGE_CSV_WARNING_BYTES:
            size_gb = file_size / (1024**3)
            LOGGER.warning("Large CSV detected for streaming: %s (%.2f GiB)", path, size_gb)
    except OSError:
        pass

    with path.open("r", encoding="utf-8-sig", errors="ignore", newline="") as handle:
        yield from csv.DictReader(handle, delimiter=delimiter)


def read_csv(path: Path) -> list[dict[str, str]]:
    return list(iter_csv_rows(path))


def normalize_key(name: str) -> str:
    value = unicodedata.normalize("NFKD", str(name))
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = re.sub(r"[^a-z0-9]+", "_", value.strip().lower())
    value = re.sub(r"_+", "_", value)
    return value.strip("_")


def normalize_row_keys(row: dict[str, str]) -> dict[str, str]:
    return {normalize_key(key): value for key, value in row.items()}


def find_input_file(input_dir: Path, candidates: list[str]) -> Path:
    for candidate in candidates:
        candidate_path = input_dir / candidate
        if candidate_path.is_file():
            return candidate_path

    for candidate in candidates:
        doubled_extension = input_dir / f"{candidate}.csv"
        if doubled_extension.is_file():
            return doubled_extension

    found_entries = sorted(item.name for item in input_dir.iterdir()) if input_dir.exists() else []
    expected = ", ".join(candidates)
    found = ", ".join(found_entries) if found_entries else "(geen bestanden gevonden)"
    raise FileNotFoundError(
        f"Geen geldig inputbestand gevonden in '{input_dir}'. "
        f"Verwacht één van: {expected}. Gevonden: {found}."
    )


def _format_detected_files(input_dir: Path) -> str:
    entries = sorted(item.name for item in input_dir.iterdir()) if input_dir.exists() else []
    if not entries:
        return "Detected files: (geen bestanden gevonden)"
    return f"Detected files: {', '.join(entries)}"


def detect_input_dir(input_dir: Path) -> Path:
    csv_files = sorted(item for item in input_dir.iterdir() if item.is_file() and item.suffix.lower() == ".csv")
    if csv_files:
        return input_dir

    subdirs = sorted(item for item in input_dir.iterdir() if item.is_dir())
    if len(subdirs) == 1:
        subdir_csv_files = sorted(
            item for item in subdirs[0].iterdir() if item.is_file() and item.suffix.lower() == ".csv"
        )
        if subdir_csv_files:
            print(f"Detected subfolder {subdirs[0]} with CSV files; using it")
            return subdirs[0]

    return input_dir


def load_contacts_by_enterprise(input_dir: Path, establishments: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    try:
        contacts_file = find_input_file(input_dir, INPUT_FILE_CANDIDATES["contact"])
    except FileNotFoundError:
        return {}

    establishment_to_enterprise: dict[str, str] = {}
    for row in establishments:
        normalized_row = normalize_row_keys(row)
        establishment_number = (normalized_row.get("establishment_number") or "").strip()
        enterprise_number = (normalized_row.get("enterprise_number") or "").strip()
        if establishment_number and enterprise_number:
            establishment_to_enterprise[establishment_number] = enterprise_number

    contacts_by_enterprise: dict[str, dict[str, str]] = {}
    for raw_row in iter_csv_rows(contacts_file):
        row = normalize_row_keys(raw_row)
        enterprise_number = (row.get("enterprise_number") or "").strip()
        if not enterprise_number:
            establishment_number = (row.get("establishment_number") or "").strip()
            enterprise_number = establishment_to_enterprise.get(establishment_number, "")

        if not enterprise_number:
            continue

        phone = (row.get("phone") or "").strip()
        email = (row.get("email") or "").strip()
        if not phone and not email:
            continue

        existing = contacts_by_enterprise.get(enterprise_number, {"phone": "", "email": ""})
        if phone and not existing["phone"]:
            existing["phone"] = phone
        if email and not existing["email"]:
            existing["email"] = email
        contacts_by_enterprise[enterprise_number] = existing

    return contacts_by_enterprise


def months_since(start_date: str) -> int:
    started = datetime.strptime(start_date, "%Y-%m-%d").date()
    today = date.today()
    return (today.year - started.year) * 12 + (today.month - started.month)


def parse_postcodes(raw: str) -> set[str]:
    parsed = {item.strip() for item in raw.split(",") if item.strip()}
    if parsed:
        return parsed
    return set(TARGET_POSTCODES)


def score_record(
    age_months: int,
    sector_bucket: str,
    has_nace: bool,
    has_phone: bool,
    has_email: bool,
    max_months: int,
) -> tuple[int, str]:
    score = 0
    reasons: list[str] = []

    if age_months <= max_months:
        score += 30
        reasons.append("new<18m")

    if sector_bucket in {"beauty", "horeca", "health"}:
        score += 15
        reasons.append("sector_high")

    if not has_nace:
        score -= 5
        reasons.append("no_nace")

    if has_phone:
        score += 5
        reasons.append("has_phone")

    if has_email:
        score += 3
        reasons.append("has_email")

    score = max(0, min(100, score))
    return score, "|".join(reasons)


def build_records(input_dir: Path, selected_postcodes: set[str], max_months: int, verbose: bool = False) -> list[dict[str, Any]]:
    resolved_input_dir = detect_input_dir(input_dir)
    if verbose:
        print(_format_detected_files(resolved_input_dir))

    enterprises = read_csv(find_input_file(resolved_input_dir, INPUT_FILE_CANDIDATES["enterprise"]))
    establishments = read_csv(find_input_file(resolved_input_dir, INPUT_FILE_CANDIDATES["establishment"]))
    activities = read_csv(find_input_file(resolved_input_dir, INPUT_FILE_CANDIDATES["activity"]))
    contacts_by_enterprise = load_contacts_by_enterprise(resolved_input_dir, establishments)

    establishment_by_enterprise = {
        row["enterprise_number"].strip(): row for row in establishments if row.get("enterprise_number", "").strip()
    }

    activities_by_enterprise: dict[str, list[str]] = {}
    for row in iter_csv_rows(find_input_file(resolved_input_dir, INPUT_FILE_CANDIDATES["activity"])):
        enterprise_number = row.get("enterprise_number", "").strip()
        nace_code = row.get("nace_code", "").strip()
        if enterprise_number and nace_code:
            activities_by_enterprise.setdefault(enterprise_number, []).append(nace_code)

    source_version = resolved_input_dir.name
    records: list[dict[str, Any]] = []

    for enterprise in enterprises:
        enterprise_number = enterprise.get("enterprise_number", "").strip()
        est = establishment_by_enterprise.get(enterprise_number, {})
        contact = contacts_by_enterprise.get(enterprise_number, {"phone": "", "email": ""})

        postal_code = (est.get("postal_code") or enterprise.get("postal_code") or "").strip()
        start_date = enterprise.get("start_date", "").strip()
        if not start_date:
            continue

        age_months = months_since(start_date)
        in_postcode_set = not selected_postcodes or postal_code in selected_postcodes
        if selected_postcodes and not in_postcode_set:
            continue

        nace_codes = activities_by_enterprise.get(enterprise_number, [])
        first_nace_code = nace_codes[0] if nace_codes else None
        sector_bucket = bucket_from_nace(first_nace_code)
        website = (enterprise.get("website") or "").strip()
        has_website = bool(website)
        phone = contact["phone"]
        email = contact["email"]

        score_total, score_reasons = score_record(
            age_months=age_months,
            sector_bucket=sector_bucket,
            has_nace=bool(nace_codes),
            has_phone=bool(phone),
            has_email=bool(email),
            max_months=max_months,
        )

        record = {
            "enterprise_number": enterprise_number,
            "name": enterprise.get("name", "").strip(),
            "status": enterprise.get("status", "").strip(),
            "start_date": start_date,
            "address": (est.get("address") or enterprise.get("address") or "").strip(),
            "postal_code": postal_code,
            "city": (est.get("city") or enterprise.get("city") or "").strip(),
            "nace_codes": ",".join(nace_codes),
            "sector_bucket": sector_bucket,
            "has_website": "yes" if has_website else "no",
            "phone": phone,
            "email": email,
            "score_total": score_total,
            "score_reasons": score_reasons,
            "source_files_version": source_version,
        }
        validate_record(record)
        records.append(record)

    return records


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input)
    output_file = Path(args.output)
    selected_postcodes = parse_postcodes(args.postcodes)

    records = build_records(
        input_dir=input_dir,
        selected_postcodes=selected_postcodes,
        max_months=args.months,
        verbose=args.verbose,
    )
    total_records = len(records)

    filtered = [row for row in records if int(row["score_total"]) >= args.min_score]
    if args.limit > 0:
        filtered = filtered[: args.limit]

    export_leads(output_path=output_file, records=filtered, total_records=total_records)


if __name__ == "__main__":
    main()
