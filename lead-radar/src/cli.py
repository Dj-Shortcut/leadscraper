"""CLI entrypoint voor Lead Radar verwerking."""

from __future__ import annotations

import argparse
import csv
from datetime import date, datetime
from pathlib import Path
from typing import Any

from .config import TARGET_POSTCODES
from .export import export_leads
from .transform import bucket_from_nace
from .validate import validate_record


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lead Radar CSV pipeline")
    parser.add_argument("--input", required=True, help="Input map met bron-CSV's")
    parser.add_argument("--output", required=True, help="Output CSV pad")
    parser.add_argument("--postcodes", default="", help="Komma-gescheiden lijst postcodes")
    parser.add_argument("--months", type=int, default=18, help="Maximale leeftijd in maanden")
    parser.add_argument("--min-score", type=int, default=40, help="Minimale score voor output")
    parser.add_argument("--limit", type=int, default=200, help="Maximum records in output")
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


def read_csv(path: Path) -> list[dict[str, str]]:
    try:
        delimiter = detect_delimiter(path)
    except (csv.Error, OSError):
        delimiter = ";"

    with path.open("r", encoding="utf-8-sig", errors="ignore", newline="") as handle:
        return list(csv.DictReader(handle, delimiter=delimiter))


def find_input_file(input_dir: Path, candidates: list[str]) -> Path:
    for candidate in candidates:
        candidate_path = input_dir / candidate
        if candidate_path.is_file():
            return candidate_path

    found_entries = sorted(item.name for item in input_dir.iterdir()) if input_dir.exists() else []
    expected = ", ".join(candidates)
    found = ", ".join(found_entries) if found_entries else "(geen bestanden gevonden)"
    raise FileNotFoundError(
        f"Geen geldig inputbestand gevonden in '{input_dir}'. "
        f"Verwacht één van: {expected}. Gevonden: {found}."
    )


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

    score = max(0, min(100, score))
    return score, "|".join(reasons)


def build_records(input_dir: Path, selected_postcodes: set[str], max_months: int) -> list[dict[str, Any]]:
    resolved_input_dir = detect_input_dir(input_dir)

    enterprises = read_csv(find_input_file(resolved_input_dir, ["enterprises.csv", "enterprise.csv"]))
    establishments = read_csv(find_input_file(resolved_input_dir, ["establishments.csv", "establishment.csv"]))
    activities = read_csv(find_input_file(resolved_input_dir, ["activities.csv", "activity.csv"]))

    establishment_by_enterprise = {
        row["enterprise_number"].strip(): row for row in establishments if row.get("enterprise_number", "").strip()
    }

    activities_by_enterprise: dict[str, list[str]] = {}
    for row in activities:
        enterprise_number = row.get("enterprise_number", "").strip()
        nace_code = row.get("nace_code", "").strip()
        if enterprise_number and nace_code:
            activities_by_enterprise.setdefault(enterprise_number, []).append(nace_code)

    source_version = resolved_input_dir.name
    records: list[dict[str, Any]] = []

    for enterprise in enterprises:
        enterprise_number = enterprise.get("enterprise_number", "").strip()
        est = establishment_by_enterprise.get(enterprise_number, {})

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

        score_total, score_reasons = score_record(
            age_months=age_months,
            sector_bucket=sector_bucket,
            has_nace=bool(nace_codes),
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

    records = build_records(input_dir=input_dir, selected_postcodes=selected_postcodes, max_months=args.months)
    total_records = len(records)

    filtered = [row for row in records if int(row["score_total"]) >= args.min_score]
    if args.limit > 0:
        filtered = filtered[: args.limit]

    export_leads(output_path=output_file, records=filtered, total_records=total_records)


if __name__ == "__main__":
    main()
