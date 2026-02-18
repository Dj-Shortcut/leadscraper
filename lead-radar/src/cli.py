"""CLI entrypoint voor Lead Radar verwerking."""

from __future__ import annotations

import argparse
import csv
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

from .export import export_leads
from .validate import validate_record

TARGET_SECTORS = {
    "56": "HORECA",
    "47": "RETAIL",
    "62": "IT_SERVICES",
    "70": "BUSINESS_SERVICES",
    "96": "PERSONAL_SERVICES",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lead Radar CSV pipeline")
    parser.add_argument("--input", required=True, help="Input map met bron-CSV's")
    parser.add_argument("--output", required=True, help="Output CSV pad")
    parser.add_argument("--postcodes", default="", help="Komma-gescheiden lijst postcodes")
    parser.add_argument("--months", type=int, default=18, help="Maximale leeftijd in maanden")
    parser.add_argument("--min-score", type=int, default=40, help="Minimale score voor output")
    parser.add_argument("--limit", type=int, default=200, help="Maximum records in output")
    return parser.parse_args()


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def months_since(start_date: str) -> int:
    started = datetime.strptime(start_date, "%Y-%m-%d").date()
    today = date.today()
    return (today.year - started.year) * 12 + (today.month - started.month)


def parse_postcodes(raw: str) -> set[str]:
    return {item.strip() for item in raw.split(",") if item.strip()}


def derive_sector_bucket(nace_codes: Iterable[str]) -> str:
    for code in nace_codes:
        prefix = code[:2]
        if prefix in TARGET_SECTORS:
            return TARGET_SECTORS[prefix]
    return "OTHER"


def score_record(
    status: str,
    has_website: bool,
    age_months: int,
    in_postcode_set: bool,
    sector_bucket: str,
    max_months: int,
) -> tuple[int, str]:
    score = 0
    reasons: list[str] = []

    if status.upper() == "ACTIVE":
        score += 20
        reasons.append("active_status")

    if age_months <= max_months:
        score += 25
        reasons.append("recent_start")

    if sector_bucket != "OTHER":
        score += 20
        reasons.append(f"target_sector:{sector_bucket}")

    if not has_website:
        score += 20
        reasons.append("no_website_detected")

    if in_postcode_set:
        score += 15
        reasons.append("preferred_postcode")

    score = max(0, min(100, score))
    return score, ",".join(reasons)


def build_records(input_dir: Path, selected_postcodes: set[str], max_months: int) -> list[dict[str, Any]]:
    enterprises = read_csv(input_dir / "enterprises.csv")
    establishments = read_csv(input_dir / "establishments.csv")
    activities = read_csv(input_dir / "activities.csv")

    establishment_by_enterprise = {
        row["enterprise_number"].strip(): row for row in establishments if row.get("enterprise_number", "").strip()
    }

    activities_by_enterprise: dict[str, list[str]] = {}
    for row in activities:
        enterprise_number = row.get("enterprise_number", "").strip()
        nace_code = row.get("nace_code", "").strip()
        if enterprise_number and nace_code:
            activities_by_enterprise.setdefault(enterprise_number, []).append(nace_code)

    source_version = input_dir.name
    records: list[dict[str, Any]] = []

    for enterprise in enterprises:
        enterprise_number = enterprise.get("enterprise_number", "").strip()
        est = establishment_by_enterprise.get(enterprise_number, {})

        postal_code = (est.get("postal_code") or enterprise.get("postal_code") or "").strip()
        start_date = enterprise.get("start_date", "").strip()
        if not start_date:
            continue

        age_months = months_since(start_date)
        if age_months > max_months:
            continue

        in_postcode_set = not selected_postcodes or postal_code in selected_postcodes
        if selected_postcodes and not in_postcode_set:
            continue

        nace_codes = activities_by_enterprise.get(enterprise_number, [])
        sector_bucket = derive_sector_bucket(nace_codes)
        website = (enterprise.get("website") or "").strip()
        has_website = bool(website)

        score_total, score_reasons = score_record(
            status=enterprise.get("status", ""),
            has_website=has_website,
            age_months=age_months,
            in_postcode_set=in_postcode_set,
            sector_bucket=sector_bucket,
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
