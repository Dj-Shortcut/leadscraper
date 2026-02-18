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


def iter_csv_rows(path: Path, *, encoding: str = "utf-8-sig", max_bad_lines: int = 1000) -> Iterator[dict[str, str]]:
    if max_bad_lines < 0:
        raise ValueError("max_bad_lines moet >= 0 zijn")

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

    encodings = [encoding]
    if encoding.lower() != "latin-1":
        encodings.append("latin-1")

    last_decode_error: UnicodeDecodeError | None = None
    for selected_encoding in encodings:
        bad_lines = 0
        line_index = 1
        try:
            with path.open("r", encoding=selected_encoding, errors="strict", newline="") as handle:
                reader = csv.DictReader(handle, delimiter=delimiter)
                try:
                    for line_index, row in enumerate(reader, start=2):
                        yield row
                    if bad_lines:
                        LOGGER.warning("bad lines skipped: %s", bad_lines)
                    return
                except (OSError, csv.Error) as err:
                    LOGGER.warning(
                        "CSV stream error in %s at line %s: %s. Falling back to line-by-line parsing.",
                        path,
                        line_index,
                        err,
                    )

                    fieldnames = list(reader.fieldnames or [])
                    if not fieldnames:
                        raise csv.Error("CSV header ontbreekt of kon niet gelezen worden")

                    for fallback_line_index, line in enumerate(handle, start=line_index + 1):
                        try:
                            parsed_rows = list(csv.reader([line], delimiter=delimiter))
                        except (OSError, csv.Error) as parse_error:
                            bad_lines += 1
                            LOGGER.warning(
                                "Skipping bad line %s in %s: %s",
                                fallback_line_index,
                                path,
                                parse_error,
                            )
                            if bad_lines > max_bad_lines:
                                raise RuntimeError(
                                    f"Max bad lines exceeded ({max_bad_lines}) while reading {path}"
                                ) from parse_error
                            continue

                        if not parsed_rows:
                            bad_lines += 1
                            LOGGER.warning("Skipping empty/bad line %s in %s", fallback_line_index, path)
                            if bad_lines > max_bad_lines:
                                raise RuntimeError(f"Max bad lines exceeded ({max_bad_lines}) while reading {path}")
                            continue

                        values = parsed_rows[0]
                        if len(values) != len(fieldnames):
                            bad_lines += 1
                            LOGGER.warning(
                                "Skipping bad line %s in %s: expected %s columns, got %s",
                                fallback_line_index,
                                path,
                                len(fieldnames),
                                len(values),
                            )
                            if bad_lines > max_bad_lines:
                                raise RuntimeError(f"Max bad lines exceeded ({max_bad_lines}) while reading {path}")
                            continue

                        yield dict(zip(fieldnames, values))

                    if bad_lines:
                        LOGGER.warning("bad lines skipped: %s", bad_lines)
                    return
        except UnicodeDecodeError as decode_error:
            last_decode_error = decode_error
            LOGGER.warning(
                "Failed reading %s with encoding %s (%s).",
                path,
                selected_encoding,
                decode_error,
            )
            continue

    if last_decode_error is not None:
        raise last_decode_error


def read_csv(path: Path, *, encoding: str = "utf-8-sig", max_bad_lines: int = 1000) -> list[dict[str, str]]:
    return list(iter_csv_rows(path, encoding=encoding, max_bad_lines=max_bad_lines))


def normalize_key(name: str) -> str:
    value = unicodedata.normalize("NFKD", str(name))
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = re.sub(r"[^a-z0-9]+", "_", value.strip().lower())
    value = re.sub(r"_+", "_", value)
    return value.strip("_")


def normalize_row_keys(row: dict[str, str]) -> dict[str, str]:
    return {normalize_key(key): value for key, value in row.items()}


def normalize_identifier(value: str) -> str:
    return re.sub(r"\D", "", str(value or "").strip().strip('"').strip("'"))


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


def load_contacts_by_enterprise(
    input_dir: Path,
    establishments: list[dict[str, str]],
    *,
    encoding: str,
    max_bad_lines: int,
) -> dict[str, dict[str, str]]:
    try:
        contacts_file = find_input_file(input_dir, INPUT_FILE_CANDIDATES["contact"])
    except FileNotFoundError:
        return {}

    establishment_to_enterprise: dict[str, str] = {}
    for row in establishments:
        normalized_row = normalize_row_keys(row)
        establishment_number = normalize_identifier(normalized_row.get("establishment_number") or "")
        enterprise_number = normalize_identifier(normalized_row.get("enterprise_number") or "")
        if establishment_number and enterprise_number:
            establishment_to_enterprise[establishment_number] = enterprise_number

    contacts_by_enterprise: dict[str, dict[str, str]] = {}
    for raw_row in iter_csv_rows(contacts_file, encoding=encoding, max_bad_lines=max_bad_lines):
        row = normalize_row_keys(raw_row)

        entity_number = normalize_identifier(
            row.get("entitynumber")
            or row.get("entity_number")
            or row.get("enterprise_number")
            or row.get("establishment_number")
            or ""
        )
        entity_contact = (row.get("entitycontact") or row.get("entity_contact") or "").strip().upper()
        contact_type = (row.get("contacttype") or row.get("contact_type") or "").strip().upper()
        contact_value = (row.get("value") or row.get("phone") or row.get("email") or row.get("website") or "").strip()

        if contact_type not in {"TEL", "EMAIL", "WEB"} or not contact_value:
            continue

        enterprise_number = ""
        is_establishment = entity_contact in {"EST", "ESTABLISHMENT", "VESTIGING"}
        if is_establishment:
            enterprise_number = establishment_to_enterprise.get(entity_number, "")
        else:
            enterprise_number = entity_number

        if not enterprise_number:
            continue

        existing = contacts_by_enterprise.get(
            enterprise_number,
            {"phone": "", "email": "", "website": "", "has_website": "no"},
        )
        if contact_type == "TEL" and not existing["phone"]:
            existing["phone"] = contact_value
        if contact_type == "EMAIL" and not existing["email"]:
            existing["email"] = contact_value
        if contact_type == "WEB" and not existing["website"]:
            existing["website"] = contact_value
            existing["has_website"] = "yes"

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
    has_website: bool,
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

    if has_website:
        reasons.append("has_website")

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
        normalize_identifier(row["enterprise_number"]): row
        for row in establishments
        if normalize_identifier(row.get("enterprise_number", ""))
    }

    activities_by_enterprise: dict[str, list[str]] = {}
    for row in iter_csv_rows(find_input_file(resolved_input_dir, ["activities.csv", "activity.csv"])):
        enterprise_number = normalize_identifier(row.get("enterprise_number", ""))
        nace_code = row.get("nace_code", "").strip()
        if enterprise_number and nace_code:
            activities_by_enterprise.setdefault(enterprise_number, []).append(nace_code)

    source_version = resolved_input_dir.name
    records: list[dict[str, Any]] = []

    for enterprise in enterprises:
        enterprise_number = normalize_identifier(enterprise.get("enterprise_number", ""))
        est = establishment_by_enterprise.get(enterprise_number, {})
        contact = contacts_by_enterprise.get(
            enterprise_number,
            {"phone": "", "email": "", "website": "", "has_website": "no"},
        )

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
        website = contact["website"] or (enterprise.get("website") or "").strip()
        has_website = bool(website)
        phone = contact["phone"]
        email = contact["email"]

        score_total, score_reasons = score_record(
            age_months=age_months,
            sector_bucket=sector_bucket,
            has_nace=bool(nace_codes),
            has_phone=bool(phone),
            has_email=bool(email),
            has_website=has_website,
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
            "website": website,
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
