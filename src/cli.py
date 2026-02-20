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

from .config import TARGET_POSTCODES, build_runtime_config
from .export import export_leads
from .integrations import build_drive_download_url, download_file, extract_zip_file, upload_csv_to_google_sheet
from .transform import bucket_from_nace
from .validate import validate_record

LOGGER = logging.getLogger(__name__)
LARGE_CSV_WARNING_BYTES = 1_000_000_000

INPUT_FILE_CANDIDATES: dict[str, list[str]] = {
    "enterprise": ["enterprises.csv", "enterprise.csv"],
    "establishment": ["establishments.csv", "establishment.csv"],
    "address": ["addresses.csv", "address.csv"],
    "activity": ["activities.csv", "activity.csv"],
    "contact": ["contacts.csv", "contact.csv"],
    "denomination": ["denominations.csv", "denomination.csv"],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lead Radar CSV pipeline")
    parser.add_argument("--input", required=True, help="Input map met bron-CSV's")
    parser.add_argument("--output", required=True, help="Output CSV pad")
    parser.add_argument("--country", default="BE", help="Landcode (momenteel enkel BE)")
    parser.add_argument("--city", default="", help="Filter op stad/gemeente")
    parser.add_argument("--query", default="", help="Keyword filter op bedrijfsnaam of sector")
    parser.add_argument("--postcodes", default="", help="Komma-gescheiden lijst postcodes")
    parser.add_argument("--months", type=int, default=18, help="Maximale leeftijd in maanden")
    parser.add_argument("--min-score", type=int, default=40, help="Minimale score voor output")
    parser.add_argument("--limit", type=int, default=200, help="Maximum records in output")
    parser.add_argument(
        "--lite",
        action="store_true",
        help="Lite mode: skip activities.csv/NACE verwerking en bouw leads op basis van identiteit + contact",
    )
    parser.add_argument("--verbose", action="store_true", help="Toon detectie-info over inputbestanden")
    parser.add_argument("--fast", action="store_true", help="Gebruik snelle postcode-pipeline met pandas chunking")
    parser.add_argument("--chunksize", type=int, default=200_000, help="Chunkgrootte voor --fast pandas reads")
    parser.add_argument(
        "--debug-stats",
        action="store_true",
        help="Print debug statistieken over output (start_date bereik, aantallen en sample ondernemingsnummers)",
    )
    parser.add_argument("--input-drive-zip", default="", help="Google Drive link naar ZIP met bron-CSV's")
    parser.add_argument(
        "--download-dir",
        default="data/downloads",
        help="Lokale map voor gedownloade en uitgepakte bestanden",
    )
    parser.add_argument(
        "--sheet-url",
        default="",
        help="Google Sheet URL om output naar tabblad te pushen (vereist GOOGLE_SERVICE_ACCOUNT_JSON)",
    )
    parser.add_argument("--sheet-tab", default="Leads", help="Google Sheet tabbladnaam voor upload")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Valideer input en toon preview zonder outputbestand te schrijven",
    )
    return parser.parse_args()


def resolve_input_dir(args: argparse.Namespace) -> Path:
    """Resolve the input directory for the pipeline.

    Returns the path to the extracted Drive ZIP directory when
    ``--input-drive-zip`` is provided and download/extract succeeds.
    Falls back to ``--input`` when Drive handling raises ``OSError`` and the
    local input directory exists.
    """
    if not args.input_drive_zip:
        return Path(args.input)

    download_root = Path(args.download_dir)
    zip_path = download_root / "kbo_dump.zip"
    extracted_dir = download_root / "extracted"

    download_url = build_drive_download_url(args.input_drive_zip)
    try:
        download_file(download_url, zip_path)
        extract_zip_file(zip_path, extracted_dir)
        return extracted_dir
    except OSError as err:
        fallback_input = Path(args.input)
        if fallback_input.exists():
            print(f"WARNING: failed to download/extract Drive ZIP ({err}). Falling back to --input: {fallback_input}")
            return fallback_input
        raise


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
    normalized = value.strip("_")

    aliases = {
        "zipcode": "postal_code",
        "municipalitynl": "city",
        "municipalityfr": "city_fr",
        "streetnl": "street",
        "housenumber": "house_number",
    }
    return aliases.get(normalized, normalized)


def normalize_row_keys(row: dict[str, str]) -> dict[str, str]:
    return {normalize_key(key): value for key, value in row.items()}


def iter_csv_rows_normalized(
    path: Path,
    *,
    encoding: str = "utf-8-sig",
    max_bad_lines: int = 1000,
) -> Iterator[dict[str, str]]:
    for row in iter_csv_rows(path, encoding=encoding, max_bad_lines=max_bad_lines):
        yield normalize_row_keys(row)


def normalize_identifier(value: str) -> str:
    return re.sub(r"\D", "", str(value or "").strip().strip('"').strip("'"))


def normalize_id(value: str | None) -> str:
    """Normalize enterprise/establishment identifiers to digits-only format."""
    return normalize_identifier(value or "")


def _first_non_empty(row: dict[str, str], candidates: list[str]) -> str:
    for key in candidates:
        value = str(row.get(key, "") or "").strip()
        if value:
            return value
    return ""


def _find_by_keywords(row: dict[str, str], keywords: list[str]) -> str:
    for key, value in row.items():
        lowered = key.lower()
        if any(keyword.lower() in lowered for keyword in keywords):
            cleaned = str(value or "").strip()
            if cleaned:
                return cleaned
    return ""


def _build_address(establishment: dict[str, str]) -> tuple[str, str, str]:
    street = _first_non_empty(
        establishment,
        [
            "street",
            "street_nl",
            "street_fr",
            "street_de",
            "street_name",
        ],
    ) or _find_by_keywords(establishment, ["street"])
    house_number = _first_non_empty(establishment, ["house_number", "housenumber", "number"]) or _find_by_keywords(
        establishment,
        ["house"],
    )
    box = _first_non_empty(establishment, ["box", "bus", "box_number"])

    postal_code = _first_non_empty(
        establishment,
        ["postal_code", "postcode", "post_code", "zip_code", "zip"],
    ) or _find_by_keywords(establishment, ["postcode", "postalcode", "post_code"])

    city = _first_non_empty(
        establishment,
        ["city", "municipality", "municipality_nl", "municipality_fr", "municipality_de", "commune"],
    ) or _find_by_keywords(establishment, ["municipality", "city"])

    if not street:
        legacy_address = _first_non_empty(establishment, ["address", "full_address"])
        if legacy_address:
            return legacy_address, postal_code, city

    address_parts = [street, house_number]
    address = " ".join(part for part in address_parts if part).strip()
    if box:
        address = f"{address} box {box}".strip() if address else box
    return address, postal_code, city


def _map_enterprise_row(raw_row: dict[str, str]) -> dict[str, str]:
    row = raw_row
    enterprise_number = normalize_id(_first_non_empty(row, ["enterprise_number", "enterprisenumber", "entity_number"]))
    name = _first_non_empty(
        row,
        [
            "name",
            "denomination",
            "denomination_nl",
            "denomination_fr",
            "legal_name",
            "tradename",
        ],
    )
    status = _first_non_empty(row, ["status", "enterprise_status"])
    start_date = _first_non_empty(row, ["start_date", "startdate", "creation_date"])
    postal_code = _first_non_empty(row, ["postal_code", "postcode", "post_code"])
    city = _first_non_empty(row, ["city", "municipality", "municipality_nl", "municipality_fr"])
    address = _first_non_empty(row, ["address", "street", "street_name"])
    website = _first_non_empty(row, ["website", "web", "url"])

    return {
        "enterprise_number": enterprise_number,
        "name": name,
        "status": status,
        "start_date": start_date,
        "postal_code": postal_code,
        "city": city,
        "address": address,
        "website": website,
    }


def _map_establishment_row(raw_row: dict[str, str]) -> dict[str, str]:
    row = raw_row
    enterprise_number = normalize_id(_first_non_empty(row, ["enterprise_number", "enterprisenumber", "entity_number"]))
    establishment_number = normalize_id(
        _first_non_empty(row, ["establishment_number", "establishmentnumber", "entity_number"])
    )
    address, postal_code, city = _build_address(row)

    if not address:
        address = _first_non_empty(row, ["address", "full_address"])

    return {
        "enterprise_number": enterprise_number,
        "establishment_number": establishment_number,
        "address": address,
        "postal_code": postal_code,
        "city": city,
    }


def _load_enterprises(
    input_dir: Path,
    *,
    encoding: str = "utf-8-sig",
    max_bad_lines: int = 1000,
) -> list[dict[str, str]]:
    enterprises_file = find_input_file(input_dir, INPUT_FILE_CANDIDATES["enterprise"])
    return [
        _map_enterprise_row(row)
        for row in iter_csv_rows_normalized(enterprises_file, encoding=encoding, max_bad_lines=max_bad_lines)
    ]


def _load_establishments(
    input_dir: Path,
    *,
    encoding: str = "utf-8-sig",
    max_bad_lines: int = 1000,
) -> list[dict[str, str]]:
    establishments_file = find_input_file(input_dir, INPUT_FILE_CANDIDATES["establishment"])
    return [
        _map_establishment_row(row)
        for row in iter_csv_rows_normalized(establishments_file, encoding=encoding, max_bad_lines=max_bad_lines)
    ]


def load_addresses_by_establishment(
    input_dir: Path,
    *,
    encoding: str = "utf-8-sig",
    max_bad_lines: int = 1000,
) -> dict[str, dict[str, str]]:
    try:
        address_file = find_input_file(input_dir, INPUT_FILE_CANDIDATES["address"])
    except FileNotFoundError:
        return {}

    addresses_by_establishment: dict[str, dict[str, str]] = {}
    for row in iter_csv_rows(address_file, encoding=encoding, max_bad_lines=max_bad_lines):
        normalized_row = normalize_row_keys(row)
        establishment_number = normalize_id(
            _first_non_empty(
                normalized_row,
                [
                    "establishment_number",
                    "establishmentnumber",
                    "entitynumber",
                    "entity_number",
                ],
            )
            or _find_by_keywords(normalized_row, ["establishment"])
        )
        if not establishment_number:
            continue

        address, postal_code, city = _build_address(normalized_row)
        if not address and not postal_code:
            continue

        addresses_by_establishment[establishment_number] = {
            "address": address,
            "postal_code": postal_code,
            "city": city,
        }

    return addresses_by_establishment


def _debug_postcode_diagnostics(postcode_samples: list[dict[str, Any]], *, verbose: bool) -> None:
    if not verbose:
        return

    if not postcode_samples:
        print("Verbose postcode diagnostics: no records available before postcode filter")
        return

    empty_count = sum(1 for item in postcode_samples if not item["computed_postcode"])
    non_empty_count = len(postcode_samples) - empty_count
    postcode_counts: dict[str, int] = {}
    for item in postcode_samples:
        postcode = item["computed_postcode"]
        if postcode:
            postcode_counts[postcode] = postcode_counts.get(postcode, 0) + 1

    top_postcodes = sorted(postcode_counts.items(), key=lambda pair: pair[1], reverse=True)[:10]
    sample_preview = postcode_samples[:3]

    print(
        "Verbose postcode diagnostics: "
        f"total={len(postcode_samples)}, empty={empty_count}, non_empty={non_empty_count}, top10={top_postcodes}"
    )
    print(f"Verbose postcode diagnostics sample (first {len(sample_preview)}): {sample_preview}")


def normalize_status(value: str) -> str:
    mapping = {"AC": "ACTIVE", "IN": "INACTIVE"}
    cleaned = str(value or "").strip()
    return mapping.get(cleaned.upper(), cleaned)


def is_active_status(value: str) -> bool:
    cleaned = str(value or "").strip().upper()
    return cleaned == "AC" or normalize_status(cleaned) == "ACTIVE"


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
    encoding: str = "utf-8-sig",
    max_bad_lines: int = 1000,
) -> dict[str, dict[str, str]]:
    try:
        contacts_file = find_input_file(input_dir, INPUT_FILE_CANDIDATES["contact"])
    except FileNotFoundError:
        return {}

    establishment_to_enterprise: dict[str, str] = {}
    for row in establishments:
        establishment_number = normalize_id(row.get("establishment_number") or "")
        enterprise_number = normalize_id(row.get("enterprise_number") or "")
        if establishment_number and enterprise_number:
            establishment_to_enterprise[establishment_number] = enterprise_number

    contacts_by_enterprise: dict[str, dict[str, str]] = {}
    for raw_row in iter_csv_rows(contacts_file, encoding=encoding, max_bad_lines=max_bad_lines):
        row = normalize_row_keys(raw_row)

        entity_number = normalize_id(
            row.get("entitynumber")
            or row.get("entity_number")
            or row.get("enterprise_number")
            or row.get("establishment_number")
            or ""
        )
        entity_contact = (row.get("entitycontact") or row.get("entity_contact") or "").strip().upper()
        contact_type = (row.get("contacttype") or row.get("contact_type") or "").strip().upper()

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

        if contact_type in {"TEL", "EMAIL", "WEB", "FAX"}:
            contact_value = (row.get("value") or "").strip()
            if contact_type == "TEL" and contact_value and not existing["phone"]:
                existing["phone"] = contact_value
            if contact_type == "EMAIL" and contact_value and not existing["email"]:
                existing["email"] = contact_value
            if contact_type == "WEB" and contact_value and not existing["website"]:
                existing["website"] = contact_value
                existing["has_website"] = "yes"
        else:
            phone_value = (row.get("phone") or "").strip()
            email_value = (row.get("email") or "").strip()
            website_value = (row.get("website") or row.get("web") or "").strip()
            if phone_value and not existing["phone"]:
                existing["phone"] = phone_value
            if email_value and not existing["email"]:
                existing["email"] = email_value
            if website_value and not existing["website"]:
                existing["website"] = website_value
                existing["has_website"] = "yes"

        contacts_by_enterprise[enterprise_number] = existing

    return contacts_by_enterprise


def load_denominations_by_enterprise(
    input_dir: Path,
    *,
    encoding: str = "utf-8-sig",
    max_bad_lines: int = 1000,
) -> dict[str, str]:
    try:
        denomination_file = find_input_file(input_dir, INPUT_FILE_CANDIDATES["denomination"])
    except FileNotFoundError:
        return {}

    # Prefer legal denominations (001). For ties, prefer Dutch/French/English and first seen.
    language_priority = {
        "nl": 0,
        "n": 0,
        "fr": 1,
        "f": 1,
        "en": 2,
        "e": 2,
        "de": 3,
        "d": 3,
    }
    default_language_rank = 4
    default_type_rank = 2
    type_priority = {"001": 0, "1": 0, "002": 1, "2": 1}

    selected: dict[str, tuple[int, int, int, str]] = {}
    for index, raw_row in enumerate(iter_csv_rows(denomination_file, encoding=encoding, max_bad_lines=max_bad_lines)):
        row = normalize_row_keys(raw_row)

        enterprise_number = normalize_id(
            row.get("entity_number") or row.get("enterprise_number") or row.get("entitynumber") or ""
        )
        denomination = (row.get("denomination") or row.get("name") or "").strip()
        if not enterprise_number or not denomination:
            continue

        denomination_type = (row.get("type_of_denomination") or row.get("typeofdenomination") or "").strip()
        language = (row.get("language") or row.get("language_code") or row.get("lang") or "").strip().lower()

        ranking = (
            type_priority.get(denomination_type, default_type_rank),
            language_priority.get(language, default_language_rank),
            index,
            denomination,
        )

        previous = selected.get(enterprise_number)
        if previous is None or ranking < previous:
            selected[enterprise_number] = ranking

    return {enterprise_number: ranked[3] for enterprise_number, ranked in selected.items()}


def months_since(start_date: str) -> int | None:
    started = parse_date(start_date)
    if started is None:
        return None
    today = date.today()
    return (today.year - started.year) * 12 + (today.month - started.month)


def parse_date(date_str: str | None) -> date | None:
    if date_str is None:
        return None

    cleaned = str(date_str).strip()
    if not cleaned or cleaned in {"0", "0000-00-00", "00-00-0000", "0000/00/00"}:
        return None

    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue
    return None


def parse_postcodes(raw: str) -> set[str]:
    parsed = {
        normalized
        for item in raw.split(",")
        if (normalized := normalize_postal_code(item))
    }
    if parsed:
        return parsed
    return set(TARGET_POSTCODES)


def normalize_postal_code(value: str | None) -> str:
    cleaned = str(value or "").strip()
    if not cleaned:
        return ""

    if re.fullmatch(r"\d{4}", cleaned):
        return cleaned

    match = re.search(r"\b(\d{4})\b", cleaned)
    if match:
        return match.group(1)

    return cleaned


def _get_postcode(row: dict[str, Any]) -> str:
    # try common normalized + raw variants
    value = (
        row.get("postal_code")
        or row.get("postcode")
        or row.get("zipcode")
        or row.get("Zipcode")
        or row.get("postalCode")
    )
    if value is None:
        return ""
    return normalize_postal_code(str(value))


def score_record(
    age_months: int | None,
    sector_bucket: str,
    has_nace: bool,
    has_phone: bool,
    has_email: bool,
    has_website: bool,
    max_months: int,
) -> tuple[int, str]:
    score = 0
    reasons: list[str] = []

    if age_months is not None and age_months <= max_months:
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


def build_records(
    input_dir: Path,
    selected_postcodes: set[str],
    max_months: int,
    *,
    min_score: int = 0,
    limit: int | None = None,
    verbose: bool = False,
    lite: bool = False,
    city: str = "",
    query: str = "",
) -> list[dict[str, Any]]:
    resolved_input_dir = detect_input_dir(input_dir)
    if verbose:
        print(_format_detected_files(resolved_input_dir))

    enterprises = _load_enterprises(resolved_input_dir)
    establishments = _load_establishments(resolved_input_dir)
    addresses_by_establishment = load_addresses_by_establishment(resolved_input_dir)
    for establishment in establishments:
        establishment_number = normalize_id(establishment.get("establishment_number", ""))
        if not establishment_number:
            continue
        address_data = addresses_by_establishment.get(establishment_number)
        if not address_data:
            continue
        establishment["address"] = establishment.get("address") or address_data.get("address", "")
        establishment["postal_code"] = establishment.get("postal_code") or address_data.get("postal_code", "")
        establishment["city"] = establishment.get("city") or address_data.get("city", "")

    contacts_by_enterprise = load_contacts_by_enterprise(resolved_input_dir, establishments)
    denominations_by_enterprise = load_denominations_by_enterprise(resolved_input_dir)

    if verbose:
        print(
            f"Loaded counts: enterprises={len(enterprises)}, "
            f"establishments={len(establishments)}, contacts={len(contacts_by_enterprise)}"
        )

    establishment_by_enterprise: dict[str, dict[str, str]] = {}
    for row in establishments:
        enterprise_number = normalize_id(row.get("enterprise_number", ""))
        if not enterprise_number:
            continue

        existing = establishment_by_enterprise.get(enterprise_number)
        if not existing:
            establishment_by_enterprise[enterprise_number] = row
            continue

        existing_has_postcode = bool(_get_postcode(existing))
        candidate_has_postcode = bool(_get_postcode(row))
        if candidate_has_postcode and not existing_has_postcode:
            establishment_by_enterprise[enterprise_number] = row
            continue

        if candidate_has_postcode == existing_has_postcode:
            existing_has_address = bool((existing.get("address") or "").strip())
            candidate_has_address = bool((row.get("address") or "").strip())
            if candidate_has_address and not existing_has_address:
                establishment_by_enterprise[enterprise_number] = row

    activities_by_enterprise: dict[str, list[str]] = {}
    if not lite:
        activity_file = find_input_file(resolved_input_dir, INPUT_FILE_CANDIDATES["activity"])
        for row in iter_csv_rows(activity_file):
            normalized_row = normalize_row_keys(row)
            enterprise_number = normalize_id(
                normalized_row.get("enterprise_number")
                or normalized_row.get("enterprisenumber")
                or normalized_row.get("entity_number")
                or ""
            )
            nace_code = (normalized_row.get("nace_code") or "").strip()
            if enterprise_number and nace_code:
                activities_by_enterprise.setdefault(enterprise_number, []).append(nace_code)

    source_version = resolved_input_dir.name
    records: list[dict[str, Any]] = []
    active_enterprises_kept = 0
    join_with_establishment_kept = 0
    join_with_contact_kept = 0
    postcode_filter_kept = 0
    postcode_samples: list[dict[str, Any]] = []

    for enterprise in enterprises:
        if not is_active_status(enterprise.get("status", "")):
            continue
        active_enterprises_kept += 1

        enterprise_number = normalize_id(enterprise.get("enterprise_number", ""))
        est = establishment_by_enterprise.get(enterprise_number, {})
        if est:
            join_with_establishment_kept += 1
        contact = contacts_by_enterprise.get(
            enterprise_number,
            {"phone": "", "email": "", "website": "", "has_website": "no"},
        )
        if contacts_by_enterprise.get(enterprise_number):
            join_with_contact_kept += 1

        est_postal_code = _get_postcode(est)
        enterprise_postal_code = _get_postcode(enterprise)
        postal_code = est_postal_code or enterprise_postal_code

        if verbose:
            postcode_samples.append(
                {
                    "enterprise_number": enterprise_number,
                    "establishment_number": normalize_id(est.get("establishment_number", "")),
                    "computed_postcode": postal_code,
                    "est_postal_code": est_postal_code,
                    "enterprise_postal_code": enterprise_postal_code,
                    "est_keys": sorted(est.keys())[:12],
                }
            )
        start_date = enterprise.get("start_date", "").strip()
        if not start_date:
            continue

        age_months = months_since(start_date)
        in_postcode_set = not selected_postcodes or postal_code in selected_postcodes
        if selected_postcodes and not in_postcode_set:
            continue
        postcode_filter_kept += 1
        if age_months is None or age_months > max_months:
            continue

        nace_codes = activities_by_enterprise.get(enterprise_number, []) if not lite else []
        first_nace_code = nace_codes[0] if nace_codes else None
        sector_bucket = bucket_from_nace(first_nace_code) if not lite else ""
        website = contact["website"] or (enterprise.get("website") or "").strip()
        has_website = bool(website)
        phone = contact["phone"]
        email = contact["email"]

        if lite:
            score_total = 0
            lite_reasons = ["lite_mode"]
            if phone:
                lite_reasons.append("has_phone")
            if email:
                lite_reasons.append("has_email")
            if has_website:
                lite_reasons.append("has_website")
            score_reasons = "|".join(lite_reasons)
        else:
            score_total, score_reasons = score_record(
                age_months=age_months,
                sector_bucket=sector_bucket,
                has_nace=bool(nace_codes),
                has_phone=bool(phone),
                has_email=bool(email),
                has_website=has_website,
                max_months=max_months,
            )

        enterprise_name = (enterprise.get("name") or "").strip() or denominations_by_enterprise.get(
            enterprise_number, ""
        )

        record = {
            "enterprise_number": enterprise_number,
            "name": enterprise_name,
            "status": normalize_status(enterprise.get("status", "")),
            "start_date": start_date,
            "address": (est.get("address") or enterprise.get("address") or "").strip(),
            "postal_code": postal_code,
            "city": (est.get("city") or enterprise.get("city") or "").strip(),
            "nace_codes": ",".join(nace_codes) if not lite else "",
            "sector_bucket": sector_bucket,
            "has_website": "yes" if has_website else "no",
            "website": website,
            "phone": phone,
            "email": email,
            "score_total": score_total,
            "score_reasons": score_reasons,
            "source_files_version": source_version,
        }
        if city and city.lower() not in (record["city"] or "").lower():
            continue
        haystack = f"{record['name']} {record['sector_bucket']}".lower()
        if query and query not in haystack:
            continue
        if int(record["score_total"]) < min_score:
            continue
        validate_record(record)
        records.append(record)
        if limit is not None and limit > 0 and len(records) >= limit:
            break

    if verbose and enterprises:
        print(f"Verbose counters: enterprises loaded={len(enterprises)}")
        print(f"Verbose counters: enterprises kept after active-filter={active_enterprises_kept}")
        print(f"Verbose counters: after join with establishment={join_with_establishment_kept}")
        print(f"Verbose counters: after join with contact={join_with_contact_kept}")
        print(f"Verbose counters: after postcode filter={postcode_filter_kept}")
        _debug_postcode_diagnostics(postcode_samples, verbose=verbose)

        with_establishment = sum(
            1
            for enterprise in enterprises
            if establishment_by_enterprise.get(enterprise["enterprise_number"])
        )
        with_contact = sum(
            1
            for enterprise in enterprises
            if contacts_by_enterprise.get(enterprise["enterprise_number"])
        )
        establishment_ratio = (with_establishment / len(enterprises)) * 100
        contact_ratio = (with_contact / len(enterprises)) * 100
        print(
            "Join stats: "
            f"enterprises_with_establishment={with_establishment}/{len(enterprises)} ({establishment_ratio:.1f}%), "
            f"enterprises_with_contact={with_contact}/{len(enterprises)} ({contact_ratio:.1f}%)"
        )
        preview = [
            {
                "enterprise_number": row["enterprise_number"],
                "name": row["name"],
                "postal_code": row["postal_code"],
                "city": row["city"],
                "phone": row["phone"],
                "email": row["email"],
                "website": row["website"],
            }
            for row in records[:3]
        ]
        print(f"Preview (first {len(preview)} records): {preview}")

    return records


def main() -> None:
    args = parse_args()
    try:
        runtime = build_runtime_config(args)
    except ValueError as err:
        raise SystemExit(f"Configuration error: {err}") from err
    input_dir = resolve_input_dir(args)
    selected_postcodes = parse_postcodes(runtime.postcodes)

    min_score = 0 if args.lite else runtime.min_score
    if args.fast:
        from .fast_pipeline import build_records_fast

        records = build_records_fast(
            input_dir=input_dir,
            selected_postcodes=selected_postcodes,
            max_months=runtime.months,
            min_score=min_score,
            limit=runtime.limit,
            verbose=args.verbose,
            lite=args.lite,
            chunksize=args.chunksize,
        )
        if runtime.city or runtime.query:
            lowered_city = runtime.city.lower()
            records = [
                row for row in records
                if (not lowered_city or lowered_city in (row.get("city", "").lower()))
                and (
                    not runtime.query
                    or runtime.query in f"{row.get('name', '')} {row.get('sector_bucket', '')}".lower()
                )
            ]
    else:
        records = build_records(
            input_dir=input_dir,
            selected_postcodes=selected_postcodes,
            max_months=runtime.months,
            min_score=min_score,
            limit=runtime.limit,
            verbose=args.verbose,
            lite=args.lite,
            city=runtime.city,
            query=runtime.query,
        )
    total_records = len(records)

    if args.debug_stats:
        _print_debug_stats(records)

    if runtime.dry_run:
        print(f"Dry run complete: {total_records} records would be written to {runtime.output}")
        return

    export_leads(output_path=runtime.output, records=records, total_records=total_records)

    if args.sheet_url:
        try:
            upload_csv_to_google_sheet(sheet_url=args.sheet_url, csv_path=runtime.output, worksheet_name=args.sheet_tab)
            print(f"Uploaded leads to Google Sheet tab '{args.sheet_tab}'")
        except (RuntimeError, OSError) as err:
            print(f"WARNING: unable to upload to Google Sheet: {err}")


def _print_debug_stats(records: list[dict[str, Any]]) -> None:
    enterprise_numbers = [normalize_id(record.get("enterprise_number", "")) for record in records]
    unique_enterprises = sorted({number for number in enterprise_numbers if number})

    parsed_dates = [parsed for record in records if (parsed := parse_date(record.get("start_date")))]
    min_start = min(parsed_dates).isoformat() if parsed_dates else "n/a"
    max_start = max(parsed_dates).isoformat() if parsed_dates else "n/a"

    print(f"Debug stats: total_records={len(records)}")
    print(f"Debug stats: unique_enterprises={len(unique_enterprises)}")
    print(f"Debug stats: min_start_date={min_start}")
    print(f"Debug stats: max_start_date={max_start}")
    print(f"Debug stats: sample_enterprise_numbers={unique_enterprises[:10]}")


if __name__ == "__main__":
    main()
