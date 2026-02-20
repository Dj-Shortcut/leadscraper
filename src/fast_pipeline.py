"""Fast postcode pipeline using pandas chunked reads."""

from __future__ import annotations

from pathlib import Path
from time import perf_counter
from typing import Any, Iterator

from .cli import (
    INPUT_FILE_CANDIDATES,
    _debug_postcode_diagnostics,
    _get_postcode,
    _map_enterprise_row,
    _map_establishment_row,
    build_records,
    detect_input_dir,
    find_input_file,
    is_active_status,
    load_contacts_by_enterprise,
    load_denominations_by_enterprise,
    months_since,
    normalize_id,
    normalize_key,
    normalize_postal_code,
    normalize_status,
    score_record,
)
from .transform import bucket_from_nace
from .validate import validate_record

ADDRESS_USECOLS = [
    "EntityNumber",
    "entity_number",
    "entitynumber",
    "EstablishmentNumber",
    "establishment_number",
    "establishmentnumber",
    "Zipcode",
    "postal_code",
    "postcode",
    "post_code",
    "zip_code",
    "zip",
    "MunicipalityNL",
    "MunicipalityFR",
    "city",
    "street",
    "street_nl",
    "street_fr",
    "StreetNL",
    "StreetFR",
    "HouseNumber",
    "house_number",
    "housenumber",
    "number",
    "box",
    "bus",
    "box_number",
    "address",
    "full_address",
]

ESTABLISHMENT_USECOLS = [
    "EnterpriseNumber",
    "enterprise_number",
    "enterprisenumber",
    "entity_number",
    "EstablishmentNumber",
    "establishment_number",
    "establishmentnumber",
    "EntityNumber",
    "street",
    "street_nl",
    "street_fr",
    "street_de",
    "street_name",
    "StreetNL",
    "StreetFR",
    "HouseNumber",
    "house_number",
    "housenumber",
    "number",
    "box",
    "bus",
    "box_number",
    "Zipcode",
    "postal_code",
    "postcode",
    "post_code",
    "zip_code",
    "zip",
    "city",
    "MunicipalityNL",
    "MunicipalityFR",
    "address",
    "full_address",
]

ENTERPRISE_USECOLS = [
    "EnterpriseNumber",
    "enterprise_number",
    "enterprisenumber",
    "entity_number",
    "name",
    "Denomination",
    "denomination",
    "denomination_nl",
    "denomination_fr",
    "legal_name",
    "tradename",
    "status",
    "enterprise_status",
    "StartDate",
    "start_date",
    "startdate",
    "creation_date",
    "postal_code",
    "postcode",
    "post_code",
    "city",
    "municipality",
    "municipality_nl",
    "municipality_fr",
    "address",
    "street",
    "street_name",
    "website",
    "web",
    "url",
]

ACTIVITY_USECOLS = [
    "EnterpriseNumber",
    "enterprise_number",
    "enterprisenumber",
    "entity_number",
    "nace_code",
    "nace",
    "activity_code",
]


def _import_pandas():
    try:
        import pandas as pd

        return pd
    except ModuleNotFoundError as exc:
        raise RuntimeError("--fast vereist pandas. Installeer pandas om de fast pipeline te gebruiken.") from exc


def _iter_csv_chunks(path: Path, *, chunksize: int, usecols: list[str] | None = None):
    pd = _import_pandas()
    from .cli import detect_delimiter

    delimiter = detect_delimiter(path)
    for encoding in ("utf-8-sig", "latin-1"):
        try:
            yield from pd.read_csv(
                path,
                sep=delimiter,
                dtype=str,
                chunksize=chunksize,
                keep_default_na=False,
                encoding=encoding,
                usecols=usecols,
            )
            return
        except UnicodeDecodeError:
            continue
        except ValueError:
            yield from pd.read_csv(
                path,
                sep=delimiter,
                dtype=str,
                chunksize=chunksize,
                keep_default_na=False,
                encoding=encoding,
            )
            return


def _normalize_chunk_columns(chunk):
    return chunk.rename(columns={column: normalize_key(str(column)) for column in chunk.columns})


def _first_present_column(chunk, candidates: list[str]) -> str | None:
    for candidate in candidates:
        if candidate in chunk.columns:
            return candidate
    return None


def _scan_addresses_for_postcodes(
    addresses_file: Path,
    postcodes_set: set[str],
    chunksize: int,
) -> tuple[set[str], dict[str, dict[str, str]], int]:
    establishment_ids: set[str] = set()
    addresses_by_establishment: dict[str, dict[str, str]] = {}
    scanned_rows = 0

    for chunk in _iter_csv_chunks(addresses_file, chunksize=chunksize, usecols=ADDRESS_USECOLS):
        chunk = _normalize_chunk_columns(chunk)
        scanned_rows += len(chunk)

        postcode_col = _first_present_column(chunk, ["postal_code", "postcode", "post_code", "zip_code", "zip"])
        establishment_col = _first_present_column(
            chunk,
            ["establishment_number", "establishmentnumber", "entity_number", "entitynumber"],
        )
        if not postcode_col or not establishment_col:
            continue

        normalized_postcodes = chunk[postcode_col].map(normalize_postal_code)
        filtered = chunk[normalized_postcodes.isin(postcodes_set)]
        if filtered.empty:
            continue

        for row in filtered.to_dict(orient="records"):
            establishment_number = normalize_id(row.get(establishment_col) or "")
            if not establishment_number:
                continue
            mapped = _map_establishment_row(row)
            establishment_ids.add(establishment_number)
            addresses_by_establishment[establishment_number] = {
                "address": mapped.get("address", ""),
                "postal_code": mapped.get("postal_code", ""),
                "city": mapped.get("city", ""),
            }

    return establishment_ids, addresses_by_establishment, scanned_rows


def _scan_establishments(
    establishments_file: Path,
    establishment_ids: set[str],
    chunksize: int,
) -> tuple[set[str], list[dict[str, str]], int]:
    enterprise_ids: set[str] = set()
    establishments_subset: list[dict[str, str]] = []
    scanned_rows = 0

    if not establishment_ids:
        return enterprise_ids, establishments_subset, scanned_rows

    for chunk in _iter_csv_chunks(establishments_file, chunksize=chunksize, usecols=ESTABLISHMENT_USECOLS):
        chunk = _normalize_chunk_columns(chunk)
        scanned_rows += len(chunk)

        establishment_col = _first_present_column(
            chunk,
            ["establishment_number", "establishmentnumber", "entity_number"],
        )
        if not establishment_col:
            continue

        normalized_establishments = chunk[establishment_col].map(normalize_id)
        filtered = chunk[normalized_establishments.isin(establishment_ids)]
        if filtered.empty:
            continue

        for row in filtered.to_dict(orient="records"):
            mapped = _map_establishment_row(row)
            enterprise_number = normalize_id(mapped.get("enterprise_number") or "")
            if not enterprise_number:
                continue
            enterprise_ids.add(enterprise_number)
            establishments_subset.append(mapped)

    return enterprise_ids, establishments_subset, scanned_rows


def iter_enterprises_filtered(
    enterprises_file: Path,
    enterprise_ids_set: set[str],
    chunksize: int,
) -> Iterator[dict[str, str]]:
    for chunk in _iter_csv_chunks(enterprises_file, chunksize=chunksize, usecols=ENTERPRISE_USECOLS):
        chunk = _normalize_chunk_columns(chunk)
        enterprise_col = _first_present_column(chunk, ["enterprise_number", "enterprisenumber", "entity_number"])
        if not enterprise_col:
            continue

        normalized_ids = chunk[enterprise_col].map(normalize_id)
        filtered = chunk[normalized_ids.isin(enterprise_ids_set)] if enterprise_ids_set else chunk
        for row in filtered.to_dict(orient="records"):
            yield _map_enterprise_row(row)


def _load_activities_for_enterprises(
    activity_file: Path,
    enterprise_ids_set: set[str],
    chunksize: int,
) -> dict[str, list[str]]:
    activities_by_enterprise: dict[str, list[str]] = {}
    if not enterprise_ids_set:
        return activities_by_enterprise

    for chunk in _iter_csv_chunks(activity_file, chunksize=chunksize, usecols=ACTIVITY_USECOLS):
        chunk = _normalize_chunk_columns(chunk)
        enterprise_col = _first_present_column(chunk, ["enterprise_number", "enterprisenumber", "entity_number"])
        nace_col = _first_present_column(chunk, ["nace_code", "nace", "activity_code"])
        if not enterprise_col or not nace_col:
            continue

        normalized_ids = chunk[enterprise_col].map(normalize_id)
        filtered = chunk[normalized_ids.isin(enterprise_ids_set)]
        if filtered.empty:
            continue

        for row in filtered.to_dict(orient="records"):
            enterprise_number = normalize_id(row.get(enterprise_col) or "")
            nace_code = str(row.get(nace_col) or "").strip()
            if enterprise_number and nace_code:
                activities_by_enterprise.setdefault(enterprise_number, []).append(nace_code)

    return activities_by_enterprise


def build_records_fast(
    input_dir: Path,
    selected_postcodes: set[str],
    max_months: int,
    *,
    min_score: int = 0,
    limit: int | None = None,
    verbose: bool = False,
    lite: bool = False,
    chunksize: int = 200_000,
) -> list[dict[str, Any]]:
    if not selected_postcodes:
        return build_records(
            input_dir=input_dir,
            selected_postcodes=selected_postcodes,
            max_months=max_months,
            min_score=min_score,
            limit=limit,
            verbose=verbose,
            lite=lite,
        )

    resolved_input_dir = detect_input_dir(input_dir)
    if verbose:
        from .cli import _format_detected_files

        print(_format_detected_files(resolved_input_dir))

    addresses_file = find_input_file(resolved_input_dir, INPUT_FILE_CANDIDATES["address"])
    establishments_file = find_input_file(resolved_input_dir, INPUT_FILE_CANDIDATES["establishment"])
    enterprises_file = find_input_file(resolved_input_dir, INPUT_FILE_CANDIDATES["enterprise"])

    t0 = perf_counter()
    establishment_ids, addresses_by_establishment, address_rows_scanned = _scan_addresses_for_postcodes(
        addresses_file,
        selected_postcodes,
        chunksize,
    )
    t1 = perf_counter()

    enterprise_ids, establishments_subset, establishment_rows_scanned = _scan_establishments(
        establishments_file,
        establishment_ids,
        chunksize,
    )
    t2 = perf_counter()

    for establishment in establishments_subset:
        establishment_number = normalize_id(establishment.get("establishment_number", ""))
        if not establishment_number:
            continue
        address_data = addresses_by_establishment.get(establishment_number)
        if not address_data:
            continue
        establishment["address"] = establishment.get("address") or address_data.get("address", "")
        establishment["postal_code"] = establishment.get("postal_code") or address_data.get("postal_code", "")
        establishment["city"] = establishment.get("city") or address_data.get("city", "")

    contacts_by_enterprise = load_contacts_by_enterprise(resolved_input_dir, establishments_subset)
    denominations_by_enterprise = load_denominations_by_enterprise(resolved_input_dir)

    establishment_by_enterprise: dict[str, dict[str, str]] = {}
    for row in establishments_subset:
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
    t3 = perf_counter()
    if not lite:
        activity_file = find_input_file(resolved_input_dir, INPUT_FILE_CANDIDATES["activity"])
        activities_by_enterprise = _load_activities_for_enterprises(activity_file, enterprise_ids, chunksize)
    t4 = perf_counter()

    source_version = resolved_input_dir.name
    records: list[dict[str, Any]] = []
    postcode_samples: list[dict[str, Any]] = []
    enterprises_processed = 0
    active_enterprises_kept = 0

    for enterprise in iter_enterprises_filtered(enterprises_file, enterprise_ids, chunksize):
        enterprises_processed += 1
        if not is_active_status(enterprise.get("status", "")):
            continue
        active_enterprises_kept += 1

        enterprise_number = normalize_id(enterprise.get("enterprise_number", ""))
        est = establishment_by_enterprise.get(enterprise_number, {})
        contact = contacts_by_enterprise.get(
            enterprise_number,
            {"phone": "", "email": "", "website": "", "has_website": "no"},
        )

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
        if selected_postcodes and postal_code not in selected_postcodes:
            continue
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
        if int(record["score_total"]) < min_score:
            continue
        validate_record(record)
        records.append(record)
        if limit is not None and limit > 0 and len(records) >= limit:
            break

    t5 = perf_counter()
    if verbose:
        print(f"Fast counters: address rows scanned={address_rows_scanned}")
        print(f"Fast counters: establishment rows scanned={establishment_rows_scanned}")
        print(f"Fast counters: establishments in area={len(establishment_ids)}")
        print(f"Fast counters: enterprise ids in area={len(enterprise_ids)}")
        print(f"Fast counters: enterprises scanned in subset={enterprises_processed}")
        print(f"Fast counters: enterprises kept after active-filter={active_enterprises_kept}")
        print(f"Fast counters: records output={len(records)}")
        print(f"Fast timing: addresses scan once={(t1 - t0):.2f}s")
        print(f"Fast timing: establishments scan once={(t2 - t1):.2f}s")
        print(f"Fast timing: enterprises scan once={(t5 - t2):.2f}s")
        if not lite:
            print(f"Fast timing: activities scan once={(t4 - t3):.2f}s")
        _debug_postcode_diagnostics(postcode_samples, verbose=verbose)

    return records
