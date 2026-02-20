"""CSV-export en samenvatting voor Lead Radar."""

from __future__ import annotations

import csv
from collections import Counter
from pathlib import Path
from typing import Iterable, Mapping, Any

OUTPUT_COLUMNS = [
    "enterprise_number",
    "name",
    "status",
    "start_date",
    "address",
    "postal_code",
    "city",
    "nace_codes",
    "sector_bucket",
    "has_website",
    "website",
    "phone",
    "email",
    "score_total",
    "score_reasons",
    "source_files_version",
]


def export_leads(
    output_path: Path,
    records: Iterable[Mapping[str, Any]],
    total_records: int,
) -> int:
    """Schrijf UTF-8 CSV, sorteer op score en print een korte summary."""
    prepared = sorted(records, key=lambda row: int(row.get("score_total", 0)), reverse=True)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        for row in prepared:
            writer.writerow({column: row.get(column, "") for column in OUTPUT_COLUMNS})

    print(f"Aantal records totaal: {total_records}")
    print(f"Aantal na filters: {len(prepared)}")
    print("Top 10 sector buckets:")
    counts = Counter(str(row.get("sector_bucket", "UNKNOWN")) for row in prepared)
    for sector, count in counts.most_common(10):
        print(f"- {sector}: {count}")

    return len(prepared)
