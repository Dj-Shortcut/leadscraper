import csv
from pathlib import Path

import pytest

from src import cli


FIXTURE_INPUT = Path(__file__).parent / "fixtures" / "minimal_kbo"


def test_fixture_smoke_build_records_parses_expected_fields() -> None:
    records = cli.build_records(FIXTURE_INPUT, selected_postcodes={"9400"}, max_months=18)

    assert len(records) == 1
    record = records[0]
    assert record["enterprise_number"] == "0123456789"
    assert record["name"] == "Fixture Salon"
    assert record["sector_bucket"] == "beauty"
    assert record["phone"] == "+32111222333"
    assert record["email"] == "hello@fixture-salon.example"
    assert record["status"] == "ACTIVE"
    assert record["postal_code"] == "9400"
    assert record["city"] == "Ninove"


def test_cli_main_end_to_end_writes_expected_output(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    output_file = tmp_path / "out" / "leads.csv"

    monkeypatch.setattr(
        "sys.argv",
        [
            "cli",
            "--input",
            str(FIXTURE_INPUT),
            "--output",
            str(output_file),
            "--postcodes",
            "9400",
            "--min-score",
            "0",
        ],
    )

    cli.main()

    assert output_file.parent.exists()
    assert output_file.exists()

    with output_file.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert rows
    first = rows[0]
    assert first["enterprise_number"] == "0123456789"
    assert first["sector_bucket"] == "beauty"
    assert first["email"] == "hello@fixture-salon.example"


def test_normalize_key_maps_kbo_aliases() -> None:
    assert cli.normalize_key("Zipcode") == "postal_code"
    assert cli.normalize_key("MunicipalityNL") == "city"
    assert cli.normalize_key("MunicipalityFR") == "city_fr"
    assert cli.normalize_key("StreetNL") == "street"
    assert cli.normalize_key("HouseNumber") == "house_number"
