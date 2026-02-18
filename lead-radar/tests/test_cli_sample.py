from pathlib import Path

import pytest

from src.cli import build_records, find_input_file
from src.config import TARGET_POSTCODES
from src.export import export_leads


ALLOWED_BUCKETS = {"beauty", "horeca", "health", "retail", "service_trades", "other"}


def test_pipeline_runs_on_sample_and_writes_output(tmp_path: Path) -> None:
    records = build_records(Path("data/sample"), selected_postcodes=set(TARGET_POSTCODES), max_months=18)
    assert records
    assert all(record["postal_code"] in TARGET_POSTCODES for record in records)
    assert all(record["sector_bucket"] in ALLOWED_BUCKETS for record in records)

    output_path = tmp_path / "sample_leads.csv"
    written = export_leads(output_path=output_path, records=records, total_records=len(records))

    assert written == len(records)
    assert written > 0
    assert output_path.exists()
    content = output_path.read_text(encoding="utf-8")
    assert "enterprise_number" in content


def test_read_csv_supports_comma_delimiter(tmp_path: Path) -> None:
    csv_path = tmp_path / "comma.csv"
    csv_path.write_text("enterprise_number,name\n1,Acme\n", encoding="utf-8")

    from src.cli import read_csv

    rows = read_csv(csv_path)
    assert rows == [{"enterprise_number": "1", "name": "Acme"}]


def test_find_input_file_accepts_singular_and_plural_names(tmp_path: Path) -> None:
    singular = tmp_path / "enterprise.csv"
    singular.write_text("enterprise_number;name\n1;Acme\n", encoding="utf-8")

    found = find_input_file(tmp_path, ["enterprises.csv", "enterprise.csv"])
    assert found == singular


def test_find_input_file_error_lists_expected_and_found(tmp_path: Path) -> None:
    (tmp_path / "unexpected.csv").write_text("id\n1\n", encoding="utf-8")

    with pytest.raises(FileNotFoundError) as exc:
        find_input_file(tmp_path, ["activities.csv", "activity.csv"])

    message = str(exc.value)
    assert "activities.csv" in message
    assert "activity.csv" in message
    assert "unexpected.csv" in message


def test_build_records_detects_single_subfolder_with_csv_files(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    input_root = tmp_path / "raw"
    detected = input_root / "2026-02-18"
    detected.mkdir(parents=True)

    (detected / "enterprise.csv").write_text(
        "enterprise_number;name;status;start_date;postal_code;city;website\n"
        "0123456789;Acme;ACTIVE;2026-01-01;9400;Ninove;https://acme.example\n",
        encoding="utf-8",
    )
    (detected / "establishment.csv").write_text(
        "enterprise_number;address;postal_code;city\n0123456789;Main street 1;9400;Ninove\n",
        encoding="utf-8",
    )
    (detected / "activity.csv").write_text(
        "enterprise_number;nace_code\n0123456789;96021\n",
        encoding="utf-8",
    )

    records = build_records(input_root, selected_postcodes={"9400"}, max_months=18)

    captured = capsys.readouterr()
    assert "Detected subfolder" in captured.out
    assert len(records) == 1
    assert records[0]["source_files_version"] == "2026-02-18"
