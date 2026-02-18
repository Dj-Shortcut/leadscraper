from pathlib import Path
from typing import Any

import pytest

from src import cli
from src.cli import build_records, find_input_file, iter_csv_rows
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
    assert "phone" in content
    assert "email" in content


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


def test_find_input_file_accepts_doubled_csv_extension(tmp_path: Path) -> None:
    doubled = tmp_path / "enterprise.csv.csv"
    doubled.write_text("enterprise_number;name\n1;Acme\n", encoding="utf-8")

    found = find_input_file(tmp_path, ["enterprises.csv", "enterprise.csv"])
    assert found == doubled


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
        "enterprise_number;nace_code\n0123456789;96.02\n",
        encoding="utf-8",
    )

    records = build_records(input_root, selected_postcodes={"9400"}, max_months=18)

    captured = capsys.readouterr()
    assert "Detected subfolder" in captured.out
    assert len(records) == 1
    assert records[0]["source_files_version"] == "2026-02-18"


def test_build_records_enriches_phone_and_email_when_contacts_exists(tmp_path: Path) -> None:
    (tmp_path / "enterprises.csv").write_text(
        "enterprise_number;name;status;start_date;postal_code;city\n"
        "0123456789;Acme;ACTIVE;2026-01-01;9400;Ninove\n",
        encoding="utf-8",
    )
    (tmp_path / "establishments.csv").write_text(
        "enterprise_number;establishment_number;address;postal_code;city\n"
        "0123456789;2.987.654.321;Main street 1;9400;Ninove\n",
        encoding="utf-8",
    )
    (tmp_path / "activities.csv").write_text(
        "enterprise_number;nace_code\n0123456789;96.02\n",
        encoding="utf-8",
    )
    (tmp_path / "contact.csv").write_text(
        "establishment_number;phone;email\n"
        "2.987.654.321;+32123456789;hello@acme.example\n",
        encoding="utf-8",
    )

    records = build_records(tmp_path, selected_postcodes={"9400"}, max_months=18)

    assert len(records) == 1
    assert records[0]["phone"] == "+32123456789"
    assert records[0]["email"] == "hello@acme.example"
    assert records[0]["score_total"] == 53
    assert "has_phone" in records[0]["score_reasons"]
    assert "has_email" in records[0]["score_reasons"]


def test_build_records_supports_contacts_with_doubled_csv_extension(tmp_path: Path) -> None:
    (tmp_path / "enterprise.csv").write_text(
        "enterprise_number;name;status;start_date;postal_code;city\n"
        "0123456789;Acme;ACTIVE;2026-01-01;9400;Ninove\n",
        encoding="utf-8",
    )
    (tmp_path / "establishment.csv").write_text(
        "enterprise_number;establishment_number;address;postal_code;city\n"
        "0123456789;2.987.654.321;Main street 1;9400;Ninove\n",
        encoding="utf-8",
    )
    (tmp_path / "activity.csv").write_text(
        "enterprise_number;nace_code\n0123456789;96.02\n",
        encoding="utf-8",
    )
    (tmp_path / "contact.csv.csv").write_text(
        "establishment_number;phone;email\n"
        "2.987.654.321;+32123456789;hello@acme.example\n",
        encoding="utf-8",
    )

    records = build_records(tmp_path, selected_postcodes={"9400"}, max_months=18)

    assert len(records) == 1
    assert records[0]["phone"] == "+32123456789"
    assert records[0]["email"] == "hello@acme.example"


def test_build_records_verbose_prints_detected_files(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    (tmp_path / "enterprises.csv").write_text(
        "enterprise_number;name;status;start_date;postal_code;city\n"
        "0123456789;Acme;ACTIVE;2026-01-01;9400;Ninove\n",
        encoding="utf-8",
    )
    (tmp_path / "establishments.csv").write_text(
        "enterprise_number;address;postal_code;city\n"
        "0123456789;Main street 1;9400;Ninove\n",
        encoding="utf-8",
    )
    (tmp_path / "activities.csv").write_text(
        "enterprise_number;nace_code\n0123456789;96.02\n",
        encoding="utf-8",
    )

    build_records(tmp_path, selected_postcodes={"9400"}, max_months=18, verbose=True)

    captured = capsys.readouterr()
    assert "Detected files:" in captured.out
    assert "activities.csv" in captured.out
    assert "enterprises.csv" in captured.out
    assert "establishments.csv" in captured.out


def test_build_records_without_contacts_file_falls_back_gracefully(tmp_path: Path) -> None:
    (tmp_path / "enterprises.csv").write_text(
        "enterprise_number;name;status;start_date;postal_code;city\n"
        "0123456789;Acme;ACTIVE;2026-01-01;9400;Ninove\n",
        encoding="utf-8",
    )
    (tmp_path / "establishments.csv").write_text(
        "enterprise_number;address;postal_code;city\n"
        "0123456789;Main street 1;9400;Ninove\n",
        encoding="utf-8",
    )
    (tmp_path / "activities.csv").write_text(
        "enterprise_number;nace_code\n0123456789;96.02\n",
        encoding="utf-8",
    )

    records = build_records(tmp_path, selected_postcodes={"9400"}, max_months=18)

    assert len(records) == 1
    assert records[0]["phone"] == ""
    assert records[0]["email"] == ""


def test_iter_csv_rows_falls_back_to_line_by_line_on_stream_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    csv_path = tmp_path / "broken.csv"
    csv_path.write_text(
        "enterprise_number;name\n"
        "1;Acme\n"
        "2;Broken;extra\n"
        "3;Bravo\n",
        encoding="utf-8",
    )

    original_dict_reader = cli.csv.DictReader

    class FailingDictReader:
        def __init__(self, handle: Any, delimiter: str) -> None:
            self._reader = original_dict_reader(handle, delimiter=delimiter)
            self.fieldnames = self._reader.fieldnames
            self._index = 0

        def __iter__(self) -> "FailingDictReader":
            return self

        def __next__(self) -> dict[str, str]:
            if self._index == 0:
                self._index += 1
                return next(self._reader)
            raise OSError("Invalid argument")

    monkeypatch.setattr(cli.csv, "DictReader", FailingDictReader)

    rows = list(iter_csv_rows(csv_path, max_bad_lines=10))
    assert rows == [
        {"enterprise_number": "1", "name": "Acme"},
        {"enterprise_number": "3", "name": "Bravo"},
    ]


def test_iter_csv_rows_stops_when_max_bad_lines_exceeded(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    csv_path = tmp_path / "too_many_bad.csv"
    csv_path.write_text(
        "enterprise_number;name\n"
        "1;Acme\n"
        "2;Broken;extra\n",
        encoding="utf-8",
    )

    original_dict_reader = cli.csv.DictReader

    class FailingDictReader:
        def __init__(self, handle: Any, delimiter: str) -> None:
            self._reader = original_dict_reader(handle, delimiter=delimiter)
            self.fieldnames = self._reader.fieldnames
            self._index = 0

        def __iter__(self) -> "FailingDictReader":
            return self

        def __next__(self) -> dict[str, str]:
            if self._index == 0:
                self._index += 1
                return next(self._reader)
            raise OSError("Invalid argument")

    monkeypatch.setattr(cli.csv, "DictReader", FailingDictReader)

    with pytest.raises(RuntimeError, match="Max bad lines exceeded"):
        list(iter_csv_rows(csv_path, max_bad_lines=0))


def test_iter_csv_rows_falls_back_to_latin_1_encoding(tmp_path: Path) -> None:
    csv_path = tmp_path / "latin1.csv"
    csv_path.write_bytes("enterprise_number;name\n1;caf\xe9\n".encode("latin-1"))

    rows = list(iter_csv_rows(csv_path, encoding="utf-8-sig"))
    assert rows == [{"enterprise_number": "1", "name": "café"}]
