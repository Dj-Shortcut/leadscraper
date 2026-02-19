import zipfile
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
    assert any(record["phone"] for record in records)
    assert any(record["email"] for record in records)

    output_path = tmp_path / "sample_leads.csv"
    written = export_leads(output_path=output_path, records=records, total_records=len(records))

    assert written == len(records)
    assert written > 0
    assert output_path.exists()
    content = output_path.read_text(encoding="utf-8")
    assert "enterprise_number" in content
    assert "phone" in content
    assert "email" in content
    assert "website" in content


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


def test_build_records_enriches_contacts_from_kbo_contact_schema(tmp_path: Path) -> None:
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
        "EntityNumber;EntityContact;ContactType;Value\n"
        "\"2.987.654.321\";EST;TEL;+32123456789\n"
        "\"2.987.654.321\";EST;EMAIL;hello@acme.example\n"
        "\"0200.362.210\";ENT;WEB;https://ignored.example\n"
        "\"2.987.654.321\";EST;FAX;+329999999\n",
        encoding="utf-8",
    )

    records = build_records(tmp_path, selected_postcodes={"9400"}, max_months=18)

    assert len(records) == 1
    assert records[0]["enterprise_number"] == "0123456789"
    assert records[0]["phone"] == "+32123456789"
    assert records[0]["email"] == "hello@acme.example"
    assert records[0]["website"] == ""
    assert records[0]["score_total"] == 53
    assert "has_phone" in records[0]["score_reasons"]
    assert "has_email" in records[0]["score_reasons"]


def test_build_records_maps_establishment_contact_to_enterprise(tmp_path: Path) -> None:
    (tmp_path / "enterprises.csv").write_text(
        "enterprise_number;name;status;start_date;postal_code;city\n"
        "0200362210;Beta;ACTIVE;2026-01-01;9400;Ninove\n",
        encoding="utf-8",
    )
    (tmp_path / "establishments.csv").write_text(
        "enterprise_number;establishment_number;address;postal_code;city\n"
        "0200362210;2.123.456.789;Main street 2;9400;Ninove\n",
        encoding="utf-8",
    )
    (tmp_path / "activities.csv").write_text(
        "enterprise_number;nace_code\n0200362210;56.10\n",
        encoding="utf-8",
    )
    (tmp_path / "contact.csv").write_text(
        "EntityNumber;EntityContact;ContactType;Value\n"
        "\"2.123.456.789\";EST;WEB;https://beta.example\n",
        encoding="utf-8",
    )

    records = build_records(tmp_path, selected_postcodes={"9400"}, max_months=18)

    assert len(records) == 1
    assert records[0]["website"] == "https://beta.example"
    assert records[0]["has_website"] == "yes"
    assert "has_website" in records[0]["score_reasons"]


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


def test_build_records_streams_activity_file_once(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
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

    original_iter_csv_rows = cli.iter_csv_rows
    original_read_csv = cli.read_csv
    activity_iter_calls = 0
    activity_read_calls = 0

    def counting_iter_csv_rows(*args: Any, **kwargs: Any):  # type: ignore[no-untyped-def]
        nonlocal activity_iter_calls
        path = args[0]
        if isinstance(path, Path) and path.name in {"activity.csv", "activities.csv"}:
            activity_iter_calls += 1
        return original_iter_csv_rows(*args, **kwargs)

    def counting_read_csv(*args: Any, **kwargs: Any):  # type: ignore[no-untyped-def]
        nonlocal activity_read_calls
        path = args[0]
        if isinstance(path, Path) and path.name in {"activity.csv", "activities.csv"}:
            activity_read_calls += 1
        return original_read_csv(*args, **kwargs)

    monkeypatch.setattr(cli, "iter_csv_rows", counting_iter_csv_rows)
    monkeypatch.setattr(cli, "read_csv", counting_read_csv)

    records = build_records(tmp_path, selected_postcodes={"9400"}, max_months=18)

    assert len(records) == 1
    assert activity_iter_calls == 1
    assert activity_read_calls == 0


def test_load_addresses_by_establishment_reads_address_file(tmp_path: Path) -> None:
    (tmp_path / "address.csv").write_text(
        "establishment_number;street;house_number;postal_code;city\n"
        "2.123.456.789;Main street;9;9400;Ninove\n",
        encoding="utf-8",
    )

    mapping = cli.load_addresses_by_establishment(tmp_path)

    assert mapping == {
        "2123456789": {
            "address": "Main street 9",
            "postal_code": "9400",
            "city": "Ninove",
        }
    }


def test_build_records_merges_address_data_into_establishment(tmp_path: Path) -> None:
    (tmp_path / "enterprises.csv").write_text(
        "enterprise_number;name;status;start_date;postal_code;city\n"
        "0123456789;Acme;AC;2026-01-01;;\n",
        encoding="utf-8",
    )
    (tmp_path / "establishments.csv").write_text(
        "enterprise_number;establishment_number\n"
        "0123456789;2.123.456.789\n",
        encoding="utf-8",
    )
    (tmp_path / "address.csv").write_text(
        "establishment_number;street;house_number;postal_code;city\n"
        "2.123.456.789;Main street;9;9400;Ninove\n",
        encoding="utf-8",
    )
    (tmp_path / "activities.csv").write_text(
        "enterprise_number;nace_code\n0123456789;96.02\n",
        encoding="utf-8",
    )

    records = build_records(tmp_path, selected_postcodes={"9400"}, max_months=10000)

    assert len(records) == 1
    assert records[0]["postal_code"] == "9400"
    assert records[0]["address"] == "Main street 9"
    assert records[0]["city"] == "Ninove"
    assert records[0]["status"] == "ACTIVE"


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



def test_build_records_lite_mode_without_activities_file(tmp_path: Path) -> None:
    (tmp_path / "enterprises.csv").write_text(
        "enterprise_number;name;status;start_date;postal_code;city\n"
        "0200362210;Beta;ACTIVE;2026-01-01;9400;Ninove\n",
        encoding="utf-8",
    )
    (tmp_path / "establishments.csv").write_text(
        "enterprise_number;establishment_number;address;postal_code;city\n"
        "0200362210;2.123.456.789;Main street 2;9400;Ninove\n",
        encoding="utf-8",
    )
    (tmp_path / "contact.csv").write_text(
        "EntityNumber;EntityContact;ContactType;Value\n"
        "\"2.123.456.789\";EST;TEL;+3211223344\n"
        "\"2.123.456.789\";EST;EMAIL;hello@beta.example\n"
        "\"2.123.456.789\";EST;WEB;https://beta.example\n",
        encoding="utf-8",
    )

    records = build_records(tmp_path, selected_postcodes={"9400"}, max_months=18, lite=True)

    assert len(records) == 1
    assert records[0]["enterprise_number"] == "0200362210"
    assert records[0]["phone"] == "+3211223344"
    assert records[0]["email"] == "hello@beta.example"
    assert records[0]["website"] == "https://beta.example"
    assert records[0]["nace_codes"] == ""
    assert records[0]["sector_bucket"] == ""
    assert records[0]["score_total"] == 0
    assert "lite_mode" in records[0]["score_reasons"]




def test_build_records_limit_applies_after_postcode_and_month_filters(tmp_path: Path) -> None:
    (tmp_path / "enterprises.csv").write_text(
        "enterprise_number;name;status;start_date;postal_code;city\n"
        "0200362201;Old Co;ACTIVE;2000-01-01;9400;Ninove\n"
        "0200362202;Wrong Zip;ACTIVE;2026-01-01;9500;Geraardsbergen\n"
        "0200362203;Fresh Co;ACTIVE;2026-01-01;9400;Ninove\n",
        encoding="utf-8",
    )
    (tmp_path / "establishments.csv").write_text(
        "enterprise_number;address;postal_code;city\n"
        "0200362201;Old street 1;9400;Ninove\n"
        "0200362202;Wrong street 2;9500;Geraardsbergen\n"
        "0200362203;Fresh street 3;9400;Ninove\n",
        encoding="utf-8",
    )
    (tmp_path / "activities.csv").write_text(
        "enterprise_number;nace_code\n"
        "0200362203;96021\n",
        encoding="utf-8",
    )

    records = build_records(
        tmp_path,
        selected_postcodes={"9400"},
        max_months=18,
        min_score=0,
        limit=1,
    )

    assert len(records) == 1
    assert records[0]["enterprise_number"] == "0200362203"
def test_main_lite_mode_sets_min_score_to_zero(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    input_dir = tmp_path / "raw"
    input_dir.mkdir(parents=True)

    (input_dir / "enterprises.csv").write_text(
        "enterprise_number;name;status;start_date;postal_code;city\n"
        "0200362210;Beta;ACTIVE;2026-01-01;9400;Ninove\n",
        encoding="utf-8",
    )
    (input_dir / "establishments.csv").write_text(
        "enterprise_number;address;postal_code;city\n"
        "0200362210;Main street 2;9400;Ninove\n",
        encoding="utf-8",
    )

    output_path = tmp_path / "leads_lite.csv"
    monkeypatch.setattr(
        "sys.argv",
        [
            "cli",
            "--input",
            str(input_dir),
            "--output",
            str(output_path),
            "--lite",
        ],
    )

    cli.main()

    content = output_path.read_text(encoding="utf-8")
    assert "enterprise_number" in content
    assert "0200362210" in content


def test_build_records_maps_kbo_pascal_case_and_dotted_identifiers(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    (tmp_path / "enterprises.csv").write_text(
        "EnterpriseNumber;Status;StartDate;Denomination\n"
        "0123.456.789;ACTIVE;2026-01-01;KBO Alpha\n",
        encoding="utf-8",
    )
    (tmp_path / "establishments.csv").write_text(
        "EnterpriseNumber;EstablishmentNumber;StreetNL;HouseNumber;Box;PostCode;MunicipalityNL\n"
        "0123.456.789;2.123.456.789;Nieuwstraat;10;A;9400;Ninove\n",
        encoding="utf-8",
    )
    (tmp_path / "contact.csv").write_text(
        "EntityNumber;EntityContact;ContactType;Value\n"
        "2.123.456.789;EST;TEL;+321234\n"
        "2.123.456.789;EST;EMAIL;alpha@example.com\n"
        "2.123.456.789;EST;WEB;https://alpha.example\n",
        encoding="utf-8",
    )

    records = build_records(tmp_path, selected_postcodes={"9400"}, max_months=18, lite=True, verbose=True)

    captured = capsys.readouterr()
    assert "Loaded counts:" in captured.out
    assert "enterprises loaded=" in captured.out
    assert "enterprises kept after active-filter=" in captured.out
    assert "after join with establishment=" in captured.out
    assert "after join with contact=" in captured.out
    assert "after postcode filter=" in captured.out
    assert "Join stats:" in captured.out
    assert "Preview (first" in captured.out
    assert len(records) == 1
    assert records[0]["enterprise_number"] == "0123456789"
    assert records[0]["name"] == "KBO Alpha"
    assert records[0]["address"] == "Nieuwstraat 10 box A"
    assert records[0]["postal_code"] == "9400"
    assert records[0]["city"] == "Ninove"
    assert records[0]["phone"] == "+321234"
    assert records[0]["email"] == "alpha@example.com"
    assert records[0]["website"] == "https://alpha.example"


def test_build_records_keeps_ac_and_active_statuses_and_skips_inactive(tmp_path: Path) -> None:
    (tmp_path / "enterprises.csv").write_text(
        "enterprise_number;name;status;start_date;postal_code;city\n"
        "0200362201;Ac Co;AC;2026-01-01;9400;Ninove\n"
        "0200362202;Active Co;ACTIVE;2026-01-01;9400;Ninove\n"
        "0200362203;Inactive Co;IN;2026-01-01;9400;Ninove\n",
        encoding="utf-8",
    )
    (tmp_path / "establishments.csv").write_text(
        "enterprise_number;address;postal_code;city\n"
        "0200362201;Ac street 1;9400;Ninove\n"
        "0200362202;Active street 2;9400;Ninove\n"
        "0200362203;Inactive street 3;9400;Ninove\n",
        encoding="utf-8",
    )
    (tmp_path / "activities.csv").write_text(
        "enterprise_number;nace_code\n"
        "0200362201;96021\n"
        "0200362202;96021\n"
        "0200362203;96021\n",
        encoding="utf-8",
    )

    records = build_records(tmp_path, selected_postcodes={"9400"}, max_months=18, min_score=0)

    assert len(records) == 2
    assert {record["enterprise_number"] for record in records} == {"0200362201", "0200362202"}
    assert all(record["status"] == "ACTIVE" for record in records)


def test_resolve_input_dir_downloads_and_extracts_zip(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    args = cli.argparse.Namespace(
        input=str(tmp_path / "raw"),
        input_drive_zip="https://drive.google.com/file/d/abc123/view?usp=sharing",
        download_dir=str(tmp_path / "downloads"),
    )

    calls: dict[str, str] = {}
    expected_output_dir = tmp_path / "downloads" / "extracted"
    expected_resolved_path = expected_output_dir

    def fake_build(url: str) -> str:
        calls["build"] = url
        return "https://drive.google.com/uc?export=download&id=abc123"

    def fake_download(url: str, destination: Path) -> Path:
        calls["download_url"] = url
        calls["download_dest"] = str(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(destination, "w") as zip_handle:
            zip_handle.writestr("dummy.csv", "id;name\n1;alpha\n")
        return destination

    def fake_extract(zip_path: Path, output_dir: Path) -> Path:
        calls["zip_path"] = str(zip_path)
        calls["extract_dest"] = str(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "dummy.csv").write_text("id;name\n1;alpha\n", encoding="utf-8")
        return output_dir

    monkeypatch.setattr(cli, "build_drive_download_url", fake_build)
    monkeypatch.setattr(cli, "download_file", fake_download)
    monkeypatch.setattr(cli, "extract_zip_file", fake_extract)

    resolved = cli.resolve_input_dir(args)

    assert calls["build"] == args.input_drive_zip
    assert calls["download_url"].startswith("https://drive.google.com/")
    assert Path(calls["download_dest"]).parent == Path(args.download_dir)
    assert calls["zip_path"] == calls["download_dest"]
    assert Path(calls["extract_dest"]) == expected_output_dir
    assert resolved == expected_resolved_path
    assert (expected_output_dir / "dummy.csv").exists()


def test_resolve_input_dir_falls_back_to_local_input_when_drive_download_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    fallback_input = tmp_path / "raw"
    fallback_input.mkdir(parents=True)
    args = cli.argparse.Namespace(
        input=str(fallback_input),
        input_drive_zip="https://drive.google.com/file/d/abc123/view?usp=sharing",
        download_dir=str(tmp_path / "downloads"),
    )

    helper_calls = {"extract": 0}
    monkeypatch.setattr(cli, "build_drive_download_url", lambda _: "https://drive.google.com/uc?export=download&id=abc123")

    def failing_download(url: str, destination: Path) -> Path:
        raise OSError("network blocked")

    def fake_extract(zip_path: Path, output_dir: Path) -> Path:
        helper_calls["extract"] += 1
        return output_dir

    monkeypatch.setattr(cli, "download_file", failing_download)
    monkeypatch.setattr(cli, "extract_zip_file", fake_extract)

    resolved = cli.resolve_input_dir(args)
    captured = capsys.readouterr()

    assert resolved == fallback_input
    assert "WARNING: failed to download/extract Drive ZIP" in captured.out
    assert helper_calls["extract"] == 0


def test_resolve_input_dir_uses_local_input_when_no_drive_zip(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    local_input = tmp_path / "raw"
    local_input.mkdir(parents=True)
    args = cli.argparse.Namespace(
        input=str(local_input),
        input_drive_zip="",
        download_dir=str(tmp_path / "downloads"),
    )

    helper_calls = {"build": 0, "download": 0, "extract": 0}

    def fake_build(url: str) -> str:
        helper_calls["build"] += 1
        return url

    def fake_download(url: str, destination: Path) -> Path:
        helper_calls["download"] += 1
        return destination

    def fake_extract(zip_path: Path, output_dir: Path) -> Path:
        helper_calls["extract"] += 1
        return output_dir

    monkeypatch.setattr(cli, "build_drive_download_url", fake_build)
    monkeypatch.setattr(cli, "download_file", fake_download)
    monkeypatch.setattr(cli, "extract_zip_file", fake_extract)

    resolved = cli.resolve_input_dir(args)

    assert resolved == local_input
    assert helper_calls == {"build": 0, "download": 0, "extract": 0}


def test_resolve_input_dir_raises_for_invalid_drive_url(tmp_path: Path) -> None:
    args = cli.argparse.Namespace(
        input=str(tmp_path / "raw"),
        input_drive_zip="https://example.com/not-drive",
        download_dir=str(tmp_path / "downloads"),
    )

    with pytest.raises(ValueError, match="Not a Google Drive URL"):
        cli.resolve_input_dir(args)


def test_resolve_input_dir_is_idempotent_when_output_dir_exists(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    args = cli.argparse.Namespace(
        input=str(tmp_path / "raw"),
        input_drive_zip="https://drive.google.com/file/d/abc123/view?usp=sharing",
        download_dir=str(tmp_path / "downloads"),
    )
    existing_extracted_dir = tmp_path / "downloads" / "extracted"
    existing_extracted_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(cli, "build_drive_download_url", lambda _: "https://drive.google.com/uc?export=download&id=abc123")

    def fake_download(url: str, destination: Path) -> Path:
        destination.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(destination, "w") as zip_handle:
            zip_handle.writestr("dummy.csv", "id;name\n1;alpha\n")
        return destination

    def fake_extract(zip_path: Path, output_dir: Path) -> Path:
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "dummy.csv").write_text("id;name\n1;alpha\n", encoding="utf-8")
        return output_dir

    monkeypatch.setattr(cli, "download_file", fake_download)
    monkeypatch.setattr(cli, "extract_zip_file", fake_extract)

    resolved = cli.resolve_input_dir(args)

    assert resolved == existing_extracted_dir
    assert (existing_extracted_dir / "dummy.csv").exists()


def test_months_since_supports_iso_and_kbo_date_formats() -> None:
    iso_months = cli.months_since("1960-08-09")
    kbo_months = cli.months_since("09-08-1960")

    assert isinstance(iso_months, int)
    assert iso_months == kbo_months


def test_months_since_returns_none_for_invalid_or_placeholder_dates() -> None:
    assert cli.months_since("") is None
    assert cli.months_since("0000-00-00") is None
    assert cli.months_since("not-a-date") is None
