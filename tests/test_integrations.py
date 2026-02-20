from pathlib import Path

from src.integrations import (
    build_drive_download_url,
    extract_google_drive_file_id,
    extract_google_sheet_id,
    extract_zip_file,
)


def test_extract_google_drive_file_id_from_file_url() -> None:
    url = "https://drive.google.com/file/d/169qB_45xf57l_6drT1ScZuIPIQPUG2oH/view?usp=sharing"
    assert extract_google_drive_file_id(url) == "169qB_45xf57l_6drT1ScZuIPIQPUG2oH"


def test_build_drive_download_url() -> None:
    url = "https://drive.google.com/file/d/169qB_45xf57l_6drT1ScZuIPIQPUG2oH/view?usp=sharing"
    assert (
        build_drive_download_url(url)
        == "https://drive.google.com/uc?export=download&id=169qB_45xf57l_6drT1ScZuIPIQPUG2oH"
    )


def test_extract_google_sheet_id() -> None:
    url = "https://docs.google.com/spreadsheets/d/1phKaRKPVybV_8PAsLOS7deEgRQn5HiVWNz2BlvVwnD0/edit?usp=drive_link"
    assert extract_google_sheet_id(url) == "1phKaRKPVybV_8PAsLOS7deEgRQn5HiVWNz2BlvVwnD0"


def test_extract_zip_file(tmp_path: Path) -> None:
    import zipfile

    zip_path = tmp_path / "sample.zip"
    with zipfile.ZipFile(zip_path, "w") as zip_handle:
        zip_handle.writestr("nested/enterprise.csv", "enterprise_number;name\n1;Acme\n")

    output = tmp_path / "out"
    extract_zip_file(zip_path, output)

    extracted_file = output / "nested" / "enterprise.csv"
    assert extracted_file.exists()
    assert "Acme" in extracted_file.read_text(encoding="utf-8")
