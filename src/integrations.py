"""Google Drive and Google Sheets integrations for Lead Radar."""

from __future__ import annotations

import csv
import re
import zipfile
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen


_CHUNK_SIZE = 1024 * 1024


def extract_google_drive_file_id(url: str) -> str:
    """Extract a Drive file id from common Google Drive URL formats."""
    parsed = urlparse(url)
    if parsed.netloc not in {"drive.google.com", "www.drive.google.com"}:
        raise ValueError("Not a Google Drive URL")

    match = re.search(r"/file/d/([a-zA-Z0-9_-]+)", parsed.path)
    if match:
        return match.group(1)

    query = parse_qs(parsed.query)
    file_id = query.get("id", [""])[0]
    if file_id:
        return file_id

    raise ValueError("Unable to parse Google Drive file id")


def build_drive_download_url(url: str) -> str:
    file_id = extract_google_drive_file_id(url)
    return f"https://drive.google.com/uc?export=download&id={file_id}"


def download_file(url: str, destination: Path) -> Path:
    """Download a URL to destination path using stdlib only."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(request) as response, destination.open("wb") as output_handle:
        while True:
            chunk = response.read(_CHUNK_SIZE)
            if not chunk:
                break
            output_handle.write(chunk)
    return destination


def extract_zip_file(zip_path: Path, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zip_handle:
        zip_handle.extractall(output_dir)
    return output_dir


def extract_google_sheet_id(url: str) -> str:
    parsed = urlparse(url)
    if parsed.netloc not in {"docs.google.com", "www.docs.google.com"}:
        raise ValueError("Not a Google Sheets URL")

    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", parsed.path)
    if not match:
        raise ValueError("Unable to parse Google Sheet id")
    return match.group(1)


def upload_csv_to_google_sheet(*, sheet_url: str, csv_path: Path, worksheet_name: str = "Leads") -> None:
    """Upload CSV rows into a Google Sheet worksheet using service-account credentials.

    Requires env var ``GOOGLE_SERVICE_ACCOUNT_JSON`` to point to a service account json file.
    """
    import os

    credentials_path = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if not credentials_path:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is not set")

    try:
        import gspread  # type: ignore
    except ImportError as exc:  # pragma: no cover - dependency checked in runtime
        raise RuntimeError("gspread dependency is required for Google Sheets upload") from exc

    client = gspread.service_account(filename=credentials_path)
    sheet_id = extract_google_sheet_id(sheet_url)
    spreadsheet = client.open_by_key(sheet_id)

    try:
        worksheet = spreadsheet.worksheet(worksheet_name)
        worksheet.clear()
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=2000, cols=40)

    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.reader(handle))

    if not rows:
        rows = [["no_data"]]

    worksheet.update("A1", rows)
