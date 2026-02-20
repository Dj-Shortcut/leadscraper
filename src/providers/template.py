"""Template provider contract for country-specific ingestion."""

from __future__ import annotations

from typing import Any


class TemplateProvider:
    country = "XX"

    def search(self, query: str, limit: int) -> list[dict[str, Any]]:
        """Fetch raw records from a country-specific source."""
        return []

    def normalize(self, raw_record: dict[str, Any]) -> dict[str, Any]:
        """Map raw source data to Leadscraper output schema."""
        return {
            "enterprise_number": str(raw_record.get("id", "")).strip(),
            "name": str(raw_record.get("name", "")).strip(),
            "address": str(raw_record.get("address", "")).strip(),
            "postal_code": str(raw_record.get("postal_code", "")).strip(),
            "city": str(raw_record.get("city", "")).strip(),
            "phone": "",
            "email": "",
            "website": "",
            "source_files_version": "provider-template",
        }

    def enrich(self, record: dict[str, Any]) -> dict[str, Any]:
        """Optionally enrich normalized records (contacts, website, categorization)."""
        return record
