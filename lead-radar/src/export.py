"""Export utilities for processed output."""

from pathlib import Path


def export_text(content: str, destination: Path) -> None:
    """Write text content to destination."""
    destination.write_text(content, encoding="utf-8")
