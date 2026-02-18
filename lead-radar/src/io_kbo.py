"""I/O helpers for KBO source files."""

from pathlib import Path


def load_kbo_file(path: Path) -> str:
    """Load a KBO file as text."""
    return path.read_text(encoding="utf-8")
