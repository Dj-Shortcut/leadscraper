# Contributing

Bedankt dat je wil bijdragen aan Leadscraper.

## Development setup
1. Maak een virtuele omgeving.
2. Installeer development dependencies:
   ```bash
   pip install -e .[dev]
   ```
3. Run checks:
   ```bash
   ruff check .
   black --check .
   pytest
   ```

## Workflow
- Werk op een feature branch.
- Voeg tests toe voor bugfixes/features.
- Houd commits klein en beschrijvend.
- Open een PR met context, impact en testbewijs.

## Code style
- Python 3.11+
- Type hints waar zinvol
- Format met Black, lint met Ruff
