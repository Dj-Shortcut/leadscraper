# Leadscraper

![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![CI](https://github.com/Dj-Shortcut/leadscraper/actions/workflows/ci.yml/badge.svg)
![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)

Leadscraper verwerkt bedrijfsbrondata (zoals KBO CSV dumps) naar een gescoorde leadlijst met contactverrijking, dedupe-vriendelijke output en filters op land/regio/query.

## Features
- CSV pipeline voor enterprise/establishment/activity/contact datasets
- Scoring en sector-bucketing voor leadprioritering
- CLI met consistente flags (`--country`, `--city`, `--query`, `--limit`, `--dry-run`, ...)
- Output naar CSV + optionele Google Sheets upload
- Snelle mode (`--fast`) voor grote datasets
- Uitbreidbaar provider-model per land (`src/providers/`)

## Repository structuur
- `src/` — core pipeline, config, providers
- `scripts/` — helper scripts (benchmark)
- `docs/` — country setup + FAQ
- `examples/` — sample config en output
- `tests/` — pytest tests
- `data/sample/` — sample inputfixtures

## Quickstart
### Requirements
- Python 3.11+

### Install
```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e .[dev]
```

### Config
```bash
cp .env.example .env
```

### Run
```bash
leadscraper \
  --input data/sample \
  --output data/processed/sample_leads.csv \
  --country BE \
  --city Ninove \
  --query beauty \
  --limit 500
```

Dry-run (geen output schrijven):
```bash
leadscraper --input data/sample --output data/processed/sample_leads.csv --country BE --dry-run
```

## CLI voorbeelden
```bash
# Basisrun
leadscraper --input data/raw/2026-02-18 --output data/processed/leads.csv --country BE --postcodes 9400

# Fast mode
leadscraper --input data/raw --output data/processed/leads_fast.csv --country BE --fast --limit 0

# Google Sheets upload
leadscraper --input data/raw --output data/processed/leads.csv --country BE --sheet-url "https://docs.google.com/spreadsheets/d/..." --sheet-tab Leads
```

## Output
Standaard outputkolommen:
- `enterprise_number`
- `name`
- `status`
- `start_date`
- `address`
- `postal_code`
- `city`
- `nace_codes`
- `sector_bucket`
- `phone`, `email`, `website`
- `score_total`, `score_reasons`
- `source_files_version`

Outputlocatie = pad in `--output`.

## Dedupe strategie
Aanbevolen sleutel voor cross-run dedupe:
- `hash(normalized_name + normalized_address)`

Minimum bruikbare leadvelden:
- naam
- adres/postcode/stad
- minstens één contactveld (`phone` of `email` of `website`)

## Troubleshooting
- **Geen inputbestanden gevonden**: check bestandsnamen (`enterprise(s).csv`, `establishment(s).csv`, ...).
- **0 resultaten**: verlaag `--min-score`, controleer `--postcodes`/`--city` filters.
- **Rate limited**: verlaag throughput en volg backoff-richtlijnen in `docs/COUNTRY_SETUP.md`.

## Gebruik in je eigen land
Zie `docs/COUNTRY_SETUP.md` voor provider-contract (`search`, `normalize`, `enrich`) en stapsgewijze onboarding.

## Legal / Responsible use
- Respecteer altijd Terms of Service van gebruikte databronnen.
- Gebruik de output niet voor spam of misbruik.
- Bewaar geen gevoelige persoonsgegevens zonder rechtsgrond.
- Implementeer rate limiting/retries om diensten niet te overbelasten.
- Jij als gebruiker bent volledig verantwoordelijk voor compliant gebruik.

## Roadmap / limitations
- Momenteel productief getest op BE/KBO-achtige datasets.
- Provider routering voor meerdere landen staat als volgende stap.
- Containerized deployment/release automation volgt later.

## License
MIT License. Copyright (c) 2026 Dj-Shortcut. Zie `LICENSE`.

## Checklist
- [x] `.env.example` compleet
- [x] Outputschema gedocumenteerd
- [x] Dedupe strategie beschreven
- [x] Retry/backoff/rate-limit guidance aanwezig
- [x] Country provider template aanwezig (`src/providers/template.py`)
- [x] CI workflow actief
- [ ] Deployment guide (Docker) — optioneel/volgende iteratie
