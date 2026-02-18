# Lead Radar

Lead Radar verwerkt KBO Open Data CSV-dumps naar een geprioriteerde lead-lijst.

## 1) KBO Open Data downloaden

1. Download de gewenste KBO Open Data bestanden (enterprises, establishments, activities).
2. Maak een versie-map met datum onder `data/raw/<YYYY-MM-DD>/`.
3. Plaats de bronbestanden met deze namen:
   - `enterprises.csv`
   - `establishments.csv`
   - `activities.csv`

> Verwacht CSV met `;` als delimiter (KBO-stijl). De CLI detecteert automatisch delimiter en leest ook komma-CSV indien nodig.

Voorbeeld:

```bash
mkdir -p data/raw/2026-02-18
cp ~/Downloads/enterprises.csv data/raw/2026-02-18/
cp ~/Downloads/establishments.csv data/raw/2026-02-18/
cp ~/Downloads/activities.csv data/raw/2026-02-18/
```

## 2) Quickstart met sample data

De repository bevat fake testdata in `data/sample/`, zodat je de pipeline lokaal kan draaien zonder echte dump.

```bash
python -m src.cli \
  --input data/sample \
  --output data/processed/sample_leads.csv
```

## 3) CLI-commando's

Standaardcommand:

```bash
python -m src.cli --input data/raw/2026-02-18 --output data/processed/leads_ninove.csv
```

Met filters:

```bash
python -m src.cli \
  --input data/raw/2026-02-18 \
  --output data/processed/leads_ninove.csv \
  --postcodes "9400,9300" \
  --months 18 \
  --min-score 40 \
  --limit 200
```

## 4) Sector bucketing

`sector_bucket` komt uit `bucket_from_nace()` met deze buckets:
- `beauty`
- `horeca`
- `health`
- `retail`
- `service_trades`
- `other`

## 5) Scoringregels

`score_reasons` gebruikt `|` als separator.

- `new<18m;+30` wanneer startdatum binnen `--months` valt
- `sector;+30` wanneer `sector_bucket` in `{beauty, horeca, health}` zit
- `missing_nace;-5` wanneer geen NACE-code aanwezig is
- `active_status;+10` bonus wanneer status `ACTIVE` is

## 6) Outputlocaties

- Verwerkte output wordt geschreven naar het pad in `--output`.
- Voorbeelden:
  - `data/processed/leads_ninove.csv`
  - `data/processed/sample_leads.csv`

De CLI print ook een summary met:
- aantal records totaal
- aantal na filters
- top 10 sector buckets
