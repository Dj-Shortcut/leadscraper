# Lead Radar

Lead Radar verwerkt KBO Open Data CSV-dumps naar een geprioriteerde lead-lijst.

## 1) KBO Open Data downloaden

1. Download de gewenste KBO Open Data bestanden (enterprises, establishments, activities).
2. Maak een versie-map met datum onder `data/raw/<YYYY-MM-DD>/`.
3. Plaats de bronbestanden met deze namen:
   - `enterprises.csv`
   - `establishments.csv`
   - `activities.csv`

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

## 4) Outputlocaties

- Verwerkte output wordt geschreven naar het pad in `--output`.
- Voorbeelden:
  - `data/processed/leads_ninove.csv`
  - `data/processed/sample_leads.csv`

De CLI print ook een summary met:
- aantal records totaal
- aantal na filters
- top 10 sector buckets
