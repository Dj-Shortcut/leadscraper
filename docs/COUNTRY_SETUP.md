# Country setup guide

Deze guide legt uit hoe je Leadscraper aanpast voor een nieuw land/regio.

## 1. Wat is land-afhankelijk?
- **Landcode/regiofilters**: `--country`, `--city`, `--postcodes`.
- **Bronnen/endpoints**: company registry, maps, directories, lokale open data.
- **ID-format**: bv. KBO, Companies House, SIREN/SIRET.
- **Taal/zoekwoorden**: lokale termen voor sectoren en query's.

## 2. Provider contract
Maak per land een provider in `src/providers/<country>.py` met deze methodes:
- `search(query, limit)`
- `normalize(raw_record)`
- `enrich(record)`

Gebruik `src/providers/template.py` als startpunt.

## 3. Nieuwe provider toevoegen (stappen)
1. Kopieer `src/providers/template.py` naar bv. `src/providers/nl.py`.
2. Vul datasource logic in `search()`.
3. Map bronvelden naar outputschema in `normalize()`.
4. Voeg optionele enrichment toe in `enrich()` (website/contact checks).
5. Voeg provider toe aan router/selector in je scraper flow.
6. Voeg fixtures en tests toe voor normalisatie + dedupe.

## 4. Voorbeeldconfig
```bash
leadscraper \
  --input data/raw \
  --output data/processed/leads_be_ninove.csv \
  --country BE \
  --city Ninove \
  --query beauty \
  --limit 500
```

## 5. Rate limits en backoff
Aanbevolen defaults:
- `max_retries=5`
- `initial_backoff=2s`
- `max_backoff=60s`
- `jitter=0.1-0.3`

Bij blokkering:
- Verlaag `--limit`
- Verlaag request concurrency
- Verhoog backoff en respecteer `Retry-After`

## 6. Outputschema (minimum)
- `enterprise_number`
- `name`
- `address`
- `postal_code`
- `city`
- `phone`
- `email`
- `website`
- `sector_bucket`
- `score_total`
- `source_files_version`
