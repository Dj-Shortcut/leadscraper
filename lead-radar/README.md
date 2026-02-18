# Lead Radar

Lead Radar verwerkt KBO Open Data CSV-dumps naar een geprioriteerde lead-lijst.

## 1) KBO Open Data downloaden

1. Download de gewenste KBO Open Data bestanden.
2. Zorg dat deze 4 bestanden beschikbaar zijn (enkelvoud of meervoud werkt):
   - `enterprises.csv` of `enterprise.csv`
   - `establishments.csv` of `establishment.csv`
   - `activities.csv` of `activity.csv` (niet nodig in `--lite` mode)
   - optioneel: `contacts.csv` of `contact.csv` (KBO contact-schema)

De tool detecteert automatisch welke variant aanwezig is.

Contactbestand verwacht velden volgens KBO-schema:
- `EntityNumber` (ondernemingsnummer of vestigingsnummer)
- `EntityContact` (`ENT` of `EST`)
- `ContactType` (`TEL`, `EMAIL`, `WEB`, ...)
- `Value` (de effectieve waarde)

`EntityNumber` wordt genormaliseerd naar digits-only (punten/quotes verwijderd). Bij `EntityContact=EST` wordt het vestigingsnummer via `establishments.csv` gemapt naar het ondernemingsnummer.

> Verwacht CSV met `;` als delimiter (KBO-stijl). De CLI detecteert automatisch delimiter en leest ook komma-CSV indien nodig.

Voorbeeld:

```bash
mkdir -p data/raw/2026-02-18
cp ~/Downloads/enterprises.csv data/raw/2026-02-18/
cp ~/Downloads/establishments.csv data/raw/2026-02-18/
cp ~/Downloads/activities.csv data/raw/2026-02-18/
cp ~/Downloads/contact.csv data/raw/2026-02-18/
```

## 2) Quickstart met sample data

De repository bevat fake testdata in `data/sample/`, inclusief een mini `contact.csv` in KBO-formaat (`;` delimiter), zodat je de pipeline lokaal kan draaien zonder echte dump.

```bash
python -m src.cli --input data/sample --output data/processed/sample_leads.csv --postcodes 9400
```

## 3) CLI-commando's

Real run:

```bash
python -m src.cli --input data/raw --output data/processed/leads_ninove.csv --min-score 40

# Lite mode (zonder activities.csv)
python -m src.cli --input data/raw --output data/processed/leads_ninove_lite.csv --lite
```

Met filters:

```bash
python -m src.cli \
  --input data/raw/2026-02-18 \
  --output data/processed/leads_ninove.csv \
  --postcodes "9400" \
  --months 18 \
  --min-score 40 \
  --limit 200
```

Ninove-voorbeeld (kort):

```bash
python -m src.cli --input data/raw/2026-02-18 --output data/processed/leads_ninove.csv --postcodes 9400
```


## Google Drive ZIP + Google Sheets workflow

Je kan nu rechtstreeks een publieke Google Drive ZIP downloaden en de output naar een Google Sheet sturen.

Voorbeeld met jouw links:

```bash
python -m src.cli   --input data/raw   --input-drive-zip "https://drive.google.com/file/d/169qB_45xf57l_6drT1ScZuIPIQPUG2oH/view?usp=sharing"   --download-dir data/downloads   --output data/processed/leads.csv   --lite   --sheet-url "https://docs.google.com/spreadsheets/d/1phKaRKPVybV_8PAsLOS7deEgRQn5HiVWNz2BlvVwnD0/edit?usp=drive_link"   --sheet-tab Leads
```

> Google Sheets upload vereist service-account credentials via env var `GOOGLE_SERVICE_ACCOUNT_JSON` en package `gspread`.

`resolve_input_dir(args)` returnt altijd het effectieve inputpad voor de pipeline: bij succesvolle `--input-drive-zip` verwerking is dat `--download-dir/extracted`; zonder Drive ZIP of bij download/extract-fallback is dat `--input`.

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

- `new<18m` wanneer startdatum binnen `--months` valt (`+30`)
- `sector_high` wanneer `sector_bucket` in `{beauty, horeca, health}` zit (`+15`)
- `no_nace` wanneer geen NACE-code aanwezig is (`-5`)
- `has_phone` wanneer telefoon aanwezig is (`+5`)
- `has_email` wanneer email aanwezig is (`+3`)
- `has_website` wanneer website aanwezig is (informatieve reden, geen extra punten)

## 6) Outputlocaties

- Verwerkte output wordt geschreven naar het pad in `--output`.
- Voorbeelden:
  - `data/processed/leads_ninove.csv`
  - `data/processed/sample_leads.csv`

De CLI print ook een summary met:
- aantal records totaal
- aantal na filters
- top 10 sector buckets
