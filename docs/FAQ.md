# FAQ

## Waarom krijg ik 0 results?
- Check dat je inputbestanden correct heten (`enterprise(s).csv`, `establishment(s).csv`, ...).
- Gebruik `--verbose` voor input-detectie.
- Controleer postcodefilter (`--postcodes`) en `--min-score`.

## Ik word rate-limited
- Verlaag volume (`--limit`).
- Gebruik retries/backoff volgens `docs/COUNTRY_SETUP.md`.
- Respecteer de ToS van de databron.

## Hoe dedupe ik leads?
Gebruik een stabiele sleutel, bv. `hash(normalized_name + normalized_address)`.

## Waarom ontbreken telefoon of e-mail velden?
Niet elke bron bevat contactdata. Controleer of `contact.csv` aanwezig is.
