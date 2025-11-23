# Weather & GIBS Data Aggregator

Unified Python runner that pulls historical weather data (Open-Meteo, NOAA ISD/LCD, Meteostat, NASA POWER, IEM ASOS) and NASA GIBS imagery into a consistent cache with a single terminal UI.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp config.json.template config.json
```

Edit `config.json` with your NOAA token (`providers.noaa.token` is shared by ISD/LCD, override per-provider only if you need different tokens), GIBS layers, and locations (lat/lon plus `bbox` for GIBS).

## Running

```bash
# One-shot export (recommended first run)
python scripts/combined_export.py --once

# Limit providers
python scripts/combined_export.py --once --providers open_meteo,gibs

# Custom range
python scripts/combined_export.py --once --since 2023-01-01 --until 2023-01-31

# Faster GIBS cadence (e.g., every 6 hours)
python scripts/combined_export.py --once --gibs-frequency 6h
```

Flags:
- `--limit-locations N` limit work to first N locations from config.
- `--gibs-frequency` set timestamp spacing for GIBS (e.g., `1d`, `6h`, `30m`).
- `--gibs-refresh-days` force re-download of the most recent N days for GIBS (default 1).

## Data layout

```
data/
├── open_meteo/{location}/YYYY-MM-DD.csv
├── noaa_isd/{location}/YYYY-MM-DD.csv
├── noaa_lcd/{location}/YYYY-MM-DD.csv
├── meteostat/{location}/YYYY-MM-DD.csv
├── nasa_power/{location}/YYYY-MM-DD.csv
├── iem_asos/{location}/YYYY-MM-DD.csv
└── gibs/{layer}/{location}/YYYY-MM-DDTHHMMSSZ.png
```

## Progress UI

The terminal shows a live grid per provider and location with color bars (green=cached, orange=processing, red=failed) plus a recent activity tail. Logs are written to `logs/combined_export.log`.

## Threading & performance

- Provider-level concurrency: up to 6 providers in parallel.
- Weather providers: batched per-location date work with up to 10 workers and 30-day batches.
- GIBS: per-layer/location/timestamp fan-out with up to 8 workers (tunable in the script).

## Notes

- NOAA token strongly recommended for large ranges. Add it once at `providers.noaa.token` (legacy `providers.noaa_isd`/`providers.noaa_lcd` entries are now optional overrides).
- Ensure each location has a `bbox` for GIBS requests.
- GIBS output uses 512x512 PNGs by default (set in script constants if you need a different size).
