# Weather & GIBS Data Aggregator

Unified Python tool for pulling historical weather data and NASA GIBS satellite imagery into a consistent local cache.

## Providers

| Provider | Type | Description |
|----------|------|-------------|
| Open-Meteo | Weather | Free weather API |
| NOAA ISD | Weather | Integrated Surface Database |
| NOAA LCD | Weather | Local Climatological Data |
| Meteostat | Weather | Historical weather data |
| GIBS | Imagery | NASA satellite imagery (54 layers) |

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Configuration

Edit `config.json` to configure:

```json
{
  "date_range": {
    "start": "2020-01-01",
    "end": "today"
  },
  "providers": {
    "gibs": {
      "frequency": "1d",
      "layers": ["MODIS_Terra_CorrectedReflectance_TrueColor", ...]
    }
  },
  "locations": {
    "new_york_ny": {
      "lat": 40.78,
      "lon": -73.97,
      "noaaIsdStation": "72505394728",
      "noaaLcdStation": "72505394728",
      "bbox": [-74.2, 40.6, -73.7, 41.0]
    }
  }
}
```

- **date_range**: Use `"today"` for end date to always fetch up to current date
- **locations**: Each needs `lat`/`lon`, NOAA station IDs, and `bbox` for GIBS

## Running

```bash
# Run export (uses dates from config)
python scripts/combined_export.py --once

# Override date range
python scripts/combined_export.py --once --since 2024-01-01 --until today

# Limit to specific providers
python scripts/combined_export.py --once --providers open_meteo,meteostat

# Limit locations
python scripts/combined_export.py --once --limit-locations 2

# Health check
python scripts/healthcheck.py
```

## Data Layout

```
data/
├── open_meteo/{location}.csv
├── noaa_isd/{location}.csv
├── noaa_lcd/{location}.csv
├── meteostat/{location}.csv
└── gibs/{layer}/{location}/YYYY-MM-DDTHHMMSSZ.png
```

Weather data is saved as one consolidated CSV per location. Progress is saved incrementally after each batch.

## Project Structure

```
├── config.json          # Configuration file
├── scripts/
│   ├── combined_export.py   # Main export runner
│   └── healthcheck.py       # Provider health check
└── src/
    ├── clients/         # API clients (Open-Meteo, NOAA, Meteostat, GIBS)
    ├── core/            # Config, dates, runtime helpers
    ├── exporters/       # DataFrame and image exporters
    └── visualization/   # Coverage charts
```

## Notes

- NOAA data is fetched anonymously (no token required)
- GIBS output uses 1024x1024 PNGs by default
- Weather providers run with up to 10 parallel workers per provider
- GIBS runs with up to 128 parallel workers
