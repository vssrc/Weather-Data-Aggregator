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

## GIBS layers

Configured GIBS layers (deduped from the provided list). All are daily, 2 km PNGs available in Web Mercator and Geographic projections.

| Layer | Measurement | Platform / Instrument | Time range | Product codes |
| --- | --- | --- | --- | --- |
| Brightness Temperature (Channel 01) AMSUA_NOAA15_Brightness_Temp_Channel_1 | Brightness Temperature | NOAA-15 / AMSU-A | 1998-08-03 - 2025-08-19 | STD: amsua15sp 1 |
| Brightness Temperature (Channel 02) AMSUA_NOAA15_Brightness_Temp_Channel_2 | Brightness Temperature | NOAA-15 / AMSU-A | 1998-08-03 - 2025-08-19 | STD: amsua15sp 1 |
| Brightness Temperature (Channel 03) AMSUA_NOAA15_Brightness_Temp_Channel_3 | Brightness Temperature | NOAA-15 / AMSU-A | 1998-08-03 - 2025-08-19 | STD: amsua15sp 1 |
| Brightness Temperature (Channel 04) AMSUA_NOAA15_Brightness_Temp_Channel_4 | Brightness Temperature | NOAA-15 / AMSU-A | 1998-08-03 - 2025-08-19 | STD: amsua15sp 1 |
| Brightness Temperature (Channel 05) AMSUA_NOAA15_Brightness_Temp_Channel_5 | Brightness Temperature | NOAA-15 / AMSU-A | 1998-08-03 - 2025-08-19 | STD: amsua15sp 1 |
| Brightness Temperature (Channel 06) AMSUA_NOAA15_Brightness_Temp_Channel_6 | Brightness Temperature | NOAA-15 / AMSU-A | 1998-08-03 - 2025-08-19 | STD: amsua15sp 1 |
| Brightness Temperature (Channel 07) AMSUA_NOAA15_Brightness_Temp_Channel_7 | Brightness Temperature | NOAA-15 / AMSU-A | 1998-08-03 - 2025-08-19 | STD: amsua15sp 1 |
| Brightness Temperature (Channel 08) AMSUA_NOAA15_Brightness_Temp_Channel_8 | Brightness Temperature | NOAA-15 / AMSU-A | 1998-08-03 - 2025-08-19 | STD: amsua15sp 1 |
| Brightness Temperature (Channel 09) AMSUA_NOAA15_Brightness_Temp_Channel_9 | Brightness Temperature | NOAA-15 / AMSU-A | 1998-08-03 - 2025-08-19 | STD: amsua15sp 1 |
| Brightness Temperature (Channel 10) AMSUA_NOAA15_Brightness_Temp_Channel_10 | Brightness Temperature | NOAA-15 / AMSU-A | 1998-08-03 - 2025-08-19 | STD: amsua15sp 1 |
| Brightness Temperature (Channel 12) AMSUA_NOAA15_Brightness_Temp_Channel_12 | Brightness Temperature | NOAA-15 / AMSU-A | 1998-08-03 - 2025-08-19 | STD: amsua15sp 1 |
| Brightness Temperature (Channel 13) AMSUA_NOAA15_Brightness_Temp_Channel_13 | Brightness Temperature | NOAA-15 / AMSU-A | 1998-08-03 - 2025-08-19 | STD: amsua15sp 1 |
| Brightness Temperature (Channel 15) AMSUA_NOAA15_Brightness_Temp_Channel_15 | Brightness Temperature | NOAA-15 / AMSU-A | 1998-08-03 - 2025-08-19 | STD: amsua15sp 1 |
| Brightness Temperature (Ascending) TRMM_Brightness_Temp_Asc | Brightness Temperature | TRMM / TMI | 1997-12-07 - 2015-04-08 | STD: GPM_1CTRMMTMI 07 |
| Brightness Temperature (Descending) TRMM_Brightness_Temp_Dsc | Brightness Temperature | TRMM / TMI | 1997-12-07 - 2015-04-08 | STD: GPM_1CTRMMTMI 07 |
| Flash Radiance (Level 2, Standard) LIS_TRMM_Flash_Radiance | Lightning | TRMM / LIS | 1998-01-01 - 2015-04-08 | STD: lislip 4 |
| Precipitation Rate (30-min) IMERG_Precipitation_Rate_30min | Precipitation Rate | IMERG / n/a | 1998-01-01T00:00:00Z - Present | STD: GPM_3IMERGHH 07; NRT: GPM_3IMERGHHE 07 |
| Precipitation Rate (Ascending) TRMM_Precipitation_Rate_Asc | Precipitation Rate | TRMM / TMI | 1997-12-07 - 2015-04-08 | STD: GPM_2AGPROFTRMMTMI_CLIM 07 |
| Precipitation Rate (Descending) TRMM_Precipitation_Rate_Dsc | Precipitation Rate | TRMM / TMI | 1997-12-07 - 2015-04-08 | STD: GPM_2AGPROFTRMMTMI_CLIM 07 |
| Sea Surface Temperature (L4, AVHRR-OI) GHRSST_L4_AVHRR-OI_Sea_Surface_Temperature | Sea Surface Temperature | Multi-mission / GHRSST | 1981-09-01 - Present | STD: AVHRR_OI-NCEI-L4-GLOB-v2.0 2.0 |

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
