# Weather Data Aggregator

Automated tooling for collecting, caching, and exploring weather observations and forecasts across multiple providers. The repository bundles flexible Python clients, export automation, and exploratory notebooks that share a common configuration.

---

## Project Highlights

- **Unified configuration** – `weather_config.json` centralises provider credentials, defaults, and reusable location definitions.
- **Concurrent exports** – `weather_data_export.py` fetches data from all enabled providers in parallel, looping continuously with a configurable cool-off period.
- **Batch-aware clients** – Each provider client exposes `get_*_batch` helpers capable of sending requests in configurable batches, backed by a shared threading mixin.
- **Exploratory notebooks** – Rich demos and export logic live under `notebooks/`, mirroring the Python script’s behaviour for ad-hoc analysis.
- **Progress visualisation** – After every export run a cached coverage heatmap (`data/cache_coverage.png`) highlights which locations and dates are already stored.
- **Verbose logging** – `logs/weather_export.log` captures run-by-run status, request skips, errors, and timing information.

---

## Repository Layout

```
.
├── clients/                 # Provider-specific API wrappers with batch helpers
├── data/                    # Cached CSV output (organised by provider/location/date)
├── logs/                    # Verbose export logs (created on demand)
├── notebooks/               # Jupyter notebooks for demos and manual exports
├── weather_config.json      # Provider + location configuration
├── weather_data_export.py   # Continuous export runner with concurrency + visualisation
├── requirements.txt         # Python dependencies
└── README.md
```

---

## Supported providers, datasets, and APIs

The aggregator currently integrates the following upstream services. Each client is implemented under `clients/` and driven by the matching block in `weather_config.json`.

| Provider key | Public name | Primary API / dataset | Cadence & scope | Notes |
| --- | --- | --- | --- | --- |
| `tomorrow_io` | Tomorrow.io Timelines | `https://api.tomorrow.io/v4/timelines` | Sub-hourly & hourly forecast / recent history | Rotates across multiple API keys, configurable timesteps and units. |
| `open_meteo` | Open-Meteo | Forecast & Archive APIs | Hourly & daily weather variables | Supports unit overrides per variable and forecast/past day windows. |
| `visual_crossing` | Visual Crossing | Timeline Weather API | Hourly forecast & history (JSON) | Location path parameters with optional include lists (days/hours). |
| `noaa_isd` | NOAA Integrated Surface Database | Access Data Service `dataset=global-hourly` | Hourly & sub-hourly station observations | Requires station IDs and NOAA token; parses METAR style fields. |
| `noaa_lcd` | NOAA Local Climatological Data | Access Data Service `dataset=local-climatological-data` | Hourly LCD observations | Shares the NOAA token, exposes station names when requested. |
| `weatherapi_com` | WeatherAPI.com | Forecast, History, Future & Current endpoints | Hourly forecast/history up to 14 days | Supports key rotation, AQI/alert toggles, language overrides. |
| `openweather` | OpenWeather | Current & History APIs | Hourly history via `type=hour` | Requires a single API key; converts ISO timestamps to UNIX for history requests. |
| `weatherbit` | Weatherbit | Hourly forecast & sub-hourly history endpoints | Hourly data windows | Handles UTC timestamp formatting for historical pulls. |
| `meteostat` | Meteostat Python SDK | Meteostat Hourly dataset | Hourly blended observations | Uses the local Meteostat cache/model data toggle, returns Pandas frames. |
| `nasa_power` | NASA POWER | Hourly point endpoint | Hourly satellite/model parameters | Requests configurable parameter lists and normalises responses to tabular form. |
| `iem_asos` | Iowa Environmental Mesonet ASOS | `asos1min.py` CSV service | 1-minute ASOS observations | Depends on station + network IDs and converts CSV output to DataFrames. |
| `copernicus_era5_single` | Copernicus ERA5 (single levels) | CDS dataset `reanalysis-era5-single-levels` | Hourly grid (NetCDF) | Requires CDS API key; downloads and trims NetCDF to configured area. |
| `copernicus_era5_land` | Copernicus ERA5-Land | CDS dataset `reanalysis-era5-land` | Hourly land grid (NetCDF) | Builds year/month/day requests based on day range; uses 0.1° grid. |
| `copernicus_era5_pressure` | Copernicus ERA5 (pressure) | CDS dataset `reanalysis-era5-pressure-levels` | Hourly pressure levels (NetCDF) | Adds pressure level list and product type for reanalysis requests. |
| `copernicus_era5_land_timeseries` | Copernicus ERA5-Land Timeseries | CDS dataset `reanalysis-era5-land-timeseries` | Hourly point CSV series | Retrieves CSV per point without area selection, ideal for quick summaries. |

All providers share the batch execution infrastructure in `clients/__init__.py`, enabling concurrent request fans with optional throttling.

---

## Configuration (`weather_config.json`)

Key sections:

- **`providers`** – One entry per supported API with credentials, base URLs, dataset identifiers, default units, batching hints, and documentation links.
- **`locations`** – Named latitude/longitude pairs, plus provider-specific metadata (e.g. NOAA station IDs or Copernicus area bounds) reused across notebooks and scripts.

> **Tip:** Add or adjust locations here and both the notebooks and export script will automatically process all defined sites.

### Using the configuration template

1. Copy `weather_config.json.template` to `weather_config.json`.
2. Replace every placeholder token (strings wrapped in `<...>`) with your real API keys, tokens, or CDS credentials. Leave structural values (URLs, default units, area arrays, etc.) untouched unless you need different defaults.
3. Review the `locations` block and update station identifiers or bounding boxes as needed. Lat/lon pairs in the template match the example cities already referenced by notebooks and the exporter.
4. Keep sensitive credentials out of version control—commit only the template.

---

## Automation Script

Run the continuous exporter:

```bash
python3 weather_data_export.py
```

Features:

- Executes all enabled providers concurrently (one thread per provider) while handling individual failures gracefully.
- Mirrors notebook logic: skips already cached days (except the current date) and splits data into provider/location subdirectories.
- After every cycle generates/updates `data/cache_coverage.png` to visualise coverage across the configured date range.
- Sleeps for 10 minutes (`COOLDOWN_SECONDS`) before the next run; adjust this constant at the top of the script as needed.
- Writes detailed progress and error information to `logs/weather_export.log` (also echoed to stdout).

Stop with `Ctrl+C`. The script catches interrupts cleanly and halts after the current sleep period or immediate signal.

---

## Notebooks

Located in `notebooks/`:

- `weather_api_clients_demo.ipynb` – Interactive walkthrough of every client, running both single calls and batch calls across all configured locations.
- `weather_data_export.ipynb` – Notebook counterpart to the automation script: batch exports data, skips cached days, and finishes with the same coverage heatmap.

Both notebooks respect the settings in `weather_config.json`. If you move or rename the file, update the path references inside the notebooks/script accordingly.

---

## Dependencies & Environment

Install the required packages into your environment of choice:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

The repository targets Python 3.9+ (matching the `.venv` metadata). If running in headless environments, note that `matplotlib` is configured to use the `Agg` backend for image generation, and `tqdm` supplies the optional progress bars rendered by the exporter.

---

## Logging & Outputs

- **CSV data:** `data/<provider>/<location>/<YYYY-MM-DD>.csv`
- **Coverage heatmap:** `data/cache_coverage.png`
- **Logs:** `logs/weather_export.log`

Rotate or trim directories as needed if storage grows large. Historical CSVs can be archived without impacting future runs (the exporter recreates missing files on demand).

---

## Customisation Checklist

1. **Update credentials** in `weather_config.json` for each provider you plan to use.
2. **Add locations** under `locations` with descriptive keys to enable multi-site exports.
3. **Tune concurrency** – adjust each provider’s `batchSize` in the config if APIs impose stricter rate limits.
4. **Modify date span** – change `START_DATE` / `END_DATE` near the top of the notebook/script, or parameterise them to suit automation pipelines.
5. **Adjust cooldown** – update `COOLDOWN_SECONDS` in `weather_data_export.py` to control run frequency.

---

## License

No explicit license provided. Add your preferred license text here if you plan to distribute or open-source the project.

