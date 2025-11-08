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

## Configuration (`weather_config.json`)

Key sections:

- **`providers`** – One entry per supported API with credentials, base URLs, default units, and optional `batchSize`.
- **`locations`** – Named latitude/longitude pairs reused across notebooks and scripts (e.g. `"boston_ma": {"lat": 42.3601, "lon": -71.0589}`).

> **Tip:** Add or adjust locations here and both the notebooks and export script will automatically process all defined sites.

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

The repository targets Python 3.9+ (matching the `.venv` metadata). If running in headless environments, note that `matplotlib` is configured to use the `Agg` backend for image generation.

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

