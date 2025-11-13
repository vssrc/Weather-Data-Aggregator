#!/usr/bin/env python3
"""
Automated weather data exporter.

Mirrors the logic in weather_data_export.ipynb while running provider exports
concurrently, persisting verbose logs, and looping with a cool down between runs.
"""

import datetime as dt
import argparse
import sys
import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple, Union

import matplotlib
try:
    from tqdm.auto import tqdm
except ImportError:  # pragma: no cover - runtime fallback when tqdm missing
    tqdm = None  # type: ignore[assignment]

# Ensure headless rendering is supported before importing pyplot.
matplotlib.use("Agg")

import matplotlib.pyplot as plt  # noqa: E402  (import after backend selection)
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from clients.copernicus_cds_client import CopernicusCdsClient
from clients.iem_asos_client import IemAsosClient
from clients.meteostat_client import MeteostatClient
from clients.nasa_power_client import NasaPowerClient
from clients.noaa_access_client import NoaaIsdClient, NoaaLcdClient
from clients.open_meteo_client import OpenMeteoClient
from clients.openweather_client import OpenWeatherClient
from clients.tomorrow_io_client import TomorrowIOClient
from clients.visual_crossing_client import VisualCrossingClient
from clients.weatherapi_com_client import WeatherApiClient
from clients.weatherbit_client import WeatherbitClient


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
CONFIG_PATH = Path("weather_config.json")
DATA_ROOT = Path("data")
LOG_DIR = Path("logs")
LOG_FILE = LOG_DIR / "weather_export.log"
CACHE_IMAGE_PATH = DATA_ROOT / "cache_coverage.png"

START_DATE = dt.date(2000, 1, 1)
END_DATE = dt.date(2025, 11, 5)
COOLDOWN_SECONDS = 300  # 5 minutes
MAX_PENDING_REQUESTS_PER_FLUSH = 25  # safety cap for buffered API requests

# Provider toggles (mirror notebook defaults)
USE_TOMORROW_IO = False
USE_OPEN_METEO = False
USE_VISUAL_CROSSING = False
USE_NOAA_ISD = False
USE_NOAA_LCD = False
USE_METEOSTAT = False
USE_NASA_POWER = False
USE_IEM_ASOS = False
USE_COPERNICUS_ERA5_SINGLE = True
USE_COPERNICUS_ERA5_LAND = True
USE_COPERNICUS_ERA5_PRESSURE = True
USE_COPERNICUS_ERA5_LAND_TS = True
USE_OPENWEATHER = False
USE_WEATHERBIT = False
USE_WEATHERAPI_COM = False

# Runtime overrides (set via CLI in main())
SKIP_COVERAGE: bool = False
PROVIDER_INCLUDE: Optional[set] = None  # if set, run only these providers
LOCATION_LIMIT: Optional[int] = None

# Ensure essential directories exist.
DATA_ROOT.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

# Configure logging (file + console)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
    force=True,
)

if tqdm is None:
    class _DummyTqdm:  # pragma: no cover - minimal fallback when tqdm missing
        def __init__(self, *args, **kwargs):
            self.total = kwargs.get("total")

        def __enter__(self):
            if self.total:
                logging.info("tqdm not installed; progress bars disabled.")
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            return False

        def update(self, *_args, **_kwargs):
            return None

    def tqdm(*args, **kwargs):  # type: ignore[override, no-redef]
        return _DummyTqdm(*args, **kwargs)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
LOCATION_EXTRAS: Dict[str, Dict[str, object]] = {}


def load_locations() -> List[Tuple[str, float, float]]:
    config = json.loads(CONFIG_PATH.read_text())
    locations = config.get("locations", {})
    result: List[Tuple[str, float, float]] = []
    extras: Dict[str, Dict[str, object]] = {}
    for name, coords in locations.items():
        try:
            lat = float(coords["lat"])
            lon = float(coords["lon"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"Invalid coordinates for location '{name}'.") from exc
        result.append((name, lat, lon))
        extra_fields = {key: value for key, value in coords.items() if key not in {"lat", "lon"}}
        extras[name] = extra_fields
    if not result:
        raise ValueError("Define at least one location under 'locations' in weather_config.json.")
    global LOCATION_EXTRAS  # pylint: disable=global-statement
    LOCATION_EXTRAS = extras
    return result


def iter_days(start: dt.date, end: dt.date) -> Iterable[dt.date]:
    day = start
    while day <= end:
        yield day
        day += dt.timedelta(days=1)


LOCATION_ITEMS = load_locations()
DAY_RANGE = list(iter_days(START_DATE, END_DATE))


def _set_date_range(since: Optional[str], until: Optional[str]) -> None:
    """Override global START_DATE/END_DATE and rebuild DAY_RANGE."""
    global START_DATE, END_DATE, DAY_RANGE
    if since:
        START_DATE = dt.date.fromisoformat(since)
    if until:
        END_DATE = dt.date.fromisoformat(until)
    if END_DATE < START_DATE:
        raise ValueError("--until must be on/after --since")
    DAY_RANGE = list(iter_days(START_DATE, END_DATE))


def _apply_location_limit(limit: Optional[int]) -> None:
    """Reload locations and apply an optional limit to reduce runtime."""
    global LOCATION_ITEMS
    LOCATION_ITEMS = load_locations()
    if limit is not None and limit > 0:
        LOCATION_ITEMS = LOCATION_ITEMS[:limit]


def _rebuild_provider_flags() -> None:
    """Rebuild PROVIDER_FLAGS from current USE_* globals."""
    global PROVIDER_FLAGS
    PROVIDER_FLAGS = {
        "tomorrow_io": USE_TOMORROW_IO,
        "open_meteo": USE_OPEN_METEO,
        "visual_crossing": USE_VISUAL_CROSSING,
        "noaa_isd": USE_NOAA_ISD,
        "noaa_lcd": USE_NOAA_LCD,
        "meteostat": USE_METEOSTAT,
        "nasa_power": USE_NASA_POWER,
        "iem_asos": USE_IEM_ASOS,
        "copernicus_era5_single": USE_COPERNICUS_ERA5_SINGLE,
        "copernicus_era5_land": USE_COPERNICUS_ERA5_LAND,
        "copernicus_era5_pressure": USE_COPERNICUS_ERA5_PRESSURE,
        "copernicus_era5_land_timeseries": USE_COPERNICUS_ERA5_LAND_TS,
        "openweather": USE_OPENWEATHER,
        "weatherbit": USE_WEATHERBIT,
        "weatherapi_com": USE_WEATHERAPI_COM,
    }


def _force_enable_toggles(keys: Optional[set]) -> None:
    """Force-enable provider toggle globals for any keys provided."""
    if not keys:
        return
    global USE_TOMORROW_IO, USE_OPEN_METEO, USE_VISUAL_CROSSING, USE_NOAA_ISD, USE_NOAA_LCD
    global USE_METEOSTAT, USE_NASA_POWER, USE_IEM_ASOS, USE_COPERNICUS_ERA5_SINGLE
    global USE_COPERNICUS_ERA5_LAND, USE_COPERNICUS_ERA5_PRESSURE, USE_COPERNICUS_ERA5_LAND_TS
    global USE_OPENWEATHER, USE_WEATHERBIT, USE_WEATHERAPI_COM

    if "tomorrow_io" in keys:
        USE_TOMORROW_IO = True
    if "open_meteo" in keys:
        USE_OPEN_METEO = True
    if "visual_crossing" in keys:
        USE_VISUAL_CROSSING = True
    if "noaa_isd" in keys:
        USE_NOAA_ISD = True
    if "noaa_lcd" in keys:
        USE_NOAA_LCD = True
    if "meteostat" in keys:
        USE_METEOSTAT = True
    if "nasa_power" in keys:
        USE_NASA_POWER = True
    if "iem_asos" in keys:
        USE_IEM_ASOS = True
    if "copernicus_era5_single" in keys:
        USE_COPERNICUS_ERA5_SINGLE = True
    if "copernicus_era5_land" in keys:
        USE_COPERNICUS_ERA5_LAND = True
    if "copernicus_era5_pressure" in keys:
        USE_COPERNICUS_ERA5_PRESSURE = True
    if "copernicus_era5_land_timeseries" in keys:
        USE_COPERNICUS_ERA5_LAND_TS = True
    if "openweather" in keys:
        USE_OPENWEATHER = True
    if "weatherbit" in keys:
        USE_WEATHERBIT = True
    if "weatherapi_com" in keys:
        USE_WEATHERAPI_COM = True
UTC = dt.timezone.utc


def summarise_results(provider_key: str, saved: int, skipped: int, errors: int) -> str:
    return (
        f"{provider_key}: saved={saved}, skipped={skipped}, "
        f"errors={errors}, locations={len(LOCATION_ITEMS)}"
    )


def compute_flush_threshold(batch_limit: int) -> int:
    """Clamp buffered batch size to avoid overwhelming downstream APIs."""
    return max(1, min(batch_limit, MAX_PENDING_REQUESTS_PER_FLUSH))


def _iter_date_windows(start: dt.date, end: dt.date, window_days: int) -> Iterable[Tuple[dt.date, dt.date]]:
    if window_days <= 0:
        window_days = 1
    cur = start
    while cur <= end:
        wnd_end = min(end, cur + dt.timedelta(days=window_days - 1))
        yield cur, wnd_end
        cur = wnd_end + dt.timedelta(days=1)


def _provider_max_days(provider_key: str, fallback: int) -> int:
    """Read maxDaysPerRequest from weather_config.json with a safe fallback."""
    try:
        cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        providers = cfg.get("providers", {})
        raw = providers.get(provider_key, {}).get("maxDaysPerRequest")
        if raw is None:
            return fallback
        days = int(raw)
        return max(1, days)
    except Exception:
        return max(1, fallback)


# ---------------------------------------------------------------------------
# Provider export implementations
# ---------------------------------------------------------------------------
def export_tomorrow_io() -> str:
    if not USE_TOMORROW_IO:
        logging.info("Tomorrow.io disabled; skipping.")
        return "Tomorrow.io disabled"

    provider_dir = DATA_ROOT / "tomorrow_io"
    provider_dir.mkdir(parents=True, exist_ok=True)

    try:
        client = TomorrowIOClient(config_path=CONFIG_PATH)
    except Exception as exc:  # pylint: disable=broad-except
        logging.exception("Tomorrow.io client initialisation failed: %s", exc)
        return "Tomorrow.io initialisation failed"

    fields = [
        "temperature",
        "temperatureApparent",
        "humidity",
        "dewPoint",
        "pressureSurfaceLevel",
        "pressureMeanSeaLevel",
        "visibility",
        "cloudCover",
        "uvIndex",
        "windSpeed",
        "windGust",
        "windDirection",
        "precipitationIntensity",
        "precipitationProbability",
        "precipitationType",
        "rainIntensity",
        "snowIntensity",
        "iceAccumulation",
        "solarGHI",
        "weatherCode",
    ]
    timesteps = ["5m", "1h"]
    today = dt.date.today()

    saved = skipped = errors = 0

    def _payload_to_df(payload: dict, day: dt.date) -> pd.DataFrame:
        rows = []
        for timeline in payload.get("data", {}).get("timelines", []):
            timestep = timeline.get("timestep")
            for interval in timeline.get("intervals", []):
                values = interval.get("values", {})
                row = {"timestamp": interval.get("startTime"), "timestep": timestep}
                row.update(values)
                rows.append(row)
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df[df["timestamp"].dt.date == day]
        return df.sort_values("timestamp")

    with provider_progress("tomorrow_io") as progress:
        for location_key, lat, lon in LOCATION_ITEMS:
            prefix = f"Tomorrow.io[{location_key}]"
            location_dir = provider_dir / location_key
            location_dir.mkdir(parents=True, exist_ok=True)

            historical_days: List[dt.date] = []
            historical_requests: List[Dict[str, object]] = []
            forecast_days: List[dt.date] = []
            forecast_requests: List[Dict[str, object]] = []
            batch_limit = getattr(client, "batch_size", 50) or 50
            flush_threshold = compute_flush_threshold(batch_limit)

            def _flush_requests(
                day_buffer: List[dt.date],
                request_buffer: List[Dict[str, object]],
                fetch_fn,
                label: str,
            ) -> None:
                nonlocal saved, skipped, errors
                if not request_buffer:
                    return
                call_count = len(request_buffer)
                progress.add_expected(call_count)
                try:
                    payloads = fetch_fn(list(request_buffer))
                except Exception:
                    progress.complete(call_count)
                    raise
                progress.complete(call_count)
                for req_day, payload in zip(day_buffer, payloads):
                    output_path = location_dir / f"{req_day.isoformat()}.csv"
                    if isinstance(payload, Exception):
                        errors += 1
                        logging.warning("%s %s failed for %s: %s", prefix, label, req_day, payload)
                        continue
                    try:
                        df_day = _payload_to_df(payload, req_day)
                        if df_day.empty:
                            logging.info("%s no %s data for %s", prefix, label, req_day)
                            skipped += 1
                            continue
                        df_day.to_csv(output_path, index=False)
                        saved += 1
                        logging.info("%s wrote %s", prefix, output_path)
                    except Exception as exc:  # pylint: disable=broad-except
                        errors += 1
                        logging.exception("%s unexpected %s error for %s: %s", prefix, label, req_day, exc)
                day_buffer.clear()
                request_buffer.clear()

            for day in DAY_RANGE:
                if day < today - dt.timedelta(days=1):
                    logging.debug("%s skipping %s (plan permits only last 24 hours).", prefix, day)
                    skipped += 1
                    continue

                output_path = location_dir / f"{day.isoformat()}.csv"
                if day != today and output_path.exists():
                    logging.debug("%s skipping %s (already exported).", prefix, day)
                    skipped += 1
                    continue

                start = dt.datetime.combine(day, dt.time(0, 0), tzinfo=UTC)
                end = start + dt.timedelta(days=1)

                request = {
                    "location": (lat, lon),
                    "start_time": start,
                    "end_time": end,
                    "fields": fields,
                    "timesteps": timesteps,
                    "timezone": "UTC",
                }

                if day < today:
                    historical_days.append(day)
                    historical_requests.append(dict(request))
                    if len(historical_requests) >= flush_threshold:
                        _flush_requests(historical_days, historical_requests, client.get_historical_batch, "historical")
                else:
                    forecast_days.append(day)
                    forecast_requests.append(dict(request))
                    if len(forecast_requests) >= flush_threshold:
                        _flush_requests(forecast_days, forecast_requests, client.get_forecast_batch, "forecast")

            _flush_requests(historical_days, historical_requests, client.get_historical_batch, "historical")
            _flush_requests(forecast_days, forecast_requests, client.get_forecast_batch, "forecast")

    return summarise_results("tomorrow_io", saved, skipped, errors)


def export_open_meteo() -> str:
    if not USE_OPEN_METEO:
        logging.info("Open-Meteo disabled; skipping.")
        return "Open-Meteo disabled"

    provider_dir = DATA_ROOT / "open_meteo"
    provider_dir.mkdir(parents=True, exist_ok=True)

    try:
        client = OpenMeteoClient(config_path=CONFIG_PATH)
    except Exception as exc:  # pylint: disable=broad-except
        logging.exception("Open-Meteo client initialisation failed: %s", exc)
        return "Open-Meteo initialisation failed"

    hourly = [
        "temperature_2m",
        "relative_humidity_2m",
        "dew_point_2m",
        "apparent_temperature",
        "pressure_msl",
        "surface_pressure",
        "cloud_cover",
        "cloud_cover_low",
        "cloud_cover_mid",
        "cloud_cover_high",
        "wind_speed_10m",
        "wind_direction_10m",
        "wind_gusts_10m",
        "shortwave_radiation",
        "direct_radiation",
        "diffuse_radiation",
        "global_tilted_irradiance",
        "sunshine_duration",
        "precipitation",
        "rain",
        "snowfall",
        "weather_code",
        "soil_temperature_0cm",
        "soil_moisture_0_1cm",
    ]
    today = dt.date.today()

    saved = skipped = errors = 0

    def _payload_to_df(payload: dict, day: dt.date) -> pd.DataFrame:
        hourly_data = payload.get("hourly", {})
        if not hourly_data:
            return pd.DataFrame()
        df = pd.DataFrame(hourly_data)
        if "time" not in df:
            return pd.DataFrame()
        df.rename(columns={"time": "timestamp"}, inplace=True)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df[df["timestamp"].dt.date == day]
        return df.sort_values("timestamp")

    with provider_progress("open_meteo") as progress:
        for location_key, lat, lon in LOCATION_ITEMS:
            prefix = f"Open-Meteo[{location_key}]"
            location_dir = provider_dir / location_key
            location_dir.mkdir(parents=True, exist_ok=True)

            # 1) Historical: fetch missing days in contiguous spans using range API
            def _om_fetch(params: Dict[str, object]) -> dict:
                s: dt.date = params["start"]  # type: ignore[index]
                e: dt.date = params["end"]  # type: ignore[index]
                return client.get_historical(
                    location=(lat, lon), start_date=s, end_date=e, hourly=hourly, timezone="UTC"
                )

            def _om_handle(payload: dict, s: dt.date, e: dt.date) -> Tuple[int, int, int]:
                _saved = _skipped = _errors = 0
                for offset in range(_date_span_days(s, e)):
                    day = s + dt.timedelta(days=offset)
                    output_path = location_dir / f"{day.isoformat()}.csv"
                    try:
                        df_day = _payload_to_df(payload, day)
                        if df_day.empty:
                            logging.info("%s no historical data for %s", prefix, day)
                            _skipped += 1
                            continue
                        df_day.to_csv(output_path, index=False)
                        _saved += 1
                        logging.info("%s wrote %s", prefix, output_path)
                    except Exception as exc:  # pylint: disable=broad-except
                        _errors += 1
                        logging.exception("%s unexpected historical error for %s: %s", prefix, day, exc)
                return _saved, _skipped, _errors

            spans = _group_missing_day_spans(location_dir)
            for s, e in spans:
                if e < today:
                    # Open-Meteo: window size is configurable
                    window_days = _provider_max_days('open_meteo', 30)
                    for ws, we in _iter_date_windows(s, e, window_days):
                        _fetch_range_recursive(
                            start=ws,
                            end=we,
                            fetch_fn=_om_fetch,
                            request_kwargs={},
                            handle_payload=_om_handle,
                            progress=progress,
                        )

            # 2) Forecast/near-present: fetch day-by-day using forecast endpoint
            for day in DAY_RANGE:
                if day < today:
                    continue
                output_path = location_dir / f"{day.isoformat()}.csv"
                if day != today and output_path.exists():
                    logging.debug("%s skipping %s (already exported).", prefix, day)
                    skipped += 1
                    continue
                try:
                    progress.add_expected(1)
                    payload = client.get_forecast(
                        location=(lat, lon), start_date=day, end_date=day, hourly=hourly, timezone="UTC"
                    )
                    progress.complete(1)
                except Exception as exc:
                    progress.complete(1)
                    errors += 1
                    logging.warning("%s forecast request failed for %s: %s", prefix, day, exc)
                    continue
                try:
                    df_day = _payload_to_df(payload, day)
                    if df_day.empty:
                        logging.info("%s no forecast data for %s", prefix, day)
                        skipped += 1
                        continue
                    df_day.to_csv(output_path, index=False)
                    saved += 1
                    logging.info("%s wrote %s", prefix, output_path)
                except Exception as exc:  # pylint: disable=broad-except
                    errors += 1
                    logging.exception("%s unexpected forecast error for %s: %s", prefix, day, exc)

    return summarise_results("open_meteo", saved, skipped, errors)


def export_visual_crossing() -> str:
    if not USE_VISUAL_CROSSING:
        logging.info("Visual Crossing disabled; skipping.")
        return "Visual Crossing disabled"

    provider_dir = DATA_ROOT / "visual_crossing"
    provider_dir.mkdir(parents=True, exist_ok=True)

    try:
        client = VisualCrossingClient(config_path=CONFIG_PATH)
    except Exception as exc:  # pylint: disable=broad-except
        logging.exception("Visual Crossing client initialisation failed: %s", exc)
        return "Visual Crossing initialisation failed"

    today = dt.date.today()
    max_forecast_day = today + dt.timedelta(days=15)

    saved = skipped = errors = 0

    def _payload_to_df(payload: dict, day: dt.date) -> pd.DataFrame:
        hours = []
        for daily in payload.get("days", []):
            day_date = daily.get("datetime")
            for entry in daily.get("hours", []):
                row = dict(entry)
                row["parent_day"] = day_date
                epoch = entry.get("datetimeEpoch")
                if epoch is not None:
                    row["timestamp"] = pd.to_datetime(epoch, unit="s", utc=True)
                elif entry.get("datetime") is not None and day_date is not None:
                    row["timestamp"] = pd.to_datetime(f"{day_date}T{entry['datetime']}")
                hours.append(row)
        if not hours:
            return pd.DataFrame()
        df = pd.DataFrame(hours)
        if "timestamp" not in df:
            return pd.DataFrame()
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df = df[df["timestamp"].dt.date == day]
        return df.sort_values("timestamp")

    with provider_progress("visual_crossing") as progress:
        for location_key, lat, lon in LOCATION_ITEMS:
            prefix = f"Visual Crossing[{location_key}]"
            location_dir = provider_dir / location_key
            location_dir.mkdir(parents=True, exist_ok=True)

            # Historical in contiguous spans via timeline API
            def _vc_fetch(params: Dict[str, object]) -> dict:
                s: dt.date = params["start"]  # type: ignore[index]
                e: dt.date = params["end"]  # type: ignore[index]
                return client.get_historical(
                    location=f"{lat},{lon}", start=s, end=e, include=["hours"], unit_group="metric"
                )

            def _vc_handle(payload: dict, s: dt.date, e: dt.date) -> Tuple[int, int, int]:
                _saved = _skipped = _errors = 0
                for offset in range(_date_span_days(s, e)):
                    day = s + dt.timedelta(days=offset)
                    output_path = location_dir / f"{day.isoformat()}.csv"
                    try:
                        df_day = _payload_to_df(payload, day)
                        if df_day.empty:
                            logging.info("%s no historical data for %s", prefix, day)
                            _skipped += 1
                            continue
                        df_day.to_csv(output_path, index=False)
                        _saved += 1
                        logging.info("%s wrote %s", prefix, output_path)
                    except Exception as exc:  # pylint: disable=broad-except
                        _errors += 1
                        logging.exception("%s unexpected historical error for %s: %s", prefix, day, exc)
                return _saved, _skipped, _errors

            spans = _group_missing_day_spans(location_dir)
            for s, e in spans:
                if e < today:
                    window_days = _provider_max_days('visual_crossing', 30)
                    for ws, we in _iter_date_windows(s, e, window_days):
                        _fetch_range_recursive(
                            start=ws,
                            end=we,
                            fetch_fn=_vc_fetch,
                            request_kwargs={},
                            handle_payload=_vc_handle,
                            progress=progress,
                        )

            # Forecast path: keep per-day to respect forecast constraints
            forecast_days: List[dt.date] = []
            forecast_requests: List[Dict[str, object]] = []
            batch_limit = getattr(client, "batch_size", 50) or 50
            flush_threshold = compute_flush_threshold(batch_limit)

            def _flush_requests(
                day_buffer: List[dt.date],
                request_buffer: List[Dict[str, object]],
                fetch_fn,
                label: str,
            ) -> None:
                nonlocal saved, skipped, errors
                if not request_buffer:
                    return
                call_count = len(request_buffer)
                progress.add_expected(call_count)
                try:
                    payloads = fetch_fn(list(request_buffer))
                except Exception:
                    progress.complete(call_count)
                    raise
                progress.complete(call_count)
                for req_day, payload in zip(day_buffer, payloads):
                    output_path = location_dir / f"{req_day.isoformat()}.csv"
                    if isinstance(payload, Exception):
                        errors += 1
                        logging.warning("%s %s failed for %s: %s", prefix, label, req_day, payload)
                        continue
                    try:
                        df_day = _payload_to_df(payload, req_day)
                        if df_day.empty:
                            logging.info("%s no %s data for %s", prefix, label, req_day)
                            skipped += 1
                            continue
                        df_day.to_csv(output_path, index=False)
                        saved += 1
                        logging.info("%s wrote %s", prefix, output_path)
                    except Exception as exc:  # pylint: disable=broad-except
                        errors += 1
                        logging.exception("%s unexpected %s error for %s: %s", prefix, label, req_day, exc)
                day_buffer.clear()
                request_buffer.clear()

            for day in DAY_RANGE:
                if day > max_forecast_day:
                    logging.debug("%s skipping %s (beyond forecast horizon).", prefix, day)
                    skipped += 1
                    continue

                output_path = location_dir / f"{day.isoformat()}.csv"
                if day != today and output_path.exists():
                    logging.debug("%s skipping %s (already exported).", prefix, day)
                    skipped += 1
                    continue

                start = day
                end = day + dt.timedelta(days=1)

                request = {
                    "location": f"{lat},{lon}",
                    "start": start,
                    "end": end,
                    "include": ["hours"],
                    "unit_group": "metric",
                }

                if day < today:
                    historical_days.append(day)
                    historical_requests.append(dict(request))
                    if len(historical_requests) >= flush_threshold:
                        _flush_requests(historical_days, historical_requests, client.get_historical_batch, "historical")
                else:
                    forecast_days.append(day)
                    forecast_requests.append(dict(request))
                    if len(forecast_requests) >= flush_threshold:
                        _flush_requests(forecast_days, forecast_requests, client.get_forecast_batch, "forecast")

            _flush_requests(historical_days, historical_requests, client.get_historical_batch, "historical")
            _flush_requests(forecast_days, forecast_requests, client.get_forecast_batch, "forecast")

    return summarise_results("visual_crossing", saved, skipped, errors)


def _parse_isd_temperature(value: Optional[str]) -> Optional[float]:
    if not value or value in {"", "+9999,9", "-9999,9"}:
        return None
    token = value.strip()
    if not token:
        return None
    sign = -1 if token.startswith("-") else 1
    digits = "".join(ch for ch in token if ch.isdigit())
    if not digits:
        return None
    try:
        numeric = int(digits[:4])
    except ValueError:
        return None
    return sign * numeric / 10.0


def _to_float(value: Optional[Union[str, float, int]]) -> Optional[float]:
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    try:
        return float(str(value).replace(",", ""))
    except ValueError:
        return None


def export_noaa_isd() -> str:
    if not USE_NOAA_ISD:
        logging.info("NOAA ISD disabled; skipping.")
        return "NOAA ISD disabled"

    provider_dir = DATA_ROOT / "noaa_isd"
    provider_dir.mkdir(parents=True, exist_ok=True)

    try:
        client = NoaaIsdClient(config_path=CONFIG_PATH)
    except Exception as exc:  # pylint: disable=broad-except
        logging.exception("NOAA ISD client initialisation failed: %s", exc)
        return "NOAA ISD initialisation failed"

    today = dt.date.today()
    saved = skipped = errors = 0

    def _payload_to_df(payload: List[dict], day: dt.date) -> pd.DataFrame:
        if not payload:
            return pd.DataFrame()
        df = pd.DataFrame(payload)
        if "DATE" not in df.columns:
            return pd.DataFrame()
        df["timestamp"] = pd.to_datetime(df["DATE"], errors="coerce")
        df = df.dropna(subset=["timestamp"])
        df = df[df["timestamp"].dt.date == day]
        if df.empty:
            return df
        df = df.sort_values("timestamp")
        if "TMP" in df.columns:
            df["temperature_c"] = df["TMP"].apply(_parse_isd_temperature)
        if "DEW" in df.columns:
            df["dewpoint_c"] = df["DEW"].apply(_parse_isd_temperature)
        if "SLP" in df.columns:
            df["slp_hpa"] = df["SLP"].apply(_to_float)
        return df

    with provider_progress("noaa_isd") as progress:
        for location_key, _lat, _lon in LOCATION_ITEMS:
            station_id = LOCATION_EXTRAS.get(location_key, {}).get("noaaIsdStation")
            if not station_id:
                logging.warning("NOAA ISD missing station for %s; skipping.", location_key)
                continue
            prefix = f"NOAA ISD[{location_key}]"
            location_dir = provider_dir / location_key
            location_dir.mkdir(parents=True, exist_ok=True)

            def _isd_fetch(params: Dict[str, object]) -> List[dict]:
                s: dt.date = params["start"]  # type: ignore[index]
                e: dt.date = params["end"]  # type: ignore[index]
                return client.get_observations(
                    station_id=station_id,
                    start_time=dt.datetime.combine(s, dt.time(0, 0), tzinfo=UTC),
                    end_time=dt.datetime.combine(e + dt.timedelta(days=1), dt.time(0, 0), tzinfo=UTC),
                )

            def _isd_handle(payload: List[dict], s: dt.date, e: dt.date) -> Tuple[int, int, int]:
                _saved = _skipped = _errors = 0
                for offset in range(_date_span_days(s, e)):
                    day = s + dt.timedelta(days=offset)
                    output_path = location_dir / f"{day.isoformat()}.csv"
                    try:
                        df_day = _payload_to_df(payload, day)
                        if df_day.empty:
                            logging.info("%s no data for %s", prefix, day)
                            _skipped += 1
                            continue
                        df_day.to_csv(output_path, index=False)
                        _saved += 1
                        logging.info("%s wrote %s", prefix, output_path)
                    except Exception as exc:  # pylint: disable=broad-except
                        _errors += 1
                        logging.exception("%s unexpected error for %s: %s", prefix, day, exc)
                return _saved, _skipped, _errors

            spans = _group_missing_day_spans(location_dir)
            for s, e in spans:
                if s > today:
                    continue
                window_days = _provider_max_days('noaa_isd', 7)
                for ws, we in _iter_date_windows(s, min(e, today), window_days):
                    _fetch_range_recursive(
                        start=ws,
                        end=we,
                        fetch_fn=_isd_fetch,
                        request_kwargs={},
                        handle_payload=_isd_handle,
                        progress=progress,
                    )

    return summarise_results("noaa_isd", saved, skipped, errors)


def _fahrenheit_to_celsius(value: Optional[Union[str, float, int]]) -> Optional[float]:
    numeric = _to_float(value)
    if numeric is None:
        return None
    return (numeric - 32.0) * 5.0 / 9.0


def _mph_to_mps(value: Optional[Union[str, float, int]]) -> Optional[float]:
    numeric = _to_float(value)
    if numeric is None:
        return None
    return numeric * 0.44704


def _knots_to_mps(value: Optional[Union[str, float, int]]) -> Optional[float]:
    numeric = _to_float(value)
    if numeric is None:
        return None
    return numeric * 0.514444


def _inches_to_mm(value: Optional[Union[str, float, int]]) -> Optional[float]:
    numeric = _to_float(value)
    if numeric is None:
        return None
    return numeric * 25.4


def _kelvin_to_celsius(value: Optional[Union[str, float, int]]) -> Optional[float]:
    numeric = _to_float(value)
    if numeric is None:
        return None
    return numeric - 273.15


def export_noaa_lcd() -> str:
    if not USE_NOAA_LCD:
        logging.info("NOAA LCD disabled; skipping.")
        return "NOAA LCD disabled"

    provider_dir = DATA_ROOT / "noaa_lcd"
    provider_dir.mkdir(parents=True, exist_ok=True)

    try:
        client = NoaaLcdClient(config_path=CONFIG_PATH)
    except Exception as exc:  # pylint: disable=broad-except
        logging.exception("NOAA LCD client initialisation failed: %s", exc)
        return "NOAA LCD initialisation failed"

    today = dt.date.today()
    saved = skipped = errors = 0

    def _payload_to_df(payload: List[dict], day: dt.date) -> pd.DataFrame:
        if not payload:
            return pd.DataFrame()
        df = pd.DataFrame(payload)
        if "DATE" not in df.columns:
            return pd.DataFrame()
        df["timestamp"] = pd.to_datetime(df["DATE"], errors="coerce")
        df = df.dropna(subset=["timestamp"])
        df = df[df["timestamp"].dt.date == day]
        if df.empty:
            return df
        df = df.sort_values("timestamp")
        if "HourlyDryBulbTemperature" in df.columns:
            df["temperature_f"] = pd.to_numeric(df["HourlyDryBulbTemperature"], errors="coerce")
            df["temperature_c"] = df["temperature_f"].apply(_fahrenheit_to_celsius)
        if "HourlyDewPointTemperature" in df.columns:
            df["dewpoint_f"] = pd.to_numeric(df["HourlyDewPointTemperature"], errors="coerce")
            df["dewpoint_c"] = df["dewpoint_f"].apply(_fahrenheit_to_celsius)
        return df

    with provider_progress("noaa_lcd") as progress:
        for location_key, _lat, _lon in LOCATION_ITEMS:
            station_id = LOCATION_EXTRAS.get(location_key, {}).get("noaaLcdStation")
            if not station_id:
                logging.warning("NOAA LCD missing station for %s; skipping.", location_key)
                continue
            prefix = f"NOAA LCD[{location_key}]"
            location_dir = provider_dir / location_key
            location_dir.mkdir(parents=True, exist_ok=True)

            def _lcd_fetch(params: Dict[str, object]) -> List[dict]:
                s: dt.date = params["start"]  # type: ignore[index]
                e: dt.date = params["end"]  # type: ignore[index]
                return client.get_observations(
                    station_id=station_id,
                    start_time=dt.datetime.combine(s, dt.time(0, 0), tzinfo=UTC),
                    end_time=dt.datetime.combine(e + dt.timedelta(days=1), dt.time(0, 0), tzinfo=UTC),
                )

            def _lcd_handle(payload: List[dict], s: dt.date, e: dt.date) -> Tuple[int, int, int]:
                _saved = _skipped = _errors = 0
                for offset in range(_date_span_days(s, e)):
                    day = s + dt.timedelta(days=offset)
                    output_path = location_dir / f"{day.isoformat()}.csv"
                    try:
                        df_day = _payload_to_df(payload, day)
                        if df_day.empty:
                            logging.info("%s no data for %s", prefix, day)
                            _skipped += 1
                            continue
                        df_day.to_csv(output_path, index=False)
                        _saved += 1
                        logging.info("%s wrote %s", prefix, output_path)
                    except Exception as exc:  # pylint: disable=broad-except
                        _errors += 1
                        logging.exception("%s unexpected error for %s: %s", prefix, day, exc)
                return _saved, _skipped, _errors

            spans = _group_missing_day_spans(location_dir)
            for s, e in spans:
                if s > today:
                    continue
                window_days = _provider_max_days('noaa_lcd', 31)
                for ws, we in _iter_date_windows(s, min(e, today), window_days):
                    _fetch_range_recursive(
                        start=ws,
                        end=we,
                        fetch_fn=_lcd_fetch,
                        request_kwargs={},
                        handle_payload=_lcd_handle,
                        progress=progress,
                    )

    return summarise_results("noaa_lcd", saved, skipped, errors)


def export_meteostat() -> str:
    if not USE_METEOSTAT:
        logging.info("Meteostat disabled; skipping.")
        return "Meteostat disabled"

    provider_dir = DATA_ROOT / "meteostat"
    provider_dir.mkdir(parents=True, exist_ok=True)

    try:
        client = MeteostatClient(config_path=CONFIG_PATH)
    except Exception as exc:  # pylint: disable=broad-except
        logging.exception("Meteostat client initialisation failed: %s", exc)
        return "Meteostat initialisation failed"

    today = dt.date.today()
    saved = skipped = errors = 0

    def _payload_to_df(payload: List[dict], day: dt.date) -> pd.DataFrame:
        if not payload:
            return pd.DataFrame()
        df = pd.DataFrame(payload)
        if "timestamp" not in df.columns:
            if "time" in df.columns:
                df["timestamp"] = pd.to_datetime(df["time"])
            else:
                return pd.DataFrame()
        else:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.dropna(subset=["timestamp"])
        df = df[df["timestamp"].dt.date == day]
        if df.empty:
            return df
        df = df.sort_values("timestamp")
        df.rename(
            columns={
                "temp": "temperature_c",
                "dwpt": "dewpoint_c",
                "pres": "pressure_kpa",
            },
            inplace=True,
        )
        return df

    with provider_progress("meteostat") as progress:
        for location_key, lat, lon in LOCATION_ITEMS:
            prefix = f"Meteostat[{location_key}]"
            location_dir = provider_dir / location_key
            location_dir.mkdir(parents=True, exist_ok=True)

            def _ms_fetch(params: Dict[str, object]) -> List[dict]:
                s: dt.date = params["start"]  # type: ignore[index]
                e: dt.date = params["end"]  # type: ignore[index]
                return client.get_hourly(location=(lat, lon), start_time=s, end_time=e)

            def _ms_handle(payload: List[dict], s: dt.date, e: dt.date) -> Tuple[int, int, int]:
                _saved = _skipped = _errors = 0
                for offset in range(_date_span_days(s, e)):
                    day = s + dt.timedelta(days=offset)
                    output_path = location_dir / f"{day.isoformat()}.csv"
                    try:
                        df_day = _payload_to_df(payload, day)
                        if df_day.empty:
                            logging.info("%s no data for %s", prefix, day)
                            _skipped += 1
                            continue
                        df_day.to_csv(output_path, index=False)
                        _saved += 1
                        logging.info("%s wrote %s", prefix, output_path)
                    except Exception as exc:  # pylint: disable=broad-except
                        _errors += 1
                        logging.exception("%s unexpected error for %s: %s", prefix, day, exc)
                return _saved, _skipped, _errors

            spans = _group_missing_day_spans(location_dir)
            for s, e in spans:
                if e <= today:
                    window_days = _provider_max_days('meteostat', 31)
                    for ws, we in _iter_date_windows(s, e, window_days):
                        _fetch_range_recursive(
                            start=ws,
                            end=we,
                            fetch_fn=_ms_fetch,
                            request_kwargs={},
                            handle_payload=_ms_handle,
                            progress=progress,
                        )

    return summarise_results("meteostat", saved, skipped, errors)


def export_nasa_power() -> str:
    if not USE_NASA_POWER:
        logging.info("NASA POWER disabled; skipping.")
        return "NASA POWER disabled"

    provider_dir = DATA_ROOT / "nasa_power"
    provider_dir.mkdir(parents=True, exist_ok=True)

    try:
        client = NasaPowerClient(config_path=CONFIG_PATH)
    except Exception as exc:  # pylint: disable=broad-except
        logging.exception("NASA POWER client initialisation failed: %s", exc)
        return "NASA POWER initialisation failed"

    today = dt.date.today()
    saved = skipped = errors = 0

    def _payload_to_df(payload: dict, day: dt.date) -> pd.DataFrame:
        props = payload.get("properties", {}) if isinstance(payload, dict) else {}
        parameters = props.get("parameter", {})
        if not parameters:
            return pd.DataFrame()
        rows: Dict[str, Dict[str, object]] = {}
        for param, series in parameters.items():
            if not isinstance(series, dict):
                continue
            for stamp, value in series.items():
                rows.setdefault(stamp, {})[param] = value
        if not rows:
            return pd.DataFrame()
        records = []
        for stamp, values in rows.items():
            try:
                timestamp = dt.datetime.strptime(str(stamp), "%Y%m%d%H").replace(tzinfo=dt.timezone.utc)
            except ValueError:
                continue
            if timestamp.date() != day:
                continue
            record = dict(values)
            record["timestamp"] = timestamp
            records.append(record)
        if not records:
            return pd.DataFrame()
        df = pd.DataFrame(records).sort_values("timestamp")
        rename_map = {
            "T2M": "temperature_c",
            "T2MDEW": "dewpoint_c",
            "RH2M": "rel_humidity_pct",
            "WS10M": "wind_speed_10m_m_s",
            "WD10M": "wind_dir_10m_deg",
            "PRECTOTCORR": "precip_mm_hr",
            "PS": "surface_pressure_kpa",
        }
        for raw, renamed in rename_map.items():
            if raw in df.columns:
                df.rename(columns={raw: renamed}, inplace=True)
        return df

    with provider_progress("nasa_power") as progress:
        for location_key, lat, lon in LOCATION_ITEMS:
            prefix = f"NASA POWER[{location_key}]"
            location_dir = provider_dir / location_key
            location_dir.mkdir(parents=True, exist_ok=True)

            def _np_fetch(params: Dict[str, object]) -> dict:
                s: dt.date = params["start"]  # type: ignore[index]
                e: dt.date = params["end"]  # type: ignore[index]
                return client.get_hourly(location=(lat, lon), start_time=s, end_time=e)

            def _np_handle(payload: dict, s: dt.date, e: dt.date) -> Tuple[int, int, int]:
                _saved = _skipped = _errors = 0
                for offset in range(_date_span_days(s, e)):
                    day = s + dt.timedelta(days=offset)
                    output_path = location_dir / f"{day.isoformat()}.csv"
                    try:
                        df_day = _payload_to_df(payload, day)
                        if df_day.empty:
                            logging.info("%s no NASA POWER data for %s", prefix, day)
                            _skipped += 1
                            continue
                        df_day.to_csv(output_path, index=False)
                        _saved += 1
                        logging.info("%s wrote %s", prefix, output_path)
                    except Exception as exc:  # pylint: disable=broad-except
                        _errors += 1
                        logging.exception("%s unexpected error for %s: %s", prefix, day, exc)
                return _saved, _skipped, _errors

            spans = _group_missing_day_spans(location_dir)
            for s, e in spans:
                window_days = _provider_max_days('nasa_power', 365)
                for ws, we in _iter_date_windows(s, e, window_days):
                    if ws > today:
                        continue
                    _fetch_range_recursive(
                        start=ws,
                        end=min(we, today),
                        fetch_fn=_np_fetch,
                        request_kwargs={},
                        handle_payload=_np_handle,
                        progress=progress,
                    )

    return summarise_results("nasa_power", saved, skipped, errors)


def export_iem_asos() -> str:
    if not USE_IEM_ASOS:
        logging.info("IEM ASOS disabled; skipping.")
        return "IEM ASOS disabled"

    provider_dir = DATA_ROOT / "iem_asos"
    provider_dir.mkdir(parents=True, exist_ok=True)

    try:
        client = IemAsosClient(config_path=CONFIG_PATH)
    except Exception as exc:  # pylint: disable=broad-except
        logging.exception("IEM ASOS client initialisation failed: %s", exc)
        return "IEM ASOS initialisation failed"

    today = dt.date.today()
    saved = skipped = errors = 0

    def _payload_to_df(payload: "pd.DataFrame", day: dt.date) -> pd.DataFrame:
        if payload is None or payload.empty:
            return pd.DataFrame()
        df = payload.copy()
        if "timestamp" not in df.columns:
            valid_col = next((col for col in df.columns if col.lower().startswith("valid")), None)
            if valid_col:
                df["timestamp"] = pd.to_datetime(df[valid_col], utc=True, errors="coerce")
        df = df.dropna(subset=["timestamp"])
        df = df[df["timestamp"].dt.date == day]
        if df.empty:
            return df
        df = df.sort_values("timestamp")
        rename_map = {
            "tmpf": "temperature_f",
            "dwpf": "dewpoint_f",
            "sknt": "wind_speed_knots",
            "drct": "wind_dir_deg",
            "gust_sknt": "wind_gust_knots",
            "precip": "precip_in",
            "pres1": "station_pressure_inhg",
        }
        df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True)
        if "temperature_f" in df.columns:
            df["temperature_c"] = df["temperature_f"].apply(_fahrenheit_to_celsius)
        if "dewpoint_f" in df.columns:
            df["dewpoint_c"] = df["dewpoint_f"].apply(_fahrenheit_to_celsius)
        if "wind_speed_knots" in df.columns:
            df["wind_speed_mps"] = df["wind_speed_knots"].apply(_knots_to_mps)
        if "wind_gust_knots" in df.columns:
            df["wind_gust_mps"] = df["wind_gust_knots"].apply(_knots_to_mps)
        if "precip_in" in df.columns:
            df["precip_mm"] = df["precip_in"].apply(_inches_to_mm)
        return df

    with provider_progress("iem_asos") as progress:
        for location_key, _lat, _lon in LOCATION_ITEMS:
            extras = LOCATION_EXTRAS.get(location_key, {})
            station_id = extras.get("iemStation")
            network = extras.get("iemNetwork")
            if not station_id or not network:
                logging.warning("IEM ASOS missing station/network for %s; skipping.", location_key)
                continue
            prefix = f"IEM ASOS[{station_id}]"
            location_dir = provider_dir / location_key
            location_dir.mkdir(parents=True, exist_ok=True)

            def _asos_fetch(params: Dict[str, object]) -> "pd.DataFrame":
                s: dt.date = params["start"]  # type: ignore[index]
                e: dt.date = params["end"]  # type: ignore[index]
                return client.get_observations(
                    station=station_id,
                    network=network,
                    start_time=s,
                    end_time=e,
                )

            def _asos_handle(payload: "pd.DataFrame", s: dt.date, e: dt.date) -> Tuple[int, int, int]:
                _saved = _skipped = _errors = 0
                for offset in range(_date_span_days(s, e)):
                    day = s + dt.timedelta(days=offset)
                    output_path = location_dir / f"{day.isoformat()}.csv"
                    try:
                        df_day = _payload_to_df(payload, day)
                        if df_day.empty:
                            logging.info("%s no data for %s", prefix, day)
                            _skipped += 1
                            continue
                        df_day.to_csv(output_path, index=False)
                        _saved += 1
                        logging.info("%s wrote %s", prefix, output_path)
                    except Exception as exc:  # pylint: disable=broad-except
                        _errors += 1
                        logging.exception("%s unexpected error for %s: %s", prefix, day, exc)
                return _saved, _skipped, _errors

            spans = _group_missing_day_spans(location_dir)
            for s, e in spans:
                if e <= today:
                    window_days = _provider_max_days('iem_asos', 7)
                    for ws, we in _iter_date_windows(s, e, window_days):
                        _fetch_range_recursive(
                            start=ws,
                            end=we,
                            fetch_fn=_asos_fetch,
                            request_kwargs={},
                            handle_payload=_asos_handle,
                            progress=progress,
                        )

    return summarise_results("iem_asos", saved, skipped, errors)


def _filter_day_dataframe(payload: "pd.DataFrame", day: dt.date) -> pd.DataFrame:
    if payload is None or payload.empty:
        return pd.DataFrame()
    df = payload.copy()
    if "timestamp" not in df.columns:
        return pd.DataFrame()
    df = df[df["timestamp"].dt.date == day]
    if df.empty:
        return df
    return df.sort_values("timestamp").reset_index(drop=True)


def _export_copernicus_provider(
    *,
    provider_key: str,
    enabled: bool,
    area_attr: Optional[str],
    transform_fn,
    label: str,
    requires_area: bool = True,
) -> str:
    if not enabled:
        logging.info("%s disabled; skipping.", label)
        return f"{label} disabled"

    provider_dir = DATA_ROOT / provider_key
    provider_dir.mkdir(parents=True, exist_ok=True)

    try:
        client = CopernicusCdsClient(config_path=CONFIG_PATH, provider=provider_key)
    except Exception as exc:  # pylint: disable=broad-except
        logging.exception("%s client initialisation failed: %s", label, exc)
        return f"{label} initialisation failed"

    batch_limit = getattr(client, "batch_size", 1) or 1
    del client

    today = dt.date.today()
    saved = skipped = errors = 0

    eligible_locations: List[Tuple[str, float, float, Optional[object]]] = []
    for location_key, lat, lon in LOCATION_ITEMS:
        extras = LOCATION_EXTRAS.get(location_key, {})
        area = extras.get(area_attr) if area_attr else None
        if requires_area and not area:
            logging.warning("%s missing %s for %s; skipping.", label, area_attr, location_key)
            continue
        eligible_locations.append((location_key, lat, lon, area))

    if not eligible_locations:
        logging.info("%s has no eligible locations; nothing to export.", label)
        return summarise_results(provider_key, saved, skipped, errors)

    with provider_progress(provider_key) as progress:
        progress_lock = threading.Lock()

        def _record_expected(count: int) -> None:
            if count <= 0:
                return
            with progress_lock:
                progress.add_expected(count)

        def _record_complete(count: int) -> None:
            if count <= 0:
                return
            with progress_lock:
                progress.complete(count)

        def _process_location(
            job: Tuple[str, float, float, Optional[object]]
        ) -> Tuple[int, int, int]:
            location_key, lat, lon, area = job
            prefix = f"{label}[{location_key}]"
            location_dir = provider_dir / location_key
            location_dir.mkdir(parents=True, exist_ok=True)

            try:
                local_client = CopernicusCdsClient(config_path=CONFIG_PATH, provider=provider_key)
            except Exception as exc:  # pylint: disable=broad-except
                logging.exception("%s client initialisation failed: %s", prefix, exc)
                raise

            local_saved = local_skipped = local_errors = 0

            # Compute contiguous missing spans and fetch them in provider-configured windows
            spans = _group_missing_day_spans(location_dir)
            default_window = 7 if provider_key.endswith("land_timeseries") else 31
            window_days = _provider_max_days(provider_key, default_window)

            for s, e in spans:
                if s > today:
                    continue
                for ws, we in _iter_date_windows(s, min(e, today), window_days):
                    _record_expected(1)
                    try:
                        payload = local_client.get_dataset(
                            area=area,
                            start_date=ws,
                            end_date=we,
                            latitude=lat,
                            longitude=lon,
                        )
                    except Exception as exc:
                        _record_complete(1)
                        msg = str(exc)
                        if "MultiAdaptorNoDataError" in msg or "NoData" in msg:
                            # Count all days in the window as skipped
                            local_skipped += _date_span_days(ws, we)
                            logging.info("%s no data for %s..%s (reported by API)", prefix, ws, we)
                            continue
                        local_errors += 1
                        logging.warning("%s request failed for %s..%s: %s", prefix, ws, we, exc)
                        continue
                    _record_complete(1)
                    # Split payload into per-day outputs
                    for offset in range(_date_span_days(ws, we)):
                        day = ws + dt.timedelta(days=offset)
                        out = location_dir / f"{day.isoformat()}.csv"
                        try:
                            df_day = transform_fn(payload, day)
                            if df_day.empty:
                                local_skipped += 1
                                continue
                            df_day.to_csv(out, index=False)
                            local_saved += 1
                            logging.info("%s wrote %s", prefix, out)
                        except Exception as exc:  # pylint: disable=broad-except
                            local_errors += 1
                            logging.exception("%s unexpected error for %s: %s", prefix, day, exc)
            return local_saved, local_skipped, local_errors

        max_workers = max(1, min(8, len(eligible_locations)))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(_process_location, job) for job in eligible_locations]
            for future in as_completed(futures):
                loc_saved, loc_skipped, loc_errors = future.result()
                saved += loc_saved
                skipped += loc_skipped
                errors += loc_errors

    return summarise_results(provider_key, saved, skipped, errors)


def _transform_era5_single(payload: "pd.DataFrame", day: dt.date) -> pd.DataFrame:
    df = _filter_day_dataframe(payload, day)
    if df.empty:
        return df
    if "t2m" in df.columns:
        df["temperature_c"] = df["t2m"].apply(_kelvin_to_celsius)
    if "tp" in df.columns:
        df["precip_mm"] = df["tp"] * 1000.0
    if "u10" in df.columns:
        df["wind_u10_mps"] = df["u10"]
    if "v10" in df.columns:
        df["wind_v10_mps"] = df["v10"]
    return df


def _transform_era5_land(payload: "pd.DataFrame", day: dt.date) -> pd.DataFrame:
    df = _filter_day_dataframe(payload, day)
    if df.empty:
        return df
    if "t2m" in df.columns:
        df["temperature_c"] = df["t2m"].apply(_kelvin_to_celsius)
    if "tp" in df.columns:
        df["precip_mm"] = df["tp"] * 1000.0
    return df


def _transform_era5_pressure(payload: "pd.DataFrame", day: dt.date) -> pd.DataFrame:
    df = _filter_day_dataframe(payload, day)
    if df.empty:
        return df
    if "temperature" in df.columns:
        df["temperature_c"] = df["temperature"].apply(_kelvin_to_celsius)
    return df


def _transform_era5_land_ts(payload: "pd.DataFrame", day: dt.date) -> pd.DataFrame:
    df = _filter_day_dataframe(payload, day)
    if df.empty:
        return df
    if "t2m" in df.columns:
        df["temperature_c"] = df["t2m"].apply(_kelvin_to_celsius)
    if "tp" in df.columns:
        df["precip_mm"] = df["tp"] * 1000.0
    if "u10" in df.columns:
        df["wind_u10_mps"] = df["u10"]
    if "v10" in df.columns:
        df["wind_v10_mps"] = df["v10"]
    return df


def export_copernicus_era5_single() -> str:
    return _export_copernicus_provider(
        provider_key="copernicus_era5_single",
        enabled=USE_COPERNICUS_ERA5_SINGLE,
        area_attr="copernicusEra5Area",
        transform_fn=_transform_era5_single,
        label="Copernicus ERA5 single",
    )


def export_copernicus_era5_land() -> str:
    return _export_copernicus_provider(
        provider_key="copernicus_era5_land",
        enabled=USE_COPERNICUS_ERA5_LAND,
        area_attr="copernicusEra5LandArea",
        transform_fn=_transform_era5_land,
        label="Copernicus ERA5-Land",
    )


def export_copernicus_era5_pressure() -> str:
    return _export_copernicus_provider(
        provider_key="copernicus_era5_pressure",
        enabled=USE_COPERNICUS_ERA5_PRESSURE,
        area_attr="copernicusEra5Area",
        transform_fn=_transform_era5_pressure,
        label="Copernicus ERA5 pressure",
    )


def export_copernicus_era5_land_ts() -> str:
    return _export_copernicus_provider(
        provider_key="copernicus_era5_land_timeseries",
        enabled=USE_COPERNICUS_ERA5_LAND_TS,
        area_attr=None,
        transform_fn=_transform_era5_land_ts,
        label="Copernicus ERA5-Land TS",
        requires_area=False,
    )


def export_openweather() -> str:
    if not USE_OPENWEATHER:
        logging.info("OpenWeather disabled; skipping.")
        return "OpenWeather disabled"

    provider_dir = DATA_ROOT / "openweather"
    provider_dir.mkdir(parents=True, exist_ok=True)

    try:
        client = OpenWeatherClient(config_path=CONFIG_PATH)
    except Exception as exc:  # pylint: disable=broad-except
        logging.exception("OpenWeather client initialisation failed: %s", exc)
        return "OpenWeather initialisation failed"

    today = dt.date.today()
    now_utc = dt.datetime.now(dt.timezone.utc)

    saved = skipped = errors = 0

    with provider_progress("openweather") as progress:
        for location_key, lat, lon in LOCATION_ITEMS:
            prefix = f"OpenWeather[{location_key}]"
            location_dir = provider_dir / location_key
            location_dir.mkdir(parents=True, exist_ok=True)

            def _ow_fetch(params: Dict[str, object]) -> dict:
                s: dt.date = params["start"]  # type: ignore[index]
                e: dt.date = params["end"]  # type: ignore[index]
                start_dt = dt.datetime.combine(s, dt.time(0, 0), tzinfo=UTC)
                end_dt = dt.datetime.combine(e + dt.timedelta(days=1), dt.time(0, 0), tzinfo=UTC)
                end_dt = min(end_dt, now_utc)
                if end_dt <= start_dt:
                    return {"list": []}
                return client.get_historical(
                    location=(lat, lon), start_time=start_dt, end_time=end_dt, interval_type="hour", units="metric"
                )

            def _ow_handle(payload: dict, s: dt.date, e: dt.date) -> Tuple[int, int, int]:
                _saved = _skipped = _errors = 0
                records = payload.get("list", []) if isinstance(payload, dict) else []
                if not records:
                    return 0, _date_span_days(s, e), 0
                try:
                    df = pd.json_normalize(records)
                    if "dt" in df.columns:
                        df["timestamp"] = pd.to_datetime(df["dt"], unit="s", utc=True)
                    elif "time" in df.columns:
                        df["timestamp"] = pd.to_datetime(df["time"], utc=True)
                except Exception:
                    return 0, 0, _date_span_days(s, e)
                for offset in range(_date_span_days(s, e)):
                    day = s + dt.timedelta(days=offset)
                    out = location_dir / f"{day.isoformat()}.csv"
                    try:
                        day_df = df[df["timestamp"].dt.date == day]
                        if day_df.empty:
                            _skipped += 1
                            continue
                        day_df.to_csv(out, index=False)
                        _saved += 1
                        logging.info("%s wrote %s", prefix, out)
                    except Exception as exc:  # pylint: disable=broad-except
                        _errors += 1
                        logging.exception("%s unexpected error for %s: %s", prefix, day, exc)
                return _saved, _skipped, _errors

            spans = _group_missing_day_spans(location_dir)
            for s, e in spans:
                if s > today:
                    continue
                window_days = _provider_max_days('openweather', 5)
                for ws, we in _iter_date_windows(s, min(e, today), window_days):
                    _fetch_range_recursive(
                        start=ws,
                        end=we,
                        fetch_fn=_ow_fetch,
                        request_kwargs={},
                        handle_payload=_ow_handle,
                        progress=progress,
                    )

    return summarise_results("openweather", saved, skipped, errors)


def export_weatherbit() -> str:
    if not USE_WEATHERBIT:
        logging.info("Weatherbit disabled; skipping.")
        return "Weatherbit disabled"

    provider_dir = DATA_ROOT / "weatherbit"
    provider_dir.mkdir(parents=True, exist_ok=True)

    try:
        client = WeatherbitClient(config_path=CONFIG_PATH)
    except Exception as exc:  # pylint: disable=broad-except
        logging.exception("Weatherbit client initialisation failed: %s", exc)
        return "Weatherbit initialisation failed"

    today = dt.date.today()
    saved = skipped = errors = 0

    def _payload_to_df(payload: dict, day: dt.date) -> pd.DataFrame:
        data = payload.get("data", [])
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        if "timestamp_utc" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp_utc"], utc=True)
        elif "ts" in df.columns:
            df["timestamp"] = pd.to_datetime(df["ts"], unit="s", utc=True)
        else:
            df["timestamp"] = pd.to_datetime(df.get("datetime", pd.Series(dtype=str)), utc=True, errors="coerce")
        df = df.dropna(subset=["timestamp"])
        df = df[df["timestamp"].dt.date == day]
        return df.sort_values("timestamp")

    with provider_progress("weatherbit") as progress:
        for location_key, lat, lon in LOCATION_ITEMS:
            prefix = f"Weatherbit[{location_key}]"
            location_dir = provider_dir / location_key
            location_dir.mkdir(parents=True, exist_ok=True)

            # Historical range path (Weatherbit subhourly supports start/end ranges; 30-day safe window)
            def _wb_fetch(params: Dict[str, object]) -> dict:
                s: dt.date = params["start"]  # type: ignore[index]
                e: dt.date = params["end"]  # type: ignore[index]
                return client.get_historical(location=(lat, lon), start_time=s, end_time=e, units="M")

            def _wb_handle(payload: dict, s: dt.date, e: dt.date) -> Tuple[int, int, int]:
                _saved = _skipped = _errors = 0
                for offset in range(_date_span_days(s, e)):
                    day = s + dt.timedelta(days=offset)
                    output_path = location_dir / f"{day.isoformat()}.csv"
                    try:
                        df_day = _payload_to_df(payload, day)
                        if df_day.empty:
                            logging.info("%s no historical data for %s", prefix, day)
                            _skipped += 1
                            continue
                        df_day.to_csv(output_path, index=False)
                        _saved += 1
                        logging.info("%s wrote %s", prefix, output_path)
                    except Exception as exc:  # pylint: disable=broad-except
                        _errors += 1
                        logging.exception("%s unexpected historical error for %s: %s", prefix, day, exc)
                return _saved, _skipped, _errors

            spans = _group_missing_day_spans(location_dir)
            for s, e in spans:
                if e < today:
                    window_days = _provider_max_days('weatherbit', 30)
                    for ws, we in _iter_date_windows(s, e, window_days):
                        _fetch_range_recursive(
                            start=ws,
                            end=we,
                            fetch_fn=_wb_fetch,
                            request_kwargs={},
                            handle_payload=_wb_handle,
                            progress=progress,
                        )

            forecast_days: List[dt.date] = []
            forecast_requests: List[Dict[str, object]] = []
            batch_limit = getattr(client, "batch_size", 50) or 50
            flush_threshold = compute_flush_threshold(batch_limit)

            def _flush_requests(
                day_buffer: List[dt.date],
                request_buffer: List[Dict[str, object]],
                fetch_fn,
                label: str,
            ) -> None:
                nonlocal saved, skipped, errors
                if not request_buffer:
                    return
                call_count = len(request_buffer)
                progress.add_expected(call_count)
                try:
                    payloads = fetch_fn(list(request_buffer))
                except Exception:
                    progress.complete(call_count)
                    raise
                progress.complete(call_count)
                for req_day, payload in zip(day_buffer, payloads):
                    output_path = location_dir / f"{req_day.isoformat()}.csv"
                    if isinstance(payload, Exception):
                        errors += 1
                        logging.warning("%s %s failed for %s: %s", prefix, label, req_day, payload)
                        continue
                    try:
                        df_day = _payload_to_df(payload, req_day)
                        if df_day.empty:
                            logging.info("%s no %s data for %s", prefix, label, req_day)
                            skipped += 1
                            continue
                        df_day.to_csv(output_path, index=False)
                        saved += 1
                        logging.info("%s wrote %s", prefix, output_path)
                    except Exception as exc:  # pylint: disable=broad-except
                        errors += 1
                        logging.exception("%s unexpected %s error for %s: %s", prefix, label, req_day, exc)
                day_buffer.clear()
                request_buffer.clear()

            for day in DAY_RANGE:
                output_path = location_dir / f"{day.isoformat()}.csv"
                if day != today and output_path.exists():
                    logging.debug("%s skipping %s (already exported).", prefix, day)
                    skipped += 1
                    continue

                start = dt.datetime.combine(day, dt.time(0, 0), tzinfo=UTC)
                end = start + dt.timedelta(days=1)

                if day < today:
                    historical_days.append(day)
                    historical_requests.append(
                        {
                            "location": (lat, lon),
                            "start_time": start,
                            "end_time": end,
                            "units": "M",
                        }
                    )
                    if len(historical_requests) >= flush_threshold:
                        _flush_requests(historical_days, historical_requests, client.get_historical_batch, "historical")
                else:
                    forecast_days.append(day)
                    forecast_requests.append(
                        {
                            "location": (lat, lon),
                            "hours": 48,
                            "units": "M",
                        }
                    )
                    if len(forecast_requests) >= flush_threshold:
                        _flush_requests(forecast_days, forecast_requests, client.get_forecast_batch, "forecast")

            _flush_requests(historical_days, historical_requests, client.get_historical_batch, "historical")
            _flush_requests(forecast_days, forecast_requests, client.get_forecast_batch, "forecast")

    return summarise_results("weatherbit", saved, skipped, errors)


def export_weatherapi() -> str:
    if not USE_WEATHERAPI_COM:
        logging.info("WeatherAPI.com disabled; skipping.")
        return "WeatherAPI.com disabled"

    provider_dir = DATA_ROOT / "weatherapi_com"
    provider_dir.mkdir(parents=True, exist_ok=True)

    try:
        client = WeatherApiClient(config_path=CONFIG_PATH)
    except Exception as exc:  # pylint: disable=broad-except
        logging.exception("WeatherAPI.com client initialisation failed: %s", exc)
        return "WeatherAPI.com initialisation failed"

    today = dt.date.today()
    forecast_horizon = today + dt.timedelta(days=14)

    saved = skipped = errors = 0

    def _rows(block: dict) -> List[dict]:
        rows = []
        for hour in block.get("hour", []):
            row = dict(hour)
            if "time_epoch" in hour:
                row["timestamp"] = pd.to_datetime(hour["time_epoch"], unit="s", utc=True)
            elif "time" in hour:
                row["timestamp"] = pd.to_datetime(hour["time"], utc=True)
            row["forecast_date"] = block.get("date")
            rows.append(row)
        return rows

    def _payload_to_df(payload: dict, day: dt.date) -> pd.DataFrame:
        forecast = payload.get("forecast", {})
        forecast_days = forecast.get("forecastday", [])
        rows = []
        for block in forecast_days:
            rows.extend(_rows(block))
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        if "timestamp" not in df:
            if "time_epoch" in df.columns:
                df["timestamp"] = pd.to_datetime(df["time_epoch"], unit="s", utc=True)
            elif "time" in df.columns:
                df["timestamp"] = pd.to_datetime(df["time"], utc=True)
            else:
                return pd.DataFrame()
        df = df[df["timestamp"].dt.date == day]
        return df.sort_values("timestamp")

    with provider_progress("weatherapi_com") as progress:
        for location_key, lat, lon in LOCATION_ITEMS:
            prefix = f"WeatherAPI.com[{location_key}]"
            location_dir = provider_dir / location_key
            location_dir.mkdir(parents=True, exist_ok=True)

            # Historical range path (WeatherAPI supports dt+end_dt, commonly up to 7-30 days)
            def _wa_fetch(params: Dict[str, object]) -> dict:
                s: dt.date = params["start"]  # type: ignore[index]
                e: dt.date = params["end"]  # type: ignore[index]
                return client.get_historical(location=(lat, lon), date=s, end_date=e)

            def _wa_handle(payload: dict, s: dt.date, e: dt.date) -> Tuple[int, int, int]:
                _saved = _skipped = _errors = 0
                for offset in range(_date_span_days(s, e)):
                    day = s + dt.timedelta(days=offset)
                    output_path = location_dir / f"{day.isoformat()}.csv"
                    try:
                        df_day = _payload_to_df(payload, day)
                        if df_day.empty:
                            logging.info("%s no historical data for %s", prefix, day)
                            _skipped += 1
                            continue
                        df_day.to_csv(output_path, index=False)
                        _saved += 1
                        logging.info("%s wrote %s", prefix, output_path)
                    except Exception as exc:  # pylint: disable=broad-except
                        _errors += 1
                        logging.exception("%s unexpected historical error for %s: %s", prefix, day, exc)
                return _saved, _skipped, _errors

            spans = _group_missing_day_spans(location_dir)
            for s, e in spans:
                if e < today:
                    window_days = _provider_max_days('weatherapi_com', 7)
                    for ws, we in _iter_date_windows(s, e, window_days):
                        _fetch_range_recursive(
                            start=ws,
                            end=we,
                            fetch_fn=_wa_fetch,
                            request_kwargs={},
                            handle_payload=_wa_handle,
                            progress=progress,
                        )

            # Forecast path remains per-day
            historical_days: List[dt.date] = []
            historical_requests: List[Dict[str, object]] = []
            forecast_days: List[dt.date] = []
            forecast_requests: List[Dict[str, object]] = []
            batch_limit = getattr(client, "batch_size", 50) or 50
            flush_threshold = compute_flush_threshold(batch_limit)

            def _flush_requests(
                day_buffer: List[dt.date],
                request_buffer: List[Dict[str, object]],
                fetch_fn,
                label: str,
            ) -> None:
                nonlocal saved, skipped, errors
                if not request_buffer:
                    return
                call_count = len(request_buffer)
                progress.add_expected(call_count)
                try:
                    payloads = fetch_fn(list(request_buffer))
                except Exception:
                    progress.complete(call_count)
                    raise
                progress.complete(call_count)
                for req_day, payload in zip(day_buffer, payloads):
                    output_path = location_dir / f"{req_day.isoformat()}.csv"
                    if isinstance(payload, Exception):
                        errors += 1
                        logging.warning("%s %s failed for %s: %s", prefix, label, req_day, payload)
                        continue
                    try:
                        df_day = _payload_to_df(payload, req_day)
                        if df_day.empty:
                            logging.info("%s no %s data for %s", prefix, label, req_day)
                            skipped += 1
                            continue
                        df_day.to_csv(output_path, index=False)
                        saved += 1
                        logging.info("%s wrote %s", prefix, output_path)
                    except Exception as exc:  # pylint: disable=broad-except
                        errors += 1
                        logging.exception("%s unexpected %s error for %s: %s", prefix, label, req_day, exc)
                day_buffer.clear()
                request_buffer.clear()

            for day in DAY_RANGE:
                if day > forecast_horizon:
                    logging.debug("%s skipping %s (beyond 14-day forecast window).", prefix, day)
                    skipped += 1
                    continue

                output_path = location_dir / f"{day.isoformat()}.csv"
                if day != today and output_path.exists():
                    logging.debug("%s skipping %s (already exported).", prefix, day)
                    skipped += 1
                    continue

                if day < today:
                    historical_days.append(day)
                    historical_requests.append(
                        {
                            "location": (lat, lon),
                            "date": day,
                        }
                    )
                    if len(historical_requests) >= flush_threshold:
                        _flush_requests(historical_days, historical_requests, client.get_historical_batch, "historical")
                else:
                    forecast_days.append(day)
                    forecast_requests.append(
                        {
                            "location": (lat, lon),
                            "days": 1,
                            "start_date": day,
                            "end_date": day,
                        }
                    )
                    if len(forecast_requests) >= flush_threshold:
                        _flush_requests(forecast_days, forecast_requests, client.get_forecast_batch, "forecast")

            _flush_requests(historical_days, historical_requests, client.get_historical_batch, "historical")
            _flush_requests(forecast_days, forecast_requests, client.get_forecast_batch, "forecast")

    return summarise_results("weatherapi_com", saved, skipped, errors)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
PROVIDER_FUNCTIONS = {
    "tomorrow_io": export_tomorrow_io,
    "open_meteo": export_open_meteo,
    "visual_crossing": export_visual_crossing,
    "noaa_isd": export_noaa_isd,
    "noaa_lcd": export_noaa_lcd,
    "meteostat": export_meteostat,
    "nasa_power": export_nasa_power,
    "iem_asos": export_iem_asos,
    "copernicus_era5_single": export_copernicus_era5_single,
    "copernicus_era5_land": export_copernicus_era5_land,
    "copernicus_era5_pressure": export_copernicus_era5_pressure,
    "copernicus_era5_land_timeseries": export_copernicus_era5_land_ts,
    "openweather": export_openweather,
    "weatherbit": export_weatherbit,
    "weatherapi_com": export_weatherapi,
}

PROVIDER_FLAGS = {
    "tomorrow_io": USE_TOMORROW_IO,
    "open_meteo": USE_OPEN_METEO,
    "visual_crossing": USE_VISUAL_CROSSING,
    "noaa_isd": USE_NOAA_ISD,
    "noaa_lcd": USE_NOAA_LCD,
    "meteostat": USE_METEOSTAT,
    "nasa_power": USE_NASA_POWER,
    "iem_asos": USE_IEM_ASOS,
    "copernicus_era5_single": USE_COPERNICUS_ERA5_SINGLE,
    "copernicus_era5_land": USE_COPERNICUS_ERA5_LAND,
    "copernicus_era5_pressure": USE_COPERNICUS_ERA5_PRESSURE,
    "copernicus_era5_land_timeseries": USE_COPERNICUS_ERA5_LAND_TS,
    "openweather": USE_OPENWEATHER,
    "weatherbit": USE_WEATHERBIT,
    "weatherapi_com": USE_WEATHERAPI_COM,
}


PROVIDER_LABELS = {
    "tomorrow_io": "Tomorrow.io",
    "open_meteo": "Open-Meteo",
    "visual_crossing": "Visual Crossing",
    "noaa_isd": "NOAA ISD",
    "noaa_lcd": "NOAA LCD",
    "meteostat": "Meteostat",
    "nasa_power": "NASA POWER",
    "iem_asos": "IEM ASOS",
    "copernicus_era5_single": "Copernicus ERA5 (single)",
    "copernicus_era5_land": "Copernicus ERA5-Land",
    "copernicus_era5_pressure": "Copernicus ERA5 (pressure)",
    "copernicus_era5_land_timeseries": "Copernicus ERA5-Land TS",
    "openweather": "OpenWeather",
    "weatherbit": "Weatherbit",
    "weatherapi_com": "WeatherAPI.com",
}

PROVIDER_RESOLUTION = {
    "tomorrow_io": "No cached data yet (expected 5m/1h timeline)",
    "open_meteo": "Hourly cadence (1h)",
    "visual_crossing": "Hourly cadence (1h)",
    "noaa_isd": "Sub-hourly METAR (median ~53 min)",
    "noaa_lcd": "Sub-hourly LCD (median ~53 min)",
    "meteostat": "Hourly multi-source blend (1h)",
    "nasa_power": "Hourly NASA POWER (satellite/model)",
    "iem_asos": "1-min ASOS observations",
    "copernicus_era5_single": "Hourly ERA5 single levels",
    "copernicus_era5_land": "Hourly ERA5-Land (0.1 deg grid)",
    "copernicus_era5_pressure": "Hourly ERA5 pressure levels (0.25 deg grid)",
    "copernicus_era5_land_timeseries": "Hourly ERA5-Land point series",
    "openweather": "Hourly observations (1h)",
    "weatherbit": "No cached data yet",
    "weatherapi_com": "Hourly forecast/history (1h)",
}

PROVIDER_BAR_POSITIONS: Dict[str, int] = {}
GLOBAL_API_BAR: Optional["tqdm"] = None
GLOBAL_API_LOCK = threading.Lock()


def _increment_global_api_total(count: int) -> None:
    if count <= 0 or tqdm is None:
        return
    bar = GLOBAL_API_BAR
    if bar is None:
        return
    with GLOBAL_API_LOCK:
        bar.total = (bar.total or 0) + count
        bar.refresh()


def _increment_global_api_progress(count: int) -> None:
    if count <= 0 or tqdm is None:
        return
    bar = GLOBAL_API_BAR
    if bar is None:
        return
    with GLOBAL_API_LOCK:
        bar.update(count)


class _NullProviderProgress:
    def __enter__(self) -> "_NullProviderProgress":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        return False

    def add_expected(self, count: int) -> None:
        return None

    def complete(self, count: int) -> None:
        return None


class _ProviderProgress(_NullProviderProgress):
    def __init__(self, provider_key: str) -> None:
        if tqdm is None:
            raise RuntimeError("tqdm must be available for provider progress.")
        position = PROVIDER_BAR_POSITIONS.get(provider_key, 1)
        label = PROVIDER_LABELS.get(provider_key, provider_key)
        self.bar = tqdm(
            total=0,
            desc=f"{label} API calls",
            unit="call",
            position=position,
            leave=False,
            dynamic_ncols=True,
        )

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.bar.close()
        return False

    def add_expected(self, count: int) -> None:
        if count <= 0:
            return
        self.bar.total = (self.bar.total or 0) + count
        self.bar.refresh()
        _increment_global_api_total(count)

    def complete(self, count: int) -> None:
        if count <= 0:
            return
        self.bar.update(count)
        _increment_global_api_progress(count)


def provider_progress(provider_key: str) -> _NullProviderProgress:
    if tqdm is None:
        return _NullProviderProgress()
    return _ProviderProgress(provider_key)


def generate_cached_coverage_chart(output_path: Path) -> None:
    date_index = pd.date_range(START_DATE, END_DATE, freq="D")
    active_providers = list(PROVIDER_FUNCTIONS.keys())

    if not active_providers:
        logging.info("No providers configured for chart; skipping.")
        return

    cmap = matplotlib.colors.ListedColormap(["#f0f0f0", "#2ca02c"])
    fig, axes = plt.subplots(len(active_providers), 1, figsize=(16, 2.5 * len(active_providers)), sharex=True)
    if len(active_providers) == 1:
        axes = [axes]

    last_im = None
    for ax, provider_key in zip(axes, active_providers):
        provider_dir = DATA_ROOT / provider_key
        coverage_rows = []
        location_names = []

        for location_name, *_coords in LOCATION_ITEMS:
            location_dir = provider_dir / location_name
            row = []
            for day in date_index:
                file_path = location_dir / f"{day.date().isoformat()}.csv"
                row.append(file_path.exists())
            coverage_rows.append(row)
            location_names.append(location_name)

        if not coverage_rows:
            ax.text(0.5, 0.5, "No location data available", ha="center", va="center")
            ax.set_axis_off()
            continue

        data = np.array(coverage_rows, dtype=int)
        im = ax.imshow(data, aspect="auto", interpolation="nearest", cmap=cmap, vmin=0, vmax=1)
        last_im = im

        ax.set_yticks(range(len(location_names)))
        ax.set_yticklabels(location_names)

        tick_count = min(len(date_index), 10)
        if tick_count > 0:
            tick_positions = np.linspace(0, len(date_index) - 1, tick_count, dtype=int)
            ax.set_xticks(tick_positions)
            ax.set_xticklabels([date_index[i].date().isoformat() for i in tick_positions], rotation=45, ha="right")

        label = PROVIDER_LABELS.get(provider_key, provider_key)
        resolution = PROVIDER_RESOLUTION.get(provider_key, "")
        title_suffix = f" ({resolution})" if resolution else ""
        ax.set_title(f"{label}{title_suffix}")
        ax.set_ylabel("Location")

    axes[-1].set_xlabel("Date")
    fig.suptitle(f"Cached coverage {START_DATE.isoformat()} â€” {END_DATE.isoformat()}")
    plt.tight_layout(rect=(0, 0, 1, 0.97))
    if last_im is not None:
        cbar = fig.colorbar(last_im, ax=axes, orientation="horizontal", fraction=0.025, pad=0.08)
        cbar.set_ticks([0, 1])
        cbar.set_ticklabels(["Missing", "Cached"])

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150)
    plt.close(fig)
    logging.info("Saved cached coverage chart to %s", output_path)


def run_export_once() -> None:
    enabled_providers = {
        key: func for key, func in PROVIDER_FUNCTIONS.items() if PROVIDER_FLAGS.get(key, False)
    }

    # Restrict/override via --providers if specified
    global PROVIDER_INCLUDE
    if PROVIDER_INCLUDE:
        enabled_providers = {k: v for k, v in enabled_providers.items() if k in PROVIDER_INCLUDE}
        # Also allow enabling explicitly requested providers even if toggled off
        for k in list(PROVIDER_INCLUDE):
            if k in PROVIDER_FUNCTIONS and k not in enabled_providers:
                enabled_providers[k] = PROVIDER_FUNCTIONS[k]

    if not enabled_providers:
        logging.warning("No providers enabled; nothing to export.")
        return

    total_providers = len(enabled_providers)
    logging.info("Starting export run across %d providers.", total_providers)

    if not SKIP_COVERAGE:
        logging.info("Refreshing coverage chart before provider exports.")
        try:
            generate_cached_coverage_chart(CACHE_IMAGE_PATH)
        except Exception as exc:  # pylint: disable=broad-except
            logging.exception("Failed to generate pre-run coverage chart: %s", exc)

    provider_order = list(enabled_providers.keys())
    global PROVIDER_BAR_POSITIONS
    PROVIDER_BAR_POSITIONS = {key: idx + 1 for idx, key in enumerate(provider_order)}

    global GLOBAL_API_BAR
    if tqdm is not None:
        GLOBAL_API_BAR = tqdm(
            total=0,
            desc="API calls",
            unit="call",
            dynamic_ncols=True,
            position=0,
            leave=False,
        )
    else:
        GLOBAL_API_BAR = None

    try:
        with ThreadPoolExecutor(max_workers=total_providers) as executor:
            futures = {executor.submit(func): key for key, func in enabled_providers.items()}
            for future in as_completed(futures):
                provider_key = futures[future]
                try:
                    result_summary = future.result()
                    logging.info("Provider %s completed: %s", provider_key, result_summary)
                except Exception as exc:  # pylint: disable=broad-except
                    logging.exception(
                        "Provider %s raised unexpected exception: %s", provider_key, exc
                    )
    finally:
        if GLOBAL_API_BAR is not None:
            GLOBAL_API_BAR.close()
            GLOBAL_API_BAR = None

    if not SKIP_COVERAGE:
        logging.info("Provider exports complete. Generating coverage chart.")
        try:
            generate_cached_coverage_chart(CACHE_IMAGE_PATH)
        except Exception as exc:  # pylint: disable=broad-except
            logging.exception("Failed to generate coverage chart: %s", exc)


def main() -> None:
    parser = argparse.ArgumentParser(description="Weather data export runner")
    parser.add_argument("--once", action="store_true", help="Run a single cycle and exit")
    parser.add_argument("--skip-coverage", action="store_true", help="Skip coverage chart")
    parser.add_argument(
        "--providers", type=str, default=None, help="Comma-separated provider keys to run"
    )
    parser.add_argument("--since", type=str, default=None, help="Start date YYYY-MM-DD")
    parser.add_argument("--until", type=str, default=None, help="End date YYYY-MM-DD")
    parser.add_argument(
        "--limit-locations", type=int, default=None, help="Limit number of locations"
    )

    args = parser.parse_args()

    # Apply runtime overrides
    global SKIP_COVERAGE, PROVIDER_INCLUDE, LOCATION_LIMIT
    SKIP_COVERAGE = bool(args.skip_coverage)
    PROVIDER_INCLUDE = (
        {p.strip() for p in args.providers.split(",") if p.strip()} if args.providers else None
    )
    LOCATION_LIMIT = args.limit_locations

    # Update dates and locations
    _set_date_range(args.since, args.until)
    _apply_location_limit(LOCATION_LIMIT)

    # If --providers was supplied, force-enable their toggles and rebuild flags
    _force_enable_toggles(PROVIDER_INCLUDE)
    _rebuild_provider_flags()

    logging.info("Weather export script initialised.")

    try:
        if args.once:
            run_start = time.time()
            try:
                run_export_once()
            except Exception as exc:  # pylint: disable=broad-except
                logging.exception("Unexpected error during export run: %s", exc)
            elapsed = time.time() - run_start
            logging.info("Export run completed in %.2f seconds.", elapsed)
            return

        while True:
            run_start = time.time()
            try:
                run_export_once()
            except Exception as exc:  # pylint: disable=broad-except
                logging.exception("Unexpected error during export run: %s", exc)
            elapsed = time.time() - run_start
            logging.info("Export run completed in %.2f seconds.", elapsed)

            logging.info("Sleeping for %d seconds before next run.", COOLDOWN_SECONDS)
            try:
                time.sleep(COOLDOWN_SECONDS)
            except KeyboardInterrupt:
                logging.info("Received interrupt during sleep; exiting.")
                raise
    except KeyboardInterrupt:
        logging.info("Weather export script interrupted; shutting down.")

 
def _group_missing_day_spans(location_dir: Path, *, include_today: bool = True) -> List[Tuple[dt.date, dt.date]]:
    spans: List[Tuple[dt.date, dt.date]] = []
    start: Optional[dt.date] = None
    end: Optional[dt.date] = None
    today = dt.date.today()
    for day in DAY_RANGE:
        out = location_dir / f"{day.isoformat()}.csv"
        needs = (day == today and include_today) or (not out.exists())
        if not needs:
            if start is not None:
                spans.append((start, end or start))
                start = end = None
            continue
        if start is None:
            start = end = day
        else:
            # extend consecutive span
            if end and (day - end == dt.timedelta(days=1)):
                end = day
            else:
                spans.append((start, end or start))
                start = end = day
    if start is not None:
        spans.append((start, end or start))
    return spans


def _date_span_days(start: dt.date, end: dt.date) -> int:
    return (end - start).days + 1


def _fetch_range_recursive(
    *,
    start: dt.date,
    end: dt.date,
    fetch_fn,
    request_kwargs: Dict[str, object],
    handle_payload,
    progress,
) -> Tuple[int, int, int]:
    """Attempt range fetch; on failure, split and recurse until single days.

    Returns (saved, skipped, errors) aggregated across recursion.
    """
    saved = skipped = errors = 0
    try:
        progress.add_expected(1)
        payload = fetch_fn({**request_kwargs, "start": start, "end": end})
        progress.complete(1)
        s, k, e = handle_payload(payload, start, end)
        return s, k, e
    except Exception as exc:  # pylint: disable=broad-except
        progress.complete(1)
        if _date_span_days(start, end) <= 1:
            logging.warning("Range request failed for %s: %s", start, exc)
            return 0, 0, 1
        mid = start + dt.timedelta(days=_date_span_days(start, end) // 2 - 1)
        left_end = mid
        right_start = mid + dt.timedelta(days=1)
        s1, k1, e1 = _fetch_range_recursive(
            start=start,
            end=left_end,
            fetch_fn=fetch_fn,
            request_kwargs=request_kwargs,
            handle_payload=handle_payload,
            progress=progress,
        )
        s2, k2, e2 = _fetch_range_recursive(
            start=right_start,
            end=end,
            fetch_fn=fetch_fn,
            request_kwargs=request_kwargs,
            handle_payload=handle_payload,
            progress=progress,
        )
        return s1 + s2, k1 + k2, e1 + e2

if __name__ == "__main__":
    main()
