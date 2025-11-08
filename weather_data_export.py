#!/usr/bin/env python3
"""
Automated weather data exporter.

Mirrors the logic in weather_data_export.ipynb while running provider exports
concurrently, persisting verbose logs, and looping with a cool down between runs.
"""

import datetime as dt
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
USE_OPEN_METEO = True
USE_VISUAL_CROSSING = False
USE_NOAA_ISD = True
USE_NOAA_LCD = True
USE_METEOSTAT = True
USE_NASA_POWER = True
USE_IEM_ASOS = True
USE_COPERNICUS_ERA5_SINGLE = True
USE_COPERNICUS_ERA5_LAND = True
USE_COPERNICUS_ERA5_PRESSURE = True
USE_COPERNICUS_ERA5_LAND_TS = True
USE_OPENWEATHER = False
USE_WEATHERBIT = False
USE_WEATHERAPI_COM = False

# Ensure essential directories exist.
DATA_ROOT.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8")],
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
UTC = dt.timezone.utc


def summarise_results(provider_key: str, saved: int, skipped: int, errors: int) -> str:
    return (
        f"{provider_key}: saved={saved}, skipped={skipped}, "
        f"errors={errors}, locations={len(LOCATION_ITEMS)}"
    )


def compute_flush_threshold(batch_limit: int) -> int:
    """Clamp buffered batch size to avoid overwhelming downstream APIs."""
    return max(1, min(batch_limit, MAX_PENDING_REQUESTS_PER_FLUSH))


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
                output_path = location_dir / f"{day.isoformat()}.csv"
                if day != today and output_path.exists():
                    logging.debug("%s skipping %s (already exported).", prefix, day)
                    skipped += 1
                    continue

                request = {
                    "location": (lat, lon),
                    "start_date": day,
                    "end_date": day,
                    "hourly": hourly,
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

            request_days: List[dt.date] = []
            request_payloads: List[Dict[str, object]] = []
            batch_limit = getattr(client, "batch_size", 50) or 50
            flush_threshold = compute_flush_threshold(batch_limit)

            def _flush_requests() -> None:
                nonlocal saved, skipped, errors
                if not request_payloads:
                    return
                call_count = len(request_payloads)
                progress.add_expected(call_count)
                try:
                    payloads = client.get_observations_batch(list(request_payloads))
                except Exception:
                    progress.complete(call_count)
                    raise
                progress.complete(call_count)
                for req_day, payload in zip(request_days, payloads):
                    output_path = location_dir / f"{req_day.isoformat()}.csv"
                    if isinstance(payload, Exception):
                        errors += 1
                        logging.warning("%s request failed for %s: %s", prefix, req_day, payload)
                        continue
                    try:
                        df_day = _payload_to_df(payload, req_day)
                        if df_day.empty:
                            logging.info("%s no data for %s", prefix, req_day)
                            skipped += 1
                            continue
                        df_day.to_csv(output_path, index=False)
                        saved += 1
                        logging.info("%s wrote %s", prefix, output_path)
                    except Exception as exc:  # pylint: disable=broad-except
                        errors += 1
                        logging.exception("%s unexpected error for %s: %s", prefix, req_day, exc)
                request_days.clear()
                request_payloads.clear()

            for day in DAY_RANGE:
                if day > today:
                    skipped += 1
                    continue
                output_path = location_dir / f"{day.isoformat()}.csv"
                if day != today and output_path.exists():
                    skipped += 1
                    continue
                start = dt.datetime.combine(day, dt.time(0, 0), tzinfo=UTC)
                end = start + dt.timedelta(days=1)
                request_payloads.append({"station_id": station_id, "start_time": start, "end_time": end})
                request_days.append(day)
                if len(request_payloads) >= flush_threshold:
                    _flush_requests()
            _flush_requests()

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

            request_days: List[dt.date] = []
            request_payloads: List[Dict[str, object]] = []
            batch_limit = getattr(client, "batch_size", 50) or 50
            flush_threshold = compute_flush_threshold(batch_limit)

            def _flush_requests() -> None:
                nonlocal saved, skipped, errors
                if not request_payloads:
                    return
                call_count = len(request_payloads)
                progress.add_expected(call_count)
                try:
                    payloads = client.get_observations_batch(list(request_payloads))
                except Exception:
                    progress.complete(call_count)
                    raise
                progress.complete(call_count)
                for req_day, payload in zip(request_days, payloads):
                    output_path = location_dir / f"{req_day.isoformat()}.csv"
                    if isinstance(payload, Exception):
                        errors += 1
                        logging.warning("%s request failed for %s: %s", prefix, req_day, payload)
                        continue
                    try:
                        df_day = _payload_to_df(payload, req_day)
                        if df_day.empty:
                            logging.info("%s no data for %s", prefix, req_day)
                            skipped += 1
                            continue
                        df_day.to_csv(output_path, index=False)
                        saved += 1
                        logging.info("%s wrote %s", prefix, output_path)
                    except Exception as exc:  # pylint: disable=broad-except
                        errors += 1
                        logging.exception("%s unexpected error for %s: %s", prefix, req_day, exc)
                request_days.clear()
                request_payloads.clear()

            for day in DAY_RANGE:
                if day > today:
                    skipped += 1
                    continue
                output_path = location_dir / f"{day.isoformat()}.csv"
                if day != today and output_path.exists():
                    skipped += 1
                    continue
                start = dt.datetime.combine(day, dt.time(0, 0), tzinfo=UTC)
                end = start + dt.timedelta(days=1)
                request_payloads.append({"station_id": station_id, "start_time": start, "end_time": end})
                request_days.append(day)
                if len(request_payloads) >= flush_threshold:
                    _flush_requests()
            _flush_requests()

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

            request_days: List[dt.date] = []
            request_payloads: List[Dict[str, object]] = []
            batch_limit = getattr(client, "batch_size", 50) or 50
            flush_threshold = compute_flush_threshold(batch_limit)

            def _flush_requests() -> None:
                nonlocal saved, skipped, errors
                if not request_payloads:
                    return
                call_count = len(request_payloads)
                progress.add_expected(call_count)
                try:
                    payloads = client.get_hourly_batch(list(request_payloads))
                except Exception:
                    progress.complete(call_count)
                    raise
                progress.complete(call_count)
                for req_day, payload in zip(request_days, payloads):
                    output_path = location_dir / f"{req_day.isoformat()}.csv"
                    if isinstance(payload, Exception):
                        errors += 1
                        logging.warning("%s request failed for %s: %s", prefix, req_day, payload)
                        continue
                    try:
                        df_day = _payload_to_df(payload, req_day)
                        if df_day.empty:
                            logging.info("%s no Meteostat data for %s", prefix, req_day)
                            skipped += 1
                            continue
                        df_day.to_csv(output_path, index=False)
                        saved += 1
                        logging.info("%s wrote %s", prefix, output_path)
                    except Exception as exc:  # pylint: disable=broad-except
                        errors += 1
                        logging.exception("%s unexpected Meteostat error for %s: %s", prefix, req_day, exc)
                request_days.clear()
                request_payloads.clear()

            for day in DAY_RANGE:
                if day > today:
                    skipped += 1
                    continue
                output_path = location_dir / f"{day.isoformat()}.csv"
                if day != today and output_path.exists():
                    skipped += 1
                    continue
                start = dt.datetime.combine(day, dt.time(0, 0))
                end = start + dt.timedelta(days=1)
                request_payloads.append(
                    {
                        "location": (lat, lon),
                        "start_time": start,
                        "end_time": end,
                    }
                )
                request_days.append(day)
                if len(request_payloads) >= flush_threshold:
                    _flush_requests()
            _flush_requests()

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

            request_days: List[dt.date] = []
            request_payloads: List[Dict[str, object]] = []
            batch_limit = getattr(client, "batch_size", 25) or 25
            flush_threshold = compute_flush_threshold(batch_limit)

            def _flush_requests() -> None:
                nonlocal saved, skipped, errors
                if not request_payloads:
                    return
                call_count = len(request_payloads)
                progress.add_expected(call_count)
                try:
                    payloads = client.get_hourly_batch(list(request_payloads))
                except Exception:
                    progress.complete(call_count)
                    raise
                progress.complete(call_count)
                for req_day, payload in zip(request_days, payloads):
                    output_path = location_dir / f"{req_day.isoformat()}.csv"
                    if isinstance(payload, Exception):
                        errors += 1
                        logging.warning("%s request failed for %s: %s", prefix, req_day, payload)
                        continue
                    try:
                        df_day = _payload_to_df(payload, req_day)
                        if df_day.empty:
                            logging.info("%s no NASA POWER data for %s", prefix, req_day)
                            skipped += 1
                            continue
                        df_day.to_csv(output_path, index=False)
                        saved += 1
                        logging.info("%s wrote %s", prefix, output_path)
                    except Exception as exc:  # pylint: disable=broad-except
                        errors += 1
                        logging.exception("%s unexpected error for %s: %s", prefix, req_day, exc)
                request_days.clear()
                request_payloads.clear()

            for day in DAY_RANGE:
                if day > today:
                    skipped += 1
                    continue
                output_path = location_dir / f"{day.isoformat()}.csv"
                if day != today and output_path.exists():
                    skipped += 1
                    continue
                request_payloads.append(
                    {
                        "location": (lat, lon),
                        "start_time": day,
                        "end_time": day,
                    }
                )
                request_days.append(day)
                if len(request_payloads) >= flush_threshold:
                    _flush_requests()
            _flush_requests()

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

            request_days: List[dt.date] = []
            request_payloads: List[Dict[str, object]] = []
            batch_limit = getattr(client, "batch_size", 25) or 25
            flush_threshold = compute_flush_threshold(batch_limit)

            def _flush_requests() -> None:
                nonlocal saved, skipped, errors
                if not request_payloads:
                    return
                call_count = len(request_payloads)
                progress.add_expected(call_count)
                try:
                    payloads = client.get_observations_batch(list(request_payloads))
                except Exception:
                    progress.complete(call_count)
                    raise
                progress.complete(call_count)
                for req_day, payload in zip(request_days, payloads):
                    output_path = location_dir / f"{req_day.isoformat()}.csv"
                    if isinstance(payload, Exception):
                        errors += 1
                        logging.warning("%s request failed for %s: %s", prefix, req_day, payload)
                        continue
                    try:
                        df_day = _payload_to_df(payload, req_day)
                        if df_day.empty:
                            logging.info("%s no data for %s", prefix, req_day)
                            skipped += 1
                            continue
                        df_day.to_csv(output_path, index=False)
                        saved += 1
                        logging.info("%s wrote %s", prefix, output_path)
                    except Exception as exc:  # pylint: disable=broad-except
                        errors += 1
                        logging.exception("%s unexpected error for %s: %s", prefix, req_day, exc)
                request_days.clear()
                request_payloads.clear()

            for day in DAY_RANGE:
                if day > today:
                    skipped += 1
                    continue
                output_path = location_dir / f"{day.isoformat()}.csv"
                if day != today and output_path.exists():
                    skipped += 1
                    continue
                start = dt.datetime.combine(day, dt.time(0, 0))
                end = start + dt.timedelta(days=1)
                request_payloads.append(
                    {
                        "station": station_id,
                        "network": network,
                        "start_time": start,
                        "end_time": end,
                    }
                )
                request_days.append(day)
                if len(request_payloads) >= flush_threshold:
                    _flush_requests()
            _flush_requests()

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

    today = dt.date.today()
    saved = skipped = errors = 0
    batch_limit = getattr(client, "batch_size", 1) or 1
    flush_threshold = compute_flush_threshold(batch_limit)

    with provider_progress(provider_key) as progress:
        for location_key, lat, lon in LOCATION_ITEMS:
            extras = LOCATION_EXTRAS.get(location_key, {})
            area = extras.get(area_attr) if area_attr else None
            if requires_area and not area:
                logging.warning("%s missing %s for %s; skipping.", label, area_attr, location_key)
                continue

            prefix = f"{label}[{location_key}]"
            location_dir = provider_dir / location_key
            location_dir.mkdir(parents=True, exist_ok=True)
            request_days: List[dt.date] = []
            request_payloads: List[Dict[str, object]] = []

            def _flush_requests() -> None:
                nonlocal saved, skipped, errors
                if not request_payloads:
                    return
                call_count = len(request_payloads)
                progress.add_expected(call_count)
                try:
                    payloads = client.get_dataset_batch(list(request_payloads))
                except Exception:
                    progress.complete(call_count)
                    raise
                progress.complete(call_count)
                for req_day, payload in zip(request_days, payloads):
                    output_path = location_dir / f"{req_day.isoformat()}.csv"
                    if isinstance(payload, Exception):
                        errors += 1
                        logging.warning("%s request failed for %s: %s", prefix, req_day, payload)
                        continue
                    try:
                        df_day = transform_fn(payload, req_day)
                        if df_day.empty:
                            logging.info("%s no data for %s", prefix, req_day)
                            skipped += 1
                            continue
                        df_day.to_csv(output_path, index=False)
                        saved += 1
                        logging.info("%s wrote %s", prefix, output_path)
                    except Exception as exc:  # pylint: disable=broad-except
                        errors += 1
                        logging.exception("%s unexpected error for %s: %s", prefix, req_day, exc)
                request_days.clear()
                request_payloads.clear()

            for day in DAY_RANGE:
                if day > today:
                    skipped += 1
                    continue
                output_path = location_dir / f"{day.isoformat()}.csv"
                if day != today and output_path.exists():
                    skipped += 1
                    continue
                request_payloads.append(
                    {
                        "area": area,
                        "start_date": day,
                        "end_date": day,
                        "latitude": lat,
                        "longitude": lon,
                    }
                )
                request_days.append(day)
                if len(request_payloads) >= flush_threshold:
                    _flush_requests()
            _flush_requests()

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

            request_days: List[dt.date] = []
            request_payloads: List[Dict[str, object]] = []
            batch_limit = getattr(client, "batch_size", 50) or 50
            flush_threshold = compute_flush_threshold(batch_limit)

            def _flush_requests() -> None:
                nonlocal saved, skipped, errors
                if not request_payloads:
                    return
                call_count = len(request_payloads)
                progress.add_expected(call_count)
                try:
                    payloads = client.get_historical_batch(list(request_payloads))
                except Exception:
                    progress.complete(call_count)
                    raise
                progress.complete(call_count)
                for req_day, payload in zip(request_days, payloads):
                    output_path = location_dir / f"{req_day.isoformat()}.csv"
                    if isinstance(payload, Exception):
                        errors += 1
                        logging.warning("%s historical failed for %s: %s", prefix, req_day, payload)
                        continue
                    try:
                        records = payload.get("list", [])
                        if not records:
                            logging.info("%s no data for %s", prefix, req_day)
                            skipped += 1
                            continue
                        df = pd.json_normalize(records)
                        if "dt" in df.columns:
                            df["timestamp"] = pd.to_datetime(df["dt"], unit="s", utc=True)
                        elif "time" in df.columns:
                            df["timestamp"] = pd.to_datetime(df["time"], utc=True)
                        else:
                            logging.info("%s unable to determine timestamp for %s", prefix, req_day)
                            skipped += 1
                            continue
                        df = df[df["timestamp"].dt.date == req_day]
                        if df.empty:
                            logging.info("%s no filtered data for %s", prefix, req_day)
                            skipped += 1
                            continue
                        df.to_csv(output_path, index=False)
                        saved += 1
                        logging.info("%s wrote %s", prefix, output_path)
                    except Exception as exc:  # pylint: disable=broad-except
                        errors += 1
                        logging.exception("%s unexpected error for %s: %s", prefix, req_day, exc)
                request_days.clear()
                request_payloads.clear()

            for day in DAY_RANGE:
                if day > today:
                    logging.debug("%s skipping %s (future dates unsupported).", prefix, day)
                    skipped += 1
                    continue

                start = dt.datetime.combine(day, dt.time(0, 0), tzinfo=UTC)
                end = min(start + dt.timedelta(days=1), now_utc)
                if end <= start:
                    logging.debug("%s skipping %s (no elapsed time yet).", prefix, day)
                    skipped += 1
                    continue

                output_path = location_dir / f"{day.isoformat()}.csv"
                if day != today and output_path.exists():
                    logging.debug("%s skipping %s (already exported).", prefix, day)
                    skipped += 1
                    continue

                request_payloads.append(
                    {
                        "location": (lat, lon),
                        "start_time": start,
                        "end_time": end,
                        "interval_type": "hour",
                        "units": "metric",
                    }
                )
                request_days.append(day)
                if len(request_payloads) >= flush_threshold:
                    _flush_requests()

            _flush_requests()

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
    if not any(PROVIDER_FLAGS.values()):
        logging.info("No providers enabled; skipping cached coverage chart.")
        return

    date_index = pd.date_range(START_DATE, END_DATE, freq="D")
    active_providers = [key for key, enabled in PROVIDER_FLAGS.items() if enabled and key in PROVIDER_LABELS]

    if not active_providers:
        logging.info("No active providers for chart; skipping.")
        return

    cmap = matplotlib.colors.ListedColormap(["#f0f0f0", "#2ca02c"])
    fig, axes = plt.subplots(len(active_providers), 1, figsize=(16, 2.5 * len(active_providers)), sharex=True)
    if len(active_providers) == 1:
        axes = [axes]

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
    fig.suptitle(f"Cached coverage {START_DATE.isoformat()} — {END_DATE.isoformat()}")
    plt.tight_layout(rect=(0, 0, 1, 0.97))
    cbar = fig.colorbar(im, ax=axes, orientation="horizontal", fraction=0.025, pad=0.08)
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

    if not enabled_providers:
        logging.warning("No providers enabled; nothing to export.")
        return

    total_providers = len(enabled_providers)
    logging.info("Starting export run across %d providers.", total_providers)

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

    logging.info("Provider exports complete. Generating coverage chart.")
    try:
        generate_cached_coverage_chart(CACHE_IMAGE_PATH)
    except Exception as exc:  # pylint: disable=broad-except
        logging.exception("Failed to generate coverage chart: %s", exc)


def main() -> None:
    logging.info("Weather export script initialised.")
    try:
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


if __name__ == "__main__":
    main()
