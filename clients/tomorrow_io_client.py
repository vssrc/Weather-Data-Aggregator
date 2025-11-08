import argparse
import datetime as dt
import json
from pathlib import Path
import itertools
from typing import Iterable, List, Optional, Sequence, Union

try:
    import requests
except ImportError:  # pragma: no cover - dependency hint for runtime errors
    requests = None


class ConfigurationError(RuntimeError):
    """Raised when the local configuration file is missing data we need."""


class ApiError(RuntimeError):
    """Raised when Tomorrow.io responds with an error payload or status."""


class _ApiKeyPool:
    """Simple round-robin helper so we can rotate across multiple API keys."""

    def __init__(self, keys: Sequence[str]) -> None:
        sanitized = [key.strip() for key in keys if key and key.strip()]
        if not sanitized:
            raise ConfigurationError("No Tomorrow.io API keys found in config.")
        self._cycle = itertools.cycle(sanitized)

    def next(self) -> str:
        return next(self._cycle)


from . import BatchExecutorMixin
from .request_utils import build_request_headers


class TomorrowIOClient(BatchExecutorMixin):
    """
    Light-weight wrapper around the Tomorrow.io Timelines API.

    The Timelines API powers current, forecast, and historical weather data.
    Each request needs a location, one or more timesteps (ex: `1h`, `1d`,
    `current`), and the desired weather fields. Everything else is optional
    according to https://docs.tomorrow.io/reference/get-timelines.
    """

    def __init__(self, config_path: Union[str, Path] = "weather_config.json", provider: str = "tomorrow_io") -> None:
        self.config_path = Path(config_path)
        self.provider = provider
        self._config = self._load_config()
        provider_cfg = self._config["providers"][provider]

        self.base_url: str = provider_cfg.get("baseUrl", "https://api.tomorrow.io/v4").rstrip("/")
        self.default_units: str = provider_cfg.get("defaultUnits", "metric")
        self.default_timezone: str = provider_cfg.get("defaultTimezone", "UTC")
        self.default_timesteps: Sequence[str] = tuple(provider_cfg.get("defaultTimesteps", ["1h"]))
        self._key_pool = _ApiKeyPool(provider_cfg["apiKeys"])
        self.batch_size: int = int(provider_cfg.get("batchSize", 100))

        if requests is None:
            raise RuntimeError(
                "The 'requests' package is required to use TomorrowIOClient. "
                "Install it with `python3 -m pip install requests`."
            )

    def _load_config(self) -> dict:
        if not self.config_path.exists():
            raise ConfigurationError(f"Config file not found: {self.config_path}")
        with self.config_path.open("r", encoding="utf-8") as handle:
            config = json.load(handle)
        if "providers" not in config or self.provider not in config["providers"]:
            raise ConfigurationError(f"Provider '{self.provider}' is not defined in {self.config_path}")
        return config

    def _next_api_key(self) -> str:
        return self._key_pool.next()

    @staticmethod
    def _normalise_location(location: Union[str, Sequence[float], dict]) -> str:
        if isinstance(location, str):
            if not location.strip():
                raise ValueError("Location string cannot be empty.")
            return location.strip()

        if isinstance(location, (tuple, list)):
            if len(location) != 2:
                raise ValueError("Expecting a (latitude, longitude) pair.")
            lat, lon = location
            return f"{float(lat):.6f},{float(lon):.6f}"

        if isinstance(location, dict):
            try:
                lat = float(location["lat"])
                lon = float(location["lon"])
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError("Location dict needs numeric 'lat' and 'lon' keys.") from exc
            return f"{lat:.6f},{lon:.6f}"

        raise TypeError("Location must be a string, (lat, lon) pair, or {'lat': .., 'lon': ..} dictionary.")

    @staticmethod
    def _to_iso8601(timestamp: Union[str, dt.datetime, dt.date, None]) -> Optional[str]:
        if timestamp is None:
            return None
        if isinstance(timestamp, str):
            return timestamp
        if isinstance(timestamp, dt.date) and not isinstance(timestamp, dt.datetime):
            timestamp = dt.datetime.combine(timestamp, dt.time(0, 0), tzinfo=dt.timezone.utc)
        if isinstance(timestamp, dt.datetime):
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=dt.timezone.utc)
            return timestamp.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")
        raise TypeError("Timestamps must be ISO-8601 strings, datetime, or date objects.")

    @staticmethod
    def _serialise_list(values: Optional[Iterable[str]], fallback: Sequence[str]) -> str:
        if not values:
            values = fallback
        cleaned: List[str] = [value.strip() for value in values if value and value.strip()]
        if not cleaned:
            raise ValueError("At least one value is required when constructing the API request.")
        return ",".join(cleaned)

    def _request_timelines(
        self,
        *,
        location: Union[str, Sequence[float], dict],
        fields: Optional[Iterable[str]] = None,
        timesteps: Optional[Iterable[str]] = None,
        units: Optional[str] = None,
        timezone: Optional[str] = None,
        start_time: Optional[Union[str, dt.datetime, dt.date]] = None,
        end_time: Optional[Union[str, dt.datetime, dt.date]] = None,
    ) -> dict:
        params = {
            "location": self._normalise_location(location),
            "fields": self._serialise_list(fields, ("temperature", "humidity", "windSpeed")),
            "timesteps": self._serialise_list(timesteps, self.default_timesteps),
            "units": (units or self.default_units),
            "timezone": (timezone or self.default_timezone),
            "apikey": self._next_api_key(),
        }

        start_iso = self._to_iso8601(start_time)
        end_iso = self._to_iso8601(end_time)
        if start_iso:
            params["startTime"] = start_iso
        if end_iso:
            params["endTime"] = end_iso

        response = requests.get(
            f"{self.base_url}/timelines",
            params=params,
            timeout=30,
            headers=build_request_headers(),
        )
        if response.status_code >= 400:
            raise ApiError(f"Tomorrow.io error {response.status_code}: {response.text}")
        try:
            return response.json()
        except ValueError as exc:
            raise ApiError("Tomorrow.io returned a non-JSON response.") from exc

    def get_historical(
        self,
        *,
        location: Union[str, Sequence[float], dict],
        start_time: Union[str, dt.datetime, dt.date],
        end_time: Union[str, dt.datetime, dt.date],
        fields: Optional[Iterable[str]] = None,
        timesteps: Optional[Iterable[str]] = None,
        units: Optional[str] = None,
        timezone: Optional[str] = None,
    ) -> dict:
        """
        Fetch historical weather data for a specific location and time window.

        Tomorrow.io treats historical requests as Timelines queries with both
        `startTime` and `endTime` in the past. The fine print lives at
        https://docs.tomorrow.io/reference/get-timelines.
        """
        start = self._to_iso8601(start_time)
        end = self._to_iso8601(end_time)
        if not start or not end:
            raise ValueError("Both start_time and end_time are required for historical queries.")
        return self._request_timelines(
            location=location,
            fields=fields,
            timesteps=timesteps,
            units=units,
            timezone=timezone,
            start_time=start,
            end_time=end,
        )

    def get_forecast(
        self,
        *,
        location: Union[str, Sequence[float], dict],
        timeframe_hours: int = 48,
        fields: Optional[Iterable[str]] = None,
        timesteps: Optional[Iterable[str]] = None,
        units: Optional[str] = None,
        timezone: Optional[str] = None,
        start_time: Optional[Union[str, dt.datetime, dt.date]] = None,
        end_time: Optional[Union[str, dt.datetime, dt.date]] = None,
    ) -> dict:
        """
        Fetch forecast weather data for the upcoming window (defaults to 48 hours).

        You can override the automatic start/end calculations with explicit
        timestamps if you need a different cadence or horizon.
        """
        if timeframe_hours <= 0:
            raise ValueError("timeframe_hours must be positive.")

        now_utc = dt.datetime.now(dt.timezone.utc)
        start = start_time or now_utc
        end = end_time or (now_utc + dt.timedelta(hours=timeframe_hours))

        return self._request_timelines(
            location=location,
            fields=fields,
            timesteps=timesteps,
            units=units,
            timezone=timezone,
            start_time=start,
            end_time=end,
        )

    def get_forecast_batch(
        self,
        requests: Iterable[dict],
        *,
        batch_size: Optional[int] = None,
        max_workers: Optional[int] = None,
    ) -> List[dict]:
        effective_batch = self.batch_size if batch_size is None else batch_size
        return self._run_batch(requests, self.get_forecast, batch_size=effective_batch, max_workers=max_workers)

    def get_historical_batch(
        self,
        requests: Iterable[dict],
        *,
        batch_size: Optional[int] = None,
        max_workers: Optional[int] = None,
    ) -> List[dict]:
        effective_batch = self.batch_size if batch_size is None else batch_size
        return self._run_batch(requests, self.get_historical, batch_size=effective_batch, max_workers=max_workers)


def _parse_csv_list(value: str) -> List[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _parse_datetime(value: Optional[str]) -> Optional[dt.datetime]:
    if not value:
        return None
    try:
        dt_obj = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Unable to parse datetime '{value}'. Use ISO-8601 format.") from exc
    if dt_obj.tzinfo is None:
        dt_obj = dt_obj.replace(tzinfo=dt.timezone.utc)
    return dt_obj


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Query Tomorrow.io timelines for historical or forecast data.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("mode", choices=("historical", "forecast"), help="Which type of query to execute.")
    parser.add_argument(
        "location",
        help="Location identifier understood by Tomorrow.io (ex: '42.3601,-71.0589' or 'new york,ny').",
    )
    parser.add_argument(
        "--fields",
        default="temperature,humidity,windSpeed",
        help="Comma separated list of weather fields documented by Tomorrow.io.",
    )
    parser.add_argument(
        "--timesteps",
        default="1h",
        help="Comma separated timesteps (ex: current,1h,1d).",
    )
    parser.add_argument("--units", choices=("metric", "imperial"), help="Unit system for the response.")
    parser.add_argument("--timezone", help="Timezone identifier or 'auto' to follow the location.")
    parser.add_argument("--config", default="weather_config.json", help="Path to the local configuration JSON file.")
    parser.add_argument(
        "--start",
        type=_parse_datetime,
        help="Start time (ISO-8601). Optional for forecasts; required for historical queries.",
    )
    parser.add_argument(
        "--end",
        type=_parse_datetime,
        help="End time (ISO-8601). Optional for forecasts; required for historical queries.",
    )
    parser.add_argument(
        "--hours",
        type=int,
        default=48,
        help="Forecast window in hours when start/end are not provided explicitly.",
    )

    args = parser.parse_args()

    client = TomorrowIOClient(config_path=args.config)
    fields = _parse_csv_list(args.fields)
    timesteps = _parse_csv_list(args.timesteps)

    if args.mode == "historical":
        if args.start is None or args.end is None:
            parser.error("--start and --end are required for historical queries.")
        payload = client.get_historical(
            location=args.location,
            start_time=args.start,
            end_time=args.end,
            fields=fields,
            timesteps=timesteps,
            units=args.units,
            timezone=args.timezone,
        )
    else:
        payload = client.get_forecast(
            location=args.location,
            timeframe_hours=args.hours,
            start_time=args.start,
            end_time=args.end,
            fields=fields,
            timesteps=timesteps,
            units=args.units,
            timezone=args.timezone,
        )

    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
