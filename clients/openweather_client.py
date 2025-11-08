import datetime as dt
import json
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple, Union

try:
    import requests
except ImportError:  # pragma: no cover - runtime guard
    requests = None


class ConfigurationError(RuntimeError):
    """Raised when the OpenWeather configuration is missing required values."""


class ApiError(RuntimeError):
    """Raised when OpenWeather responds with an error."""


from . import BatchExecutorMixin
from .request_utils import build_request_headers


class OpenWeatherClient(BatchExecutorMixin):
    """
    Lightweight wrapper around OpenWeather Current and Historical APIs.

    Docs:
      - Current weather: https://openweathermap.org/current
      - Historical weather: https://openweathermap.org/history
    """

    def __init__(self, config_path: Union[str, Path] = "weather_config.json", provider: str = "openweather") -> None:
        if requests is None:
            raise RuntimeError(
                "The 'requests' package is required to use OpenWeatherClient. "
                "Install it with `python3 -m pip install requests`."
            )

        self.config_path = Path(config_path)
        self.provider = provider
        config = self._load_config()
        provider_cfg = config["providers"][provider]

        self.api_key: str = provider_cfg["apiKey"]
        self.current_url: str = provider_cfg.get("currentUrl", "https://api.openweathermap.org/data/2.5/weather")
        self.history_url: str = provider_cfg.get(
            "historyUrl", "https://history.openweathermap.org/data/2.5/history/city"
        )
        self.default_units: str = provider_cfg.get("defaultUnits", "metric")
        self.default_language: str = provider_cfg.get("defaultLanguage", "en")
        self.default_history_type: str = provider_cfg.get("defaultHistoryType", "hour")
        # Backwards compatibility with previous attribute name
        self.forecast_url: str = self.current_url
        self.batch_size: int = int(provider_cfg.get("batchSize", 100))

    def _load_config(self) -> dict:
        if not self.config_path.exists():
            raise ConfigurationError(f"Config file not found: {self.config_path}")
        with self.config_path.open("r", encoding="utf-8") as handle:
            config = json.load(handle)
        providers = config.get("providers", {})
        if self.provider not in providers:
            raise ConfigurationError(f"Provider '{self.provider}' is not defined in {self.config_path}")
        return config

    @staticmethod
    def _normalise_location(location: Union[str, Sequence[float], Dict[str, float]]) -> Tuple[float, float]:
        if isinstance(location, str):
            parts = [part.strip() for part in location.split(",")]
            if len(parts) != 2:
                raise ValueError("Provide location as 'latitude,longitude'.")
            lat, lon = map(float, parts)
            return lat, lon
        if isinstance(location, (tuple, list)):
            if len(location) != 2:
                raise ValueError("Expecting (latitude, longitude).")
            lat, lon = location
            return float(lat), float(lon)
        if isinstance(location, dict):
            try:
                lat = float(location["lat"])
                lon = float(location["lon"])
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError("Location dict needs numeric 'lat' and 'lon' keys.") from exc
            return lat, lon
        raise TypeError("Location must be a string, (lat, lon) pair, or {'lat': .., 'lon': ..} dictionary.")

    @staticmethod
    def _to_unix(timestamp: Union[str, dt.date, dt.datetime]) -> int:
        if isinstance(timestamp, str):
            parsed = dt.datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        elif isinstance(timestamp, dt.datetime):
            parsed = timestamp
        elif isinstance(timestamp, dt.date):
            parsed = dt.datetime.combine(timestamp, dt.time(0, 0), tzinfo=dt.timezone.utc)
        else:
            raise TypeError("Timestamps must be ISO strings, date, or datetime objects.")

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return int(parsed.timestamp())

    def _request(self, url: str, params: Dict[str, Union[str, int, float]]) -> dict:
        query = dict(params)
        query["appid"] = self.api_key
        response = requests.get(url, params=query, timeout=30, headers=build_request_headers())
        if response.status_code >= 400:
            raise ApiError(f"OpenWeather error {response.status_code}: {response.text}")
        try:
            return response.json()
        except ValueError as exc:
            raise ApiError("OpenWeather returned a non-JSON response.") from exc

    def get_current(
        self,
        *,
        location: Union[str, Sequence[float], Dict[str, float]],
        units: Optional[str] = None,
        language: Optional[str] = None,
        **extra_params: Union[str, int, float],
    ) -> dict:
        """Fetch current weather data for the supplied location."""
        lat, lon = self._normalise_location(location)
        params: Dict[str, Union[str, int, float]] = {
            "lat": round(lat, 6),
            "lon": round(lon, 6),
            "units": units or self.default_units,
            "lang": language or self.default_language,
        }
        if extra_params:
            params.update(extra_params)
        return self._request(self.current_url, params)

    def get_historical(
        self,
        *,
        location: Union[str, Sequence[float], Dict[str, float]],
        start_time: Union[str, dt.date, dt.datetime],
        end_time: Union[str, dt.date, dt.datetime],
        interval_type: Optional[str] = None,
        units: Optional[str] = None,
        **extra_params: Union[str, int, float],
    ) -> dict:
        """
        Fetch historical weather data using the History API.

        The API expects UNIX timestamps for `start` and `end`, and supports `type=hour`
        for hourly aggregations.
        """
        if interval_type is None:
            interval_type = self.default_history_type

        start_unix = self._to_unix(start_time)
        end_unix = self._to_unix(end_time)
        if end_unix <= start_unix:
            raise ValueError("end_time must be after start_time when requesting OpenWeather history.")

        lat, lon = self._normalise_location(location)
        params: Dict[str, Union[str, int, float]] = {
            "lat": round(lat, 6),
            "lon": round(lon, 6),
            "start": start_unix,
            "end": end_unix,
            "type": interval_type,
            "units": units or self.default_units,
        }
        if extra_params:
            params.update(extra_params)
        return self._request(self.history_url, params)

    def get_current_batch(
        self,
        requests: Iterable[dict],
        *,
        batch_size: Optional[int] = None,
        max_workers: Optional[int] = None,
    ) -> List[dict]:
        effective_batch = self.batch_size if batch_size is None else batch_size
        return self._run_batch(requests, self.get_current, batch_size=effective_batch, max_workers=max_workers)

    def get_historical_batch(
        self,
        requests: Iterable[dict],
        *,
        batch_size: Optional[int] = None,
        max_workers: Optional[int] = None,
    ) -> List[dict]:
        effective_batch = self.batch_size if batch_size is None else batch_size
        return self._run_batch(requests, self.get_historical, batch_size=effective_batch, max_workers=max_workers)
