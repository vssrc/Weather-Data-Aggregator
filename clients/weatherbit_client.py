import datetime as dt
import json
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Union

try:
    import requests
except ImportError:  # pragma: no cover - runtime guard
    requests = None


class ConfigurationError(RuntimeError):
    """Raised when the Weatherbit configuration is missing required values."""


class ApiError(RuntimeError):
    """Raised when Weatherbit returns a non-success response."""


from . import BatchExecutorMixin
from .request_utils import build_request_headers


class WeatherbitClient(BatchExecutorMixin):
    """
    Wrapper for Weatherbit hourly forecast and historical endpoints.

    Docs: https://www.weatherbit.io/api
    """

    def __init__(self, config_path: Union[str, Path] = "weather_config.json", provider: str = "weatherbit") -> None:
        if requests is None:
            raise RuntimeError(
                "The 'requests' package is required to use WeatherbitClient. "
                "Install it with `python3 -m pip install requests`."
            )

        self.config_path = Path(config_path)
        self.provider = provider
        config = self._load_config()
        provider_cfg = config["providers"][provider]

        self.api_key: str = provider_cfg["apiKey"]
        self.forecast_url: str = provider_cfg.get("forecastUrl", "https://api.weatherbit.io/v2.0/forecast/hourly")
        self.historical_url: str = provider_cfg.get("historicalUrl", "https://api.weatherbit.io/v2.0/history/subhourly")
        self.default_units: str = provider_cfg.get("defaultUnits", "M")
        self.default_hours: int = int(provider_cfg.get("defaultHours", 48))
        self.default_timezone: str = provider_cfg.get("defaultTimezone", "UTC")
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
    def _normalise_location(location: Union[str, Sequence[float], Dict[str, float]]) -> Dict[str, float]:
        if isinstance(location, str):
            parts = [part.strip() for part in location.split(",")]
            if len(parts) != 2:
                raise ValueError("Provide location as 'latitude,longitude'.")
            lat, lon = map(float, parts)
            return {"lat": lat, "lon": lon}
        if isinstance(location, (tuple, list)):
            if len(location) != 2:
                raise ValueError("Expecting (latitude, longitude).")
            lat, lon = location
            return {"lat": float(lat), "lon": float(lon)}
        if isinstance(location, dict):
            try:
                lat = float(location["lat"])
                lon = float(location["lon"])
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError("Location dict needs numeric 'lat' and 'lon' keys.") from exc
            return {"lat": lat, "lon": lon}
        raise TypeError("Location must be a string, (lat, lon) pair, or {'lat': .., 'lon': ..} dictionary.")

    @staticmethod
    def _format_datetime(value: Union[str, dt.date, dt.datetime]) -> str:
        if isinstance(value, str):
            token = value.strip()
            if not token:
                raise ValueError("Datetime string cannot be empty.")
            return token
        if isinstance(value, dt.datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=dt.timezone.utc)
            return value.astimezone(dt.timezone.utc).strftime("%Y-%m-%d:%H")
        if isinstance(value, dt.date):
            return dt.datetime.combine(value, dt.time(0, 0), tzinfo=dt.timezone.utc).strftime("%Y-%m-%d:%H")
        raise TypeError("Datetime must be string, date, or datetime.")

    def _request(self, url: str, params: Dict[str, Union[str, int, float]]) -> dict:
        query = dict(params)
        query["key"] = self.api_key
        response = requests.get(url, params=query, timeout=30, headers=build_request_headers())
        if response.status_code >= 400:
            raise ApiError(f"Weatherbit error {response.status_code}: {response.text}")
        try:
            return response.json()
        except ValueError as exc:
            raise ApiError("Weatherbit returned a non-JSON response.") from exc

    def get_forecast(
        self,
        *,
        location: Union[str, Sequence[float], Dict[str, float]],
        hours: Optional[int] = None,
        units: Optional[str] = None,
        **extra_params: Union[str, int, float],
    ) -> dict:
        """
        Fetch hourly forecast (up to 120 hours depending on plan).
        """
        loc = self._normalise_location(location)
        params: Dict[str, Union[str, int, float]] = {
            "lat": loc["lat"],
            "lon": loc["lon"],
            "hours": int(hours or self.default_hours),
            "units": units or self.default_units,
        }
        if extra_params:
            params.update(extra_params)
        return self._request(self.forecast_url, params)

    def get_historical(
        self,
        *,
        location: Union[str, Sequence[float], Dict[str, float]],
        start_time: Union[str, dt.date, dt.datetime],
        end_time: Union[str, dt.date, dt.datetime],
        units: Optional[str] = None,
        **extra_params: Union[str, int, float],
    ) -> dict:
        """
        Fetch historical hourly data (Weatherbit accepts UTC timestamps in start/end).
        """
        loc = self._normalise_location(location)
        params: Dict[str, Union[str, int, float]] = {
            "lat": loc["lat"],
            "lon": loc["lon"],
            "start_date": self._format_datetime(start_time),
            "end_date": self._format_datetime(end_time),
            "units": units or self.default_units,
        }
        if self.default_timezone:
            params.setdefault("tz", self.default_timezone)
        if extra_params:
            params.update(extra_params)
        return self._request(self.historical_url, params)

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
