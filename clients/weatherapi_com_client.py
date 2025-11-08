import datetime as dt
import json
from pathlib import Path
from threading import Lock
from typing import Dict, Iterable, List, Optional, Sequence, Tuple, Union

try:
    import requests
except ImportError:  # pragma: no cover - runtime guard
    requests = None


class ConfigurationError(RuntimeError):
    """Raised when the WeatherAPI.com configuration is missing required values."""


class ApiError(RuntimeError):
    """Raised when WeatherAPI.com returns an error response."""


from . import BatchExecutorMixin
from .request_utils import build_request_headers


class WeatherApiClient(BatchExecutorMixin):
    """
    Wrapper for WeatherAPI.com endpoints used in this project.

    Docs: https://www.weatherapi.com/docs/
    """

    def __init__(self, config_path: Union[str, Path] = "weather_config.json", provider: str = "weatherapi_com") -> None:
        if requests is None:
            raise RuntimeError(
                "The 'requests' package is required to use WeatherApiClient. Install it with "
                "`python3 -m pip install requests`."
            )

        self.config_path = Path(config_path)
        self.provider = provider
        config = self._load_config()
        provider_cfg = config["providers"][provider]

        api_keys = self._collect_api_keys(provider_cfg)
        if not api_keys:
            raise ConfigurationError("WeatherAPI.com requires at least one API key in the config.")
        self._api_keys: List[str] = api_keys
        self._api_key_index = 0
        self._api_key_lock = Lock()
        self.api_key: str = self._api_keys[0]
        self.base_url: str = provider_cfg.get("baseUrl", "https://api.weatherapi.com/v1").rstrip("/")
        self.default_forecast_days: int = int(provider_cfg.get("defaultForecastDays", 3))
        self.default_aqi: str = provider_cfg.get("defaultAqi", "no")
        self.default_alerts: str = provider_cfg.get("defaultAlerts", "no")
        self.default_lang: Optional[str] = provider_cfg.get("defaultLang")
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
    def _collect_api_keys(provider_cfg: dict) -> List[str]:
        keys: List[str] = []
        for value in provider_cfg.get("apiKeys") or []:
            if value is None:
                continue
            token = str(value).strip()
            if token:
                keys.append(token)
        if not keys:
            fallback = provider_cfg.get("apiKey")
            if fallback:
                token = str(fallback).strip()
                if token:
                    keys.append(token)
        return keys

    def _next_api_key(self) -> str:
        with self._api_key_lock:
            key = self._api_keys[self._api_key_index]
            self._api_key_index = (self._api_key_index + 1) % len(self._api_keys)
            self.api_key = key
            return key

    @staticmethod
    def _normalise_query(location: Union[str, Sequence[float], Dict[str, float]]) -> str:
        if isinstance(location, str):
            if not location.strip():
                raise ValueError("Location string cannot be empty.")
            return location.strip()
        if isinstance(location, (tuple, list)):
            if len(location) != 2:
                raise ValueError("Location tuple/list must be (latitude, longitude).")
            lat, lon = location
            return f"{float(lat):.6f},{float(lon):.6f}"
        if isinstance(location, dict):
            try:
                lat = float(location["lat"])
                lon = float(location["lon"])
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError("Location dict must contain numeric 'lat' and 'lon' keys.") from exc
            return f"{lat:.6f},{lon:.6f}"
        raise TypeError("Location must be a string, (lat, lon) tuple/list, or {'lat': .., 'lon': ..} dict.")

    @staticmethod
    def _to_date(value: Union[str, dt.date, dt.datetime]) -> str:
        if isinstance(value, str):
            token = value.strip()
            if not token:
                raise ValueError("Date string cannot be empty.")
            return token
        if isinstance(value, dt.datetime):
            return value.date().isoformat()
        if isinstance(value, dt.date):
            return value.isoformat()
        raise TypeError("Date must be provided as a string, date, or datetime.")

    def _request(self, path: str, params: Optional[Dict[str, Union[str, int, float]]] = None) -> dict:
        url = f"{self.base_url}/{path.lstrip('/')}"
        query = dict(params or {})
        query["key"] = self._next_api_key()
        response = requests.get(url, params=query, timeout=30, headers=build_request_headers())
        if response.status_code >= 400:
            raise ApiError(f"WeatherAPI.com error {response.status_code}: {response.text}")
        try:
            return response.json()
        except ValueError as exc:
            raise ApiError("WeatherAPI.com returned a non-JSON response.") from exc

    def get_forecast(
        self,
        *,
        location: Union[str, Sequence[float], Dict[str, float]],
        days: Optional[int] = None,
        start_date: Optional[Union[str, dt.date, dt.datetime]] = None,
        end_date: Optional[Union[str, dt.date, dt.datetime]] = None,
        unix_start: Optional[int] = None,
        unix_end: Optional[int] = None,
        hour: Optional[int] = None,
        aqi: Optional[str] = None,
        alerts: Optional[str] = None,
        lang: Optional[str] = None,
        tp: Optional[int] = None,
    ) -> dict:
        """
        Fetch forecast data (includes daily + hourly segments).

        WeatherAPI.com accepts optional `dt`/`end_dt` parameters for date-scoped forecast
        (up to 14 days ahead on standard plans).
        """
        clamped_days = days or self.default_forecast_days
        clamped_days = max(1, min(14, int(clamped_days)))
        params: Dict[str, Union[str, int, float]] = {
            "q": self._normalise_query(location),
            "days": clamped_days,
            "aqi": aqi or self.default_aqi,
            "alerts": alerts or self.default_alerts,
        }
        if start_date:
            params["dt"] = self._to_date(start_date)
        if end_date:
            params["end_dt"] = self._to_date(end_date)
        if unix_start is not None:
            params["unixdt"] = int(unix_start)
        if unix_end is not None:
            params["unixend_dt"] = int(unix_end)
        if hour is not None:
            params["hour"] = int(hour)
        if lang or self.default_lang:
            params["lang"] = lang or self.default_lang or "en"
        if tp is not None:
            params["tp"] = int(tp)
        return self._request("forecast.json", params)

    def get_historical(
        self,
        *,
        location: Union[str, Sequence[float], Dict[str, float]],
        date: Union[str, dt.date, dt.datetime],
        end_date: Optional[Union[str, dt.date, dt.datetime]] = None,
        unix_start: Optional[int] = None,
        unix_end: Optional[int] = None,
        hour: Optional[int] = None,
        lang: Optional[str] = None,
    ) -> dict:
        """
        Fetch historical weather for a date or date range (max 30 days per call per docs).
        """
        params: Dict[str, Union[str, int, float]] = {
            "q": self._normalise_query(location),
            "dt": self._to_date(date),
        }
        if end_date:
            params["end_dt"] = self._to_date(end_date)
        if unix_start is not None:
            params["unixdt"] = int(unix_start)
        if unix_end is not None:
            params["unixend_dt"] = int(unix_end)
        if hour is not None:
            params["hour"] = int(hour)
        if lang or self.default_lang:
            params["lang"] = lang or self.default_lang or "en"
        return self._request("history.json", params)

    def get_real_time(
        self,
        *,
        location: Union[str, Sequence[float], Dict[str, float]],
        lang: Optional[str] = None,
    ) -> dict:
        """Convenience helper for the realtime endpoint."""
        params: Dict[str, Union[str, int, float]] = {
            "q": self._normalise_query(location),
        }
        if lang or self.default_lang:
            params["lang"] = lang or self.default_lang or "en"
        return self._request("current.json", params)

    def get_future(
        self,
        *,
        location: Union[str, Sequence[float], Dict[str, float]],
        date: Optional[Union[str, dt.date, dt.datetime]] = None,
        lang: Optional[str] = None,
    ) -> dict:
        """
        Fetch future weather (3-hour interval) for dates 14-365 days ahead.
        """
        params: Dict[str, Union[str, int, float]] = {
            "q": self._normalise_query(location),
        }
        if date:
            params["dt"] = self._to_date(date)
        if lang or self.default_lang:
            params["lang"] = lang or self.default_lang or "en"
        return self._request("future.json", params)

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

    def get_real_time_batch(
        self,
        requests: Iterable[dict],
        *,
        batch_size: Optional[int] = None,
        max_workers: Optional[int] = None,
    ) -> List[dict]:
        effective_batch = self.batch_size if batch_size is None else batch_size
        return self._run_batch(requests, self.get_real_time, batch_size=effective_batch, max_workers=max_workers)

    def get_future_batch(
        self,
        requests: Iterable[dict],
        *,
        batch_size: Optional[int] = None,
        max_workers: Optional[int] = None,
    ) -> List[dict]:
        effective_batch = self.batch_size if batch_size is None else batch_size
        return self._run_batch(requests, self.get_future, batch_size=effective_batch, max_workers=max_workers)
