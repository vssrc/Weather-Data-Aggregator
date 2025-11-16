import datetime as dt
import json
from pathlib import Path
from typing import Optional, List, Tuple, Union, Iterable, Sequence, Dict
from src.core.dates import iter_date_windows

try:
    import requests
except ImportError:  # pragma: no cover - dependency hint for runtime errors
    requests = None


class ConfigurationError(RuntimeError):
    """Raised when the Open-Meteo configuration is missing required values."""


class ApiError(RuntimeError):
    """Raised when Open-Meteo responds with a non-success status."""


from .config_loader import load_provider_config
from .base import BatchExecutorMixin, WeatherClient
from .request_utils import build_request_headers, normalise_location
import pandas as pd

class OpenMeteoClient(WeatherClient, BatchExecutorMixin):
    """
    Minimal wrapper around the Open-Meteo API.

    Forecast reference: https://open-meteo.com/en/docs
    Historical archive reference: https://open-meteo.com/en/docs/historical-weather-api
    """

    def __init__(self, config_path: Union[str, Path] = "config.json", provider: str = "open_meteo") -> None:
        if requests is None:
            raise RuntimeError(
                "The 'requests' package is required to use OpenMeteoClient. "
                "Install it with `python3 -m pip install requests`."
            )

        self.config_path = Path(config_path)
        self.provider = provider

        # Load config for maxDaysPerRequest only (optional)
        try:
            import json
            if self.config_path.exists():
                config = json.loads(self.config_path.read_text())
                provider_cfg = config.get("providers", {}).get(provider, {})
            else:
                provider_cfg = {}
        except Exception:
            provider_cfg = {}

        self._config = {"providers": {provider: provider_cfg}}

        # Hardcoded defaults - no need for config
        self.forecast_url: str = "https://api.open-meteo.com/v1/forecast"
        self.historical_url: str = "https://archive-api.open-meteo.com/v1/archive"
        self.default_timezone: str = "UTC"
        self.default_hourly: Sequence[str] = ("temperature_2m", "relative_humidity_2m", "wind_speed_10m")
        self.default_daily: Sequence[str] = ()
        self.default_units: Dict[str, str] = {}
        self.batch_size: int = 1000

    @staticmethod
    def _serialise_list(values: Optional[Iterable[str]], fallback: Sequence[str]) -> Optional[str]:
        if values is None:
            values = fallback
        if not values:
            return None
        cleaned = [value.strip() for value in values if value and value.strip()]
        return ",".join(cleaned) if cleaned else None

    @staticmethod
    def _to_date_str(value: Union[str, dt.date, dt.datetime, None]) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, str):
            return value
        if isinstance(value, dt.datetime):
            value = value.date()
        if isinstance(value, dt.date):
            return value.isoformat()
        raise TypeError("Dates must be provided as strings, date, or datetime objects.")

    def _apply_units(self, params: Dict[str, Union[str, int, float]], units: Optional[Dict[str, str]]) -> None:
        combined = dict(self.default_units)
        if units:
            combined.update(units)
        for key, value in combined.items():
            params[key] = value

    @staticmethod
    def _execute(url: str, params: Dict[str, Union[str, int, float]]) -> dict:
        response = requests.get(url, params=params, timeout=30, headers=build_request_headers())
        if response.status_code >= 400:
            raise ApiError(f"Open-Meteo error {response.status_code}: {response.text}")
        try:
            return response.json()
        except ValueError as exc:
            raise ApiError("Open-Meteo returned a non-JSON response.") from exc

    def get_forecast(
        self,
        *,
        location: Union[str, Sequence[float], Dict[str, float]],
        hourly: Optional[Iterable[str]] = None,
        daily: Optional[Iterable[str]] = None,
        timezone: Optional[str] = None,
        start_date: Optional[Union[str, dt.date, dt.datetime]] = None,
        end_date: Optional[Union[str, dt.date, dt.datetime]] = None,
        forecast_days: Optional[int] = None,
        past_days: Optional[int] = None,
        units: Optional[Dict[str, str]] = None,
        **extra_params: Union[str, int, float],
    ) -> dict:
        """
        Fetch forecast data for a specific location.

        You can scope the response using either start/end dates or the
        `forecast_days` / `past_days` convenience parameters from Open-Meteo.
        """
        lat, lon = normalise_location(location)
        params: Dict[str, Union[str, int, float]] = {
            "latitude": round(lat, 6),
            "longitude": round(lon, 6),
        }

        hourly_str = self._serialise_list(hourly, self.default_hourly)
        if hourly_str:
            params["hourly"] = hourly_str

        daily_str = self._serialise_list(daily, self.default_daily)
        if daily_str:
            params["daily"] = daily_str

        if timezone or self.default_timezone:
            params["timezone"] = timezone or self.default_timezone

        start = self._to_date_str(start_date)
        end = self._to_date_str(end_date)
        if start:
            params["start_date"] = start
        if end:
            params["end_date"] = end

        if forecast_days is not None:
            params["forecast_days"] = int(forecast_days)
        if past_days is not None:
            params["past_days"] = int(past_days)

        self._apply_units(params, units)

        params.update(extra_params)
        return self._execute(self.forecast_url, params)

    def _get_historical(
        self,
        *,
        location: Union[str, Sequence[float], Dict[str, float]],
        start_date: Union[str, dt.date, dt.datetime],
        end_date: Union[str, dt.date, dt.datetime],
        hourly: Optional[Iterable[str]] = None,
        daily: Optional[Iterable[str]] = None,
        timezone: Optional[str] = None,
        units: Optional[Dict[str, str]] = None,
        **extra_params: Union[str, int, float],
    ) -> dict:
        """Fetch historical weather data from the Open-Meteo archive API."""
        lat, lon = normalise_location(location)
        params: Dict[str, Union[str, int, float]] = {
            "latitude": round(lat, 6),
            "longitude": round(lon, 6),
            "start_date": self._to_date_str(start_date),
            "end_date": self._to_date_str(end_date),
        }


        hourly_str = self._serialise_list(hourly, self.default_hourly)
        if hourly_str:
            params["hourly"] = hourly_str

        daily_str = self._serialise_list(daily, self.default_daily)
        if daily_str:
            params["daily"] = daily_str

        if timezone or self.default_timezone:
            params["timezone"] = timezone or self.default_timezone

        self._apply_units(params, units)

        params.update(extra_params)
        return self._execute(self.historical_url, params)

    def get_historical_data(
        self,
        *,
        location: Union[str, Tuple[float, float]],
        start_date: Union[str, dt.date],
        end_date: Union[str, dt.date],
    ) -> pd.DataFrame:
        all_dfs = []
        max_days_per_request = int(self._config.get("providers", {}).get(self.provider, {}).get("maxDaysPerRequest", 30))
        
        for window_start, window_end in iter_date_windows(start_date, end_date, max_days_per_request):
            data = self._get_historical(
                location=location,
                start_date=window_start,
                end_date=window_end,
            )
            df = pd.DataFrame(data.get("hourly"))
            if not df.empty and "time" in df.columns:
                df = df.rename(columns={"time": "timestamp"})
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                all_dfs.append(df)
        
        if not all_dfs:
            return pd.DataFrame()
            
        return pd.concat(all_dfs, ignore_index=True)

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
        return self._run_batch(requests, self._get_historical, batch_size=effective_batch, max_workers=max_workers)
