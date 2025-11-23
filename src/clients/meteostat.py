import datetime as dt
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union

try:
    from meteostat import Hourly, Point  # type: ignore[import]
except ImportError:  # pragma: no cover - runtime guard
    Hourly = None  # type: ignore[assignment]
    Point = None  # type: ignore[assignment]

import pandas as pd

from .config_loader import load_provider_config
from .base import BatchExecutorMixin, WeatherClient

class ConfigurationError(RuntimeError):
    """Raised when the Meteostat configuration is missing required values."""


from .request_utils import normalise_location

class MeteostatClient(WeatherClient, BatchExecutorMixin):
    """Lightweight wrapper around the Meteostat Python library for hourly data."""

    def __init__(self, config_path: Union[str, Path] = "config.json", provider: str = "meteostat") -> None:
        if Hourly is None or Point is None:
            raise RuntimeError(
                "The 'meteostat' package is required to use MeteostatClient. "
                "Install it with `python3 -m pip install meteostat`."
            )

        self.config_path = Path(config_path)
        self.provider = provider

        # Load config for maxDaysPerRequest only (optional)
        try:
            import json
            if Path(config_path).exists():
                config = json.loads(Path(config_path).read_text())
                provider_cfg = config.get("providers", {}).get(provider, {})
            else:
                provider_cfg = {}
        except Exception:
            provider_cfg = {}

        self._config = {"providers": {provider: provider_cfg}}

        # Hardcoded defaults
        self.default_timezone: str = "UTC"
        self.batch_size: int = 50
        self.request_throttle_seconds = 0.0
        self.include_model_data: bool = False

    @staticmethod
    def _normalise_time(value: Union[str, dt.date, dt.datetime]) -> dt.datetime:
        if isinstance(value, dt.datetime):
            return value
        if isinstance(value, dt.date):
            return dt.datetime.combine(value, dt.time(0, 0))
        if isinstance(value, str):
            return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        raise TypeError("Time values must be datetime, date, or ISO string.")

    def _get_hourly(
        self,
        *,
        location: Union[str, Sequence[float], Mapping[str, float]],
        start_time: Union[str, dt.date, dt.datetime],
        end_time: Union[str, dt.date, dt.datetime],
        model: Optional[str] = None,
    ) -> pd.DataFrame:
        lat, lon = normalise_location(location)
        start = self._normalise_time(start_time)
        end = self._normalise_time(end_time)

        # If end time is at midnight (00:00:00), set it to end of day (23:59:59)
        # to ensure we get the full day's hourly data
        if end.hour == 0 and end.minute == 0 and end.second == 0:
            end = end.replace(hour=23, minute=59, second=59)

        point = Point(lat, lon)
        query = Hourly(point, start, end, model=model or ("gfs" if self.include_model_data else None))
        df = query.fetch()
        if df.empty:
            return pd.DataFrame()

        df = df.reset_index().rename(columns={"time": "timestamp"})
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df

    def get_historical_data(
        self,
        *,
        location: Union[str, Tuple[float, float]],
        start_date: Union[str, dt.date],
        end_date: Union[str, dt.date],
    ) -> pd.DataFrame:
        """Return historical data as a DataFrame for consistency with other providers."""
        df = self._get_hourly(
            location=location,
            start_time=start_date,
            end_time=end_date,
        )
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame()

    def get_hourly_batch(
        self,
        requests: Iterable[Mapping[str, object]],
        *,
        batch_size: Optional[int] = None,
        max_workers: Optional[int] = None,
    ) -> List[Union[List[dict], Exception]]:
        effective_batch = self.batch_size if batch_size is None else batch_size
        return self._run_batch(
            requests,
            self._get_hourly,
            batch_size=effective_batch,
            max_workers=max_workers,
        )
