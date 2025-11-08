import datetime as dt
import json
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union

try:
    from meteostat import Hourly, Point  # type: ignore[import]
except ImportError:  # pragma: no cover - runtime guard
    Hourly = None  # type: ignore[assignment]
    Point = None  # type: ignore[assignment]

import pandas as pd

from . import BatchExecutorMixin


class ConfigurationError(RuntimeError):
    """Raised when the Meteostat configuration is missing required values."""


class MeteostatClient(BatchExecutorMixin):
    """Lightweight wrapper around the Meteostat Python library for hourly data."""

    def __init__(self, config_path: Union[str, Path] = "weather_config.json", provider: str = "meteostat") -> None:
        if Hourly is None or Point is None:
            raise RuntimeError(
                "The 'meteostat' package is required to use MeteostatClient. "
                "Install it with `python3 -m pip install meteostat`."
            )

        self.config_path = Path(config_path)
        self.provider = provider

        config = self._load_config()
        provider_cfg = config["providers"][provider]

        self.default_timezone: str = provider_cfg.get("defaultTimezone", "UTC")
        self.batch_size: int = int(provider_cfg.get("batchSize", 50))
        self.request_throttle_seconds = float(provider_cfg.get("requestThrottleSeconds", 0.0))
        self.include_model_data: bool = bool(provider_cfg.get("includeModelData", False))

    def _load_config(self) -> dict:
        if not self.config_path.exists():
            raise ConfigurationError(f"Config file not found: {self.config_path}")
        with self.config_path.open("r", encoding="utf-8") as handle:
            config = json.load(handle)  # type: ignore[name-defined]
        providers = config.get("providers", {})
        if self.provider not in providers:
            raise ConfigurationError(f"Provider '{self.provider}' is not defined in {self.config_path}")
        return config

    @staticmethod
    def _normalise_location(location: Union[str, Sequence[float], Mapping[str, float]]) -> Tuple[float, float]:
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
        if isinstance(location, Mapping):
            try:
                lat = float(location["lat"])
                lon = float(location["lon"])
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError("Location mapping must contain numeric 'lat' and 'lon'.") from exc
            return lat, lon
        raise TypeError("Location must be a string, (lat, lon) tuple, or mapping with 'lat'/'lon'.")

    @staticmethod
    def _normalise_time(value: Union[str, dt.date, dt.datetime]) -> dt.datetime:
        if isinstance(value, dt.datetime):
            return value
        if isinstance(value, dt.date):
            return dt.datetime.combine(value, dt.time(0, 0))
        if isinstance(value, str):
            return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        raise TypeError("Time values must be datetime, date, or ISO string.")

    def get_hourly(
        self,
        *,
        location: Union[str, Sequence[float], Mapping[str, float]],
        start_time: Union[str, dt.date, dt.datetime],
        end_time: Union[str, dt.date, dt.datetime],
        model: Optional[str] = None,
    ) -> List[dict]:
        lat, lon = self._normalise_location(location)
        start = self._normalise_time(start_time)
        end = self._normalise_time(end_time)

        point = Point(lat, lon)
        query = Hourly(point, start, end, model=model or ("gfs" if self.include_model_data else None))
        df = query.fetch()
        if df.empty:
            return []

        df = df.reset_index().rename(columns={"time": "timestamp"})
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df.to_dict("records")

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
            self.get_hourly,
            batch_size=effective_batch,
            max_workers=max_workers,
        )
