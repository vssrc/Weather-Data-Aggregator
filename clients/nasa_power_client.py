import datetime as dt
import json
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union

try:
    import requests
except ImportError:  # pragma: no cover - runtime guard
    requests = None

from . import BatchExecutorMixin


class ConfigurationError(RuntimeError):
    """Raised when the NASA POWER configuration is missing required values."""


class ApiError(RuntimeError):
    """Raised when NASA POWER responds with an error."""


class NasaPowerClient(BatchExecutorMixin):
    """
    Minimal client for the NASA POWER hourly point endpoint.

    https://power.larc.nasa.gov/docs/services/api/temporal/hourly/
    """

    def __init__(self, config_path: Union[str, Path] = "weather_config.json", provider: str = "nasa_power") -> None:
        if requests is None:
            raise RuntimeError(
                "The 'requests' package is required to use NasaPowerClient. "
                "Install it with `python3 -m pip install requests`."
            )

        self.config_path = Path(config_path)
        self.provider = provider
        config = self._load_config()
        provider_cfg = config["providers"][provider]

        self.base_url: str = provider_cfg.get("baseUrl", "https://power.larc.nasa.gov/api/temporal/hourly/point").rstrip("/")
        self.parameters: Sequence[str] = provider_cfg.get("parameters", ["T2M", "RH2M", "WS10M"])
        self.community: str = provider_cfg.get("community", "SB")
        self.time_standard: str = provider_cfg.get("timeStandard", "UTC")
        self.response_format: str = provider_cfg.get("format", "JSON")
        self.include_metadata: bool = bool(provider_cfg.get("includeMetadata", False))
        self.user_agent: str = provider_cfg.get("userAgent", "nasa-power-client/1.0")
        self.batch_size: int = int(provider_cfg.get("batchSize", 25))

    def _load_config(self) -> dict:
        if not self.config_path.exists():
            raise ConfigurationError(f"Config file not found: {self.config_path}")
        with self.config_path.open("r", encoding="utf-8") as handle:
            config = json.load(handle)
        providers = config.get("providers", {})
        if self.provider not in providers:
            raise ConfigurationError(f"Provider '{self.provider}' is not defined in {self.config_path}")
        provider_cfg = providers[self.provider]
        if not provider_cfg.get("parameters"):
            raise ConfigurationError(f"Provider '{self.provider}' must define 'parameters' in {self.config_path}")
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
        raise TypeError("Location must be a string, (lat, lon) pair, or mapping.")

    @staticmethod
    def _format_date(value: Union[str, dt.date, dt.datetime]) -> str:
        if isinstance(value, dt.datetime):
            return value.strftime("%Y%m%d")
        if isinstance(value, dt.date):
            return value.strftime("%Y%m%d")
        if isinstance(value, str):
            token = value.strip()
            if len(token) == 8 and token.isdigit():
                return token
            # Attempt to parse ISO
            parsed = dt.datetime.fromisoformat(token.replace("Z", "+00:00"))
            return parsed.strftime("%Y%m%d")
        raise TypeError("Dates must be datetime, date, or YYYY-MM-DD string.")

    def _request(self, params: Mapping[str, Union[str, int, float]]) -> dict:
        query = dict(params)
        query.setdefault("format", self.response_format)
        query.setdefault("community", self.community)
        query.setdefault("time-standard", self.time_standard)

        response = requests.get(
            self.base_url,
            params=query,
            headers={"User-Agent": self.user_agent},
            timeout=60,
        )
        if response.status_code >= 400:
            raise ApiError(f"NASA POWER error {response.status_code}: {response.text}")
        try:
            return response.json()
        except ValueError as exc:
            raise ApiError("NASA POWER returned a non-JSON response.") from exc

    def get_hourly(
        self,
        *,
        location: Union[str, Sequence[float], Mapping[str, float]],
        start_time: Union[str, dt.date, dt.datetime],
        end_time: Union[str, dt.date, dt.datetime],
        parameters: Optional[Sequence[str]] = None,
        **extra_params: Union[str, int, float],
    ) -> dict:
        lat, lon = self._normalise_location(location)
        start = self._format_date(start_time)
        end = self._format_date(end_time)
        params: Dict[str, Union[str, int, float]] = {
            "latitude": f"{lat:.4f}",
            "longitude": f"{lon:.4f}",
            "start": start,
            "end": end,
            "parameters": ",".join(parameters or self.parameters),
        }
        if extra_params:
            params.update(extra_params)
        return self._request(params)

    def get_hourly_batch(
        self,
        requests_payload: Iterable[Mapping[str, object]],
        *,
        batch_size: Optional[int] = None,
        max_workers: Optional[int] = None,
    ) -> List[Union[dict, Exception]]:
        effective_batch = self.batch_size if batch_size is None else batch_size
        return self._run_batch(
            requests_payload,
            self.get_hourly,
            batch_size=effective_batch,
            max_workers=max_workers,
        )
