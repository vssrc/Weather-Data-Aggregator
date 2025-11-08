import datetime as dt
import json
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union

try:
    import requests
except ImportError:  # pragma: no cover - runtime guard
    requests = None


class ConfigurationError(RuntimeError):
    """Raised when the NOAA access configuration is missing required values."""


class ApiError(RuntimeError):
    """Raised when the NOAA access API responds with an error."""


from . import BatchExecutorMixin


def _to_iso8601(value: Union[str, dt.date, dt.datetime]) -> str:
    if isinstance(value, str):
        token = value.strip()
        if not token:
            raise ValueError("Timestamp string cannot be empty.")
        return token
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.timezone.utc)
        return value.astimezone(dt.timezone.utc).isoformat()
    if isinstance(value, dt.date):
        return dt.datetime.combine(value, dt.time(0, 0), tzinfo=dt.timezone.utc).isoformat()
    raise TypeError("Timestamp must be a string, date, or datetime instance.")


class NoaaAccessClient(BatchExecutorMixin):
    """
    Lightweight wrapper around the NCEI Access Data Service (v1).

    This client issues GET requests against https://www.ncei.noaa.gov/access/services/data/v1
    using provider-specific configuration for datasets such as "global-hourly" (ISD) or
    "local-climatological-data" (LCD v2).
    """

    def __init__(self, config_path: Union[str, Path] = "weather_config.json", provider: str = "noaa_isd") -> None:
        if requests is None:
            raise RuntimeError(
                "The 'requests' package is required to use NoaaAccessClient. "
                "Install it with `python3 -m pip install requests`."
            )
        self.config_path = Path(config_path)
        self.provider = provider

        config = self._load_config()
        provider_cfg = config["providers"][provider]

        self.base_url: str = provider_cfg.get("baseUrl", "https://www.ncei.noaa.gov/access/services/data/v1").rstrip("/")
        self.dataset: str = provider_cfg["dataset"]
        self.token: str = provider_cfg["token"]
        self.default_units: Optional[str] = provider_cfg.get("defaultUnits", "metric")
        self.default_data_types: Sequence[str] = provider_cfg.get("defaultDataTypes", [])
        self.response_format: str = provider_cfg.get("responseFormat", "json")
        self.station_attribute: str = provider_cfg.get("stationAttribute", "")
        self.include_station_name: bool = bool(provider_cfg.get("includeStationName", False))
        self.options: Mapping[str, Union[str, int, bool]] = provider_cfg.get("options", {})
        self.batch_size: int = int(provider_cfg.get("batchSize", 100))

    def _load_config(self) -> dict:
        if not self.config_path.exists():
            raise ConfigurationError(f"Config file not found: {self.config_path}")
        with self.config_path.open("r", encoding="utf-8") as handle:
            config = json.load(handle)
        providers = config.get("providers", {})
        if self.provider not in providers:
            raise ConfigurationError(f"Provider '{self.provider}' is not defined in {self.config_path}")
        provider_cfg = providers[self.provider]
        if "dataset" not in provider_cfg:
            raise ConfigurationError(f"Provider '{self.provider}' must define 'dataset' in {self.config_path}")
        if not provider_cfg.get("token"):
            raise ConfigurationError(f"Provider '{self.provider}' must define 'token' in {self.config_path}")
        return config

    def _request(self, params: Mapping[str, Union[str, int, float]]) -> List[dict]:
        query = dict(params)
        query["dataset"] = self.dataset
        query["format"] = self.response_format
        headers = {"token": self.token}
        response = requests.get(self.base_url, params=query, headers=headers, timeout=60)
        if response.status_code >= 400:
            raise ApiError(f"NOAA access error {response.status_code}: {response.text}")
        if self.response_format.lower() == "json":
            data = response.json()
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                return [data]
            raise ApiError("NOAA access API returned an unexpected JSON payload.")
        # Fallback for csv/plain text
        return [dict(raw=response.text)]

    def get_observations(
        self,
        *,
        station_id: str,
        start_time: Union[str, dt.date, dt.datetime],
        end_time: Union[str, dt.date, dt.datetime],
        data_types: Optional[Sequence[str]] = None,
        units: Optional[str] = None,
        options: Optional[Mapping[str, Union[str, int, bool]]] = None,
    ) -> List[dict]:
        params: Dict[str, Union[str, int, float]] = {
            "stations": station_id,
            "startDate": _to_iso8601(start_time),
            "endDate": _to_iso8601(end_time),
        }
        if units or self.default_units:
            params["units"] = units or self.default_units or "standard"
        selected_types = list(data_types) if data_types is not None else list(self.default_data_types)
        if selected_types:
            params["dataTypes"] = ",".join(selected_types)
        effective_options = dict(self.options)
        if self.include_station_name:
            effective_options["includeStationName"] = True
        if options:
            effective_options.update(options)
        for key, value in effective_options.items():
            params[key] = value
        return self._request(params)

    def get_observations_batch(
        self,
        requests_payload: Iterable[Mapping[str, object]],
        *,
        batch_size: Optional[int] = None,
        max_workers: Optional[int] = None,
    ) -> List[Union[List[dict], Exception]]:
        effective_batch = self.batch_size if batch_size is None else batch_size
        return self._run_batch(
            requests_payload,
            self.get_observations,
            batch_size=effective_batch,
            max_workers=max_workers,
        )


class NoaaIsdClient(NoaaAccessClient):
    def __init__(self, config_path: Union[str, Path] = "weather_config.json") -> None:
        super().__init__(config_path=config_path, provider="noaa_isd")


class NoaaLcdClient(NoaaAccessClient):
    def __init__(self, config_path: Union[str, Path] = "weather_config.json") -> None:
        super().__init__(config_path=config_path, provider="noaa_lcd")
