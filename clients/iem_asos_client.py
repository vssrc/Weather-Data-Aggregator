import datetime as dt
import json
from io import StringIO
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union

try:
    import pandas as pd
    import requests
except ImportError:  # pragma: no cover - runtime guard
    pd = None  # type: ignore[assignment]
    requests = None  # type: ignore[assignment]

from . import BatchExecutorMixin


class ConfigurationError(RuntimeError):
    """Raised when the IEM ASOS configuration is missing required values."""


class ApiError(RuntimeError):
    """Raised when the IEM ASOS API responds with an error."""


class IemAsosClient(BatchExecutorMixin):
    """
    Minimal client for the Iowa Environmental Mesonet 1-minute ASOS service.

    Docs: https://mesonet.agron.iastate.edu/request/asos/1min.phtml
    """

    def __init__(self, config_path: Union[str, Path] = "weather_config.json", provider: str = "iem_asos") -> None:
        if requests is None or pd is None:
            raise RuntimeError(
                "The 'requests' and 'pandas' packages are required to use IemAsosClient. "
                "Install them with `python3 -m pip install requests pandas`."
            )

        self.config_path = Path(config_path)
        self.provider = provider
        config = self._load_config()
        provider_cfg = config["providers"][provider]

        self.base_url: str = provider_cfg.get("baseUrl", "https://mesonet.agron.iastate.edu/cgi-bin/request/asos1min.py")
        self.default_vars: Sequence[str] = provider_cfg.get(
            "defaultVars", ["tmpf", "dwpf", "relh", "sped", "drct", "gust", "p01i", "alti", "vsby"]
        )
        self.timezone: str = provider_cfg.get("timezone", "UTC")
        self.sample: str = provider_cfg.get("sample", "1min")
        self.delimiter: str = provider_cfg.get("delimiter", "comma")
        self.batch_size: int = int(provider_cfg.get("batchSize", 25))

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
    def _ensure_datetime(value: Union[str, dt.date, dt.datetime]) -> dt.datetime:
        if isinstance(value, dt.datetime):
            return value
        if isinstance(value, dt.date):
            return dt.datetime.combine(value, dt.time(0, 0))
        if isinstance(value, str):
            value = value.replace("Z", "+00:00")
            return dt.datetime.fromisoformat(value)
        raise TypeError("Expected datetime, date, or ISO string.")

    def _build_params(
        self,
        *,
        station: str,
        network: str,
        start_time: dt.datetime,
        end_time: dt.datetime,
        variables: Optional[Sequence[str]] = None,
    ) -> Dict[str, Union[str, int]]:
        params: Dict[str, Union[str, int]] = {
            "station": station,
            "network": network,
            "tz": self.timezone,
            "sample": self.sample,
            "what": "download",
            "delim": self.delimiter,
            "gis": "no",
            "year1": start_time.year,
            "month1": start_time.month,
            "day1": start_time.day,
            "hour1": start_time.hour,
            "minute1": start_time.minute,
            "year2": end_time.year,
            "month2": end_time.month,
            "day2": end_time.day,
            "hour2": end_time.hour,
            "minute2": end_time.minute,
        }
        vars_list = variables or self.default_vars
        if vars_list:
            params["vars"] = ",".join(vars_list)
        return params

    def _request(self, params: Mapping[str, Union[str, int]]) -> pd.DataFrame:
        response = requests.get(self.base_url, params=params, timeout=60)
        if response.status_code >= 400:
            raise ApiError(f"IEM ASOS error {response.status_code}: {response.text}")
        text = response.text.strip()
        if not text:
            return pd.DataFrame()
        df = pd.read_csv(StringIO(text))
        if df.empty:
            return df
        # standardize timestamp column
        timestamp_col = next((col for col in df.columns if col.lower().startswith("valid")), None)
        if timestamp_col:
            df["timestamp"] = pd.to_datetime(df[timestamp_col], utc=True, errors="coerce")
        return df

    def get_observations(
        self,
        *,
        station: str,
        network: str,
        start_time: Union[str, dt.date, dt.datetime],
        end_time: Union[str, dt.date, dt.datetime],
        variables: Optional[Sequence[str]] = None,
    ) -> pd.DataFrame:
        start = self._ensure_datetime(start_time)
        end = self._ensure_datetime(end_time)
        params = self._build_params(station=station, network=network, start_time=start, end_time=end, variables=variables)
        return self._request(params)

    def get_observations_batch(
        self,
        requests_payload: Iterable[Mapping[str, object]],
        *,
        batch_size: Optional[int] = None,
        max_workers: Optional[int] = None,
    ) -> List[Union[pd.DataFrame, Exception]]:
        effective_batch = self.batch_size if batch_size is None else batch_size
        return self._run_batch(
            requests_payload,
            self.get_observations,
            batch_size=effective_batch,
            max_workers=max_workers,
        )
