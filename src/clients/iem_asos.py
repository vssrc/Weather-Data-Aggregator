import datetime as dt
from io import StringIO
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union
from src.core.dates import iter_date_windows

try:
    import pandas as pd
    import requests
except ImportError:  # pragma: no cover - runtime guard
    pd = None  # type: ignore[assignment]
    requests = None  # type: ignore[assignment]

from .config_loader import load_provider_config
from .base import BatchExecutorMixin, WeatherClient

class ConfigurationError(RuntimeError):
    """Raised when the IEM ASOS configuration is missing required values."""


class ApiError(RuntimeError):
    """Raised when the IEM ASOS API responds with an error."""


class IemAsosClient(WeatherClient, BatchExecutorMixin):
    """
    Minimal client for the Iowa Environmental Mesonet 1-minute ASOS service.

    Docs: https://mesonet.agron.iastate.edu/request/asos/1min.phtml
    """

    def __init__(self, config_path: Union[str, Path] = "config.json", provider: str = "iem_asos") -> None:
        if requests is None or pd is None:
            raise RuntimeError(
                "The 'requests' and 'pandas' packages are required to use IemAsosClient. "
                "Install them with `python3 -m pip install requests pandas`."
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
        self.base_url: str = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos1min.py"
        self.default_vars: Sequence[str] = ["tmpf", "dwpf", "sknt", "drct", "gust_sknt", "precip", "pres1"]
        self.timezone: str = "UTC"
        self.sample: str = "1min"
        self.delimiter: str = "comma"
        self.batch_size: int = 25

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

    def _get_observations(
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

    def get_historical_data(
        self,
        *,
        location: Dict[str, str],
        start_date: Union[str, dt.date],
        end_date: Union[str, dt.date],
    ) -> pd.DataFrame:
        all_dfs = []
        max_days_per_request = int(self._config.get("providers", {}).get(self.provider, {}).get("maxDaysPerRequest", 7))
        
        for window_start, window_end in iter_date_windows(start_date, end_date, max_days_per_request):
            df = self._get_observations(
                station=location["station"],
                network=location["network"],
                start_time=window_start,
                end_time=window_end,
            )
            if not df.empty:
                all_dfs.append(df)
        
        if not all_dfs:
            return pd.DataFrame()
            
        return pd.concat(all_dfs, ignore_index=True)

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
            self._get_observations,
            batch_size=effective_batch,
            max_workers=max_workers,
        )
