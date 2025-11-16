import datetime as dt
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union
from src.core.dates import iter_date_windows

try:
    import requests
except ImportError:  # pragma: no cover - runtime guard
    requests = None

from .config_loader import load_provider_config
from .base import BatchExecutorMixin, WeatherClient
import pandas as pd

class ConfigurationError(RuntimeError):
    """Raised when the NASA POWER configuration is missing required values."""


class ApiError(RuntimeError):
    """Raised when NASA POWER responds with an error."""


from .request_utils import normalise_location

class NasaPowerClient(WeatherClient, BatchExecutorMixin):
    """
    Minimal client for the NASA POWER hourly point endpoint.

    https://power.larc.nasa.gov/docs/services/api/temporal/hourly/
    """

    def __init__(self, config_path: Union[str, Path] = "config.json", provider: str = "nasa_power") -> None:
        if requests is None:
            raise RuntimeError(
                "The 'requests' package is required to use NasaPowerClient. "
                "Install it with `python3 -m pip install requests`."
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

        self.provider_cfg = provider_cfg

        # Hardcoded defaults
        self.base_url: str = "https://power.larc.nasa.gov/api/temporal/hourly/point"
        self.parameters: Sequence[str] = ["T2M", "T2MDEW", "RH2M", "WS10M", "WD10M", "PRECTOTCORR", "PS"]
        self.community: str = "SB"
        self.time_standard: str = "UTC"
        self.response_format: str = "JSON"
        self.include_metadata: bool = False
        self.user_agent: str = "DL-Project/1.0"
        self.batch_size: int = 50

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

    def _get_hourly(
        self,
        *,
        location: Union[str, Sequence[float], Mapping[str, float]],
        start_time: Union[str, dt.date, dt.datetime],
        end_time: Union[str, dt.date, dt.datetime],
        parameters: Optional[Sequence[str]] = None,
        **extra_params: Union[str, int, float],
    ) -> dict:
        lat, lon = normalise_location(location)
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


    def get_historical_data(
        self,
        *,
        location: Union[str, Tuple[float, float]],
        start_date: Union[str, dt.date],
        end_date: Union[str, dt.date],
    ) -> pd.DataFrame:
        all_dfs = []
        max_days_per_request = int(self.provider_cfg.get("maxDaysPerRequest", 365)) # Get maxDaysPerRequest from config
        
        for window_start, window_end in iter_date_windows(start_date, end_date, max_days_per_request):
            data = self._get_hourly(
                location=location,
                start_time=window_start,
                end_time=window_end,
            )
            parameters = data.get('properties', {}).get('parameter', {})
            rows = {}
            for param, series in parameters.items():
                for stamp, value in series.items():
                    rows.setdefault(stamp, {})[param] = value
            records = []
            for stamp, values in rows.items():
                ts = dt.datetime.strptime(stamp, '%Y%m%d%H')
                values = dict(values)
                values['timestamp'] = ts
                records.append(values)
            
            if records:
                all_dfs.append(pd.DataFrame(records))
        
        if not all_dfs:
            return pd.DataFrame()

        df = pd.concat(all_dfs, ignore_index=True)
        return df

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
            self._get_hourly,
            batch_size=effective_batch,
            max_workers=max_workers,
        )
