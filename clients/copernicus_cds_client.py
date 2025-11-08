import datetime as dt
import json
import os
import tempfile
import zipfile
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union

try:  # pragma: no cover - optional dependency import guards
    import cdsapi
except ImportError:  # pragma: no cover
    cdsapi = None  # type: ignore[assignment]

try:
    import pandas as pd
except ImportError:  # pragma: no cover
    pd = None  # type: ignore[assignment]

try:
    import xarray as xr
except ImportError:  # pragma: no cover
    xr = None  # type: ignore[assignment]

from . import BatchExecutorMixin


class ConfigurationError(RuntimeError):
    """Raised when the Copernicus CDS configuration is missing required values."""


class CopernicusCdsClient(BatchExecutorMixin):
    """
    Wrapper around the Copernicus Climate Data Store API using cdsapi.

    Supports ERA5 single levels, ERA5-Land, ERA5 pressure levels, and ERA5-Land
    point time-series datasets.
    """

    def __init__(
        self,
        config_path: Union[str, Path] = "weather_config.json",
        provider: str = "copernicus_era5_single",
    ) -> None:
        self.config_path = Path(config_path)
        self.provider = provider
        config = self._load_config()
        provider_cfg = config["providers"][provider]

        self.api_url: str = provider_cfg["apiUrl"]
        self.api_key: str = provider_cfg["apiKey"]
        self.dataset: str = provider_cfg.get("dataset", "reanalysis-era5-single-levels")
        self.mode: str = provider_cfg.get("mode", "single_levels")
        self.product_type: Optional[str] = provider_cfg.get("productType")
        self.variables: Sequence[str] = provider_cfg.get(
            "variables",
            ["2m_temperature", "total_precipitation", "10m_u_component_of_wind", "10m_v_component_of_wind"],
        )
        self.time_steps: Sequence[str] = provider_cfg.get(
            "timeSteps", [f"{hour:02d}:00" for hour in range(24)]
        )
        self.pressure_levels: Sequence[str] = provider_cfg.get(
            "pressureLevels", ["1000", "925", "850", "700", "500"]
        )
        self.format: str = provider_cfg.get("format", "netcdf")
        self.grid: Sequence[float] = provider_cfg.get("grid", [0.25, 0.25])
        self.batch_size: int = int(provider_cfg.get("batchSize", 1))

        self.client = cdsapi.Client(url=self.api_url, key=self.api_key, quiet=True)

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
    def _ensure_date(value: Union[str, dt.date, dt.datetime]) -> str:
        if isinstance(value, dt.datetime):
            return value.strftime("%Y-%m-%d")
        if isinstance(value, dt.date):
            return value.strftime("%Y-%m-%d")
        if isinstance(value, str):
            return dt.datetime.fromisoformat(value).strftime("%Y-%m-%d")
        raise TypeError("Dates must be datetime, date, or ISO string.")

    @staticmethod
    def _ensure_date_obj(value: Union[str, dt.date, dt.datetime]) -> dt.date:
        if isinstance(value, dt.datetime):
            return value.date()
        if isinstance(value, dt.date):
            return value
        if isinstance(value, str):
            return dt.datetime.fromisoformat(value).date()
        raise TypeError("Dates must be datetime, date, or ISO string.")

    def _extract_dataset_path(self, archive_path: str) -> str:
        if zipfile.is_zipfile(archive_path):
            with zipfile.ZipFile(archive_path) as zf:
                member = zf.namelist()[0]
                tmp_nc = tempfile.mktemp(suffix=".nc")
                with zf.open(member) as src, open(tmp_nc, "wb") as dst:
                    dst.write(src.read())
            return tmp_nc
        return archive_path

    def _build_request(
        self,
        *,
        area: Optional[Sequence[float]],
        start_date: Union[str, dt.date, dt.datetime],
        end_date: Union[str, dt.date, dt.datetime],
        latitude: float,
        longitude: float,
        variables: Optional[Sequence[str]] = None,
    ) -> Dict[str, Union[str, float, Sequence[str], Sequence[float], int]]:
        start = self._ensure_date_obj(start_date)
        end = self._ensure_date_obj(end_date)
        variables = list(variables or self.variables)
        time_steps = list(self.time_steps)
        request: Dict[str, Union[str, float, Sequence[str], Sequence[float], int]]

        if self.mode in {"single_levels", "pressure_levels"}:
            if not area:
                raise ConfigurationError("Area parameter is required for ERA5 single/pressure level datasets.")
            request = {
                "variable": variables,
                "date": f"{self._ensure_date(start)}/{self._ensure_date(end)}",
                "time": time_steps,
                "area": list(area),
                "grid": list(self.grid),
                "format": self.format,
            }
            if self.product_type:
                request["product_type"] = self.product_type
            if self.mode == "pressure_levels":
                request["pressure_level"] = list(self.pressure_levels)
        elif self.mode == "land":
            if not area:
                raise ConfigurationError("Area parameter is required for ERA5-Land dataset.")
            date_range = pd.date_range(start, end, freq="D")
            year = sorted({str(d.year) for d in date_range})
            month = sorted({f"{d.month:02d}" for d in date_range})
            day = sorted({f"{d.day:02d}" for d in date_range})
            request = {
                "variable": variables,
                "year": year,
                "month": month,
                "day": day,
                "time": time_steps,
                "area": list(area),
                "grid": list(self.grid),
                "format": self.format,
            }
        elif self.mode == "land_timeseries":
            request = {
                "variable": variables,
                "start_year": start.year,
                "start_month": start.month,
                "start_day": start.day,
                "end_year": end.year,
                "end_month": end.month,
                "end_day": end.day,
                "latitude": float(latitude),
                "longitude": float(longitude),
                "format": self.format,
            }
        else:
            raise ConfigurationError(f"Unsupported Copernicus CDS mode '{self.mode}'.")

        return request

    def _parse_netcdf(self, dataset_path: str, latitude: float, longitude: float) -> pd.DataFrame:
        ds = xr.load_dataset(dataset_path)
        try:
            if "latitude" in ds.coords and "longitude" in ds.coords:
                selected = ds.sel(latitude=latitude, longitude=longitude, method="nearest")
            else:
                selected = ds
            df = selected.to_dataframe().reset_index()
        finally:
            ds.close()
        timestamp_col = next((col for col in ("valid_time", "time") if col in df.columns), None)
        if timestamp_col:
            df.rename(columns={timestamp_col: "timestamp"}, inplace=True)
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
            df = df.dropna(subset=["timestamp"]).reset_index(drop=True)
        return df

    def _parse_csv(self, dataset_path: str) -> pd.DataFrame:
        df = pd.read_csv(dataset_path)
        timestamp_col = next((col for col in df.columns if str(col).lower().startswith(("time", "valid"))), None)
        if timestamp_col:
            df["timestamp"] = pd.to_datetime(df[timestamp_col], utc=True, errors="coerce")
        return df

    def get_dataset(
        self,
        *,
        area: Optional[Sequence[float]],
        start_date: Union[str, dt.date, dt.datetime],
        end_date: Union[str, dt.date, dt.datetime],
        latitude: float,
        longitude: float,
        variables: Optional[Sequence[str]] = None,
    ) -> pd.DataFrame:
        request = self._build_request(
            area=area,
            start_date=start_date,
            end_date=end_date,
            latitude=latitude,
            longitude=longitude,
            variables=variables,
        )

        tmp_suffix = ".zip" if self.format.lower() in {"netcdf", "csv"} else ".tmp"
        tmp_path = tempfile.mktemp(suffix=tmp_suffix)
        try:
            self.client.retrieve(self.dataset, request, tmp_path)
            dataset_path = self._extract_dataset_path(tmp_path)
            if self.format.lower() == "csv" and self.mode == "land_timeseries":
                return self._parse_csv(dataset_path)
            return self._parse_netcdf(dataset_path, latitude=latitude, longitude=longitude)
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            if "dataset_path" in locals() and dataset_path != tmp_path and os.path.exists(dataset_path):
                os.remove(dataset_path)

    def get_dataset_batch(
        self,
        requests_payload: Iterable[Mapping[str, object]],
        *,
        batch_size: Optional[int] = None,
        max_workers: Optional[int] = None,
    ) -> List[Union[pd.DataFrame, Exception]]:
        effective_batch = self.batch_size if batch_size is None else batch_size
        return self._run_batch(
            requests_payload,
            self.get_dataset,
            batch_size=effective_batch,
            max_workers=max_workers,
        )

    # Backwards compatibility helpers for legacy callers
    def get_single_levels(self, **kwargs) -> pd.DataFrame:  # pragma: no cover - shim
        return self.get_dataset(**kwargs)

    def get_single_levels_batch(self, *args, **kwargs) -> List[Union[pd.DataFrame, Exception]]:  # pragma: no cover
        return self.get_dataset_batch(*args, **kwargs)
