import datetime as dt
import json
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Union

try:
    import requests
except ImportError:  # pragma: no cover - dependency hint for runtime errors
    requests = None


class ConfigurationError(RuntimeError):
    """Raised when the Visual Crossing configuration is missing required values."""


class ApiError(RuntimeError):
    """Raised when Visual Crossing responds with a non-success status."""


from . import BatchExecutorMixin
from .request_utils import build_request_headers


class VisualCrossingClient(BatchExecutorMixin):
    """
    Wrapper around Visual Crossing's Timeline Weather API.

    Reference: https://www.visualcrossing.com/resources/documentation/weather-api/timeline-weather-api/
    """

    def __init__(self, config_path: Union[str, Path] = "weather_config.json", provider: str = "visual_crossing") -> None:
        if requests is None:
            raise RuntimeError(
                "The 'requests' package is required to use VisualCrossingClient. "
                "Install it with `python3 -m pip install requests`."
            )

        self.config_path = Path(config_path)
        self.provider = provider
        config = self._load_config()
        provider_cfg = config["providers"][provider]

        self.api_key: str = provider_cfg["apiKey"]
        self.base_url: str = provider_cfg.get(
            "baseUrl", "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"
        ).rstrip("/")
        self.default_unit_group: str = provider_cfg.get("defaultUnitGroup", "metric")
        self.default_include: Sequence[str] = tuple(provider_cfg.get("defaultInclude", []))
        self.default_content_type: str = provider_cfg.get("defaultContentType", "json")
        self.default_language: str = provider_cfg.get("defaultLanguage", "en")
        self.batch_size: int = int(provider_cfg.get("batchSize", 100))

    def _load_config(self) -> dict:
        if not self.config_path.exists():
            raise ConfigurationError(f"Config file not found: {self.config_path}")
        with self.config_path.open("r", encoding="utf-8") as handle:
            config = json.load(handle)
        providers = config.get("providers", {})
        if self.provider not in providers:
            raise ConfigurationError(f"Provider '{self.provider}' is not defined in {self.config_path}")
        if not providers[self.provider].get("apiKey"):
            raise ConfigurationError(f"Provider '{self.provider}' requires an API key in {self.config_path}")
        return config

    @staticmethod
    def _normalise_location(location: Union[str, Sequence[float], Dict[str, float]]) -> str:
        if isinstance(location, str):
            if not location.strip():
                raise ValueError("Location string cannot be empty.")
            return location.strip()

        if isinstance(location, (tuple, list)):
            if len(location) != 2:
                raise ValueError("Expecting (latitude, longitude).")
            lat, lon = location
            return f"{float(lat):.6f},{float(lon):.6f}"

        if isinstance(location, dict):
            try:
                lat = float(location["lat"])
                lon = float(location["lon"])
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError("Location dict needs numeric 'lat' and 'lon' keys.") from exc
            return f"{lat:.6f},{lon:.6f}"

        raise TypeError("Location must be a string, (lat, lon) pair, or {'lat': .., 'lon': ..} dictionary.")

    @staticmethod
    def _to_path_token(value: Union[str, dt.date, dt.datetime, None]) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, str):
            token = value.strip()
            if not token:
                raise ValueError("Date token cannot be empty.")
            return token
        if isinstance(value, dt.datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=dt.timezone.utc)
            return value.isoformat().replace("+00:00", "Z")
        if isinstance(value, dt.date):
            return value.isoformat()
        raise TypeError("Dates must be provided as ISO strings, date, or datetime objects.")

    @staticmethod
    def _serialise_include(include: Optional[Iterable[str]], fallback: Sequence[str]) -> Optional[str]:
        if include is None:
            include = fallback
        if not include:
            return None
        cleaned = [item.strip() for item in include if item and item.strip()]
        return ",".join(cleaned) if cleaned else None

    def _build_params(
        self,
        *,
        include: Optional[Iterable[str]],
        unit_group: Optional[str],
        content_type: Optional[str],
        language: Optional[str],
        elements: Optional[Iterable[str]],
        extra_params: Optional[Dict[str, Union[str, int, float]]],
    ) -> Dict[str, Union[str, int, float]]:
        params: Dict[str, Union[str, int, float]] = {
            "key": self.api_key,
        }

        include_value = self._serialise_include(include, self.default_include)
        if include_value:
            params["include"] = include_value

        params["unitGroup"] = unit_group or self.default_unit_group
        params["contentType"] = content_type or self.default_content_type

        if language or self.default_language:
            params["lang"] = language or self.default_language

        if elements:
            cleaned = [elem.strip() for elem in elements if elem and elem.strip()]
            if cleaned:
                params["elements"] = ",".join(cleaned)

        if extra_params:
            params.update(extra_params)

        return params

    @staticmethod
    def _execute(url: str, params: Dict[str, Union[str, int, float]]) -> dict:
        response = requests.get(url, params=params, timeout=30, headers=build_request_headers())
        if response.status_code >= 400:
            raise ApiError(f"Visual Crossing error {response.status_code}: {response.text}")
        try:
            return response.json()
        except ValueError as exc:
            raise ApiError("Visual Crossing returned a non-JSON response.") from exc

    def _timeline_request(
        self,
        *,
        location: Union[str, Sequence[float], Dict[str, float]],
        start: Optional[Union[str, dt.date, dt.datetime]],
        end: Optional[Union[str, dt.date, dt.datetime]],
        include: Optional[Iterable[str]],
        unit_group: Optional[str],
        content_type: Optional[str],
        language: Optional[str],
        elements: Optional[Iterable[str]],
        extra_params: Optional[Dict[str, Union[str, int, float]]],
    ) -> dict:
        location_token = self._normalise_location(location)
        url_parts = [self.base_url, location_token]

        start_token = self._to_path_token(start)
        end_token = self._to_path_token(end)

        if start_token:
            url_parts.append(start_token)
        if end_token:
            if not start_token:
                raise ValueError("An end date requires a start date for Visual Crossing timeline requests.")
            url_parts.append(end_token)

        url = "/".join(part.strip("/") for part in url_parts)
        params = self._build_params(
            include=include,
            unit_group=unit_group,
            content_type=content_type,
            language=language,
            elements=elements,
            extra_params=extra_params,
        )
        return self._execute(url, params)

    def get_forecast(
        self,
        *,
        location: Union[str, Sequence[float], Dict[str, float]],
        start: Optional[Union[str, dt.date, dt.datetime]] = None,
        end: Optional[Union[str, dt.date, dt.datetime]] = None,
        include: Optional[Iterable[str]] = None,
        unit_group: Optional[str] = None,
        content_type: Optional[str] = None,
        language: Optional[str] = None,
        elements: Optional[Iterable[str]] = None,
        **extra_params: Union[str, int, float],
    ) -> dict:
        """
        Fetch forecast (and optionally current) data for a location.

        Omitting start/end returns the default forecast window. Dynamic
        ranges such as 'next7days' can be supplied via the `start` parameter.
        """
        return self._timeline_request(
            location=location,
            start=start,
            end=end,
            include=include,
            unit_group=unit_group,
            content_type=content_type,
            language=language,
            elements=elements,
            extra_params=extra_params or None,
        )

    def get_historical(
        self,
        *,
        location: Union[str, Sequence[float], Dict[str, float]],
        start: Union[str, dt.date, dt.datetime],
        end: Union[str, dt.date, dt.datetime],
        include: Optional[Iterable[str]] = None,
        unit_group: Optional[str] = None,
        content_type: Optional[str] = None,
        language: Optional[str] = None,
        elements: Optional[Iterable[str]] = None,
        **extra_params: Union[str, int, float],
    ) -> dict:
        """
        Fetch historical weather data for the given location and date range.
        """
        return self._timeline_request(
            location=location,
            start=start,
            end=end,
            include=include,
            unit_group=unit_group,
            content_type=content_type,
            language=language,
            elements=elements,
            extra_params=extra_params or None,
        )

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
