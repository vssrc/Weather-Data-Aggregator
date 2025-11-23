from __future__ import annotations

import datetime as dt
import time
from typing import Iterable, Optional, Sequence, Tuple, Union

try:
    import requests
except ImportError:  # pragma: no cover - runtime guard
    requests = None

from .request_utils import build_request_headers


class GibsConfigurationError(RuntimeError):
    """Raised when the GIBS configuration is invalid or missing."""


class GibsApiError(RuntimeError):
    """Raised when the GIBS API returns an error response."""


class GibsClient:
    """Lightweight NASA GIBS WMS client for PNG imagery."""

    base_url: str = "https://gibs.earthdata.nasa.gov/wms/epsg4326/best/wms.cgi"

    def __init__(self, api_key: Optional[str] = None, *, retries: int = 3, retry_delay: float = 1.0) -> None:
        if requests is None:
            raise RuntimeError(
                "The 'requests' package is required to use GibsClient. "
                "Install it with `python3 -m pip install requests`."
            )
        self.api_key = api_key
        self.retries = max(1, retries)
        self.retry_delay = max(0.0, retry_delay)

    @staticmethod
    def _normalize_bbox(bbox: Sequence[float]) -> Tuple[float, float, float, float]:
        if len(bbox) != 4:
            raise GibsConfigurationError("GIBS bbox must be [min_lon, min_lat, max_lon, max_lat].")
        try:
            min_lon, min_lat, max_lon, max_lat = map(float, bbox)
        except (TypeError, ValueError) as exc:
            raise GibsConfigurationError("GIBS bbox must contain numeric values.") from exc
        if min_lon >= max_lon or min_lat >= max_lat:
            raise GibsConfigurationError("GIBS bbox has non-positive extent.")
        return min_lon, min_lat, max_lon, max_lat

    @staticmethod
    def _format_time(value: Union[str, dt.date, dt.datetime]) -> str:
        if isinstance(value, str):
            token = value.strip()
            if not token:
                raise GibsConfigurationError("Timestamp string cannot be empty for GIBS request.")
            # Normalize to Z if looks like ISO without timezone
            if token.endswith("Z") or "+" in token:
                return token
            try:
                parsed = dt.datetime.fromisoformat(token)
            except ValueError as exc:
                raise GibsConfigurationError("Invalid ISO timestamp for GIBS request.") from exc
            return parsed.replace(microsecond=0).isoformat() + "Z"

        if isinstance(value, dt.datetime):
            ts = value
            if ts.tzinfo is not None:
                ts = ts.astimezone(dt.timezone.utc).replace(tzinfo=None)
            return ts.replace(microsecond=0).isoformat() + "Z"

        if isinstance(value, dt.date):
            return dt.datetime.combine(value, dt.time(0, 0)).isoformat() + "Z"

        raise GibsConfigurationError("GIBS timestamp must be str, date, or datetime.")

    def _request(self, params: dict) -> bytes:
        headers = build_request_headers({"User-Agent": "gibs-client/1.0"})
        attempt = 0
        last_exc: Optional[Exception] = None
        while attempt < self.retries:
            attempt += 1
            try:
                resp = requests.get(self.base_url, params=params, headers=headers, timeout=60)
                if resp.status_code >= 400:
                    raise GibsApiError(f"GIBS error {resp.status_code}: {resp.text[:200]}")
                content = resp.content
                if not content:
                    raise GibsApiError("Empty response from GIBS.")
                return content
            except Exception as exc:  # pylint: disable=broad-except
                last_exc = exc
                if attempt < self.retries:
                    time.sleep(self.retry_delay)
        if last_exc:
            raise last_exc
        raise GibsApiError("Unknown error contacting GIBS.")

    def get_image(
        self,
        *,
        bbox: Sequence[float],
        timestamp: Union[str, dt.date, dt.datetime],
        layer: str,
        width: int = 512,
        height: int = 512,
        styles: str = "",
        transparent: bool = True,
    ) -> Tuple[bytes, str]:
        """
        Fetch a PNG image for a given bbox, time, and layer.

        Returns:
            (image_bytes, filename)
        """
        if not layer or not isinstance(layer, str):
            raise GibsConfigurationError("Layer name must be a non-empty string.")
        min_lon, min_lat, max_lon, max_lat = self._normalize_bbox(bbox)
        time_str = self._format_time(timestamp)

        params = {
            "SERVICE": "WMS",
            "VERSION": "1.3.0",
            "REQUEST": "GetMap",
            "BBOX": f"{min_lat},{min_lon},{max_lat},{max_lon}",  # EPSG:4326 expects lat,lon order
            "CRS": "EPSG:4326",
            "WIDTH": int(width),
            "HEIGHT": int(height),
            "LAYERS": layer,
            "STYLES": styles,
            "FORMAT": "image/png",
            "TIME": time_str,
            "TRANSPARENT": "TRUE" if transparent else "FALSE",
        }
        if self.api_key:
            params["token"] = self.api_key

        image_bytes = self._request(params)
        sanitized_time = time_str.replace(":", "").replace("-", "")
        filename = f"gibs_{layer}_{sanitized_time}.png"
        return image_bytes, filename
