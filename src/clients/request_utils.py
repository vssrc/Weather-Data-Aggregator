from __future__ import annotations

import random
import uuid
from typing import Mapping, MutableMapping, Optional, Union, Sequence, Tuple

USER_AGENTS = [
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; Pixel 7 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPad; CPU OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 13; SAMSUNG SM-S908U) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/23.0 Chrome/120.0.0.0 Mobile Safari/537.36",
]

ACCEPT_HEADERS = [
    "application/json,text/javascript,*/*;q=0.8",
    "application/json,application/xml;q=0.9,*/*;q=0.8",
    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
]

LANGUAGE_HEADERS = [
    "en-US,en;q=0.9",
    "en-GB,en;q=0.9",
    "en-US,en-CA;q=0.8",
    "en,en-US;q=0.9",
]

DEVICE_PLATFORMS = [
    "android",
    "ios",
    "windows",
    "macos",
    "linux",
]


def build_request_headers(base: Optional[Mapping[str, str]] = None) -> MutableMapping[str, str]:
    """
    Return randomized request headers to reduce fingerprinting of outbound API calls.

    Args:
        base: Optional mapping of headers to seed the final set (values here win over defaults).
    """
    headers: MutableMapping[str, str] = dict(base or {})

    headers["User-Agent"] = headers.get("User-Agent") or random.choice(USER_AGENTS)
    headers["Accept"] = headers.get("Accept") or random.choice(ACCEPT_HEADERS)
    headers["Accept-Language"] = headers.get("Accept-Language") or random.choice(LANGUAGE_HEADERS)
    headers.setdefault("Accept-Encoding", "gzip, deflate, br")

    # Randomized device/client identifiers
    headers["X-Device-Id"] = str(uuid.uuid4())
    headers["X-Request-Id"] = str(uuid.uuid4())
    headers["X-Client-Platform"] = random.choice(DEVICE_PLATFORMS)
    headers["X-App-Version"] = headers.get("X-App-Version") or f"{random.randint(1, 5)}.{random.randint(0, 20)}.{random.randint(0, 50)}"

    return headers


def normalise_location(location: Union[str, Sequence[float], Mapping[str, float]]) -> Tuple[float, float]:
    if isinstance(location, str):
        if not location.strip():
            raise ValueError("Location string cannot be empty.")
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

    if isinstance(location, dict):
        try:
            lat = float(location["lat"])
            lon = float(location["lon"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("Location dict needs numeric 'lat' and 'lon' keys.") from exc
        return lat, lon

    raise TypeError("Location must be a string, (lat, lon) pair, or {'lat': .., 'lon': ..} dictionary.")

