# noaa_client_with_fallbacks.py
import datetime as dt
import json
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Union
import re
import urllib.parse
import logging

try:
    import requests
except ImportError:  # pragma: no cover - runtime guard
    requests = None

import pandas as pd

from .config_loader import load_provider_config
from .base import BatchExecutorMixin, WeatherClient


logger = logging.getLogger(__name__)


class ConfigurationError(RuntimeError):
    """Raised when the NOAA access configuration is missing required values."""


class ApiError(RuntimeError):
    """Raised when the NOAA access API responds with an error."""


def _to_iso8601(value: Union[str, dt.date, dt.datetime]) -> str:
    if isinstance(value, str):
        token = value.strip()
        if not token:
            raise ValueError("Timestamp string cannot be empty.")
        return token
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.timezone.utc)
        return value.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat()
    if isinstance(value, dt.date):
        # For the NCEI v1 API pure dates are accepted as YYYY-MM-DD
        return value.isoformat()
    raise TypeError("Timestamp must be a string, date, or datetime instance.")


class NoaaAccessClient(WeatherClient, BatchExecutorMixin):
    """
    NCEI Access Data Service (v1) client with station-id format fallbacks and diagnostics.
    """

    def __init__(self, config_path: Union[str, Path] = "config.json", provider: str = "noaa_isd") -> None:
        if requests is None:
            raise RuntimeError(
                "The 'requests' package is required to use NoaaAccessClient. "
                "Install it with `python3 -m pip install requests`."
            )
        self.config_path = Path(config_path)
        self.provider = provider

        # Load full config for token and locations only
        try:
            with open(self.config_path, "r", encoding="utf-8") as fh:
                full_config = json.load(fh)
        except FileNotFoundError:
            logger.warning("Config file not found at %s, using defaults", self.config_path)
            full_config = {}
        except json.JSONDecodeError as exc:
            logger.error("Invalid JSON in config file %s: %s", self.config_path, exc)
            full_config = {}
        except Exception as exc:
            logger.error("Unexpected error loading config file %s: %s", self.config_path, exc)
            full_config = {}

        self._full_config = full_config
        self._locations = full_config.get("locations", {})
        provider_cfg = full_config.get("providers", {}).get(provider, {})

        # Hardcoded defaults based on provider type
        if provider == "noaa_isd":
            self.dataset = "global-hourly"
            self.station_attribute = "noaaIsdStation"
            self.default_data_types: Sequence[str] = ["TMP", "DEW", "SLP", "WND"]
        elif provider == "noaa_lcd":
            self.dataset = "local-climatological-data"
            self.station_attribute = "noaaLcdStation"
            self.default_data_types: Sequence[str] = []
        else:
            raise ConfigurationError(f"Unknown NOAA provider: {provider}")

        self.base_url: str = "https://www.ncei.noaa.gov/access/services/data/v1"
        self.token: Optional[str] = provider_cfg.get("token")
        self.default_units: Optional[str] = "metric"
        self.response_format: str = "json"
        self.include_station_name: bool = True
        self.options: Mapping[str, Union[str, int, bool]] = {}
        self.batch_size: int = 100

        # Optional explicit stationMap (provider-specific)
        self.station_map: Mapping[str, str] = provider_cfg.get("stationMap", {})

        # Enable diagnostic logging of attempted requests (toggle in provider config)
        self.diagnostics: bool = bool(provider_cfg.get("diagnostics", False))

        # simple patterns
        self._station_id_pattern = re.compile(r"^[A-Za-z0-9:_\-]+$")

    def _looks_like_station_id(self, value: str) -> bool:
        if not isinstance(value, str) or not value:
            return False
        if "_" in value:  # underscore means friendly key in your config
            return False
        if " " in value:
            return False
        return bool(self._station_id_pattern.match(value))

    def _normalize_maybe_numeric_11char(self, token: str) -> str:
        if isinstance(token, str) and token.isdigit() and len(token) == 11:
            return f"{token[:6]}-{token[6:]}"
        return token

    def _candidates_for(self, token: str) -> List[str]:
        """
        Given a resolved token from config (e.g. '72295023174' or '722950-23174' or 'USW00094846'),
        return a short ordered list of candidates to try when querying the API.
        """
        candidates: List[str] = []
        token = str(token)
        # If it's 11-digit numeric -> hyphenated and raw
        if token.isdigit() and len(token) == 11:
            hy = f"{token[:6]}-{token[6:]}"
            candidates.append(hy)
            candidates.append(token)
            # also try USW000 + last5 as a GHCN-style id (airport network common case)
            last5 = token[6:]
            candidates.append(f"USW000{last5}")
        else:
            # if hyphen present, try as-is and with hyphen removed
            if "-" in token:
                candidates.append(token)
                candidates.append(token.replace("-", ""))
            else:
                candidates.append(token)
        # Deduplicate while preserving order
        seen = set()
        uniq = []
        for c in candidates:
            if c not in seen:
                seen.add(c)
                uniq.append(c)
        return uniq

    def _resolve_station_id(self, location: str) -> str:
        # 1) explicit provider stationMap
        if isinstance(self.station_map, Mapping) and location in self.station_map:
            return str(self.station_map[location])
        # 2) top-level locations mapping using station_attribute
        if isinstance(self._locations, Mapping) and location in self._locations:
            entry = self._locations[location]
            if isinstance(entry, Mapping) and self.station_attribute and self.station_attribute in entry:
                return str(entry[self.station_attribute])
        # 3) if looks like token already, return it
        if self._looks_like_station_id(location):
            return location
        # otherwise helpful error
        raise ConfigurationError(
            f"Station resolution failed for {location!r}. Add a top-level locations entry with the "
            f"'{self.station_attribute}' property or add provider.stationMap mapping."
        )

    def _build_url_and_headers(self, params: Mapping[str, Union[str, int, float]]) -> (str, dict):
        query = dict(params)
        query["dataset"] = self.dataset
        query["format"] = self.response_format
        qstr = urllib.parse.urlencode(query)
        url = f"{self.base_url}?{qstr}"
        headers = {"User-Agent": "noaa-client/1.0 (+https://example.org/)"}
        if self.token:
            headers["token"] = self.token
        return url, headers

    def _single_request(self, params: Mapping[str, Union[str, int, float]]) -> Dict[str, Union[int, str, list]]:
        """
        Perform a single HTTP GET and return a structured diagnostic dict:
          { "status": <int>, "text_excerpt": <str>, "json": <parsed_or_None> }
        """
        url, headers = self._build_url_and_headers(params)
        # perform request
        resp = requests.get(self.base_url, params={**params, "dataset": self.dataset, "format": self.response_format}, headers=headers, timeout=60)
        status = resp.status_code
        # short excerpt (first 1000 chars) and try json
        text_excerpt = (resp.text[:1000] + ("..." if len(resp.text) > 1000 else "")) if resp.text else ""
        json_payload = None
        try:
            if self.response_format.lower() == "json":
                json_payload = resp.json()
        except Exception:
            json_payload = None
        return {"status": status, "text_excerpt": text_excerpt, "json": json_payload, "url": resp.url}

    def _request(self, params: Mapping[str, Union[str, int, float]]) -> List[dict]:
        """
        Wrapper kept for backward compatibility but not used directly by the multi-attempt logic below.
        """
        res = self._single_request(params)
        status = res["status"]
        if status >= 400:
            raise ApiError(f"NOAA access error {status}: {res['text_excerpt'][:200]}")
        if self.response_format.lower() == "json":
            data = res["json"]
            if data is None:
                # response wasn't valid json
                raise ApiError("NOAA access API returned invalid/non-JSON payload.")
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                return [data]
            raise ApiError("NOAA access API returned an unexpected JSON payload.")
        return [{"raw": res["text_excerpt"]}]

    def _get_observations(
        self,
        *,
        station_id: str,
        start_time: Union[str, dt.date, dt.datetime],
        end_time: Union[str, dt.date, dt.datetime],
        data_types: Optional[Sequence[str]] = None,
        units: Optional[str] = None,
        options: Optional[Mapping[str, Union[str, int, bool]]] = None,
    ) -> List[dict]:
        """
        Try a short list of station-id formats until we get a non-empty result or exhaust all candidates.
        Returns list-of-dicts (observations) or raises ApiError with diagnostics.
        """
        base_params: Dict[str, Union[str, int, float]] = {
            "stations": station_id,  # placeholder - will be replaced per candidate
            "startDate": _to_iso8601(start_time),
            "endDate": _to_iso8601(end_time),
        }
        if units or self.default_units:
            base_params["units"] = units or self.default_units or "standard"
        selected_types = list(data_types) if data_types is not None else list(self.default_data_types)
        if selected_types:
            base_params["dataTypes"] = ",".join(selected_types)
        effective_options = dict(self.options)
        if self.include_station_name:
            effective_options["includeStationName"] = True
        if options:
            effective_options.update(options)
        for k, v in effective_options.items():
            base_params[k] = v

        candidates = self._candidates_for(station_id)
        attempts = []
        for cand in candidates:
            params = dict(base_params)
            params["stations"] = cand
            diagnostic = self._single_request(params)
            attempts.append((cand, diagnostic))
            # If HTTP error, continue but record it
            if diagnostic["status"] >= 400:
                if self.diagnostics:
                    logger.warning("NCEI request attempt %s -> HTTP %s url=%s excerpt=%s", cand, diagnostic["status"], diagnostic["url"], diagnostic["text_excerpt"][:200])
                continue
            # If expecting JSON, parse/inspect for empty result
            if self.response_format.lower() == "json":
                payload = diagnostic["json"]
                if payload:
                    # JSON can be list or dict - normalize to list of dicts if possible
                    if isinstance(payload, list) and payload:
                        if self.diagnostics:
                            logger.info("NCEI success for station candidate %s (returned %d items) url=%s", cand, len(payload), diagnostic["url"])
                        return payload
                    if isinstance(payload, dict) and payload:
                        if self.diagnostics:
                            logger.info("NCEI success for station candidate %s (dict payload) url=%s", cand, diagnostic["url"])
                        return [payload]
                # empty JSON payload — continue trying other candidates
                if self.diagnostics:
                    logger.info("NCEI returned empty JSON for candidate %s url=%s excerpt=%s", cand, diagnostic["url"], diagnostic["text_excerpt"][:200])
                continue
            else:
                # non-json: return raw snippet as single dict if text not empty
                if diagnostic["text_excerpt"]:
                    if self.diagnostics:
                        logger.info("NCEI returned text for candidate %s url=%s excerpt=%s", cand, diagnostic["url"], diagnostic["text_excerpt"][:200])
                    return [{"raw": diagnostic["text_excerpt"]}]
                # empty - continue
                if self.diagnostics:
                    logger.info("NCEI returned empty text for candidate %s url=%s", cand, diagnostic["url"])

        # If we get here, all candidates exhausted
        # build a helpful error with each attempt summary
        attempt_summaries = []
        for cand, diag in attempts:
            attempt_summaries.append(f"{cand}: HTTP {diag['status']} url={diag.get('url')} excerpt={diag['text_excerpt'][:200]!r}")
        raise ApiError(
            "No data returned from NCEI for any candidate station identifiers. "
            f"Original token={station_id!r} dataset={self.dataset} startDate={_to_iso8601(base_params['startDate'])} endDate={_to_iso8601(base_params['endDate'])}. "
            "Attempts:\n" + "\n".join(attempt_summaries)
        )

    def get_historical_data(
        self,
        *,
        location: str,
        start_date: Union[str, dt.date],
        end_date: Union[str, dt.date],
    ) -> pd.DataFrame:
        # resolve the token from config or accept a station-like token
        resolved = self._resolve_station_id(location)
        # _get_observations will try candidates and raise ApiError with diagnostics if unsuccessful
        obs_list = self._get_observations(
            station_id=resolved,
            start_time=start_date,
            end_time=end_date,
        )
        df = pd.DataFrame(obs_list)
        if not df.empty and "DATE" in df.columns:
            df = df.rename(columns={"DATE": "timestamp"})
            df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df

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
            self._get_observations,
            batch_size=effective_batch,
            max_workers=max_workers,
        )


class NoaaIsdClient(NoaaAccessClient):
    def __init__(self, config_path: Union[str, Path] = "config.json") -> None:
        super().__init__(config_path=config_path, provider="noaa_isd")


class NoaaLcdClient(NoaaAccessClient):
    def __init__(self, config_path: Union[str, Path] = "config.json") -> None:
        super().__init__(config_path=config_path, provider="noaa_lcd")