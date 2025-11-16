from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Tuple


class ConfigError(RuntimeError):
    """Raised when the project configuration cannot be loaded or parsed."""


def load_project_config(path: Path) -> dict:
    """Return the parsed configuration dictionary from ``config.json``."""
    config_path = Path(path)
    if not config_path.exists():
        raise ConfigError(f"Config file not found: {config_path}")
    try:
        return json.loads(config_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:  # pragma: no cover - invalid user config
        raise ConfigError(f"Config file {config_path} contains invalid JSON.") from exc


def load_locations(config: Mapping[str, object]) -> Tuple[List[Tuple[str, float, float]], Dict[str, Dict[str, object]]]:
    """Extract the configured locations list and any provider-specific extras."""
    raw_locations = config.get("locations") if isinstance(config, Mapping) else None
    if not isinstance(raw_locations, Mapping):
        raise ConfigError("The configuration must define a 'locations' mapping.")

    location_items: List[Tuple[str, float, float]] = []
    extras: Dict[str, Dict[str, object]] = {}

    for name, coords in raw_locations.items():
        if not isinstance(coords, Mapping):
            raise ConfigError(f"Location '{name}' must be an object with 'lat'/'lon'.")
        try:
            lat = float(coords["lat"])
            lon = float(coords["lon"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ConfigError(f"Invalid coordinates for location '{name}'.") from exc

        location_items.append((name, lat, lon))
        extras[name] = {
            key: value for key, value in coords.items() if key not in {"lat", "lon"}
        }

    if not location_items:
        raise ConfigError("Define at least one location under 'locations' in config.json.")

    return location_items, extras


def provider_setting(config: Mapping[str, object], provider: str, key: str, default=None):
    """Read a provider-specific setting from the loaded config."""
    providers = config.get("providers") if isinstance(config, Mapping) else None
    if not isinstance(providers, Mapping):
        return default
    provider_cfg = providers.get(provider)
    if not isinstance(provider_cfg, Mapping):
        return default
    return provider_cfg.get(key, default)


def ensure_provider_keys(
    provider_cfg: Mapping[str, object],
    *,
    required_keys: Iterable[str],
    exception_cls: type[Exception],
) -> None:
    """Ensure a provider config contains the required keys."""
    for key in required_keys:
        if key not in provider_cfg:
            raise exception_cls(f"Provider configuration is missing required key '{key}'.")
