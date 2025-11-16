from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, Mapping, Tuple


def load_provider_config(
    config_path: Path,
    provider: str,
    *,
    exception_cls: type[Exception],
    required_keys: Iterable[str] | None = None,
) -> Tuple[dict, Mapping[str, object]]:
    """
    Load the shared project config and return the provider sub-config.

    Args:
        config_path: Path to config.json.
        provider: Provider key (matches config.json.providers.*).
        exception_cls: Exception type to raise on failure so callers keep their API.
        required_keys: Optional iterable of provider-required keys.
    """
    path = Path(config_path)
    if not path.exists():
        raise exception_cls(f"Config file not found: {path}")
    try:
        config = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:  # pragma: no cover - invalid config authored by user
        raise exception_cls(f"Config file {path} contains invalid JSON.") from exc

    providers = config.get("providers")
    if not isinstance(providers, Mapping) or provider not in providers:
        raise exception_cls(f"Provider '{provider}' is not defined in {path}")

    provider_cfg = providers[provider]
    if not isinstance(provider_cfg, Mapping):
        raise exception_cls(f"Provider '{provider}' configuration must be an object.")

    for key in required_keys or ():
        if key not in provider_cfg:
            raise exception_cls(f"Provider '{provider}' configuration missing '{key}'.")

    return config, provider_cfg
