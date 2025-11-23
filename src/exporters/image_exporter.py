"""Exporter helper for binary image payloads."""

from __future__ import annotations

from pathlib import Path


class ImageExporter:
    """Save image bytes to a target directory."""

    def __init__(self, base_dir: Path):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def save(self, filename: str, payload: bytes) -> Path:
        if not filename:
            raise ValueError("Filename must be provided for image export.")
        target = self.base_dir / filename
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)
        return target
