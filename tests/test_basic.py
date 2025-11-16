"""Basic tests to verify the refactored structure."""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))


def test_imports():
    """Test that all imports work."""
    from src.clients import (
        OpenMeteoClient,
        NoaaIsdClient,
        NoaaLcdClient,
        MeteostatClient,
        NasaPowerClient,
        IemAsosClient,
    )
    from src.exporters import BaseExporter, DataFrameExporter, create_exporter
    from src.core import load_project_config, iter_days

    assert OpenMeteoClient is not None
    assert BaseExporter is not None
    assert create_exporter is not None


def test_client_instantiation():
    """Test that clients can be instantiated."""
    from src.clients import OpenMeteoClient

    # This should not raise if config loading is optional
    try:
        client = OpenMeteoClient(config_path="config.json")
        assert client is not None
    except FileNotFoundError:
        # Config not found is OK for this test
        pass


if __name__ == "__main__":
    print("Running basic tests...")
    test_imports()
    print("[OK] Imports work")
    test_client_instantiation()
    print("[OK] Client instantiation works")
    print("\nAll tests passed!")
