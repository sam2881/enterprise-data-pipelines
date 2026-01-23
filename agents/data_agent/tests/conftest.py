"""Pytest configuration and fixtures."""

import json
from pathlib import Path

import pytest

# Placeholder - implement in Phase 8
# See docs/CLAUDE_CODE_CONTEXT.md for testing requirements


@pytest.fixture
def sample_intent():
    """Load sample intent JSON for testing."""
    fixture_path = Path(__file__).parent / "fixtures" / "sample_intent.json"
    with open(fixture_path) as f:
        return json.load(f)


@pytest.fixture
def sample_pipeline_data():
    """Sample pipeline metadata for testing."""
    return {
        "pipeline_name": "customer_orders",
        "domain": "sales",
        "source_type": "file",
        "source_system": "erp_exports",
        "environment": "dev",
        "data_sensitivity": "internal",
        "business_owner": "sales_team@company.com",
        "technical_owner": "data_eng@company.com",
    }


@pytest.fixture
def sample_schema():
    """Sample schema definition for testing."""
    return {
        "columns": [
            {"name": "order_id", "type": "STRING", "nullable": False},
            {"name": "customer_id", "type": "STRING", "nullable": False},
            {"name": "order_date", "type": "DATE", "nullable": False},
            {"name": "total_amount", "type": "DECIMAL(18,2)", "nullable": False},
        ],
        "primary_key": ["order_id"],
    }


@pytest.fixture
def mock_metadata_repository(mocker):
    """Mock MetadataRepository for unit tests."""
    mock = mocker.patch("src.metadata.repository.MetadataRepository")
    return mock


@pytest.fixture
def mock_settings(mocker):
    """Mock Settings for unit tests."""
    mock = mocker.patch("src.config.settings.get_settings")
    mock.return_value.environment = "dev"
    mock.return_value.database_url = "postgresql://localhost/test"
    return mock
