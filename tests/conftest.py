"""Shared test fixtures and configuration."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any, Callable, Generator
from unittest.mock import Mock

import pytest


@pytest.fixture
def temp_json_file() -> Generator[Path, Any, None]:
    """Create a temporary JSON file for testing."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        temp_path = Path(f.name)
    yield temp_path
    if temp_path.exists():
        temp_path.unlink()


@pytest.fixture
def temp_config_file() -> Generator[Path, Any, None]:
    """Create a temporary config file for testing."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".config", delete=False) as f:
        temp_path = Path(f.name)
    yield temp_path
    if temp_path.exists():
        temp_path.unlink()


@pytest.fixture
def sample_credentials() -> dict[str, str]:
    """Sample valid Snowflake credentials for testing."""
    return {
        "account": "test_account",
        "user": "test_user",
        "password": "test_password",
        "database": "test_db",
        "schema": "test_schema",
        "warehouse": "test_warehouse",
        "role": "test_role",
    }


@pytest.fixture
def sample_legacy_credentials() -> dict[str, str]:
    """Sample legacy format Snowflake credentials for testing."""
    return {
        "accountname": "test_account",
        "username": "test_user",
        "dbname": "test_db",
        "schemaname": "test_schema",
        "warehousename": "test_warehouse",
        "rolename": "test_role",
    }


@pytest.fixture
def mock_boto3_session() -> tuple[Mock, Mock]:
    """Mock boto3 session for AWS tests."""
    mock_session = Mock()
    mock_client = Mock()
    mock_session.client.return_value = mock_client
    return mock_session, mock_client


@pytest.fixture
def write_json_file(temp_json_file: Path) -> Callable[..., Any]:
    """Helper to write JSON data to a temporary file."""

    def _write_json(data: dict[str, Any]) -> Path:
        temp_json_file.write_text(json.dumps(data))
        return temp_json_file

    return _write_json


@pytest.fixture
def write_config_file(temp_config_file: Path) -> Callable[..., Any]:
    """Helper to write config data to a temporary file."""

    def _write_config(section_data: dict[str, str], section_name: str = "connections") -> Path:
        lines = [f"[{section_name}]"]
        for key, value in section_data.items():
            lines.append(f"{key} = {value}")
        temp_config_file.write_text("\n".join(lines))
        return temp_config_file

    return _write_config
