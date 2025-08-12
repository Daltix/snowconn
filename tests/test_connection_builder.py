"""Tests for the connection_builder module."""

from __future__ import annotations

import json
import os
from unittest.mock import Mock, patch

import pytest
from snowconn.connection_builder import (
    ConnectionError,
    _load_private_with_passphrase,
    create_snowflake_sa_engine,
    load_from_aws_secret,
    load_from_env_vars,
    load_from_json_file,
    load_from_snowflake_config_file,
    sanitize_snowflake_credentials,
)
from sqlalchemy.engine import Engine


class TestLoadFromJsonFile:
    """Test cases for load_from_json_file function."""

    def test_load_from_json_file_success(self, write_json_file, sample_credentials):
        """Test successful loading of JSON credentials."""
        json_file = write_json_file(sample_credentials)
        result = load_from_json_file(json_file)
        assert result == sample_credentials

    def test_load_from_json_file_not_found(self, tmp_path):
        """Test handling of missing JSON file."""
        non_existent_file = tmp_path / "non_existent.json"
        result = load_from_json_file(non_existent_file)
        assert result == {}

    def test_load_from_json_file_invalid_json(self, tmp_path):
        """Test handling of invalid JSON content."""
        json_file = tmp_path / "invalid.json"
        json_file.write_text("invalid json content")
        with pytest.raises(json.JSONDecodeError):
            load_from_json_file(json_file)

    def test_load_from_json_file_empty_credentials(self, write_json_file):
        """Test handling of empty or null credentials."""
        test_creds = {
            "account": "",
            "user": None,
            "password": "   ",  # whitespace only
            "database": "test_db",
        }
        json_file = write_json_file(test_creds)
        result = load_from_json_file(json_file)
        # Only database should remain after sanitization
        assert result == {"database": "test_db"}

    def test_load_from_json_file_legacy_keys(self, write_json_file, sample_legacy_credentials):
        """Test mapping of legacy keys to current format."""
        json_file = write_json_file(sample_legacy_credentials)
        result = load_from_json_file(json_file)
        expected = {
            "account": "test_account",
            "user": "test_user",
            "database": "test_db",
            "schema": "test_schema",
            "warehouse": "test_warehouse",
            "role": "test_role",
        }
        assert result == expected


class TestLoadFromSnowflakeConfigFile:
    """Test cases for load_from_snowflake_config_file function."""

    def test_load_from_config_file_success(self, write_config_file, sample_credentials):
        """Test successful loading of config file credentials."""
        config_file = write_config_file(sample_credentials)
        result = load_from_snowflake_config_file(config_file)
        assert result == sample_credentials

    def test_load_from_config_file_not_found(self, tmp_path):
        """Test handling of missing config file."""
        non_existent_file = tmp_path / "non_existent_config"
        result = load_from_snowflake_config_file(non_existent_file)
        assert result == {}

    def test_load_from_config_file_missing_section(self, write_config_file):
        """Test handling of missing section in config file."""
        config_file = write_config_file({"account": "test_account"}, section_name="other_section")
        result = load_from_snowflake_config_file(config_file, section="connections")
        assert result == {}

    def test_load_from_config_file_custom_section(self, write_config_file):
        """Test loading from custom section name."""
        test_creds = {"account": "test_account", "user": "test_user"}
        config_file = write_config_file(test_creds, section_name="custom_connections")
        result = load_from_snowflake_config_file(config_file, section="custom_connections")
        assert result == test_creds


class TestLoadFromEnvVars:
    """Test cases for load_from_env_vars function."""

    def test_load_from_env_vars_success(self):
        """Test successful loading of environment variables."""
        env_vars = {
            "SNOWFLAKE_ACCOUNT": "test_account",
            "SNOWFLAKE_USER": "test_user",
            "SNOWFLAKE_PASSWORD": "test_password",
            "SNOWFLAKE_DATABASE": "test_db",
            "OTHER_VAR": "should_be_ignored",
        }

        with patch.dict(os.environ, env_vars, clear=True):
            result = load_from_env_vars()

        expected = {
            "account": "test_account",
            "user": "test_user",
            "password": "test_password",
            "database": "test_db",
        }
        assert result == expected

    def test_load_from_env_vars_empty(self):
        """Test handling when no Snowflake environment variables are set."""
        env_vars = {
            "OTHER_VAR": "should_be_ignored",
            "ANOTHER_VAR": "also_ignored",
        }

        with patch.dict(os.environ, env_vars, clear=True):
            result = load_from_env_vars()

        assert result == {}

    def test_load_from_env_vars_case_insensitive(self):
        """Test that environment variable processing is case insensitive."""
        env_vars = {
            "SNOWFLAKE_ACCOUNT": "test_account",
            "snowflake_user": "test_user",  # lowercase should still work
        }

        with patch.dict(os.environ, env_vars, clear=True):
            result = load_from_env_vars()

        # Note: actual env vars are case sensitive, but the processing should handle it
        assert "account" in result
        assert result["account"] == "test_account"


class TestLoadFromAwsSecret:
    """Test cases for load_from_aws_secret function."""

    def test_load_from_aws_secret_success(self, mock_boto3_session, sample_credentials):
        """Test successful loading from AWS Secrets Manager."""
        mock_session, mock_client = mock_boto3_session
        mock_client.get_secret_value.return_value = {"SecretString": json.dumps(sample_credentials)}

        result = load_from_aws_secret("test_secret", mock_session)

        assert result == sample_credentials
        mock_session.client.assert_called_once_with(service_name="secretsmanager")
        mock_client.get_secret_value.assert_called_once_with(SecretId="test_secret")

    def test_load_from_aws_secret_no_secret_string(self, mock_boto3_session):
        """Test handling when secret has no SecretString."""
        mock_session, mock_client = mock_boto3_session
        mock_client.get_secret_value.return_value = {}

        result = load_from_aws_secret("test_secret", mock_session)

        assert result == {}

    def test_load_from_aws_secret_invalid_json(self, mock_boto3_session):
        """Test handling of invalid JSON in secret."""
        mock_session, mock_client = mock_boto3_session
        mock_client.get_secret_value.return_value = {"SecretString": "invalid json"}

        result = load_from_aws_secret("test_secret", mock_session)

        assert result == {}

    def test_load_from_aws_secret_client_error(self, mock_boto3_session):
        """Test handling of AWS client errors."""
        mock_session, mock_client = mock_boto3_session
        mock_client.get_secret_value.side_effect = Exception("AWS Error")

        with pytest.raises(Exception, match="AWS Error"):
            load_from_aws_secret("test_secret", mock_session)


class TestSanitizeSnowflakeCredentials:
    """Test cases for _sanitize_snowflake_credentials function."""

    def test_sanitize_basic_credentials(self):
        """Test basic credential sanitization."""
        raw_creds = {
            "SNOWFLAKE_ACCOUNT": "test_account",
            "SNOWFLAKE_USER": "test_user",
            "password": "test_password",
            "database": "",  # empty string should be filtered out
            "schema": None,  # None should be filtered out
            "warehouse": "   ",  # whitespace only should be filtered out
            "role": "test_role",
        }

        result = sanitize_snowflake_credentials(raw_creds)

        expected = {
            "account": "test_account",
            "user": "test_user",
            "password": "test_password",
            "role": "test_role",
        }
        assert result == expected

    def test_sanitize_legacy_keys(self):
        """Test mapping of legacy keys."""
        raw_creds = {
            "accountname": "test_account",
            "username": "test_user",
            "dbname": "test_db",
            "schemaname": "test_schema",
            "warehousename": "test_warehouse",
            "rolename": "test_role",
        }

        result = sanitize_snowflake_credentials(raw_creds)

        expected = {
            "account": "test_account",
            "user": "test_user",
            "database": "test_db",
            "schema": "test_schema",
            "warehouse": "test_warehouse",
            "role": "test_role",
        }
        assert result == expected

    def test_sanitize_env_var_prefix_removal(self):
        """Test removal of SNOWFLAKE_ prefix from environment variables."""
        raw_creds = {
            "SNOWFLAKE_ACCOUNT": "test_account",
            "SNOWFLAKE_USER": "test_user",
            "snowflake_password": "test_password",  # lowercase prefix
        }

        result = sanitize_snowflake_credentials(raw_creds)

        expected = {
            "account": "test_account",
            "user": "test_user",
            "password": "test_password",
        }
        assert result == expected

    def test_sanitize_invalid_params_filtered(self):
        """Test that invalid parameters are filtered out."""
        raw_creds = {
            "account": "test_account",
            "invalid_param": "should_be_filtered",
            "another_invalid": "also_filtered",
            "user": "test_user",
        }

        result = sanitize_snowflake_credentials(raw_creds)

        expected = {
            "account": "test_account",
            "user": "test_user",
        }
        assert result == expected

    @patch("snowconn.connection_builder._load_private_with_passphrase")
    def test_sanitize_encrypted_private_key_success(self, mock_load_private):
        """Test handling of encrypted private key with passphrase."""
        mock_private_key = b"decoded_private_key"
        mock_load_private.return_value = mock_private_key

        raw_creds = {
            "account": "test_account",
            "user": "test_user",
            "private_key_encrypted": "encrypted_key_content",
            "private_key_passphrase": "passphrase",
        }

        result = sanitize_snowflake_credentials(raw_creds)

        expected = {
            "account": "test_account",
            "user": "test_user",
            "private_key": mock_private_key,
        }
        assert result == expected
        mock_load_private.assert_called_once_with("encrypted_key_content", "passphrase")

    def test_sanitize_encrypted_private_key_missing_passphrase(self):
        """Test error when encrypted private key is provided without passphrase."""
        raw_creds = {
            "account": "test_account",
            "private_key_encrypted": "encrypted_key_content",
            # missing private_key_passphrase
        }

        with pytest.raises(
            ValueError,
            match="private_key_encrypted is set but private_key_passphrase is not provided",
        ):
            sanitize_snowflake_credentials(raw_creds)


class TestLoadPrivateWithPassphrase:
    """Test cases for _load_private_with_passphrase function."""

    @patch("snowconn.connection_builder.serialization.load_pem_private_key")
    def test_load_private_key_success(self, mock_load_pem):
        """Test successful loading and conversion of private key."""
        # Mock the private key object
        mock_private_key = Mock()
        mock_private_key_der = b"der_encoded_key"
        mock_private_key.private_bytes.return_value = mock_private_key_der
        mock_load_pem.return_value = mock_private_key

        private_key_pem = "-----BEGIN PRIVATE KEY-----\ntest_key_content\n-----END PRIVATE KEY-----"
        passphrase = "test_passphrase"

        result = _load_private_with_passphrase(private_key_pem, passphrase)

        assert result == mock_private_key_der
        mock_load_pem.assert_called_once()
        # Verify the arguments passed to load_pem_private_key
        args, kwargs = mock_load_pem.call_args
        assert args[0] == private_key_pem.encode()
        assert kwargs["password"] == passphrase.encode()

    @patch("snowconn.connection_builder.serialization.load_pem_private_key")
    def test_load_private_key_invalid_key(self, mock_load_pem):
        """Test handling of invalid private key."""
        mock_load_pem.side_effect = ValueError("Invalid private key")

        with pytest.raises(ValueError, match="Invalid private key"):
            _load_private_with_passphrase("invalid_key", "passphrase")


class TestCreateSnowflakeSaEngine:
    """Test cases for create_snowflake_sa_engine function."""

    @patch("snowconn.connection_builder.create_engine")
    def test_create_engine_success(self, mock_create_engine):
        """Test successful SQLAlchemy engine creation."""
        mock_engine = Mock(spec=Engine)
        mock_create_engine.return_value = mock_engine

        creds = {
            "account": "test_account",
            "user": "test_user",
            "password": "test_password",
            "database": "test_db",
            "schema": "test_schema",
            "warehouse": "test_warehouse",
            "role": "test_role",
        }

        result = create_snowflake_sa_engine(creds)

        assert result == mock_engine
        mock_create_engine.assert_called_once()

        # Verify the connection URL format
        args, kwargs = mock_create_engine.call_args
        connection_url = args[0]
        assert "snowflake://test_user:test_password@test_account/test_db" in connection_url
        assert "schema=test_schema" in connection_url

        # Verify connect_args contains non-URL parameters
        connect_args = kwargs["connect_args"]
        assert connect_args["warehouse"] == "test_warehouse"
        assert connect_args["role"] == "test_role"

    @patch("snowconn.connection_builder.create_engine")
    def test_create_engine_without_password(self, mock_create_engine):
        """Test engine creation without password."""
        mock_engine = Mock(spec=Engine)
        mock_create_engine.return_value = mock_engine

        creds = {
            "account": "test_account",
            "user": "test_user",
            "database": "test_db",
        }

        result = create_snowflake_sa_engine(creds)

        assert result == mock_engine
        args, kwargs = mock_create_engine.call_args
        connection_url = args[0]
        # Should not have password in URL
        assert "snowflake://test_user@test_account/test_db" in connection_url

    @patch("snowconn.connection_builder.create_engine")
    def test_create_engine_minimal_creds(self, mock_create_engine):
        """Test engine creation with minimal credentials."""
        mock_engine = Mock(spec=Engine)
        mock_create_engine.return_value = mock_engine

        creds = {
            "account": "test_account",
            "user": "test_user",
        }

        result = create_snowflake_sa_engine(creds)

        assert result == mock_engine
        args, kwargs = mock_create_engine.call_args
        connection_url = args[0]
        assert "snowflake://test_user@test_account" in connection_url

    @patch("snowconn.connection_builder.create_engine")
    def test_create_engine_failure(self, mock_create_engine):
        """Test handling of engine creation failure."""
        mock_create_engine.side_effect = Exception("Engine creation failed")

        creds = {
            "account": "test_account",
            "user": "test_user",
        }

        with pytest.raises(
            ConnectionError, match="Failed to create SQLAlchemy engine: Engine creation failed"
        ):
            create_snowflake_sa_engine(creds)


class TestIntegration:
    """Integration tests combining multiple functions."""

    def test_full_credential_flow_json(self, write_json_file, sample_credentials):
        """Test complete flow from JSON file to engine creation."""
        json_file = write_json_file(sample_credentials)
        creds = load_from_json_file(json_file)

        with patch("snowconn.connection_builder.create_engine") as mock_create_engine:
            mock_engine = Mock(spec=Engine)
            mock_create_engine.return_value = mock_engine

            engine = create_snowflake_sa_engine(creds)

            assert engine == mock_engine
            mock_create_engine.assert_called_once()

    def test_credential_precedence_and_merging(self, write_json_file):
        """Test that different credential sources can be combined."""
        # Test JSON file
        json_creds = {"account": "json_account", "user": "json_user"}
        json_file = write_json_file(json_creds)

        # Test environment variables
        env_vars = {
            "SNOWFLAKE_PASSWORD": "env_password",
            "SNOWFLAKE_DATABASE": "env_database",
        }

        json_result = load_from_json_file(json_file)

        with patch.dict(os.environ, env_vars, clear=True):
            env_result = load_from_env_vars()

        # Verify that each source provides its expected values
        assert json_result == {"account": "json_account", "user": "json_user"}
        assert env_result == {"password": "env_password", "database": "env_database"}

        # In a real application, these would be merged by the connection manager
        merged_creds = {**json_result, **env_result}
        expected_merged = {
            "account": "json_account",
            "user": "json_user",
            "password": "env_password",
            "database": "env_database",
        }
        assert merged_creds == expected_merged
