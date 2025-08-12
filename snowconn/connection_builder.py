"""This module provides functions to build Snowflake connections using various methods.

Some of the methods:
- JSON files
- snowflake config files
- environment variables
- AWS Secrets Manager.
"""

from __future__ import annotations

import configparser
import json
import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from snowflake.connector.connection import DEFAULT_CONFIGURATION
from sqlalchemy import create_engine


if TYPE_CHECKING:
    import boto3
    from sqlalchemy.engine import Engine


logger = logging.getLogger(__name__)

AVAILABLE_PARAMS = {
    *DEFAULT_CONFIGURATION.keys(),
    "private_key_encrypted",
    "private_key_passphrase",
}

# Key aliases for Snowflake connection parameters
KEY_ALIASES = {
    # SnowSQL specific
    "accountname": "account",
    "username": "user",
    "dbname": "database",
    "schemaname": "schema",
    "warehousename": "warehouse",
    "rolename": "role",
    # other compatible mapping
    "rsa_private_key": "private_key_encrypted",
    "pk_passphrase": "private_key_passphrase",
}

CREDS_ENV_VARS_PREFIX = "SNOWFLAKE_"
SNOWFLAKE_SETTINGS_JSON_PATH = Path(
    os.environ.get("SNOWFLAKE_SETTINGS_JSON_PATH", "/etc/config/snowflake_creds.json"),
)
SNOWFLAKE_CONFIG_FILE_PATH = Path(
    os.environ.get("SNOWFLAKE_CONFIG_FILE_PATH", Path.home() / ".snowsql" / "config"),
)
RESERVED_ENV_KEYS = {
    "SNOWFLAKE_SETTINGS_JSON_PATH",
    "SNOWFLAKE_CONFIG_FILE_PATH",
}


class ConnectionError(Exception):
    """Base exception for connection-related errors."""


def load_from_json_file(file: Path = SNOWFLAKE_SETTINGS_JSON_PATH) -> dict[str, Any]:
    """Load snowflake json config file.

    Returns:
        dict[str, Any]: snowflake config
    """
    logger.debug(f"Trying read snowflake credentials from json config file {file}")
    try:
        creds_dict = json.loads(file.read_text())
    except FileNotFoundError:
        logger.debug("File not found.")
        return {}
    creds = _sanitize_snowflake_credentials(creds_dict)
    if creds:
        logger.info(f"Loaded snowflake credentials from json config file: {file}")
    else:
        logger.debug("No credentials found in json config file")
    return creds


def load_from_snowflake_config_file(
    file: Path = SNOWFLAKE_CONFIG_FILE_PATH,
    section: str = "connections",
) -> dict[str, Any]:
    """Load snowflake credentials from a config file.

    Returns:
        dict[str, Any]: snowflake config
    """
    logger.debug(f"Trying read snowflake credentials from config file {file}")
    config = configparser.ConfigParser()
    if not file.exists():
        logger.debug("File not found.")
        return {}
    config.read(file)
    try:
        creds = dict(config[section])
        creds = _sanitize_snowflake_credentials(creds)
        if creds:
            logger.info(f"Loaded snowflake credentials from snowflake config file: {file}")
        else:
            logger.debug("No credentials found in snowflake config file")
        return creds
    except KeyError:
        return {}


def load_from_env_vars() -> dict[str, Any]:
    """Load snowflake credentials from environment variables.

    This function looks for environment variables that start with the prefix
    `SNOWFLAKE_` and returns a dictionary of the credentials.

    Returns:
        dict[str, Any]: snowflake credentials
    """
    logger.debug(
        f"Trying read snowflake credentials from environment variables with prefix {CREDS_ENV_VARS_PREFIX}",
    )
    env_creds = {
        cred_key: cred_value
        for cred_key, cred_value in os.environ.items()
        if cred_key.startswith(CREDS_ENV_VARS_PREFIX)
    }
    creds = _sanitize_snowflake_credentials(env_creds)
    if creds:
        logger.info(
            f"Loaded snowflake credentials from environment variables with prefix {CREDS_ENV_VARS_PREFIX}"
        )
    else:
        logger.debug("No credentials found in environment variables")
    return creds


def load_from_aws_secret(secret_name: str, session: boto3.Session) -> dict[str, Any]:
    """Load Snowflake credentials from AWS Secrets Manager.

    Args:
        secret_name (str): The name of the secret in AWS Secrets Manager.
        session (boto3.Session): A boto3 session object.

    Returns:
        dict[str, Any]: Snowflake credentials
    """
    logger.debug(f"Trying to load snowflake credentials from AWS Secrets Manager: {secret_name}")
    client = session.client(service_name="secretsmanager")
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)

    secret_str = get_secret_value_response.get("SecretString")
    if not secret_str:
        logger.debug(f"No secret string found for secret: {secret_name}")
        return {}
    try:
        data = json.loads(secret_str)
    except json.JSONDecodeError:
        logger.error(f"Failed to decode JSON from secret: {secret_name}")
        return {}
    creds = _sanitize_snowflake_credentials(data)
    if creds:
        logger.info(f"Loaded snowflake credentials from AWS Secrets Manager: {secret_name}")
    else:
        logger.debug("No credentials found in AWS Secrets Manager")
    return creds


# utilities
def _sanitize_snowflake_credentials(creds: dict[str, Any]) -> dict[str, Any]:
    """Sanitize and normalize snowflake credentials.

    This function filters out empty values and maps legacy keys to their current equivalents.
    It also handles encrypted private keys with passphrases.

    Args:
        creds (Dict[str, Any]): raw credentials

    Returns:
        Dict[str, Any]: normalized credentials
    """
    norm_creds = {}
    for cred_key, cred_val in creds.items():
        if cred_val is None or (isinstance(cred_val, str) and not cred_val.strip()):
            continue
        norm_key = cred_key.lower()
        if norm_key.startswith(CREDS_ENV_VARS_PREFIX.lower()):
            norm_key = norm_key[len(CREDS_ENV_VARS_PREFIX) :]
        norm_key = norm_key.strip()
        mapped_norm_key = KEY_ALIASES.get(norm_key, norm_key)
        if mapped_norm_key in AVAILABLE_PARAMS:
            norm_creds[mapped_norm_key] = cred_val

    # deal with encrypted private key strings
    _private_key_encrypted = norm_creds.pop("private_key_encrypted", None)
    _private_key_passphrase = norm_creds.pop("private_key_passphrase", None)
    if _private_key_encrypted and not _private_key_passphrase:
        raise ValueError("private_key_encrypted is set but private_key_passphrase is not provided")
    if _private_key_encrypted and _private_key_passphrase:
        norm_creds["private_key"] = _load_private_with_passphrase(
            _private_key_encrypted,
            _private_key_passphrase,
        )
    return norm_creds


def _load_private_with_passphrase(private_key_pem: str, passphrase: str) -> bytes:
    private_key = serialization.load_pem_private_key(
        private_key_pem.encode(),
        password=passphrase.encode(),
        backend=default_backend(),
    )

    # Convert to DER format (required by snowflake.connector)
    private_key_der = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return private_key_der


def create_snowflake_sa_engine(creds: dict[str, Any]) -> Engine:
    """Create a SQLAlchemy engine for Snowflake using the provided credentials.

    Args:
        creds (Dict[str, Any]): Snowflake credentials.

    Returns:
        Engine: SQLAlchemy engine instance.

    Raises:
        ConnectionError: If the engine creation fails.
    """
    creds = _sanitize_snowflake_credentials(creds)
    url_param_keys = {"user", "password", "account", "database", "schema"}
    connect_args = {k: creds[k] for k in creds if k not in url_param_keys and creds[k] is not None}

    try:
        # Build connection URL
        account = creds["account"]
        user = creds["user"]
        password = creds.get("password", "")
        database = creds.get("database", "")
        schema = creds.get("schema", "")

        # Handle password in URL
        user_pass = f"{user}:{password}" if password else user

        # Build URL
        url_parts = [f"snowflake://{user_pass}@{account}"]
        if database:
            url_parts.append(f"/{database}")

        # Add query parameters
        query_params = []
        if schema:
            query_params.append(f"schema={schema}")

        if query_params:
            url_parts.append("?" + "&".join(query_params))

        connection_url = "".join(url_parts)

        logger.debug("Creating SQLAlchemy engine")
        return create_engine(connection_url, connect_args=connect_args)
    except Exception as e:
        raise ConnectionError(f"Failed to create SQLAlchemy engine: {e}") from e
