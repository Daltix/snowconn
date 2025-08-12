"""Provides the SnowConn class for connecting to Snowflake using different credentials sources.

Supports executing queries, reading/writing pandas DataFrames, and managing connections.

Usage:
    conn = SnowConn.connect(methods=['local'])
    results = conn.execute_simple("SELECT * FROM my_table;")
    df = conn.read_df("SELECT * FROM my_table;")
    conn.write_df(df, table="MY_TABLE", schema="PUBLIC")
    conn.close()
"""

from __future__ import annotations

import json
import logging
import warnings
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Iterable, Literal

import snowflake.connector

from snowconn.connection_builder import (
    SNOWFLAKE_CONFIG_FILE_PATH,
    create_snowflake_sa_engine,
    load_from_aws_secret,
    load_from_snowflake_config_file,
)


if TYPE_CHECKING:
    import pandas as pd
    from snowflake.connector import SnowflakeConnection
    from sqlalchemy.engine.base import Connection, Engine


class InvalidMethodException(Exception):  # noqa: N818
    """Exception raised for invalid connection methods."""


class SnowConn:
    """SnowConn is a class that provides methods to connect to Snowflake using various methods."""

    _alchemy_engine = None
    _connection = None
    _raw_connection = None

    def __init__(self) -> None:
        self._alchemy_engine = None
        self._connection = None
        self._raw_connection = None

    def __enter__(self) -> SnowConn:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object | None,
    ) -> None:
        self.close()

    @classmethod
    def connect(
        cls,
        methods: Iterable[Literal["local", "secretsmanager", "credentials"]] = ("local",),
        *args: Any,
        **kwargs: Any,
    ) -> SnowConn:
        """Generic connect method.

        Will iterate through a list of connection methods until one succeeds
        in creating a connection
        from snowconn import SnowConn
        conn = SnowConn.autoconnect(method=['secretsmanager'], credsman_name='acme')
        """
        available_methods: dict[str, Callable[..., SnowConn]] = {
            "secretsmanager": cls.connect_secretsmanager,
            "local": cls.connect_local,
            "credentials": cls.connect_credentials,
        }
        for method in methods:
            if method in available_methods:
                try:
                    return available_methods[method](*args, **kwargs)
                except Exception as e:
                    logging.error(e)
        raise InvalidMethodException(
            f'methods {methods} are not a valid connection methods. Valid methods are "secretsmanager, local, credentials"'
        )

    @classmethod
    def connect_local(  # noqa: PLR0913
        cls,
        db: str = "public",
        schema: str = "public",
        autocommit: bool = True,
        role: str | None = None,
        warehouse: str | None = None,
        local_creds_path: str | None = None,
        **kwargs: Any,
    ) -> SnowConn:
        """Creates an engine and connection using your local snowsql credentials."""
        connect_args = kwargs.get("connect_args", {})
        conn = cls()
        creds = conn._get_local_creds(local_creds_path)
        conn._create_engine(creds, db, schema, autocommit, role, warehouse, connect_args)
        return conn

    def _get_local_creds(self, local_creds_path: str | None = None) -> dict[str, Any]:
        snowsql_config = Path(local_creds_path) if local_creds_path else SNOWFLAKE_CONFIG_FILE_PATH
        return load_from_snowflake_config_file(
            file=snowsql_config,
            section="connections",
        )

    @classmethod
    def connect_secretsmanager(  # noqa: PLR0913
        cls,
        credsman_name: str,
        db: str = "public",
        schema: str = "public",
        autocommit: bool = True,
        role: str | None = None,
        aws_region_name: str = "eu-west-1",
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        warehouse: str | None = None,
        region_name: str | None = None,
        **kwargs: Any,
    ) -> SnowConn:
        """Creates an engine and connection using credentials from AWS secrets manager."""
        try:
            import boto3  # noqa
        except ImportError as e:
            logging.warning("boto3 not installed, cannot execute connect_secretsmanager")
            raise e
        if region_name:
            warnings.warn(
                """
                The argument 'region_name' will be deprecated in future versions.
                Please use 'region_name' instead
                """,
                DeprecationWarning,
                stacklevel=2,
            )
            aws_region_name = region_name

        connect_args = kwargs.get("connect_args", {})

        conn = SnowConn()
        creds = conn._get_secretsmanager_creds(
            credsman_name, aws_region_name, aws_access_key_id, aws_secret_access_key
        )
        conn._create_engine(creds, db, schema, autocommit, role, warehouse, connect_args)
        return conn

    @classmethod
    def credsman_connect(cls, *args: Any, **kwargs: Any) -> SnowConn:
        """Legacy Secretsmanager connection."""
        warnings.warn(
            """
            Method `credsman_connect` will be deprecated in future versions.
            Please use `SnowConn.connect(methods=['secretsmanager']` instead
            """,
            DeprecationWarning,
            stacklevel=2,
        )
        return cls.connect_secretsmanager(*args, **kwargs)

    @classmethod
    def connect_credentials(  # noqa: PLR0913
        cls,
        account: str,
        username: str,
        password: str,
        authenticator: str | None = None,
        db: str = "public",
        schema: str = "public",
        autocommit: bool = True,
        role: str | None = None,
        warehouse: str | None = None,
        **kwargs: Any,
    ) -> SnowConn:
        """Creates an engine and connection to snowflake db using the provided credentials."""
        conn = cls()
        creds = {
            "ACCOUNT": account,
            "USERNAME": username,
            "PASSWORD": password,
            "AUTHENTICATOR": authenticator,
        }

        connect_args = kwargs.get("connect_args", {})
        conn._create_engine(creds, db, schema, autocommit, role, warehouse, connect_args)
        return conn

    def _get_secretsmanager_creds(
        self,
        credsman_name: str,
        region_name: str,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
    ) -> Any:
        import boto3  # noqa

        aws_creds: dict[str, str] = {}
        if aws_access_key_id and aws_secret_access_key:
            aws_creds = {
                "aws_access_key_id": aws_access_key_id,
                "aws_secret_access_key": aws_secret_access_key,
            }

        session = boto3.Session(**aws_creds, region_name=region_name)  # type: ignore
        return load_from_aws_secret(secret_name=credsman_name, session=session)

    def _create_engine(  # noqa: PLR0913
        self,
        creds: dict[str, Any],
        db: str,
        schema: str,
        autocommit: bool = True,
        role: str | None = None,
        warehouse: str | None = None,
        connect_args: dict[str, Any] | None = None,
    ) -> None:
        role = role if role else creds.get("ROLE")
        connect_args = connect_args or {}
        engine_creds = {
            **creds,
            "autocommit": autocommit,
            "schema": schema,
            "warehouse": warehouse,
            "role": role,
            "database": db,
            **connect_args,
        }
        engine = create_snowflake_sa_engine(engine_creds)
        self._alchemy_engine = engine
        self._connection = self._alchemy_engine.connect()
        self._raw_connection = self._connection.connection.connection

    def get_alchemy_engine(self) -> Engine:
        """Returns the sqlalchemy engine.

        Result of a call to either connect() or credsman_connect().
        """
        return self._alchemy_engine  # type: ignore[return-value]

    def get_connection(self) -> Connection:
        """Returns the snowflake SQLAlchemy connection object.

        Result of calling engine.connect() on an SQLAlchemy engine
        """
        return self._connection  # type: ignore[return-value]

    def get_raw_connection(self) -> SnowflakeConnection:
        """Returns the lower level snowflake-connector connection object."""
        return self._raw_connection  # type: ignore[return-value]

    def execute_simple(self, sql: str) -> list[dict[Any, Any]]:
        """Executes a single SQL statement.

        Reads the result set into memory and
        returns an array of dictionaries. This method is for executing single
        queries from which the result set can be fit into memory in regular
        python data structures. This is a wrapper around the snowflake
        connector execute() method found here: https://docs.snowflake.net/manuals/user-guide/python-connector-api.html#execute

        :param sql: string containing a single SQL statement
        :return: array of dictionaries
        """
        types_to_parse = (5, 9, 10)
        try:
            cursor = self.get_raw_connection().cursor(snowflake.connector.DictCursor)
            results = cursor.execute(sql)
        except snowflake.connector.errors.ProgrammingError as e:
            print(sql)
            raise e

        to_parse = {desc[0] for desc in results.description if desc[1] in types_to_parse}  # type: ignore

        return [
            {
                key: json.loads(value) if key in to_parse and value is not None else value
                for key, value in entry.items()  # type: ignore
            }
            for entry in results  # type: ignore
        ]

    def execute_string(self, sql: str, *args: Any, **kwargs: Any) -> Any:
        """Executes a list of sql statements.

        This is a thin wrapper around the snowflake connector execute_string() method found here:
        https://docs.snowflake.net/manuals/user-guide/python-connector-api.html#execute_string

        :param sql:
        :return: list of cursors
        """
        try:
            cursor_list = self.get_raw_connection().execute_string(sql, *args, **kwargs)
        except snowflake.connector.errors.ProgrammingError as e:
            print(sql)
            raise e
        return cursor_list

    def execute_file(self, fname: str) -> Any:
        """Given the path to filename, execute the contents of the file using self.execute_string.

        :param fname: file path that can be open()ed
        :return: list of cursors
        """
        return self.execute_string(Path(fname).read_text())

    def read_df(self, sql: str, lowercase_columns: bool = True) -> pd.DataFrame:
        """Executes the sql passed in and reads the result into a pandas dataframe.

        If you want to use pandas, you'll have to install it yourself as it is
        not a requirement of this package due to its weight.
        :param sql: string containing a single SQL statement
        :param lowercase_columns: boolean, wether or not to lowercase column names
        (snowflake connector fetch_pandas_all returns uppercase)
        :return: pandas DataFrame
        """
        try:
            import pandas as pd  # noqa
        except ImportError as e:
            logging.warning("pandas not installed, cannot execute read_df")
            raise e
        cursor = self.get_raw_connection().cursor(snowflake.connector.DictCursor)
        cursor.execute(sql)
        read_df = cursor.fetch_pandas_all()
        if lowercase_columns:
            read_df.columns = map(str.lower, read_df.columns)  # type: ignore
        return read_df

    def write_df(  # noqa: PLR0913
        self,
        df: pd.DataFrame,
        table: str,
        schema: str | None = None,
        if_exists: Literal["fail", "replace", "append"] = "replace",
        index: bool = False,
        temporary_table: bool = False,
        chunksize: int = 5000,
        **kwargs: Any,
    ) -> None:
        """Writes a dataframe to the specified table.

        Note that you must be connected in the correct context for this to be able to work as you
        cannot specify the fully namespaced version of the table.

        There reason that we have so many params explicit in this function
        is because we want to change their defaults to something more sensible
        for daltix.

        :param df: pandas dataframe to write to the table
        :param table: the name of ONLY the table
        :param schema: The schema of the table
        :param if_exists: forwarded on to DataFrame.to_sql
        :param index: forwarded on to DataFrame.to_sql
        :param temporary_table: Runs a bit of a hack to create temp tables
        :param chunksize: forwarded on to DataFrame.to_sql
        :param kwargs: forwarded on to DataFrame.to_sql
        :return: None
        """
        import pandas as pd  # noqa: PLC0415

        if schema:
            schema = schema.upper()
        table = table.upper()

        schema_table = (('"' + schema + '".') if schema else "") + ('"' + table + '"')

        if not temporary_table:
            df.to_sql(
                table,
                con=self.get_connection(),
                schema=schema,
                if_exists=if_exists,
                index=index,
                chunksize=chunksize,
                **kwargs,
            )
        else:
            sql = pd.io.sql.get_schema(df, name=table, con=self.get_connection()).replace(  # type: ignore
                f'CREATE TABLE "{table}"', f"CREATE OR REPLACE TEMPORARY TABLE {schema_table}"
            )
            self.execute_simple(sql)
            df.to_sql(
                table,
                con=self.get_connection(),
                schema=schema,
                if_exists="append",
                index=index,
                chunksize=chunksize,
                **kwargs,
            )

    def close(self) -> None:
        """Close off the current connection.

        Also, dispose() of the engine is not documented anywhere in snowflake.

        :return: None
        """
        self.get_connection().close()
        self.get_alchemy_engine().dispose()

    def get_current_role(self) -> Any:
        """Get the current role of the connection."""
        results = self.execute_simple("show roles;")
        return next(r for r in results if r["is_current"] == "Y")["name"]
