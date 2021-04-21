import os
import json
from sqlalchemy import create_engine
import snowflake.connector
import configparser

try:
    import boto3
    import botocore
except ImportError as e:
    print('Cannot import boto3, if you want to use credsman_connect, please'
          ' ensure that boto3 is installed')


class SnowConn:
    _alchemy_engine = None
    _connection = None
    _raw_connection = None

    def __init__(self):
        self._alchemy_engine = None
        self._connection = None
        self._raw_connection = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    @classmethod
    def connect(cls, db: str = 'public', schema: str = 'public',
                autocommit: bool = True, role=None, warehouse=None,
                local_creds_path=None):
        """
        Creates an engine and connection to the specified snowflake
        db using your snowsql credentials.
        """
        conn = SnowConn()
        conn._connect(
            db=db,
            schema=schema,
            autocommit=autocommit,
            role=role,
            warehouse=warehouse,
            credsman_name=None,
            aws_region_name=None,
            aws_access_key_id=None,
            aws_secret_access_key=None,
            fallback_to_local_creds=False,
            local_creds_path=local_creds_path,
        )
        return conn

    @classmethod
    def credsman_connect(cls, credsman_name: str, db: str = 'public',
                         schema: str = 'public', autocommit: bool = True,
                         role: str = None, region_name='eu-west-1',
                         aws_access_key_id=None, aws_secret_access_key=None,
                         warehouse=None, fallback_to_local_creds=False,
                         local_creds_path=None):
        """
        Creates an engine and connection to the specified snowflake db using
        credentials from AWS secrets manager (optionally falling back to
        local credentials)
        """
        conn = SnowConn()
        conn._connect(
            db=db,
            schema=schema,
            autocommit=autocommit,
            role=role,
            warehouse=warehouse,
            credsman_name=credsman_name,
            aws_region_name=aws_region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            fallback_to_local_creds=fallback_to_local_creds,
            local_creds_path=local_creds_path,
        )
        return conn

    def _connect(self, db: str, schema: str, autocommit: bool, role: str, warehouse:str,
                credsman_name: str, aws_region_name: str, aws_access_key_id: str,
                aws_secret_access_key: str, fallback_to_local_creds: bool, local_creds_path: str):

        creds = None

        # Try to get credentials from AWS secrets manager
        if credsman_name:
            creds = self._get_credsman_creds(credsman_name, aws_region_name,
                aws_access_key_id, aws_secret_access_key)

        # Try to get local credentials if:
        # 1. No credsman_name was passed or
        # 2. fallback_to_local_creds is True and an invalid credsman_name was passed
        if (not credsman_name) or (creds is None and credsman_name and fallback_to_local_creds):
            creds = self._get_local_creds(local_creds_path)

        if creds:
            self._create_engine(creds, db, schema, autocommit, role, warehouse)
        else:
            raise RuntimeError('Cannot create engine: missing credentials')

    def _get_credsman_creds(self, credsman_name: str, aws_region_name: str,
                        aws_access_key_id: str, aws_secret_access_key: str):
        aws_creds = {};
        if aws_access_key_id and aws_secret_access_key:
            aws_creds = {
                aws_access_key_id: aws_access_key_id,
                aws_secret_access_key: aws_secret_access_key
            }

        session = boto3.session.Session(**aws_creds)
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name,
            endpoint_url=f'https://secretsmanager.{region_name}.amazonaws.com'
        )

        try:
            get_secret_value_response = client.get_secret_value(SecretId=credsman_name)
        except botocore.exceptions.ClientError as error:
            if error.response['Error']['Code'] == 'AccessDeniedException':
                return None
            else:
                raise error

        return json.loads(get_secret_value_response['SecretString'])

    def _get_local_creds(self, local_creds_path: str):
        home = os.path.expanduser("~")
        snowsql_config = local_creds_path if local_creds_path else f'{home}/.snowsql/config'

        if not os.path.exists(snowsql_config):
            raise RuntimeError(
                f'No snowsql config found in {snowsql_config}. '
                f'Please install snowsql and add in your snowflake '
                f'login credentials to the config file.'
            )

        config = configparser.ConfigParser()
        config.read(snowsql_config)

        return {
            'AUTHENTICATOR': config['connection'].get('authenticator', None),
            'USERNAME': config['connection']['username'],
            'PASSWORD': config['connection']['password'],
            'ACCOUNT': config['connection']['account'],
            'ROLE': config['connection'].get('role', None),
        }

    def _create_engine(self, creds: dict, db: str, schema: str,
                       autocommit: bool = True, role: str = None,
                       warehouse=None):

        account = creds['ACCOUNT']
        username = creds['USERNAME']
        password = creds['PASSWORD']
        authenticator = creds.get('AUTHENTICATOR', None)
        role = role if role else creds.get('ROLE', None)

        if '.' not in account:
            print(
                'You may need to configure your account name to include the '
                f'region. For example: {account}.eu-west-1')

        schema_portion = f'?schema={schema}'

        role_portion = ''
        if role:
            role_portion = f'&role={role}'

        autocommit_portion = ''
        if autocommit:
            autocommit_portion = '&autocommit=true'

        warehouse_portion = ''
        if warehouse:
            warehouse_portion = f'&warehouse={warehouse}'

        authenticator_portion = ''
        if authenticator:
            authenticator_portion = f'&authenticator={authenticator}'

        connection_string = (
            f'snowflake://{username}:{password}@{account}/{db}{schema_portion}'
            f'{role_portion}{autocommit_portion}{warehouse_portion}{authenticator_portion}'
        )

        engine = create_engine(connection_string)
        self._alchemy_engine = engine
        self._connection = self._alchemy_engine.connect()
        self._raw_connection = self._connection.connection.connection

    def get_alchemy_engine(self):
        """
        Returns the sqlalchemy engine that is the result of a call to
        either connect() or credsman_connect()
        """
        return self._alchemy_engine

    def get_connection(self):
        """
        Returns the snowflake SQLAlchemy connection object that is the
        result of calling engine.connect() on an SQLAlchemy engine
        """
        return self._connection

    def get_raw_connection(self):
        """Returns the lower level snowflake-connector connection object
        """
        return self._raw_connection

    def execute_simple(self, sql: str):
        """
        Executes a single SQL statement, reads the result set into memory and
        returns an array of dictionaries. This method is for executing single
        queries from which the result set can be fit into memory in regular
        python data structures. This is a wrapper around the snowflake
        connector execute() method found here: https://docs.snowflake.net/manuals/user-guide/python-connector-api.html#execute

        :param sql: string containing a single SQL statement
        :return: array of dictionaries
        """
        types_to_parse = (5, 9, 10)
        try:
            cursor = self._raw_connection.cursor(snowflake.connector.DictCursor)
            results = cursor.execute(sql)
        except snowflake.connector.errors.ProgrammingError as e:
            print(sql)
            raise e

        to_parse = {
            desc[0]
            for desc in results.description
            if desc[1] in types_to_parse
        }

        return [
            {
                key: json.loads(value)
                if key in to_parse and value is not None else value
                for key, value in entry.items()
            }
            for entry in results
        ]

    def execute_string(self, sql: str, *args, **kwargs):
        """
        Executes a list of sql statements. This is a thin wrapper around the
        snowflake connector execute_string() method found here:
        https://docs.snowflake.net/manuals/user-guide/python-connector-api.html#execute_string

        :param sql:
        :return: list of cursors
        """
        try:
            cursor_list = self._raw_connection.execute_string(
                sql, *args, **kwargs)
        except snowflake.connector.errors.ProgrammingError as e:
            print(sql)
            raise e
        return cursor_list

    def execute_file(self, fname: str):
        """
        Given the path to filename, execute the contents of the file using
        self.execute_string
        :param fname: file path that can be open()ed
        :return: list of cursors
        """
        with open(fname) as fh:
            sql = fh.read()
        return self.execute_string(sql)

    def read_df(self, sql: str):
        """
        Executes the sql passed in and reads the result into a pandas
        dataframe.

        If you want to use pandas, you'll have to install it yourself as it is
        not a requirement of this package due to its weight.
        :param sql: string containing a single SQL statement
        :return: pandas DataFrame
        """
        try:
            import pandas as pd
        except ImportError as e:
            print('pandas not installed, cannot execute read_df')
            raise e
        return pd.read_sql_query(sql, self._connection)

    def write_df(self, df, table: str, schema=None, if_exists: str = 'replace',
                 index: bool = False, temporary_table=False,
                 chunksize=5000, **kwargs):
        """
        Writes a dataframe to the specified table. Note that you must be
        connected in the correct context for this to be able to work as you
        cannot specify the fully namespaced version of the table.

        There reason that we have so many params explicit in this function
        is because we want to change their defaults to something more sensible
        for daltix.

        :param df: pandas dataframe to write to the table
        :param table: the name of ONLY the table
        :param if_exists: forwarded on to DataFrame.to_sql
        :param index: forwarded on to DataFrame.to_sql
        :param temporary_table: Runs a bit of a hack to create temp tables
        :param chunksize: forwarded on to DataFrame.to_sql
        :param kwargs: forwarded on to DataFrame.to_sql
        :return: None
        """

        if not temporary_table:
            df.to_sql(table, con=self._connection,
                      if_exists=if_exists, index=index, chunksize=chunksize,
                      **kwargs)
        else:
            import pandas as pd

            if schema:
                create_temporary_clause = 'CREATE OR REPLACE TEMPORARY TABLE {SCHEMA}.'.format(SCHEMA=schema)
            else:
                create_temporary_clause = 'CREATE OR REPLACE TEMPORARY TABLE'

            sql = pd.io.sql.get_schema(
                df, name=table, con=self._connection
            ).replace('CREATE TABLE', create_temporary_clause)
            self.execute_simple(sql)
            df.to_sql(table, con=self._connection,
                      if_exists='append', index=index, chunksize=chunksize,
                      **kwargs)

    def close(self):
        """
        Close off the current connection and dispose() of the engine
        is not documented anywhere in snowflake.
        :return: None
        """
        self._connection.close()
        self._alchemy_engine.dispose()

    def get_current_role(self):
        results = self.execute_simple('show roles;')
        return [r for r in results if r['is_current'] == 'Y'][0]['name']
