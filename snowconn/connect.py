import os
import json
from credsman import SecretManager
from sqlalchemy import create_engine
import snowflake.connector
import configparser


class SnowConn:

    _alchemy_engine = None
    _connection = None
    _raw_connection = None

    def __init__(self):
        self._alchemy_engine = None
        self._connection = None
        self._raw_connection = None

    @classmethod
    def connect(cls, db: str='public', schema: str='public',
                autocommit: bool=True):
        """
        Creates the SQLAlchemy engine object but does NOT create any
        connections to the database yet as that is done lazily.

        :param db: the database name
        :param schema: the schema name
        :return: None
        """
        conn = SnowConn()
        conn._connect(db, schema, autocommit=autocommit)
        return conn

    @classmethod
    def credsman_connect(cls, credsman_name: str, db: str='public',
                         schema: str='public', autocommit: bool=True,
                         *args, **kwargs):
        """
        Fetch credentials from credsman and use it to create the SQLAlchemy
        engine instance that will be used for future connections. Note that
        the context in which the process that is calling this method executes
        in must be authenticated to read the AWS Secret Manager secret with
        the provided name.

        :param credsman_name: the named of the AWS Secrets Manager secret
        :param db: the database name
        :param schema: the schema name
        :param args: forwarded to credsman
        :param kwargs: forwarded to credsman
        :return:
        """
        conn = SnowConn()
        conn._credsman_connect(credsman_name, db, schema,
                               autocommit=autocommit, *args, **kwargs)
        return conn

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

    def _connect(self, db: str='public', schema: str='public',
                 autocommit: bool=True):
        home = os.path.expanduser("~")
        snowsql_config = f'{home}/.snowsql/config'

        if not os.path.exists(snowsql_config):
            raise RuntimeError(
                f'No snowsql config found in {snowsql_config}. '
                f'Please install snowsql and add in your snowflake '
                f'login credentials to the config file.'
            )

        config = configparser.ConfigParser()
        # locally stored module that contains uname / password
        config.read(snowsql_config)
        creds = {
            'USERNAME': config['connections']['username'],
            'ACCOUNT': config['connections']['accountname'],
            'PASSWORD': config['connections']['password'],
            'ROLE': config['connections']['rolename']
        }
        self._create_engine(creds, db, schema, autocommit=autocommit)

    def _credsman_connect(self, credsman_name: str, db: str='public',
                          schema: str='public', autocommit: bool=True,
                          *args, **kwargs):
        sm = SecretManager(*args, **kwargs)
        creds = sm.get_secret(credsman_name)
        self._create_engine(creds, db, schema, autocommit=autocommit)

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
        return pd.read_sql_query(sql, self._alchemy_engine)

    def write_df(self, df, table: str, if_exists: str='replace',
                 index: bool=False, **kwargs):
        """
        Writes a dataframe to the specified table. Note that you must be
        connected in the correct context for this to be able to work as you
        cannot specify the fully namespaced version of the table.

        :param df: pandas dataframe to write to the table
        :param table: the name of ONLY the table
        :param if_exists: forwarded on to DataFrame.to_sql
        :param index: forwarded on to DataFrame.to_sql
        :param kwargs: forwarded on to DataFrame.to_sql
        :return: None
        """
        df.to_sql(table, con=self._alchemy_engine,
                  if_exists=if_exists, index=index, chunksize=5000, **kwargs)

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

    def _create_engine(self, creds: dict, db: str, schema: str,
                       autocommit: bool=True):

        username = creds['USERNAME']
        password = creds['PASSWORD']
        account = creds['ACCOUNT']
        role = creds['ROLE']
        if '.' not in account:
            print(
                'You may need to configure your account name to include the '
                f'region. For example: {account}.eu-west-1')
        autocommit_portion = ''
        if autocommit:
            autocommit_portion = '&autocommit=true'
        connection_string = (
            f'snowflake://{username}:{password}@{account}/{db}?role={role}&'
            f'schema={schema}{autocommit_portion}'
        )
        conn = create_engine(connection_string)
        self._alchemy_engine = conn
        self._connection = self._alchemy_engine.connect()
        self._raw_connection = self._connection.connection.connection
