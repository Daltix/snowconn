import os
import json
from credsman import SecretManager
from sqlalchemy import create_engine
import snowflake.connector
import configparser

alchemy_engine = None
connection = None


def credsman_connect(credsman_name: str, db: str='public',
                     schema: str='public', *args, **kwargs):
    snowconn = SnowConn()
    snowconn.credsman_connect(credsman_name, db, schema, *args, **kwargs)
    return snowconn


def connect(db: str='public', schema: str='public'):
    snowconn = SnowConn()
    snowconn.connect(db, schema)
    return snowconn


class SnowConn:

    _alchemy_engine = None
    _connection = None
    _raw_connection = None

    def __init__(self):
        self._alchemy_engine = None
        self._connection = None
        self._raw_connection = None

    def connect(self, db: str='public', schema: str='public'):
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
        self._connect(creds, db, schema)

    def credsman_connect(self, credsman_name: str, db: str='public',
                         schema: str='public', *args, **kwargs):
        sm = SecretManager(*args, **kwargs)
        creds = sm.get_secret(credsman_name)
        self._connect(creds, db, schema)

    def _connect(self, creds: dict, db, schema):
        global connection, alchemy_engine

        username = creds['USERNAME']
        password = creds['PASSWORD']
        account = creds['ACCOUNT']
        role = creds['ROLE']
        if '.' not in account:
            print(
                'You may need to configure your account name to include the '
                f'region. For example: {account}.eu-west-1')
        conn = create_engine(
            f'snowflake://{username}:{password}@{account}/{db}?role={role}&'
            f'schema={schema}'
        )
        self._alchemy_engine = conn

    def _create_connection_if_needed(self):
        if self._connection is None:
            self._connection = self._alchemy_engine.connect()

    def _create_raw_connection_if_needed(self):
        if self._raw_connection is None:
            self._raw_connection = self._alchemy_engine.raw_connection(
            ).connection

    def execute_simple(self, sql):
        self._create_raw_connection_if_needed()
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
                if key in to_parse else value
                for key, value in entry.items()
            }
            for entry in results
        ]

    def execute_string(self, sql):
        self._create_raw_connection_if_needed()
        try:
            cursor_list = self._raw_connection.execute_string(sql)
        except snowflake.connector.errors.ProgrammingError as e:
            print(sql)
            raise e
        return cursor_list

    def read_df(self, sql):
        # if you want to use pandas, you'll have to install it yourself as it is
        # not a requirement of this package. It's just too heavy and if you need
        # pandas, you probably want control over it anyways
        try:
            import pandas as pd
        except ImportError as e:
            print('pandas not installed, cannot execute read_df')
            raise e
        return pd.read_sql_query(sql, self._alchemy_engine)

    def write_df(self, df, table: str, if_exists: str='replace', index=False,
                 **kwargs):
        df.to_sql(table, con=self._alchemy_engine,
                  if_exists=if_exists, index=index, chunksize=5000, **kwargs)

    def close(self):
        if self._connection is not None:
            self._connection.close()
        if self._raw_connection is not None:
            self._raw_connection.close()
        self._alchemy_engine.dispose()
