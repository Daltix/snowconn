import os
import json
from sqlalchemy import create_engine
import snowflake.connector
import configparser

try:
    import boto3
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

    @classmethod
    def connect(cls, db: str = 'public', schema: str = 'public',
                autocommit: bool = True, role=None, warehouse=None):
        """
        Creates an engine and connection to the specified snowflake 
        db using your snowsql credentials.

        :param db: the database name
        :param schema: the schema name
        :param autocommit: check sqlalchemy for autocommit behavior
        :param role: override the default role for this user
        :return: None
        """
        conn = SnowConn()
        conn._connect(
            db, schema, autocommit=autocommit, role=role, warehouse=warehouse)
        return conn

    @classmethod
    def credsman_connect(cls, credsman_name: str, db: str = 'public',
                         schema: str = 'public', autocommit: bool = True,
                         role: str = None, region_name="eu-west-1",
                         aws_access_key_id=None, aws_secret_access_key=None,
                         warehouse=None):
        """
        Creates an engine and connection to the specified snowflake db . Note that
        the context in which the process that is calling this method executes
        in must be authenticated to read the AWS Secret Manager secret with
        the provided name.

        Assumes that you are using the AWS secrets manager stored as a json
        object that will parsing json.loads and used like the following:

        username = creds['USERNAME']
        password = creds['PASSWORD']
        account = creds['ACCOUNT']
        role = creds['ROLE']

        :param credsman_name: the named of the AWS Secrets Manager secret
        :param db: the database name
        :param schema: the schema name
        :param autocommit: check sqlalchemy for autocommit behavior
        :param role: override the default role for this user
        :param region_name: forwarded to boto3
        :param aws_access_key_id: forwarded to boto3
        :param aws_secret_access_key: forwarded to boto3
        :return:
        """
        conn = SnowConn()
        conn._credsman_connect(
            credsman_name, db, schema, autocommit=autocommit, role=role,
            region_name=region_name, aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key, warehouse=warehouse
        )
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

    def _connect(self, db: str = 'public', schema: str = 'public',
                 autocommit: bool = True, role: str = None, warehouse=None):
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
        self._create_engine(
            creds, db, schema, autocommit=autocommit, role=role,
            warehouse=warehouse)

    def _credsman_connect(self, credsman_name: str, db: str = 'public',
                          schema: str = 'public', autocommit: bool = True,
                          role=None, region_name="eu-west-1",
                          aws_access_key_id=None, aws_secret_access_key=None,
                          warehouse=None):
        if aws_access_key_id and aws_secret_access_key:
            # Start a session with boto, ensure to pass our credentials.
            session = boto3.session.Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key
            )
        else:
            # Start a session using the default boto3 behavior
            session = boto3.session.Session()

        client = session.client(
            service_name='secretsmanager',
            region_name=region_name,
            endpoint_url=f'https://secretsmanager.{region_name}.amazonaws.com'
        )
        get_secret_value_response = client.get_secret_value(
            SecretId=credsman_name
        )
        creds = json.loads(get_secret_value_response['SecretString'])
        self._create_engine(
            creds, db, schema, autocommit=autocommit, role=role,
            warehouse=warehouse)

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

    def write_df(self, df, table: str, if_exists: str = 'replace',
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
            sql = pd.io.sql.get_schema(
                df, name=table, con=self._connection
            ).replace("CREATE TABLE", "CREATE OR REPLACE TEMPORARY TABLE")
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
        # there are some cases (even with autocommit=True) that calling close
        # on the connection will invoke a rollback of some portion of things
        # done during the connection. In order to keep the behavior as
        # consistent as possible, we will avoid calling close() on the
        # connection and instead let the engine dispose of the connection
        # which is robust but has the unfortunate side effect of causing the
        # process to hang for a bit before it is closed. that means that all
        # users of snowflake_conn have a few second delay when the script exits

        # Update: I've been experimenting with my own version of this with the
        # following line uncommented and have not noticed any issues. I'm
        # thinking that the issue described above isn't a problem any more.
        # Furthermote, bringing back this line of code stops the connection
        # from hanging in some cases where an exception is thrown during
        # or right after a query.
        self._connection.close()
        self._alchemy_engine.dispose()

    def get_current_role(self):
        results = self.execute_simple('show roles;')
        return [r for r in results if r['is_current'] == 'Y'][0]['name']

    def _create_engine(self, creds: dict, db: str, schema: str,
                       autocommit: bool = True, role: str = None,
                       warehouse=None):
        if role is not None:
            creds['ROLE'] = role
        username = creds['USERNAME']
        password = creds['PASSWORD']
        account = creds['ACCOUNT']
        role = creds['ROLE']
        if '.' not in account:
            print(
                'You may need to configure your account name to include the '
                f'region. For example: {account}.eu-west-1')
        autocommit_portion = ''
        warehouse_portion = ''
        if autocommit:
            autocommit_portion = '&autocommit=true'
        if warehouse:
            warehouse_portion = f'&warehouse={warehouse}'
        connection_string = (
            f'snowflake://{username}:{password}@{account}/{db}?role={role}&'
            f'schema={schema}{autocommit_portion}{warehouse_portion}'
        )
        engine = create_engine(connection_string)
        self._alchemy_engine = engine
        self._connection = self._alchemy_engine.connect()
        self._raw_connection = self._connection.connection.connection
