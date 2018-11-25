import os
import json

from credsman import SecretManager
from sqlalchemy import create_engine
import snowflake.connector

import configparser

stage = os.environ.get('STAGE', 'dev')
alchemy_engine = None
connection = None


def credsman_connect(credsman_name: str, db):
    sm = SecretManager()
    creds = sm.get_secret(credsman_name)
    _connect(creds, db)


def connect(db: str):
    home = os.path.expanduser("~")
    snowsql_config = f'{home}/.snowsql/config'

    if not os.path.exists(snowsql_config):
        raise RuntimeError(f'No snowsql config found in {snowsql_config}. '
                           f'Please install snowsql and add in your snowflake '
                           f'login credentials to the config file.')

    config = configparser.ConfigParser()
    # locally stored module that contains uname / password
    config.read(snowsql_config)
    creds = {
        'USERNAME': config['connections']['username'],
        'ACCOUNT': config['connections']['accountname'],
        'PASSWORD': config['connections']['password'],
        'ROLE': config['connections']['rolename']
    }
    _connect(creds, db)


def _connect(creds: dict, db):
    global connection, alchemy_engine

    username = creds['USERNAME']
    password = creds['PASSWORD']
    account = creds['ACCOUNT']
    role = creds['ROLE']
    if alchemy_engine is None:
        conn = create_engine(
            f'snowflake://{username}:{password}@{account}/{db}?role={role}'
        ).connect()
        alchemy_engine = conn
    if connection is None:
        conn = snowflake.connector.connect(
            user=username,
            password=password,
            account=account,
            role=role,
            database=db
        )
        connection = conn


def exc_simple(sql):

    types_to_parse = (5, 9, 10)
    try:
        cursor = connection.cursor(snowflake.connector.DictCursor)
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


def exc_string(sql):
    try:
        cursor_list = connection.execute_string(sql)
    except snowflake.connector.errors.ProgrammingError as e:
        print(sql)
        raise e
    return cursor_list


def exc_file(fname):
    with open(fname) as fh:
        sql = fh.read()
    exc_string(sql)


def read_df(sql):
    # if you want to use pandas, you'll have to install it yourself as it is
    # not a requirement of this package. It's just too heavy and if you need
    # pandas, you probably want control over it anyways
    try:
        import pandas as pd
    except ImportError as e:
        print('pandas not installed, cannot execute read_df')
        raise e
    return pd.read_sql_query(sql, alchemy_engine)

