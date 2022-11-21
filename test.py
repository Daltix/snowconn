"""
Simplest sanity check test file
"""
import os
from snowconn import SnowConn

env = os.environ

# test connection with local creds file
try:
    with SnowConn.connect() as conn:
        print(conn.get_current_role())
        print(conn.execute_simple('select current_warehouse();'))
except Exception as e:
    print(e)


# test connection with credentials
try:
    with SnowConn.connect_credentials(
        account=env['SNOWFLAKE_ACCOUNT'],
        username=env['SNOWFLAKE_USERNAME'],
        password=env['SNOWFLAKE_PASSWORD'],
        role=env['SNOWFLAKE_ROLE'],
        warehouse=env['SNOWFLAKE_WAREHOUSE'],
    ) as conn:
        print(conn.get_current_role())
        print(conn.execute_simple('select current_warehouse();'))
except Exception as e:
    print(e)

try:
    with SnowConn.connect(
        methods=['credentials'],
        account=env['SNOWFLAKE_ACCOUNT'],
        username=env['SNOWFLAKE_USERNAME'],
        password=env['SNOWFLAKE_PASSWORD'],
        role=env['SNOWFLAKE_ROLE'],
        warehouse=env['SNOWFLAKE_WAREHOUSE'],
    ) as conn:
        print(conn.get_current_role())
        print(conn.execute_simple('select current_warehouse();'))
except Exception as e:
    print(e)
