import snowflake
from snowconn import SnowConn
import pandas as pd


conn = SnowConn.connect('daltix_prod', 'public')
print(conn.execute_simple('select count(*) from price;'))
print(conn.execute_simple('select count(*) from price;'))
assert conn.read_df('select count(*) from price;').shape[0] == 1
# print(conn.execute_file('query.sql'))
print(conn.get_current_role())
print(conn.execute_simple('use role daltix_prod_full_object;'))
print(conn.get_current_role())
conn.close()

conn2 = SnowConn.connect('daltix_prod', 'public', autocommit=False)
conn2.close()

conn3 = SnowConn.credsman_connect('daltix_etl_test', 'daltix_prod', 'public', autocommit=False)
conn3.close()

conn3 = SnowConn.credsman_connect('daltix_etl_test', 'daltix_prod', 'public')
conn3.close()

conn4 = SnowConn.connect('util_db', 'public')
conn4.execute_string("""
  create or replace table test_table (id int);
  insert into test_table values (
    (1)
  )
""")
conn4.close()

conn5 = SnowConn.connect('util_db', 'public')

df = pd.DataFrame({'hello': ['world']})
conn5.write_df(df, 'testme', temporary_table=True)
# this should print one table
print(conn5.execute_simple('select * from testme;'))
assert len(conn5.execute_simple('select * from testme;')) == 1
assert conn5.read_df('select * from testme;').shape[0] == 1

conn5.close()

conn6 = SnowConn.connect('util_db', 'public')
try:
    print(conn6.execute_simple('select * from testme;'))
    assert False, 'expected'
except snowflake.connector.errors.ProgrammingError:
    pass
conn6.close()

conn7 = SnowConn.connect('daltix_prod', 'public', role='daltix_prod_read')
assert conn7.get_current_role() == 'DALTIX_PROD_READ', conn7.get_current_role()
conn7.close()

conn8 = SnowConn.credsman_connect('daltix_etl_test', 'daltix_prod', 'public',
                                  role='public')
assert conn8.get_current_role() == 'PUBLIC', conn8.get_current_role()
conn8.close()

print('done')
