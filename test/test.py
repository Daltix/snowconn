from snowconn import SnowConn

conn = SnowConn.connect(
    db='util_db',
    schema='public',
)

print(conn.execute_simple('select current_warehouse();'))

conn.close()

conn = SnowConn.connect(
    db='util_db',
    schema='public',
    warehouse='TEST_WH'
)

print(conn.execute_simple('select current_warehouse();'))

conn.close()
