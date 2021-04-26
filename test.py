"""
Simplest sanity check test file
"""

from snowconn import SnowConn

conn = SnowConn.connect()
print(conn.get_current_role())
print(conn.execute_simple('select current_warehouse();'))
conn.close()
