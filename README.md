# SnowConn

This repository is a wrapper around the [snowflake SQLAlchemy](https://docs.snowflake.net/manuals/user-guide/sqlalchemy.html)
library. It manages the creation of connections and provides a few convenience functions that should be good enough
to cover most use cases yet be flexible enough to allow additional wrappers to be written around to serve more specific
use cases for different teams. 

## Installation

To install latest version released to pypi with pip:

```bash
pip install snowconn
```

To install the latest version directly from the repo:

```bash
pip install 'git+ssh://git@github.com/Daltix/SnowConn.git@master#egg=snowconn'
```

## Connection

Everything is implemented in a single `SnowConn` class. To import it is always the same:

```py
from snowconn import SnowConn
```

### (1) Connection using your own personal creds

Install [snowsql](https://docs.snowflake.net/manuals/user-guide/snowsql-install-config.html)
and then configure the `~/.snowsql/config` as per the instructions. You can test that it is correctly installed
by then executing `snowsql` from the command line. 

*WARNING* Be sure to configure your account name like the following:

```
accountname = eq94734.eu-west-1
```

If you don't include the `eu-west-1` part, it will hang for about a minute and then give you a permission denied.

Now that you are able to execute `snowsql` to successfully connect, you are ready to use the `SnowConn.connect` function:

```py
conn = SnowConn.connect()
```
That's it you are connected! You can connect to a specific schema / database with the following:

```py
conn = SnowConn.connect('daltix_prod', 'public')
```

### (2) Connection using aws secrets manager

You need to have boto3 installed which you can do so with the following:

```
pip install boto3
```

Now you must satisfy the folloing requirements:

1. Have a secret stored in an accessable aws account
1. The secret must have the following keys:
    - `USERNAME`
    - `PASSWORD`
    - `ACCOUNT`
    - `ROLE`

For this example, we will assume the `price_plotter` is the secret manager that we will be using. 

Now that you know the name of the secret, you MUST be sure that the context in which it is running has access to read
that secret. Once this is done, you can now execute the following code:

```py
conn = SnowConn.credsman_connect('price_plotter')
```

And you are connected! You can also pass the database and schema along

```py
conn = SnowConn.credsman_connect('price_plotter', 'daltix_prod', 'public')
```

An example of a policy that gives access to the `price_plotter` looks like this:

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "secretsmanager:GetResourcePolicy",
                "secretsmanager:GetSecretValue",
                "secretsmanager:DescribeSecret",
                "secretsmanager:ListSecretVersionIds"
            ],
            "Resource": "arn:aws:secretsmanager:eu-west-1:<your-account-number>:secret:price_plotter-AdcNpp"
        }
    ]
}
```

And an example of this in a serverless.yml looks like this:

```
iamRoleStatements:
    - Effect: Allow
      Action:
        - secretsmanager:DescribeSecret
        - secretsmanager:List*
      Resource:
        - "*"
    - Effect: Allow
      Action:
        - secretsmanager:*
      Resource:
        - { Fn::Sub: "arn:aws:secretsmanager:${AWS::Region}:${AWS::AccountId}:secret:price_plotter-??????" }
```

## API

Now that you're connected, there are a few low-level functions that you can use to programatically interact with
the snowflake tables that you have access to.

The rest of these examples assume that you have used one of the above methods to connect and have access to the
`daltix_prod.public.price` table.

### execute_simple

The exc_simple function is used for when you have a single statement to execute and the result set can fit into memory. It
takes a single argument which a string of the SQL statement that you with to execute. Take the following for example:

```py
>>> conn.execute_simple('select * from price limit 1;')
[{'DALTIX_ID': '0d3c30353035a6ab5747237a1f2600bbf5ddd27401372c5effe0f2790a88ad56', 'SHOP': 'ahed', 'COUNTRY': 'de', 'PRODUCT_ID': '616846.0', 'LOCATION': 'base', 'PRICE': 37.99, 'PROMO_PRICE': None, 'PRICE_STD': None, 'PROMO_PRICE_STD': None, 'UNIT': None, 'UNIT_STD': None, 'IS_MAIN': True, 'VENDOR': None, 'VENDOR_STD': None, 'DOWNLOADED_ON': datetime.datetime(2018, 11, 18, 0, 0, 1), 'DOWNLOADED_ON_LOCAL': datetime.datetime(2018, 11, 18, 1, 0, 1), 'DOWNLOADED_ON_DATE': datetime.date(2018, 11, 18), 'IS_LATEST_PRICE': False}]
```

### execute_string

If you have multiple sql statements in a single string that you want to execute or the resultset is larger than
will fit into memory, this is the function that you want to use. It returns a list of cursors that are a result
of each of the statements that are contained in the string. See [here](https://docs.snowflake.net/manuals/user-guide/python-connector-api.html#execute_string) for the full documentation.

```py
>>> conn.execute_string('create temporary table price_small as (select * from price limit 1); select * from price_small;')
[<snowflake.connector.cursor.SnowflakeCursor object at 0x10f537898>, <snowflake.connector.cursor.SnowflakeCursor object at 0x10f52c588>]
```

### execute_file

If you have the contents of an sql file that you want to execute, you can use this function. For example:

```bash
echo "select * from price limit 1;" > query.sql
```

```py
>>> conn.execute_file('query.sql')
>>> [<snowflake.connector.cursor.SnowflakeCursor object at 0x1188d6390>]
```
This also returns a list of cursors the same as `execute_string` does. In fact, this function is nothing more than a very
simple wrapper around `execute_string`.

### read_df

Use this function to read the results of a query into a dataframe. Note that pandas is NOT a dependency of this repo so
if you want to use it you must satisfy this dependency yourself.

It takes one sql string as an argument and returns a dataframe.

```bash
>>> conn.read_df('select daltix_id, downloaded_on, price from price limit 5;')
                                        daltix_id       downloaded_on  price
0  0d3c30353035a6ab5747237a1f2600bbf5ddd27401372c 2018-11-18 00:00:01  37.99
1  f5be8a5da3bde2da6a63fcad4e5c30823027324092234c 2018-11-18 00:00:02   9.99
2  f5be8a5da3bde2da6a63fcad4e5c30823027324092234c 2018-11-18 00:00:02   0.40
3  807e2a7706b8c515264fa55bed3891d5685ac5ee0148f0 2018-11-18 00:00:04   3.70
4  1e56339f99dc866cd4b87679aa686556a5ad2398d00c95 2018-11-18 00:00:06   3.76
>>> 
```

### write_df

Use this to write a dataframe to Snowflake. This is a very thin wrapper around the pandas [DataFrame.to_sql()](https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.to_sql.html) function.

Unfortunately, it doesn't play nice with dictionaries and arrays so the use cases are quite limited. Hopefully
we will improve upon this in the future.

### get_current_role

Returns the current role.

### close

Use this to cleanly close all connections that have ever been associated with this instance of SnowConn. If you don't
use this your process will hang for a while without saying anything before it actually exits.

## Accessing the connection objects directly

These functions are mostly wrappers around 2 connection libraries:

- [The snowflake python connector](https://docs.snowflake.net/manuals/user-guide/python-connector-api.html)
- [The snowflake SQLAlchemy library](https://docs.snowflake.net/manuals/user-guide/sqlalchemy.html)

Should you need to use either of these yourself, you can ask for the connections yourself with the following
functions:

### get_raw_connection

This will return the instance of a snowflake connector which is documented [here](https://docs.snowflake.net/manuals/user-guide/python-connector-api.html#connect). It is a good choice if you have very simple needs and for some reason none
of the functions in the rest of this repo are serving your needs.

### get_alchemy_engine

This is the result of [create_engine()](https://docs.snowflake.net/manuals/user-guide/sqlalchemy.html#connection-parameters)
which was called during `connect()` or `credsman_connect()`. It does not represent an active connection to the database
but rather acts as a factory for connections.

This is useful for using the most commonly abstracted things in other libraries such as dashboards, pandas, etc. 
However, like SQLAlchemy in general, despite being very widely supported and feature-complete, it is not the simplest 
API so it should probably not be your first choice unless you know exactly that you need it.

### get_connection

This returns the result of the creating the sqlalchemy engine and then calling `connect()` on it. Unlike the result
of `get_alchemy_engine` this represents an active connection to Snowflake and this has a session associated with it.

You can see the object documentation [here](https://docs.snowflake.net/manuals/user-guide/sqlalchemy.html#parameters-and-behavior)

## Known issues

There is a bug with `snowflake-connector` which causes some connections to Snowflake to not close properly in certain circumstances. This can cause timeout errors.

You can handle this in two ways: the first is to wrap usage of the connection in a `try/finally` block to ensure the connection is explicitly closed, like this:
```
from snowconn import SnowConn
conn = SnowConn.credsman_connect(...) # or SnowConn.connect()
try:
    result = execute_string(query) # or result = read_df(query), etc
finally:
    conn.close()
```

The second way is to use SnowConn with the `with` syntax, as follows:
```
with SnowConn.connect() as conn:
    conn.read_df(...)
```