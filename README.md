# SnowflakeConnect

## Installation

To install with pip execute:

```
pip install -U git+ssh://git@github.com/Daltix/SnowflakeConnect.git@master#egg=snconn
```

## Connection

There are two types of connections that this repo allows. 

1) Connections using your own login (the one that you would use to log into the web interface)
2) Using the credsman which is for deployed access

You should use method (1) if you are running on your own machine and use method (2) if it is a deployed
automation making the connection. Don't EVER use your own credentials to do a deployment. It's not good practice
AND snowflake is very aggressive with their disabling of users due to suspicious behavior. If your credentials
are being used from multiple IP addresses, it is very likely that your user will be blocked.

### (1) Connection using your own personal creds

Install [snowsql](https://docs.snowflake.net/manuals/user-guide/snowsql-install-config.html)
and then configure the `~/.snowsql/config` as per the instructions. You can test that it is correctly installed
by then executing `snowsql` from the command line. 

*WARNING* Be sure to configure your account name like the following:

```
accountname = ***REMOVED***.eu-west-1
```

If you don't include the `eu-west-1` part, it will hang for about a minute and then give you a permission denied.

Now that you are able to execute `snowsql` to successfully connect, you are ready to use the `connect` function:

```py
from snconn import connect

connect()
```
That's it you are connected! You can connect to a specific schema / database with the following:

```py
connect('pricing_prod', 'public')
```

### (2) Connection using credsman

You need to have credsman installed which you can do so with the following:

```
pip install -U git+ssh://git@github.com/Daltix/product-team-tooling.git@develop#egg=credsman&subdirectory=credsman
```

Now you must know the name of the secret that you want to use. You can find the list of them in the athena account [here](https://eu-west-1.console.aws.amazon.com/secretsmanager/home?region=eu-west-1#/listSecrets) For this example, we will assume the `price_plotter` is the secret manager that we will be using. 

Now that you know the name of the secret, you MUST be sure that the context in which it is running has access to read
that secret. Once this is done, you can now execute the following code:

```py
from snconn import credsman_connect

credsman_connect('price_plotter')
```

And you are connected! You can also pass the database and schema along

```py
from snconn import credsman_connect
credsman_connect('price_plotter', 'pricing_prod', 'public')
```

## API

Now that you're connected, there are a few low-level functions that you can use to programatically interact with
the snowflake tables that you have access to.

The rest of these examples assume that you have used one of the above methods to connect and have access to the
`pricing_prod.public.price` table.

### exc_simple

The exc_simple function is used for when you have a single statement to execute and the result set can fit into memory. It
takes a single argument which a string of the SQL statement that you with to execute. Take the following for example:

```py
>>> from snconn import exc_simple
>>> exc_simple('select * from price limit 1;')
[{'DALTIX_ID': '0d3c30353035a6ab5747237a1f2600bbf5ddd27401372c5effe0f2790a88ad56', 'SHOP': 'zooplus', 'COUNTRY': 'be', 'PRODUCT_ID': '616846.0', 'LOCATION': 'base', 'PRICE': 37.99, 'PROMO_PRICE': None, 'PRICE_STD': None, 'PROMO_PRICE_STD': None, 'UNIT': None, 'UNIT_STD': None, 'IS_MAIN': True, 'TAX_INCLUDED': True, 'CURRENCY': 'eur', 'VENDOR': None, 'VENDOR_STD': None, 'DOWNLOADED_ON': datetime.datetime(2018, 11, 18, 0, 0, 1), 'DOWNLOADED_ON_LOCAL': datetime.datetime(2018, 11, 18, 1, 0, 1), 'DOWNLOADED_ON_DATE': datetime.date(2018, 11, 18), 'IS_LATEST_PRICE': False}]
```

### exc_string

If you have multiple sql statements in a single string that you want to execute or the resultset is larger than
will fit into memory ,this is the function that you want to use. It returns a list of cursors that are a result
of each of the statements that are contained in the string. See [here](https://docs.snowflake.net/manuals/user-guide/python-connector-api.html#execute_string) for the full documentation.

```py
>>> from snconn import exc_string
>>> exc_string('create temporary table price_small as (select * from price limit 1); select * from price_small;')
[<snowflake.connector.cursor.SnowflakeCursor object at 0x10f537898>, <snowflake.connector.cursor.SnowflakeCursor object at 0x10f52c588>]
```

### exc_file

If you have the contents of an sql file that you want to execute, you can use this function. For example:

```bash
echo "select * from price limit 1;" > query.sql
```

```py
>>> from snconn import exc_simple
>>> exc_file('query.sql')
```
This also returns a list of cursors the same as `exc_string` does. In fact, this function is nothing more than a very
simple wrapper around `exc_string`.

### read_df

Use this function to read the results of a query into a dataframe. Note that pandas is NOT a dependency of this repo so
if you want to use it you must satisfy this dependency yourself.

It takes one sql string as an argument and returns a dataframe.

```bash
>>> from snconn import read_df
>>> read_df('select daltix_id, downloaded_on, price from price limit 5;')
                                        daltix_id       downloaded_on  price
0  0d3c30353035a6ab5747237a1f2600bbf5ddd27401372c 2018-11-18 00:00:01  37.99
1  f5be8a5da3bde2da6a63fcad4e5c30823027324092234c 2018-11-18 00:00:02   9.99
2  f5be8a5da3bde2da6a63fcad4e5c30823027324092234c 2018-11-18 00:00:02   0.40
3  807e2a7706b8c515264fa55bed3891d5685ac5ee0148f0 2018-11-18 00:00:04   3.70
4  1e56339f99dc866cd4b87679aa686556a5ad2398d00c95 2018-11-18 00:00:06   3.76
>>> 
```

## Using the connection objects

These functions are mostly wrappers around 2 connection libraries:

- [The snowflake python connector](https://docs.snowflake.net/manuals/user-guide/python-connector-api.html)
- [The snowflake SQLAlchemy library](https://docs.snowflake.net/manuals/user-guide/sqlalchemy.html)

Should you need to use either of these yourself, you can ask for the connections yourself with the following
functions:

### get_connection

This will return the instance of a snowflake connector which is documented [here](https://docs.snowflake.net/manuals/user-guide/python-connector-api.html#connect). It is a good choice if you have very simple needs and for some reason none
of the functions in the rest of this repo are serving your needs.

### get_alchemy_engine

This returns the result of the creating the sqlalchemy engine. This is useful for integrations with other tools
such as dashboards, pandas, etc. However, like SQLAlchemy in general, despite being very widely supported and
feature-complete, it is not the simplest API so it should probably not be your first choice unless you 
know exactly that you need it.

You can see the object documentation [here](https://docs.snowflake.net/manuals/user-guide/sqlalchemy.html#parameters-and-behavior)


