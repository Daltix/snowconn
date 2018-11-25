from setuptools import setup

setup(
    name='SnowflakeConnect',
    version='0.0.1',
    description='Python utilities for connection to Daltix data source',
    # url='TODO',
    author='Sam Hopkins',
    author_email='sam@daredata.engineering',
    packages=['snconn'],
    install_requires=[
        'snowflake-sqlalchemy',
    ],
    dependency_links=[
        'git+ssh://git@github.com/Daltix/product-team-tooling.git@develop#egg=credsman&subdirectory=credsman'
    ]
)
