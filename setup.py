from setuptools import setup
from os import path


this_directory = path.abspath(path.dirname(__file__))
readme_path = path.join(this_directory, 'README.md')

with open(readme_path, encoding='utf-8') as fh:
    long_description = fh.read()

setup(
    name='snowconn',
    version='3.10.0',
    description='Python utilities for connection to the Snowflake data '
                'warehouse',
    url='https://github.com/Daltix/snowconn',
    author='Daltix NV',
    author_email='snowconn@daltix.com',
    packages=['snowconn'],
    install_requires=[
        'six',
        'snowflake-connector-python==2.8.3',
        'snowflake-sqlalchemy>=1.3, <1.4',
        'sqlalchemy<=1.4.41',
    ],
    long_description=long_description,
    long_description_content_type='text/markdown',

    extras_require={
        "pandas": [
            "snowflake-connector-python[pandas]==2.7.9",
        ],
        "storage": [
            "snowflake-connector-python[secure-local-storage]==2.7.9",
            'wheel',
        ],
        "all": [
            "boto3",
            "snowflake-connector-python[secure-local-storage,pandas]==2.7.9",
        ]
    }
)
