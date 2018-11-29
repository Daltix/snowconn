from setuptools import setup

setup(
    name='snowconn',
    version='2.2.0',
    description='Python utilities for connection to Daltix snowflake data '
                'source',
    # url='TODO',
    author='Sam Hopkins',
    author_email='sam@daredata.engineering',
    packages=['snowconn'],
    install_requires=[
        'snowflake-sqlalchemy',
    ]
)
