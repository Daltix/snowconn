from setuptools import setup

setup(
    name='snconn',
    version='0.0.5',
    description='Python utilities for connection to Daltix snowflake data '
                'source',
    # url='TODO',
    author='Sam Hopkins',
    author_email='sam@daredata.engineering',
    packages=['snconn'],
    install_requires=[
        'snowflake-sqlalchemy',
    ]
)
