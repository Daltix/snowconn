from setuptools import setup

setup(
    name='snowconn',
    version='3.5.1',
    description='Python utilities for connection to Daltix snowflake data '
                'source',
    # url='TODO',
    author='Sam Hopkins',
    author_email='sam@daredata.engineering',
    packages=['snowconn'],
    install_requires=[
        'wheel==0.32.3',
        'snowflake-sqlalchemy==1.1.4',
    ]
)
