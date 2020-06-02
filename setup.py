from setuptools import setup
from os import path


this_directory = path.abspath(path.dirname(__file__))
readme_path = path.join(this_directory, 'README.md')

with open(readme_path, encoding='utf-8') as fh:
    long_description = fh.read()


setup(
    name='snowconn',
    version='3.7.0',
    description='Python utilities for connection to the Snowflake data '
                'warehouse',
    url='https://github.com/Daltix/snowconn',
    author='Daltix NV',
    author_email='snowconn@daltix.com',
    packages=['snowconn'],
    install_requires=[
        'wheel==0.32.3',
        'snowflake-sqlalchemy==1.2.3',
    ],
    long_description=long_description,
    long_description_content_type='text/markdown',
)
