from setuptools import setup
from os import path


this_directory = path.abspath(path.dirname(__file__))
readme_path = path.join(this_directory, 'README.md')

with open(readme_path, encoding='utf-8') as fh:
    long_description = fh.read()


setup(
    name='snowconn',
    version='3.5.5',
    description='Python utilities for connection to the Snowflake data '
                'warehouse',
    url='https://github.com/Daltix/snowconn',
    author='Sam Hopkins',
    author_email='sam@daredata.engineering',
    packages=['snowconn'],
    install_requires=[
        'wheel==0.32.3',
        'snowflake-sqlalchemy==1.1.4',
    ],
    long_description=long_description,
    long_description_content_type='text/markdown',
)
