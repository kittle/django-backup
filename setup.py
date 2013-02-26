from distutils.core import setup
from setuptools import find_packages

setup(
    name='django-backup',
    version     = '1.0.1',
    author        = 'Chris Cohoat',
    author_email = 'chris.cohoat@gmail.com',
    url            = 'https://github.com/chriscohoat/django-backup',
    description    = 'A backup script for the Django admin',
    packages=find_packages(),
    include_package_data=True,
    install_requires = ['boto>=2.8.0',],
)
