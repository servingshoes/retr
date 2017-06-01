#!/usr/bin/env python

from setuptools import setup

CLASSIFIERS = [
    'Development Status :: 4 - Beta',
    'Environment :: Console',
    'Environment :: Web Environment',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
    'Operating System :: OS Independent',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Topic :: Software Development :: Libraries :: Python Modules',
    'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
]

setup(
    author='Yury Pukhalsky',
    author_email='aikipooh@gmail.com',
    name='retr',
    version='1.2',
    description='Parallelised proxy-switching engine for scraping. Based on requests or aiohttp (alpha). Includes scrapy middleware.',
    long_description=open('README.md').read(),
    url='https://github.com/aikipooh/retr',
    # license='BSD License',
    # platforms=['OS Independent'],
    classifiers=CLASSIFIERS,
    packages=['retr', 'samples'],
    #include_package_data=True,
    #zip_safe=False,
    install_requires=[
        'requests>=2.0', # For the legacy (threaded) version
        'aiohttp>=2.0.6' # For the new version
    ]
)
