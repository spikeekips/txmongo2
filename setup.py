# -*- coding: utf-8 -*-

from setuptools import setup


setup(
    name='txmongo2',
    description='another mongodb driver for twisted.',
    long_description='''
mongodb driver for twisted, forked from
`https://github.com/oubiwann/txmongo.git`. still need for testing.
        ''',
    author='Spike^ekipS',
    author_email='https://github.com/spikeekips',
    url='https://github.com/spikeekips',
    version='0.2a',
    license='License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
    classifiers=(
            'Development Status :: 3 - Alpha',
            'Operating System :: MacOS :: MacOS X',
            'Operating System :: POSIX :: Linux',
            'Operating System :: POSIX',
            'Programming Language :: Python',
            'Framework :: Twisted',
        ),
    zip_safe=False,
    install_requires=(
            'Twisted',
            'pymongo',
            'nose',
        ),
    packages=('txmongo2', 'txmongo2._gridfs', ),
    package_dir={'': 'src', },
    test_suite="nose.collector",
)


