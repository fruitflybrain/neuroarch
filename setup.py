#!/usr/bin/env python

import sys, os
from glob import glob

# Install setuptools if it isn't available:
try:
    import setuptools
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()

from setuptools import find_packages
from setuptools import setup

NAME =               'neuroarch'
VERSION =            '0.4.3'
AUTHOR =             'Lev Givon, Nikul Ukani, Yiyin Zhou'
AUTHOR_EMAIL =       'lev@columbia.edu, nikul@ee.columbia.edu, yiyin@ee.columbia.edu'
URL =                'https://github.com/fruitflybrain/neuroarch/'
MAINTAINER =         'Yiyin Zhou'
MAINTAINER_EMAIL =   'yiyin@ee.columbia.edu'
DESCRIPTION =        'A graph-based platform for representing Drosophila brain architectures'
LONG_DESCRIPTION =   DESCRIPTION
DOWNLOAD_URL =       URL
LICENSE =            'BSD'
CLASSIFIERS = [
    'Development Status :: 3 - Alpha',
    'Intended Audience :: Developers',
    'Intended Audience :: Science/Research',
    'License :: OSI Approved :: BSD License',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Topic :: Database :: Front-Ends',
    'Topic :: Scientific/Engineering',
    'Topic :: Software Development']
NAMESPACE_PACKAGES = ['neuroarch']
PACKAGES =           find_packages()

docs_extras = [
    'sphinx >= 1.3',
    'sphinx_rtd_theme >= 0.1.6',
]

if __name__ == "__main__":
    if os.path.exists('MANIFEST'):
        os.remove('MANIFEST')

    setup(
        name = NAME,
        version = VERSION,
        author = AUTHOR,
        author_email = AUTHOR_EMAIL,
        license = LICENSE,
        classifiers = CLASSIFIERS,
        description = DESCRIPTION,
        long_description = LONG_DESCRIPTION,
        url = URL,
        maintainer = MAINTAINER,
        maintainer_email = MAINTAINER_EMAIL,
        namespace_packages = NAMESPACE_PACKAGES,
        packages = PACKAGES,
        include_package_data = True,
        install_requires = [
            'daff',
            'networkx>=2.4',
            'numpy',
            'pandas',
            'path.py',
            'pyorient @ https://github.com/fruitflybrain/pyorient/tarball/v1.6.1#egg=pyorient-1.6.1',
            'pyorient_native @ https://github.com/fruitflybrain/pyorient_native/tarball/master#egg=pyorient_native-1.2.3',
            'deepdiff',
            'tqdm'],
        extra_requires = {
            'doc': docs_extras,
        }
    )
