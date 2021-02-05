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
VERSION =            '0.3.0'
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
            'pyorient',
            'deepdiff'],
        dependency_links=['https://github.com/fruitflybrain/pyorient/archive/v1.5.6.tar.gz#egg=pyorient-1.5.6'],
    )
