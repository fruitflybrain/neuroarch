.. -*- rst -*-

Installation
============

Prerequisites
-------------
NeuroArch requires

* Linux (other operating systems may work, but have not been tested);
* Python 3.7;
* `OrientDB <https://orientdb.org/download>`_ 2.2.* or 3.0.*

Installation
------------

Install released version via pip: ::

  python -m pip install git+https://github.com/fruitflybrain/pyorient neuroarch

The pyorient fork enables working with OrientDB 3.0.x.

Install from source: ::

  git clone https://github.com/fruitflybrain/neuroarch.git
  cd neuroarch
  python -m pip install .

Building the Documentation
--------------------------
To build NeuroArch's HTML documentation locally, you will need to install

* `sphinx <http://sphinx-doc.org>`_.
* `sphinx_rtd_theme <https://github.com/snide/sphinx_rtd_theme>`_.

Once these are installed, run the following: ::

  cd ~/neurokernel/docs
  make html
