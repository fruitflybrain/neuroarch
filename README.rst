.. -*- rst -*-

Neuroarch
=========

Package Description
-------------------

`Fruit Fly Brain Observatory <https://www.fruitflybrain.org>`_ |
`GitHub Repository <https://github.com/fruitflybrain/neuroarch>`_ |
`Online Documentation <https://neuroarch.readthedocs.io>`_

Neuroarch is a database for codifying knowledge about fruit fly brain circuits.
It defines a hierarchical data model for representation of experimentally
obtained biological data, neural circuit models, and the relationships between
them within a single graph database. Neuroarch provides an Object Graph Mapping
(OGM) that utilizes this data model to enable powerful queries against stored
biological and model data without having to write complex low-level queries.

This package provides a Python API for

1) implementing the data model described in the Neurokernel Request for Comments #5:

`Lev E. Givon, Aurel A. Lazar, and Nikul H. Ukani. Neuroarch: A Graph db for Querying and Executing Fruit Fly Brain Circuits. Neurokernel Request for Comments, 2015. doi: 10.5281/zenodo.44225 <https://doi.org/10.5281/zenodo.44225>`_.

2) loading data into the NeuroArch database (see `this repository <https://github.com/flybrainlab/datasets>`_ for some examples of loading publically available datasets).

3) querying the database in a composable fashion.

Installation
------------
The NeuroArch database is implemented using `OrientDB <https://www.orientdb.org/>`_. Please install a GA Community Edition of OrientDB. Currently supported OrientDB versions are 2.2.x and 3.0.x.

This package can be installed using pip

.. code-block::

   pip install git+https://github.com/fruitflybrain/pyorient neuroarch

The pyorient fork enables working with OrientDB 3.0.x.

Authors & Acknowledgements
--------------------------
See the included AUTHORS file for more information.

License
-------
This software is licensed under the `BSD License
<http://www.opensource.org/licenses/bsd-license.php>`_.
See the included LICENSE file for more information.
