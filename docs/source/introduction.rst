.. -*- rst -*-

Introduction
============

Neuroarch is a database for codifying knowledge about fruit fly brain circuits. It defines a hierarchical data model for representation of experimentally obtained biological data, neural circuit models, and the relationships between them within a single graph database. Neuroarch provides an Object Graph Mapping (OGM) that utilizes this data model to enable powerful queries against stored biological and model data without having to write complex low-level queries.

This package provides a Python API for

.. highlight:: none

1. implementing the data model described in the `Neurokernel Request for Comments \#5 <https://doi.org/10.5281/zenodo.44225>`_: ::

    Lev E. Givon, Aurel A. Lazar, and Nikul H. Ukani. 
    Neuroarch: A Graph db for Querying and Executing Fruit Fly Brain Circuits.
    Neurokernel Request for Comments, 2015.
    doi: 10.5281/zenodo.44225.

2. loading data into the NeuroArch database (see this repository for some examples of loading publically available datasets).
3. querying the database in a composable fashion.

.. highlight:: default
