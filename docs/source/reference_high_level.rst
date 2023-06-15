.. -*- rst -*-

High Level API
==============

.. currentmodule:: neuroarch.na

NeuroArch Class
---------------

The :class:`NeuroArch` class provides basic operations such as connecting to the OrientDB database, loading database, and queries.

.. autoclass:: NeuroArch

Methods for Connection
^^^^^^^^^^^^^^^^^^^^^^

.. autosummary::
   :toctree: generated/

   NeuroArch.connect
   NeuroArch.reconnect

Methods for Queries
^^^^^^^^^^^^^^^^^^^

.. autosummary::
   :toctree: generated/

   NeuroArch.sql_query

Methods for Loading Biological Entities
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autosummary::
   :toctree: generated/

   NeuroArch.query_neuron
   NeuroArch.query_celltype
   NeuroArch.query_synapses
   NeuroArch.query_neuropil
   NeuroArch.query_subregion
   NeuroArch.add_Species
   NeuroArch.add_DataSource
   NeuroArch.add_Subsystem
   NeuroArch.add_Neuropil
   NeuroArch.add_Subregion
   NeuroArch.add_Tract
   NeuroArch.add_Circuit
   NeuroArch.add_Neuron
   NeuroArch.add_neurotransmitter
   NeuroArch.add_morphology
   NeuroArch.add_neuron_arborization
   NeuroArch.add_Synapse
   NeuroArch.add_synapse_arborization
   NeuroArch.add_InferredSynapse
   NeuroArch.remove_Neurons
   NeuroArch.remove_Synapses
   NeuroArch.remove_Synapses_between
   NeuroArch.update_Neuron
   NeuroArch.update_Synapse
   NeuroArch.create_model_from_circuit
   NeuroArch.add_ExecutableCircuit
   NeuroArch.add_CircuitDiagram
   NeuroArch.add_LPU
   NeuroArch.add_NeuronModel
   NeuroArch.add_Port
   NeuroArch.add_SynapseModel

Auxilliary Methods
^^^^^^^^^^^^^^^^^^

.. autosummary::
   :toctree: generated/

   NeuroArch.get
   NeuroArch.set
   NeuroArch.exists
   NeuroArch.find
   NeuroArch.find_objs
   NeuroArch.link
   NeuroArch.link_with_batch
   NeuroArch.export_tags
   NeuroArch.import_tags
   NeuroArch.remove_tag
   NeuroArch.available_DataSources

High Level Query Functions
--------------------------

.. autofunction:: outgoing_synapses
.. autofunction:: incoming_synapses
.. autofunction:: get_data

Auxilliary Functions
--------------------

.. autofunction:: load_swc


Exceptions and Warnings
-----------------------

.. autosummary::
   :toctree: generated/
   
   NotWriteableError
   DuplicateNodeError
   NodeAlreadyExistError
   RecordNotFoundError
   NodeAlreadyExistWarning
   DuplicateNodeWarning
   DataSourceError
