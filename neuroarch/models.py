#!/usr/bin/env python

"""
Neuroarch data models.
"""

# Copyright (c) 2015, Lev Givon
# All rights reserved.
# Distributed under the terms of the BSD license:
# http://www.opensource.org/licenses/bsd-license

import numbers

from pyorient.ogm.declarative import declarative_node, \
    declarative_relationship
from pyorient.ogm.property import EmbeddedMap, EmbeddedSet, String, EmbeddedList, Boolean, Integer
import pyorient.otypes

#import neuroarch.conv.pd as pd
#import neuroarch.conv.nx as nx
import neuroarch.utils as utils

from query import QueryWrapper, QueryString

def create_efficiently(graph, registry):
    """
    Efficiently create classes in OrientDB database.

    Runs `graph.create_all(registry)` if any of the classes in the registry are
    absent; otherwise, runs the faster method `graph.include(registry)`.

    Parameters
    ----------
    graph : pyorient.ogm.graph.Graph
        OrientDB graph.
    registry : collections.OrderedDict
        Registry of pyorient OGM classes for which to create OrientDB classes.
    """

    cluster_ids = utils.get_cluster_ids(graph.client)
    if not set(registry.keys()).issubset(cluster_ids.keys()):
        graph.create_all(registry)
    else:
        graph.include(registry)

class NeuroarchNodeMixin(object):
    def gremlin(self, script, args=None, namespace=None):
        """
        Return results of Gremlin query as mapped objects.
        """

        return self._graph.gremlin(script, args, namespace)

    def gremlin_raw(self, script, args=None, namespace=None):
        """
        Return results of Gremlin query as pyorient records.
        """

        graph = self._graph
        script_body = graph.scripts.script_body(script, args, namespace)
        if script_body:
            response = graph.client.gremlin(script_body)
        else:
            response = graph.client.gremlin(script)
        return response

    def update(self, **props):
        """
        Update record in database with specified properties.
        """

        self._graph.client.command('update %s merge %s where @rid = %s' % \
                (self.element_type, props, self._id))

    def get_props(self):
        """
        Retrieve record properties from database.
        """

        props = self._graph.client.query('select * from %s' % self._id)[0]
        return utils.orientrecord_to_dict(props)

    # XXX add option to these methods to control whether both nodes and edges
    # are returned:
    def owns(self, levels=1, **kwargs):
        """
        Retrieve nodes/edges owned by this node at a specific level of the ownership hierarchy.
        """

        q = QueryWrapper(self._graph, QueryString('select from %s' % self._id, 'sql'))
        return q.owns(levels=levels, **kwargs)

    def traverse_owns(self, **kwargs):
        """
        Traverse nodes/edges owned by this node at a specific level of the ownership hierarchy.
        """

        q = QueryWrapper(self._graph, QueryString('select from %s' % self._id, 'sql'))
        return q.traverse_owns(**kwargs)

    def owned_by(self, levels=1, **kwargs):
        """
        Retrieve nodes/edges that own this node at a specific level of the ownership hierarchy.
        """

        q = QueryWrapper(self._graph, QueryString('select from %s' % self._id, 'sql'))
        return q.owned_by(levels=levels, **kwargs)

    def traverse_owned_by(self, **kwargs):
        """
        Traverse nodes/edges that own this node at a specific level of the ownership hierarchy.
        """

        q = QueryWrapper(self._graph, QueryString('select from %s' % self._id, 'sql'))
        return q.traverse_owned_by(**kwargs)

class Node(declarative_node(), NeuroarchNodeMixin):
    element_type = 'Node'
    element_plural = 'Nodes'

class Relationship(declarative_relationship()):
    label = 'Relationship'

class Species(Node):
    element_type = 'Species'
    element_plural = 'Species'
    name = String(nullable=False, unique=False, indexed=True)

# Biological data nodes:
class BioNode(Node):
    element_type = 'BioNode'
    element_plural = 'BioNodes'

class DataSource(BioNode):
    element_type = 'DataSource'
    element_plural = 'DataSources'
    name = String(nullable=False, unique=False, indexed=True)
    description = String(nullable=True, unique=False, indexed=False)

class Neuropil(BioNode):
    element_type = 'Neuropil'
    element_plural = 'Neuropils'
    name = String(nullable=False, unique=False, indexed=True)
    synonyms = EmbeddedList(nullable=True, unique=False, indexed=True)

class Tract(BioNode):
    element_type = 'Tract'
    element_plural = 'Tracts'
    name = String(nullable=False, unique=False, indexed=True)

class BioSensor(BioNode):
    element_type = 'BioSensor'
    element_plural = 'BioSensors'
    name = String(nullable=False, unique=False, indexed=True)

class Circuit(BioNode):
    element_type = 'Circuit'
    element_plural = 'Circuits'
    name = String(nullable=False, unique=False, indexed=True)

class Ommatidium(Circuit):
    element_type = 'Ommatidium'
    element_plural = 'Ommatidia'
    name = String(nullable=False, unique=False, indexed=True)

class Cartridge(Circuit):
    element_type = 'Cartridge'
    element_plural = 'Cartridges'
    name = String(nullable=False, unique=False, indexed=True)

class Glomerulus(Circuit):
    element_type = 'Glomerulus'
    element_plural = 'Glomeruli'
    name = String(nullable=False, unique=False, indexed=True)

class Column(Circuit):
    element_type = 'Column'
    element_plural = 'Columns'
    name = String(nullable=False, unique=False, indexed=True)

class Neuron(BioNode):
    element_type = 'Neuron'
    element_plural = 'Neurons'
    name = String(nullable=False, unique=False, indexed=True)
    locality = Boolean(nullable=True, unique=False, indexed=True)
    label = String(nullable=True, unique=False, indexed=True)
    uname = String(nullable=True, unique=False, indexed=True)
    synonyms = EmbeddedList(nullable=True, unique=False, indexed=True)

class Synapse(BioNode):
    element_type = 'Synapse'
    element_plural = 'Synapses'
    name = String(nullable=False, unique=False, indexed=True)
    N = Integer(nullable=True, unique=False, indexed=True)
    uname = String(nullable=True, unique=False, indexed=True)

class InferredSynapse(BioNode):
    element_type = 'InferredSynapse'
    element_plural = 'InferredSynapses'
    name = String(nullable=False, unique=False, indexed=True)
    N = Integer(nullable=True, unique=False, indexed=True)

class GapJunction(BioNode):
    element_type = 'GapJunction'
    element_plural = 'GapJunctions'
    name = String(nullable=False, unique=False, indexed=True)

class PhotoreceptorCell(Neuron):
    element_type = 'PhotoreceptorCell'
    element_plural = 'PhotoreceptorCells'
    name = String(nullable=False, unique=False, indexed=True)

class ArborizationData(BioNode):
    element_type = 'ArborizationData'
    element_plural = 'ArborizationDatas'
    neuropil = String(nullable=True, unique=False, indexed=True)
    neurite = EmbeddedSet(nullable=True, unique=False, indexed=True)
    regions = EmbeddedSet(nullable=True, unique=False, indexed=True)
    dendrites = EmbeddedMap(nullable=True, unique=False, indexed=True)
    axons = EmbeddedMap(nullable=True, unique=False, indexed=True)
    name = String(nullable=True, unique=False, indexed=True)
    uname = String(nullable=True, unique=False, indexed=True)

class GeneticData(BioNode):
    element_type = 'GeneticData'
    element_plural = 'GeneticDatas'
    name = String(nullable=False, unique=False, indexed=True)

class MorphologyData(BioNode):
    element_type = 'MorphologyData'
    element_plural = 'MorphologyDatas'
    name = String(nullable=False, unique=False, indexed=True)
    morph_type = String(nullable=True, unique=False, indexed=True)
    uname = String(nullable=True, unique=False, indexed=True)
    x = EmbeddedList(nullable=True, unique=False, indexed=False)
    y = EmbeddedList(nullable=True, unique=False, indexed=False)
    z = EmbeddedList(nullable=True, unique=False, indexed=False)
    r = EmbeddedList(nullable=True, unique=False, indexed=False)
    parent = EmbeddedList(nullable=True, unique=False, indexed=False)
    identifier = EmbeddedList(nullable=True, unique=False, indexed=False)
    sample = EmbeddedList(nullable=True, unique=False, indexed=False)

class NeurotransmitterData(BioNode):
    element_type = 'NeurotransmitterData'
    element_plural = 'NeurotransmitterDatas'
    name = String(nullable=False, unique=False, indexed=True)
    Transmitters = EmbeddedList(nullable=False, unique=False, indexed=True)

# Circuit design nodes:
class DesignNode(Node):
    element_type = 'DesignNode'
    element_plural = 'DesignNodes'

class Version(DesignNode):
    element_type = 'Version'
    element_plural = 'Versions'
    version = String(nullable=False, unique=False, indexed=True)

class LPU(DesignNode):
    element_type = 'LPU'
    element_plural = 'LPUs'
    name = String(nullable=False, unique=False, indexed=True)

class Interface(DesignNode):
    element_type = 'Interface'
    element_plural = 'Interfaces'
    name = String(nullable=False, unique=False, indexed=True)

class Pattern(DesignNode):
    element_type = 'Pattern'
    element_plural = 'Patterns'
    name = String(nullable=False, unique=False, indexed=True)

class Port(DesignNode):
    element_type = 'Port'
    element_plural = 'Ports'
    selector = String(nullable=False, unique=False, indexed=True)
    port_type = String(nullable=False, unique=False)
    port_io = String(nullable=False, unique=False)

class CircuitModel(DesignNode):
    element_type = 'CircuitModel'
    element_plural = 'CircuitModels'
    name = String(nullable=False, unique=False, indexed=True)

class OmmatidiumModel(CircuitModel):
    element_type = 'OmmatidiumModel'
    element_plural = 'OmmatidiumModels'
    name = String(nullable=False, unique=False, indexed=True)

class CartridgeModel(CircuitModel):
    element_type = 'CartridgeModel'
    element_plural = 'CartridgeModels'
    name = String(nullable=False, unique=False, indexed=True)

class CRModel(CircuitModel):
    element_type = 'CRModel'
    element_plural = 'CRModels'
    name = String(nullable=False, unique=False, indexed=True)

class ColumnModel(CircuitModel):
    element_type = 'ColumnModel'
    element_plural = 'ColumnModels'
    name = String(nullable=False, unique=False, indexed=True)

class NeuronModel(DesignNode):
    element_type = 'NeuronModel'
    element_plural = 'NeuronModels'
    name = String(nullable=False, unique=False, indexed=True)

class MembraneModel(DesignNode):
    element_type = 'MembraneModel'
    element_plural = 'MembraneModels'
    name = String(nullable=False, unique=False, indexed=True)

class AxonHillockModel(DesignNode):
    element_type = 'AxonHillockModel'
    element_plural = 'AxonHillockModels'
    name = String(nullable=False, unique=False, indexed=True)

class DendriteModel(DesignNode):
    element_type = 'DendriteModel'
    element_plural = 'DendriteModels'
    name = String(nullable=False, unique=False, indexed=True)

class PhotoreceptorModel(MembraneModel):
    element_type = 'PhotoreceptorModel'
    element_plural = 'PhotoreceptorModels'
    name = String(nullable=False, unique=False, indexed=True)

# Added for AdaptiveNarx
class NarxAdaptive(MembraneModel):
    element_type = 'NarxAdaptive'
    element_plural = 'NarxAdaptives'
    name = String(nullable=False, unique=False, indexed=True)

class MorrisLecar(MembraneModel):
    element_type = 'MorrisLecar'
    element_plural = 'MorrisLecars'
    name = String(nullable=False, unique=False, indexed=True)

class LeakyIAF(AxonHillockModel):
    element_type = 'LeakyIAF'
    element_plural = 'LeakyIAFs'
    name = String(nullable=False, unique=False, indexed=True)

class BufferVoltage(MembraneModel):
    element_type = 'BufferVoltage'
    element_plural = 'BufferVoltages'
    name = String(nullable=False, unique=False, indexed=True)

class BufferPhoton(MembraneModel):
    element_type = 'BufferPhoton'
    element_plural = 'Bufferphotons'
    name = String(nullable=False, unique=False, indexed=True)

class Aggregator(DendriteModel):
    element_type = 'Aggregator'
    element_plural = 'Aggregators'
    name = String(nullable=False, unique=False, indexed=True)

class SynapseModel(DesignNode):
    element_type = 'SynapseModel'
    element_plural = 'SynapseModels'
    name = String(nullable=False, unique=False, indexed=True)

class AlphaSynapse(SynapseModel):
    element_type = 'AlphaSynapse'
    element_plural = 'AlphaSynapses'
    name = String(nullable=False, unique=False, indexed=True)

class PowerGPotGPot(SynapseModel):
    element_type = 'PowerGPotGPot'
    element_plural = 'PowerGPotGPots'
    name = String(nullable=False, unique=False, indexed=True)

class Owns(Relationship):
    label = 'Owns'

class SendsTo(Relationship):
    label = 'SendsTo'

class HasData(Relationship):
    label = 'HasData'

class Requires(Relationship):
    label = 'Requires'

class QueryResult(Node):
    element_type = 'QueryResult'
    element_plural = 'QueryResults'
    tag =  String(nullable=False, unique=True, indexed=True)
    keywords = EmbeddedList(linked_to=String(), nullable=True, unique=False, indexed=True)
    FFBOdata = EmbeddedMap(nullable=True, unique=False, indexed=True)

class QueryOwns(Relationship):
    label = 'QueryOwns'

class Models(Relationship):
    label = 'Models'

class HasQueryResults(Relationship):
    label = 'HasQueryResults'
