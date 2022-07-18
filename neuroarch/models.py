#!/usr/bin/env python

"""
Neuroarch data models.
"""

# Copyright (c) 2015, Lev Givon
# All rights reserved.
# Distributed under the terms of the BSD license:
# http://www.opensource.org/licenses/bsd-license

import inspect

from pyorient.ogm.declarative import declarative_node, \
    declarative_relationship
from pyorient.ogm.property import Property, EmbeddedMap, EmbeddedSet, String, EmbeddedList, Boolean, Integer, Double

#import neuroarch.conv.pd as pd
#import neuroarch.conv.nx as nx
import neuroarch.utils as utils

from .query import QueryWrapper, QueryString

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
    
    @property
    def props(self):
        """
        Returns record properties that have been retrieved.
        """
        return {k: getattr(self, k) for k, v in inspect.getmembers(type(self)) if isinstance(v, Property)}

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

class Node(NeuroarchNodeMixin, declarative_node()):
    element_type = 'Node'
    element_plural = 'Nodes'

class Relationship(declarative_relationship()):
    label = 'Relationship'

class MetaData(Node):
    element_type = 'MetaData'
    element_plural = 'MetaDatas'
    created_by = EmbeddedMap(linked_to=String(), nullable=False, unique=False)
    version = String(nullable=False, unique=False, indexed=True)
    # NeuroArch_version = String(nullable=False, unique=False, indexed=True)
    # min_NeuroArch_version_supported = String(nullable=False, unique=False, indexed=True)
    # OrientDB_version = String(nullable=False, unique=False, indexed=True)
    maintainer = EmbeddedMap(linked_to=String(), nullable=False, unique=False)
    
class Species(Node):
    element_type = 'Species'
    element_plural = 'Species'
    name = String(nullable=False, unique=False, indexed=True)
    stage = String(nullable=False, unique=False, indexed=True)
    sex = String(nullable=False, unique=False, indexed=True)
    synonyms = EmbeddedList(linked_to=String(), nullable=True, unique=False, indexed=True)

# Biological data nodes:
class BioNode(Node):
    element_type = 'BioNode'
    element_plural = 'BioNodes'

class DataSource(BioNode):
    element_type = 'DataSource'
    element_plural = 'DataSources'
    name = String(nullable=False, unique=False, indexed=True)
    version = String(nullable=False, unique=False, indexed=True)
    description = String(nullable=True, unique=False, indexed=False)
    url = String(nullable=True, unique=False, indexed=False)

class Subsystem(BioNode):
    element_type = 'Subsystem'
    element_plural = 'Subsystems'
    name = String(nullable=False, unique=False, indexed=True)
    synonyms = EmbeddedList(linked_to=String(), nullable=True, unique=False, indexed=True)
    version = String(nullable=True, unique=False, indexed=True)

class Neuropil(BioNode):
    element_type = 'Neuropil'
    element_plural = 'Neuropils'
    name = String(nullable=False, unique=False, indexed=True)
    synonyms = EmbeddedList(linked_to=String(), nullable=True, unique=False, indexed=True)
    version = String(nullable=True, unique=False, indexed=True)

class Subregion(BioNode):
    element_type = 'Subregion'
    element_plural = 'Subregions'
    name = String(nullable=False, unique=False, indexed=True)
    synonyms = EmbeddedList(linked_to=String(), nullable=True, unique=False, indexed=True)

class Tract(BioNode):
    element_type = 'Tract'
    element_plural = 'Tracts'
    name = String(nullable=False, unique=False, indexed=True)
    version = String(nullable=True, unique=False, indexed=True)
    neuropils = EmbeddedSet(linked_to=String(), nullable=True, unique=False, indexed=True)

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

class NeuronAndFragment(BioNode):
    element_type = 'NeuronAndFragment'
    element_plural = 'NeuronAndFragments'

class Neuron(NeuronAndFragment):
    element_type = 'Neuron'
    element_plural = 'Neurons'
    name = String(nullable=False, unique=False, indexed=True)
    locality = Boolean(nullable=True, unique=False, indexed=True)
    label = String(nullable=True, unique=False, indexed=True)
    uname = String(nullable=True, unique=False, indexed=True)
    synonyms = EmbeddedList(linked_to=String(), nullable=True, unique=False, indexed=True)
    referenceId = String(nullable=True, unique=False, indexed=True)
    info = EmbeddedMap(nullable=True, unique=False, indexed=True)

class NeuronFragment(NeuronAndFragment):
    element_type = 'NeuronFragment'
    element_plural = 'NeuronFragments'
    name = String(nullable=False, unique=False, indexed=True)
    label = String(nullable=True, unique=False, indexed=True)
    uname = String(nullable=True, unique=False, indexed=True)
    referenceId = String(nullable=True, unique=False, indexed=True)
    info = EmbeddedMap(nullable=True, unique=False, indexed=True)

class NeuronTerminal(BioNode):
    element_type = 'NeuronTerminal'
    element_plural = 'NeuronTerminals'
    name = String(nullable=False, unique=False, indexed=True)
    synonyms = EmbeddedList(linked_to=String(), nullable=True, unique=False, indexed=True)

class Synapse(BioNode):
    element_type = 'Synapse'
    element_plural = 'Synapses'
    name = String(nullable=False, unique=False, indexed=True)
    N = Integer(nullable=True, unique=False, indexed=True)
    NHP = Integer(nullable = True, unique=False, indexed = True)
    uname = String(nullable=True, unique=False, indexed=True)

class InferredSynapse(BioNode):
    element_type = 'InferredSynapse'
    element_plural = 'InferredSynapses'
    name = String(nullable=False, unique=False, indexed=True)
    N = Integer(nullable=True, unique=False, indexed=True)
    uname = String(nullable=True, unique=False, indexed=True)

class GapJunction(BioNode):
    element_type = 'GapJunction'
    element_plural = 'GapJunctions'
    name = String(nullable=False, unique=False, indexed=True)

class PhotoreceptorCell(Neuron):
    element_type = 'PhotoreceptorCell'
    element_plural = 'PhotoreceptorCells'
    #name = String(nullable=False, unique=False, indexed=True)

class ArborizationData(BioNode):
    element_type = 'ArborizationData'
    element_plural = 'ArborizationDatas'
    dendrites = EmbeddedMap(linked_to=Integer(), nullable=True, unique=False, indexed=True)
    axons = EmbeddedMap(linked_to=Integer(), nullable=True, unique=False, indexed=True)
    synapses = EmbeddedMap(linked_to=Integer(), nullable=True, unique=False, indexed=True)
    name = String(nullable=True, unique=False, indexed=True)
    uname = String(nullable=True, unique=False, indexed=True)
    type = String(nullable=True, unique=False, indexed=True)

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
    x = EmbeddedList(linked_to=Double(), nullable=True, unique=False, indexed=False)
    y = EmbeddedList(linked_to=Double(), nullable=True, unique=False, indexed=False)
    z = EmbeddedList(linked_to=Double(), nullable=True, unique=False, indexed=False)
    r = EmbeddedList(linked_to=Double(), nullable=True, unique=False, indexed=False)
    parent = EmbeddedList(linked_to=Integer(), nullable=True, unique=False, indexed=False)
    identifier = EmbeddedList(linked_to=Integer(), nullable=True, unique=False, indexed=False)
    sample = EmbeddedList(linked_to=Integer(), nullable=True, unique=False, indexed=False)
    confidence = EmbeddedList(linked_to=Double(), nullable=True, unique=False, indexed=False)
    vertices = EmbeddedList(linked_to=Double(), nullable=True, unique=False, indexed=False)
    faces = EmbeddedList(linked_to=Integer(), nullable=True, unique=False, indexed=False)

class NeurotransmitterData(BioNode):
    element_type = 'NeurotransmitterData'
    element_plural = 'NeurotransmitterDatas'
    name = String(nullable=False, unique=False, indexed=True)
    Transmitters = EmbeddedList(linked_to=String(), nullable=False, unique=False, indexed=True)

# Circuit design nodes:
class DesignNode(Node):
    element_type = 'DesignNode'
    element_plural = 'DesignNodes'

class Version(DesignNode):
    element_type = 'Version'
    element_plural = 'Versions'
    version = String(nullable=False, unique=False, indexed=True)

class ExecutableCircuit(DesignNode):
    element_type = 'ExecutableCircuit'
    element_plural = 'ExecutableCircuits'
    name = String(nullable=False, unique=False, indexed=True)
    version = String(nullable=True, unique=False, indexed=True)

class CircuitDiagram(DesignNode):
    element_type = 'CircuitDiagram'
    element_plural = 'CircuitDiagrams'
    name = String(nullable=False, unique=False, indexed=True)
    version = String(nullable=True, unique=False, indexed=True)
    diagrams = EmbeddedMap(linked_to=String(), nullable=False, unique=False, indexed=False)
    submodules = EmbeddedMap(linked_to=String(), nullable=True, unique=False, indexed=False)

class LPU(DesignNode):
    element_type = 'LPU'
    element_plural = 'LPUs'
    name = String(nullable=False, unique=False, indexed=True)
    version = String(nullable=True, unique=False, indexed=True)

class Interface(DesignNode):
    element_type = 'Interface'
    element_plural = 'Interfaces'
    name = String(nullable=False, unique=False, indexed=True)

class Pattern(DesignNode):
    element_type = 'Pattern'
    element_plural = 'Patterns'
    name = String(nullable=False, unique=False, indexed=True)
    version = String(nullable=True, unique=False, indexed=True)

class Port(DesignNode):
    element_type = 'Port'
    element_plural = 'Ports'
    selector = String(nullable=True, unique=False, indexed=True)
    port_type = String(nullable=True, unique=False)
    port_io = String(nullable=True, unique=False)

class CircuitModel(DesignNode):
    element_type = 'CircuitModel'
    element_plural = 'CircuitModels'
    name = String(nullable=False, unique=False, indexed=True)

class OmmatidiumModel(CircuitModel):
    element_type = 'OmmatidiumModel'
    element_plural = 'OmmatidiumModels'
    #name = String(nullable=False, unique=False, indexed=True)

class CartridgeModel(CircuitModel):
    element_type = 'CartridgeModel'
    element_plural = 'CartridgeModels'
    #name = String(nullable=False, unique=False, indexed=True)

class CRModel(CircuitModel):
    element_type = 'CRModel'
    element_plural = 'CRModels'
    #name = String(nullable=False, unique=False, indexed=True)

class ColumnModel(CircuitModel):
    element_type = 'ColumnModel'
    element_plural = 'ColumnModels'
    #name = String(nullable=False, unique=False, indexed=True)

class NeuronModel(DesignNode):
    element_type = 'NeuronModel'
    element_plural = 'NeuronModels'
    name = String(nullable=False, unique=False, indexed=True)
    uname = String(nullable=True, unique=False, indexed=True)
    params = EmbeddedMap(nullable=True, unique=False, indexed=True)
    states = EmbeddedMap(nullable=True, unique=False, indexed=True)

class MembraneModel(NeuronModel):
    element_type = 'MembraneModel'
    element_plural = 'MembraneModels'

class AxonHillockModel(NeuronModel):
    element_type = 'AxonHillockModel'
    element_plural = 'AxonHillockModels'

class DendriteModel(DesignNode):
    element_type = 'DendriteModel'
    element_plural = 'DendriteModels'
    name = String(nullable=False, unique=False, indexed=True)
    uname = String(nullable=True, unique=False, indexed=True)
    params = EmbeddedMap(nullable=True, unique=False, indexed=True)
    states = EmbeddedMap(nullable=True, unique=False, indexed=True)

class PhotoreceptorModel(MembraneModel):
    element_type = 'PhotoreceptorModel'
    element_plural = 'PhotoreceptorModels'
    spiking = False
    # num_microvilli = Integer(nullable=True, unique=False, indexed=True)

# Added for AdaptiveNarx
class NarxAdaptive(MembraneModel):
    element_type = 'NarxAdaptive'
    element_plural = 'NarxAdaptives'
    #name = String(nullable=False, unique=False, indexed=True)

class MorrisLecar(MembraneModel):
    element_type = 'MorrisLecar'
    element_plural = 'MorrisLecars'
    spiking = False
    # V1 = Double(nullable=True, unique=False, indexed=True)
    # V2 = Double(nullable=True, unique=False, indexed=True)
    # V3 = Double(nullable=True, unique=False, indexed=True)
    # V4 = Double(nullable=True, unique=False, indexed=True)
    # phi = Double(nullable=True, unique=False, indexed=True)
    # offset = Double(nullable=True, unique=False, indexed=True)
    # V_L = Double(nullable=True, unique=False, indexed=True)
    # V_Ca = Double(nullable=True, unique=False, indexed=True)
    # V_K = Double(nullable=True, unique=False, indexed=True)
    # g_L = Double(nullable=True, unique=False, indexed=True)
    # g_Ca = Double(nullable=True, unique=False, indexed=True)
    # g_K = Double(nullable=True, unique=False, indexed=True)
    # initV = Double(nullable=True, unique=False, indexed=True)
    # initn = Double(nullable=True, unique=False, indexed=True)

class HodgkinHuxley(AxonHillockModel):
    element_type = 'HodgkinHuxley'
    element_plural = 'HodgkinHuxleys'
    spiking = True
    # initV = Double(nullable=True, unique=False, indexed=True)
    # initn =  Double(nullable=True, unique=False, indexed=True)
    # initm = Double(nullable=True, unique=False, indexed=True)
    # inith = Double(nullable=True, unique=False, indexed=True)
    # g_K = Double(nullable=True, unique=False, indexed=True)
    # g_Na = Double(nullable=True, unique=False, indexed=True)
    # g_l =  Double(nullable=True, unique=False, indexed=True)

class HodgkinHuxleyFull(AxonHillockModel):
    element_type = 'HodgkinHuxleyFull'
    element_plural = 'HodgkinHuxleyFulls'
    spiking = True
    # initV = Double(nullable=True, unique=False, indexed=True)
    # initn =  Double(nullable=True, unique=False, indexed=True)
    # initm = Double(nullable=True, unique=False, indexed=True)
    # inith = Double(nullable=True, unique=False, indexed=True)
    # g_K = Double(nullable=True, unique=False, indexed=True)
    # g_Na = Double(nullable=True, unique=False, indexed=True)
    # g_L =  Double(nullable=True, unique=False, indexed=True)
    # E_K =Double(nullable=True, unique=False, indexed=True)
    # E_Na = Double(nullable=True, unique=False, indexed=True)
    # E_L = Double(nullable=True, unique=False, indexed=True)

class LeakyIAF(AxonHillockModel):
    element_type = 'LeakyIAF'
    element_plural = 'LeakyIAFs'
    spiking = True
    # initV = Double(nullable=True, unique=False, indexed=True)
    # threshold = Double(nullable=True, unique=False, indexed=True)
    # reset_potential = Double(nullable=True, unique=False, indexed=True)
    # capacitance = Double(nullable=True, unique=False, indexed=True)
    # resting_potential = Double(nullable=True, unique=False, indexed=True)
    # resistance = Double(nullable=True, unique=False, indexed=True)

class LeakyIAFwithRefractoryPeriod(AxonHillockModel):
    element_type = 'LeakyIAFwithRefractoryPeriod'
    element_plural = 'LeakyIAFwithRefractoryPeriods'
    spiking = True
    # initV = Double(nullable=True, unique=False, indexed=True)
    # threshold = Double(nullable=True, unique=False, indexed=True)
    # reset_potential = Double(nullable=True, unique=False, indexed=True)
    # capacitance = Double(nullable=True, unique=False, indexed=True)
    # resting_potential = Double(nullable=True, unique=False, indexed=True)
    # time_constant = Double(nullable=True, unique=False, indexed=True)
    # refractory_period = Double(nullable=True, unique=False, indexed=True)
    # bias_current = Double(nullable=True, unique=False, indexed=True)

class BufferVoltage(MembraneModel):
    element_type = 'BufferVoltage'
    element_plural = 'BufferVoltages'

class BufferPhoton(MembraneModel):
    element_type = 'BufferPhoton'
    element_plural = 'Bufferphotons'

class Aggregator(DendriteModel):
    element_type = 'Aggregator'
    element_plural = 'Aggregators'

class SynapseModel(DesignNode):
    element_type = 'SynapseModel'
    element_plural = 'SynapseModels'
    name = String(nullable=False, unique=False, indexed=True)
    uname = String(nullable=True, unique=False, indexed=True)
    params = EmbeddedMap(nullable=True, unique=False, indexed=True)
    states = EmbeddedMap(nullable=True, unique=False, indexed=True)
    # reverse = Double(nullable=True, unique=False, indexed=True)
    # gmax = Double(nullable=True, unique=False, indexed=True)

class AlphaSynapse(SynapseModel):
    element_type = 'AlphaSynapse'
    element_plural = 'AlphaSynapses'
    link_pre = 'spike_state'
    link_post = None
    # ar = Double(nullable=True, unique=False, indexed=True)
    # ad = Double(nullable=True, unique=False, indexed=True)

class PowerGPotGPot(SynapseModel):
    element_type = 'PowerGPotGPot'
    element_plural = 'PowerGPotGPots'
    link_pre = 'gpot'
    link_post = None

class SynapseAMPA(SynapseModel):
    element_type = 'SynapseAMPA'
    element_plural = 'SynapseAMPAs'
    link_pre = 'spike_state'
    link_post = None
    # st = Double(nullable=True, unique=False, indexed=True)

class SynapseGABA(SynapseModel):
    element_type = 'SynapseGABA'
    element_plural = 'SynapseGABAs'
    link_pre = 'spike_state'
    link_post = None
    # st = Double(nullable=True, unique=False, indexed=True)

class SynapseNMDA(SynapseModel):
    element_type = 'SynapseNMDA'
    element_plural = 'SynapseNMDAs'
    link_pre = 'spike_state'
    link_post = 'V'
    # st = Double(nullable=True, unique=False, indexed=True)
    # Mg = Double(nullable=True, unique=False, indexed=True)

class SigmoidSynapse(SynapseModel):
    element_type = 'SigmoidSynapse'
    element_plural = 'SigmoidSynapses'
    link_pre = 'V'
    link_post = None
    # threshold = Double(nullable=True, unique=False, indexed=True)
    # scale = Double(nullable=True, unique=False, indexed=True)
    # slope = Double(nullable=True, unique=False, indexed=True)

class Owns(Relationship):
    label = 'Owns'

class SendsTo(Relationship):
    label = 'SendsTo'
    variable = String(nullable=True, unique=False, indexed=True)

class HasData(Relationship):
    label = 'HasData'

class Requires(Relationship):
    label = 'Requires'

class ArborizesIn(Relationship):
    label = 'ArborizesIn'
    kind = EmbeddedSet(linked_to=String(), nullable=False, unique=False, indexed=True)
    N_axons = Integer(nullable=True, unique=False, indexed=True)
    N_dendrites = Integer(nullable=True, unique=False, indexed=True)

class QueryResult(Node):
    element_type = 'QueryResult'
    element_plural = 'QueryResults'
    tag =  String(nullable=False, unique=True, indexed=True)
    keywords = EmbeddedList(linked_to=String(), nullable=True, unique=False, indexed=True)
    FFBOdata = EmbeddedMap(linked_to=String(), nullable=True, unique=False, indexed=True)

class QueryOwns(Relationship):
    label = 'QueryOwns'

class Models(Relationship):
    label = 'Models'
    version = String(nullable=True, unique=False, indexed=True)

class HasQueryResults(Relationship):
    label = 'HasQueryResults'

Data_Types = ['GeneticData', 'NeurotransmitterData', 'MorphologyData', 'ArborizationData']
