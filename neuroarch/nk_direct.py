#!/usr/bin/env python

"""
"""

import copy

import networkx as nx

import neuroarch.query as query

def to_nk(graph, node_query):
    q = neuroarch.query.QueryWrapper(graph, node_query)

    # Extract nodes and edges as networkx.MultiDiGraph:
    g = q.get_as('nx')

    # Convert to Neurokernel data structures:
    return na_to_nk(g)

def na_to_nk(g):
    """
    Convert graph extracted from Neuroarch to Neurokernel-compatible data structures.

    Parameters
    ----------
    g : networkx.MultiDiGraph

    Returns
    ------
    n_dict : dict of dict of list
        Each key of `n_dict` is the name of a neuron model; the values
        are dicts that map each attribute name to a list that contains the
        attribute values for each neuron.
    s_dict : dict of dict of list
        Each key of `s_dict` is the name of a synapse model; the values are
        dicts that map each attribute name to a list that contains the
        attribute values for each each neuron.
    """

    assert isinstance(g, nx.MultiDiGraph)
    nodes = g.nodes()

    # Lexicographically sort neurons by RID so that consecutive neurons
    # of the same type are the same in Neurokernel:
    neurons = [(rid, g.node[rid]) for rid in nodes \
               if g.node[rid]['class'] == 'neuron']
    neurons.sort()

    # Output neuron data structure:
    n_dict = {}

    # Maps RIDs to consecutive indices in model param lists:
    rid_id_map = {}

    # Maps RIDs of nodes of a particular model to consecutive indices:
    rid_model_id_map = {}
    for id, (rid, neu) in enumerate(neurons):
        neu = copy.deepcopy(neu)
        model = neu['model']

        # Ensure a selector is defined for output ports:
        if 'public' not in neu:
            neu['public'] = False
        if 'selector' not in neu:
            neu['selector'] = ''

        # If a neuron model has not appeared before, add it to n_dict:
        if model not in n_dict:
            n_dict[model] = {k: [] for k in neu.keys() + ['id']}
            rid_model_id_map[model] = {}

        # Add neuron data to n_dict subdictionary:
        for key in neu.keys():
            n_dict[model][key].append(neu[key])

        # Neurons of the same model should have the same attributes:
        assert(set(n_dict[model].keys()) == set(neu.keys() + ['id']))

        # Save map between RID and ID of current node:
        rid_id_map[rid] = id
        rid_model_id_map[model][rid] = len(n_dict[model]['id'])
            
        n_dict[model]['id'].append(int(id))

    # Process output ports, i.e., those with class 'port' that have an incoming
    # edge from a node with class 'neuron' (XXX should check that edges are of
    # class 'data'):
    ports = [rid for rid in nodes if g.node[rid]['class'] == 'port']
    out_ports = [(port_rid, neuron_rid) for port_rid in ports \
                 for neuron_rid in g.predecessors(port_rid) \
                 if g.node[neuron_rid]['class'] == 'neuron']
    for port_rid, neuron_rid in out_ports:
        sel = g.node[port_rid]['selector']
        model = g.node[neuron_rid]['model']
        n_dict[model]['selector'][rid_model_id_map[model][neuron_rid]] = sel

    # Process input ports, i.e., those with class 'port' that have an outgoing
    # edge to a node with class 'synapse' (XXX should check that edges are of
    # class 'data'):
    in_ports = [(port_rid, g.node[port_rid]) for port_rid in ports \
                if 'synapse' in \
                [g.node[synapse_rid]['class'] for synapse_rid \
                 in g.successors(port_rid)]]
    for id, (port_rid, port_data) in enumerate(in_ports):
        port_data = copy.deepcopy(port_data)
        model = port_data['model']
        assert 'selector' in port_data
        if model == 'port_in_gpot':
            port_data['spiking'] = False
            port_data['public'] = False
        else:
            port_data['spiking'] = True
            port_data['public'] = False

        if 'public' not in port_data:
            port_data['public'] = False
        if model not in n_dict:
            n_dict[model] = {k: [] for k in port_data.keys() + ['id']}
            rid_model_id_map[model] = {}
            
        assert set(n_dict[model].keys()) == set(port_data.keys()+['id'])

        for key in port_data:
            n_dict[model][key].append(port_data[key])

        rid_id_map[rid] = id
        rid_model_id_map[model][rid] = len(n_dict[model]['id'])
        n_dict[model]['id'].append(int(id))

    # Remove duplicate neuron model info:
    for val in n_dict.values():
        val.pop('model')
    if not n_dict:
        n_dict = dict()

    # Process synapses:
    synapses = [(rid_id_map[pre_rid], rid_id_map[post_rid], g.node[synapse_rid]) \
                for synapse_rid in nodes \
                if g.node[synapse_rid]['class'] == 'synapse' \
                for pre_rid in g.predecessors(synapse_rid) \
                if g.node[pre_rid]['class'] == 'neuron' \
                for post_rid in g.successors(synapse_rid) \
                if g.node[post_rid]['class'] == 'neuron']

    # Sort synapse by post-synaptic ID:
    def f(x, y):
        if int(x[1]) < int(y[1]):
            return -1
        elif int(x[1]) > int(y[1]):
            return 1
        else:
            return 0
    synapses.sort(cmp=f)

    s_dict = dict()
    for id, (pre_id, post_id, syn_data) in enumerate(synapses):
        syn_data = copy.deepcopy(syn_data)
        model = syn_data['model']
        syn_data['id'] = id

        if model not in s_dict:
            s_dict[model] = {k:[] for k in syn_data.keys() + ['pre', 'post']}
        
        assert set(s_dict[model].keys()) == set(syn_data.keys() + ['pre', 'post'])

        for key in syn_data.keys():
            s_dict[model][key].append(syn_data[key])
        s_dict[model]['pre'].append(pre_id)
        s_dict[model]['post'].append(post_id)

    # Return duplicate synapse model info:
    for val in s_dict.values():
        val.pop('model')
    if not s_dict:
        s_dict = dict()
        
    return n_dict, s_dict
