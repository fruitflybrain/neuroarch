#!/usr/bin/env python

"""
Transform Neurokernel-compatible NetworkX graph to/from Neuroarch-compatible graph.
"""

# Copyright (c) 2015, Lev Givon
# All rights reserved.
# Distributed under the terms of the BSD license:
# http://www.opensource.org/licenses/bsd-license

import copy
import itertools
import numbers

import networkx as nx

def na_pat_to_nk(g):
    """
    Transform a graph containing a Neuroarch-compatible pattern into
    one that can be executed by Neurokernel.

    Parameters
    ----------
    g : networkx.MultiDiGraph
        Graph containing Neuroarch-compatible pattern.

    Returns
    -------
    g : networkx.MultiDiGraph
        Graph containing Neurokernel-executable pattern.
    """

    assert isinstance(g, nx.MultiDiGraph)

    # Find interfaces:
    g_new = nx.MultiDiGraph()
    int_id_to_name = {}
    for n, data in g.nodes(data=True):
        if data['class'] == 'Interface':
            int_id_to_name[n] = data['name']

    # Create ports in new graph:
    old_port_id_to_new = {}
    for int_id in int_id_to_name:
        for old_port_id in g.successors(int_id):
            data = g.nodes[old_port_id]
            new_port_id = data['selector']
            g_new.add_node(new_port_id,
                           **{'interface': int_id_to_name[int_id],
                                      'io': data['port_io'],
                                      'type': data['port_type']})
            old_port_id_to_new[old_port_id] = new_port_id

    # Create connections between ports in the two interfaces:
    for from_id, to_id, data in g.edges(data=True):
        if data['class'] == 'SendsTo' and \
           g.nodes[from_id]['class'] == 'Port' and \
           g.nodes[to_id]['class'] == 'Port':
            g_new.add_edge(old_port_id_to_new[from_id],
                           old_port_id_to_new[to_id])

    return g_new

def na_lpu_to_nk(g):
    """
    Transform a graph containing a Neuroarch-compatible circuit into
    one that can be executed by Neurokernel.

    Parameters
    ----------
    g : networkx.MultiDiGraph
        Graph containing Neuroarch-compatible circuit.

    Returns
    -------
    g : networkx.MultiDiGraph
        Graph containing Neurokernel-executable circuit.
    """

    assert isinstance(g, nx.MultiDiGraph)

    g_new = nx.MultiDiGraph()
    # id_to_label = {}

    for id, data in g.nodes(data=True):
        if data['class'] in ['Interface', 'LPU']:
            continue

        # Don't clobber the original graph's data:
        data = copy.deepcopy(data)
        # g_new.add_node(data['label'], **data)
        # id_to_label[id] = data['label']
        g_new.add_node(id, **data)

    # Create synapse edges:
    for from_id, to_id, data in g.edges(data = True):
        data = copy.deepcopy(data)
        if data.pop('class') == 'SendsTo':
            # g_new.add_edge(id_to_label[from_id], id_to_label[to_id], **data)
            g_new.add_edge(from_id, to_id, **data)

    return g_new

na_lpu_to_nk_new = na_lpu_to_nk