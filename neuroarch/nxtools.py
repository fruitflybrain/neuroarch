#!/usr/bin/env python

"""
NetworkX tools.
"""

# Copyright (c) 2015-2016, Lev Givon
# All rights reserved.
# Distributed under the terms of the BSD license:
# http://www.opensource.org/licenses/bsd-license

import copy
import itertools
import operator
import re

import deepdiff
import networkx as nx
import numpy as np

def nodes_has(g, nbunch, attr, value, op=operator.eq, flags=0):
    """
    Filter nodes in list by specific attribute value and comparison operator.

    Parameters
    ----------
    g : networkx.Graph
        NetworkX graph.
    nbunch : non-string iterable
        List of node identifiers.
    attr : str
        Attribute name.
    value : object
        Attribute value.
    op : function
        Comparison operator; default is `operator.eq`. The first operand
        is the node attribute and the second is `value` if the operator
        is not `re.search` or `re.match`; otherwise, `value` is assumed to be
        the regular expression pattern used for matching.
    flags : int
        Flags to pass to regular expression matcher. Ignored if 
        `op` is not `re.search` or `re.match`.

    Returns
    -------
    result : list
        List of selected nodes.
    """

    assert np.iterable(nbunch) and not isinstance(nbunch, str)
    if op in [re.search, re.match]:
        _op = lambda s, p: op(p, s, flags)
    else:
        _op = op
    return [n for n in nbunch if attr in g.node[n] \
                and _op(g.node[n][attr], value)]

def all_nodes_has(g, attr, value, op=operator.eq, flags=0):
    """
    Find all nodes in graph with specific attribute value via specified comparison operator.

    Parameters
    ----------
    g : networkx.Graph
        NetworkX graph.
    attr : str
        Attribute name.
    value : object
        Attribute value.
    op : function
        Comparison operator; default is `operator.eq`.  The first operand
        is the node attribute and the second is `value` if the operator
        is not `re.search` or `re.match`; otherwise, `value` is assumed to be
        the regular expression pattern used for matching.
    flags : int
        Flags to pass to regular expression matcher. Ignored if 
        `op` is not `re.search` or `re.match`.

    Returns
    -------
    result : list
        List of selected nodes.
    """

    return nodes_has(g, g.nodes(), attr, value, op, flags)

def edges_has(g, ebunch, attr, value, op=operator.eq, flags=0):
    """
    Filter edges in list with specific attribute value and comparison operator.

    Parameters
    ----------
    g : networkx.Graph
        NetworkX graph.
    ebunch : non-string iterable
        Edge endpoint tuples. If `g` is a MultiDiGraph, the tuples must also contain
        the keys for each respective edge.
    attr : str
        Attribute name.
    value : object
        Attribute value.
    op : function
        Comparison operator; default is `operator.eq`. The first operand
        is the node attribute and the second is `value` if the operator
        is not `re.search` or `re.match`; otherwise, `value` is assumed to be
        the regular expression pattern used for matching.
    flags : int
        Flags to pass to regular expression matcher. Ignored if 
        `op` is not `re.search` or `re.match`.

    Returns
    -------
    result : list
        List of selected edge endpoint tuples.
    """

    assert np.iterable(ebunch) and not isinstance(ebunch, str)
    if op in [re.search, re.match]:
        _op = lambda s, p: op(p, s, flags)
    else:
        _op = op
    if isinstance(g, nx.MultiDiGraph):
        return [(i, j, k) for (i, j, k) in ebunch \
                if attr in g.edge[i][j][k] and \
                _op(g.edge[i][j][k][attr], value)]
    else:
        return [(i, j) for (i, j) in ebunch \
                if attr in g.edge[i][j] and \
                _op(g.edge[i][j][attr], value)]

def all_edges_has(g, attr, value, op=operator.eq, flags=0):
    """
    Find all edges in graph with specific attribute value via specified comparison operator.

    Parameters
    ----------
    g : networkx.Graph
        NetworkX graph.
    attr : str
        Attribute name.
    value : object
        Attribute value.
    op : function
        Comparison operator; default is `operator.eq`. The first operand
        is the node attribute and the second is `value` if the operator
        is not `re.search` or `re.match`; otherwise, `value` is assumed to be
        the regular expression pattern used for matching.
    flags : int
        Flags to pass to regular expression matcher. Ignored if 
        `op` is not `re.search` or `re.match`.

    Returns
    -------
    result : list
        List of selected edge endpoint tuples.
    """

    if isinstance(g, nx.MultiDiGraph):
        e = g.edges(keys=True)
    else:
        e = g.edges()
    return edges_has(g, e, attr, value, op, flags)

def out_nodes_has(g, nbunch, attr, value, op=operator.eq, flags=0):
    """
    Find outgoing nodes with specific attribute value.

    Parameters
    ----------
    g : networkx.Graph
        NetworkX graph.
    n : non-string iterable
        List of node identifiers.
    attr : str
        Attribute name.
    value : object
        Attribute value.
    op : function
        Comparison operator; default is `operator.eq`. The first operand
        is the node attribute and the second is `value` if the operator
        is not `re.search` or `re.match`; otherwise, `value` is assumed to be
        the regular expression pattern used for matching.
    flags : int
        Flags to pass to regular expression matcher. Ignored if 
        `op` is not `re.search` or `re.match`.

    Returns
    -------
    result : list
        List of selected node identifiers.
    """

    assert np.iterable(nbunch) and not isinstance(nbunch, str)
    if op in [re.search, re.match]:
        _op = lambda s, p: op(p, s, flags)
    else:
        _op = op
    result = set()
    for n in nbunch:
        result.update([n for n in g.successors(n) \
                       if attr in g.node[n] and _op(g.node[n][attr], value)])
    return list(result)

def in_nodes_has(g, nbunch, attr, value, op=operator.eq, flags=0):
    """
    Find incoming nodes with specific attribute value.

    Parameters
    ----------
    g : networkx.Graph
        NetworkX graph.
    nbunch : object
        List of node identifiers.
    attr : str
        Attribute name.
    value : object
        Attribute value.
    op : function
        Comparison operator; default is `operator.eq`. The first operand
        is the node attribute and the second is `value` if the operator
        is not `re.search` or `re.match`; otherwise, `value` is assumed to be
        the regular expression pattern used for matching.
    flags : int
        Flags to pass to regular expression matcher. Ignored if 
        `op` is not `re.search` or `re.match`.

    Returns
    -------
    result : list
        List of selected node identifiers.
    """

    assert np.iterable(nbunch) and not isinstance(nbunch, str)
    if op in [re.search, re.match]:
        _op = lambda s, p: op(p, s, flags)
    else:
        _op = op
    result = set()
    for n in nbunch:
        result.update([n for n in g.predecessors(n) \
                       if attr in g.node[n] and _op(g.node[n][attr], value)])
    return list(result)

def out_edges_has(g, nbunch, attr, value, op=operator.eq, flags=0):
    """
    Find outgoing edges with specific attribute value.

    Parameters
    ----------
    g : networkx.Graph
        NetworkX graph.
    nbunch : iterable
        List of node identifiers.
    attr : str
        Attribute name.
    value : object
        Attribute value.
    op : function
        Comparison operator; default is `operator.eq`. The first operand
        is the node attribute and the second is `value` if the operator
        is not `re.search` or `re.match`; otherwise, `value` is assumed to be
        the regular expression pattern used for matching.
    flags : int
        Flags to pass to regular expression matcher. Ignored if 
        `op` is not `re.search` or `re.match`.

    Returns
    -------
    result : list
        List of edge endpoint tuples. If `g` is a MultiDiGraph,
        the tuples contain the endpoints and key for each respective edge.
    """

    assert np.iterable(nbunch) and not isinstance(nbunch, str)
    if op in [re.search, re.match]:
        _op = lambda s, p: op(p, s, flags)
    else:
        _op = op
    result = set()
    for n in nbunch:
        if isinstance(g, nx.MultiDiGraph):
            result.update([(i, j, k) for (i, j, k, data) in \
                           g.out_edges([n], data=True, keys=True) \
                           if attr in data and _op(data[attr], value)])
        else:
            result.update([(i, j) for (i, j, data) in \
                           g.out_edges([n], data=True) \
                           if attr in data and _op(data[attr], value)])
    return list(result)

def in_edges_has(g, nbunch, attr, value, op=operator.eq, flags=0):
    """
    Find incoming edges with specific attribute value.

    Parameters
    ----------
    g : networkx.Graph
        NetworkX graph.
    nbunch : iterable
        List of node identifiers.
    attr : str
        Attribute name.
    value : object
        Attribute value.
    op : function
        Comparison operator; default is `operator.eq`. The first operand
        is the node attribute and the second is `value` if the operator
        is not `re.search` or `re.match`; otherwise, `value` is assumed to be
        the regular expression pattern used for matching.
    flags : int
        Flags to pass to regular expression matcher. Ignored if 
        `op` is not `re.search` or `re.match`.

    Returns
    -------
    result : list
        List of edge endpoint tuples. If `g` is a MultiDiGraph,
        the tuples contain the endpoints and key for each respective edge.
    """

    assert np.iterable(nbunch) and not isinstance(nbunch, str)
    if op in [re.search, re.match]:
        _op = lambda s, p: op(p, s, flags)
    else:
        _op = op
    result = set()
    for n in nbunch:
        if isinstance(g, nx.MultiDiGraph):
            result.update([(i, j, k) for (i, j, k, data) in \
                           g.in_edges([n], data=True, keys=True) \
                           if attr in data and _op(data[attr], value)])
        else:
            result.update([(i, j) for (i, j, data) in \
                           g.in_edges([n], data=True) \
                           if attr in data and _op(data[attr], value)])
    return list(result)

def find_nonmatching_dict_pairs(a, b):
    """
    Find nonmatching pairs of dicts.

    Given two sets of dicts, try to match each dict in the first set
    with an identical dict in the second dict. Return any remaining pairs of
    non-matching dicts.

    Parameters
    ----------
    a, b : iterable
        Sets of dicts to analyze. The sets must contain the same number of dicts.

    Returns
    -------
    x, y : iterable
        Sets of dicts in `a` and `b`, respectively, that do not match.
    """

    assert len(a) == len(b)
    a = copy.deepcopy(a)
    b = copy.deepcopy(b)
    while True:
        # Look for matches between remaining dicts:
        pairs = itertools.product(a, b)
        for i, j in pairs:

            # Remove matching pair:
            if not deepdiff.DeepDiff(i, j):
                a.remove(i)
                b.remove(j)
                break

        # If no more matches are found, we are done:
        else:
            break
    return a, b

def is_isomorphic_attr(g0, g1):
    """
    Check whether two property graphs are isomorphic.

    Returns True if the graphs are structurally isomorphic and the attributes
    of each node and edge in one graph are identical to those in the isomorphic
    nodes and edges in the other graph.

    Parameters
    ----------
    g0, g1 : networkx.Graph
        NetworkX graphs. Graphs must be of the same type.
    
    Returns
    -------
    result : bool
        True if the graphs are isomorphic.
    """
    
    d = lambda a, b: False if deepdiff.DeepDiff(a, b) else True
    return nx.isomorphism.is_isomorphic(g0, g1,
                                        node_match=d,
                                        edge_match=d)

def iso_attr_diff(g0, g1):
    """
    Find differences in node/edge attributes of structurally isomorphic graphs.

    Parameters
    ----------
    g0, g1 : networkx.Graph
        NetworkX graphs. Graphs must be of the same type.

    Returns
    -------
    node_diff : dict
        Each key is a tuple containing the isomorphic node IDs in `g0` and `g1`,
        respectively; the value contains the corresponding attribute differences.
    edge_diff : dict
        Each key is a tuple containing the isomorphic edge ID tuples for `g0`
        and `g1`, respectively. If the graphs are MultiDiGraph instances, the edge
        ID tuples each contain three values: (from, to, key). Each value of 
        `edge_diff` contains the corresponding attribute differences.
    """

    assert type(g0) == type(g1)
    node_diff = {}
    edge_diff = {}
    if isinstance(g0, nx.MultiDiGraph):
        matcher = nx.isomorphism.MultiDiGraphMatcher(g0, g1)
        if not matcher.is_isomorphic():
            raise ValueError('graphs are not structurally isomorphic')
        for i0, i1 in matcher.mapping.items():
            d = deepdiff.DeepDiff(g0.node[i0], g1.node[i1])
            if d:
                node_diff[(i0, i1)] = d
        for i0, j0 in g0.edges():
            i1 = matcher.mapping[i0]
            j1 = matcher.mapping[j0]

            # Find pairs of edge attributes that don't match:
            d0_nomatch, d1_nomatch = \
                find_nonmatching_dict_pairs(g0.edge[i0][j0].values(), 
                                            g1.edge[i1][j1].values())

            # Find the keys that correspond to the nonmatching pairs of
            # attributes:
            d0_all = copy.deepcopy(g0.edge[i0][j0])
            d1_all = copy.deepcopy(g1.edge[i1][j1])
            for d0, d1 in zip(d0_nomatch, d1_nomatch):

                # Look for key corresponding to attribute dict.  Remove key if
                # found so that if multiple keys maps to one attribute dict, the
                # next iteration of the outer loop finds the next key:
                for k0 in d0_all:
                    if d0_all[k0] == d0:
                        del d0_all[k0]
                        break
                for k1 in d1_all:
                    if d1_all[k1] == d1:
                        del d1_all[k1]
                        break
                d = deepdiff.DeepDiff(g0.get_edge_data(i0, j0, k0),
                                      g1.get_edge_data(i1, j1, k1))
                if d:
                    edge_diff[((i0, j0, k0), (i1, j1, k1))] = d
    else:
        raise ValueError('graph type not yet supported')
    return node_diff, edge_diff

def read_gexf(path):
    """
    Read graph in GEXF format from path.

    Reads a graph from a GEXF file using `networkx.read_gexf()`, but strips
    'id' and 'label' attributes from all nodes and edges.

    Parameters
    ----------
    path : file or string
        File or file name to write.
        File names ending in .gz or .bz2 will be compressed.    

    Returns
    -------
    graph : NetworkX graph
        If no parallel edges are found a Graph or DiGraph is returned.
        Otherwise a MultiGraph or MultiDiGraph is returned.
    """

    g = nx.read_gexf(path)
    for n in g.nodes():
        try:
            del g.node[n]['id']
        except:
            pass
        try:
            del g.node[n]['label']
        except:
            pass
    if isinstance(g, nx.MultiDiGraph):
        for n, m, k in g.edges(keys=True):
            try:
                del g.edge[n][m][k]['id']
            except:
                pass
    else:
        for n, m in g.edges():
            try:
                del g.edge[n][m]['id']
            except:
                pass

    return g
