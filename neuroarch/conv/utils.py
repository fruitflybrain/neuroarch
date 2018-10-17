#!/usr/bin/env python

"""
Pandas/NetworkX graph conversion utilities.
"""

# Copyright (c) 2015, Lev Givon
# All rights reserved.
# Distributed under the terms of the BSD license:
# http://www.opensource.org/licenses/bsd-license

import pandas as pd
import networkx as nx
import numpy as np

def _find_field_types(d):
    """
    Create @fieldTypes property value for setting types when creating nodes with JSON content.    
    """

    field_types = []
    for k, v in d.items():
        if isinstance(v, np.float32):
            field_types.append('%s=f' % k)
        elif isinstance(v, float) or isinstance(v, np.float64):
            field_types.append('%s=d' % k)
    return ','.join(field_types)

def nx_to_pandas(g):
    """
    Convert directed multigraph stored as NetworkX MultiDiGraph into Pandas DataFrame.

    Parameters
    ----------
    g : networkx.MultiDiGraph
        Directed multigraph to convert.

    Returns
    -------
    df_node : pandas.DataFrame
        Node attributes. The NetworkX identifiers are used as the DataFrame
        index.
    df_edge : pandas.DataFrame
        Edge attributes. Source and destination node identifiers are stored in
        `out` and `in` columns, respectively.
    """

    assert isinstance(g, nx.MultiDiGraph)
    node_data = g.nodes(data=True)
    index = [n[0] for n in node_data]
    props = [n[1] for n in node_data]
    df_node = pd.DataFrame.from_records(props, index)

    index = range(g.number_of_edges())
    props = []
    for e in g.edges(data=True):
        p = {'out': e[0], 'in': e[1]}
        p.update(e[2])
        props.append(p)
    df_edge = pd.DataFrame.from_records(props, index)
    return df_node, df_edge

def pandas_to_nx(df_node, df_edge):
    """
    Convert directed multigraph stored as Pandas DataFrames into NetworkX MultiDiGraph.

    Parameters
    ----------
    df_node : pandas.DataFrame
        Node attributes. The index contents are assumed to be the node identifiers.
    df_edge : pandas.DataFrame
        Edge attributes. Source and destination node identifiers are stored in
        `out` and `in` columns, respectively.

    Returns
    -------
    g : networkx.MultiDiGraph
        Directed multigraph to convert.
    """

    assert isinstance(df_node, pd.DataFrame)
    assert isinstance(df_edge, pd.DataFrame)
    g = nx.MultiDiGraph()
    for id, props in zip(df_node.index, df_node.to_dict('record')):
        g.add_node(id, props)
    for id, props in zip(df_edge.index, df_edge.to_dict('record')):
        from_id = props['out']
        to_id = props['in']
        del props['out']
        del props['in']
        g.add_edge(from_id, to_id, **props)
    return g
