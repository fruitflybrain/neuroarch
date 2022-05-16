#!/usr/bin/env python

"""
Convert a graph between NetworkX and OrientDB
"""

# Copyright (c) 2015, Lev Givon
# All rights reserved.
# Distributed under the terms of the BSD license:
# http://www.opensource.org/licenses/bsd-license

import copy
import json
import time

import numpy as np
import networkx as nx
import pyorient.otypes
from pyorient.utils import get_hash

from .utils import _find_field_types
from ..utils import byteify, chunks

def as_nx(nodes=[], edges=[], force_rid=False, deepcopy = True):
    """
    Converts OrientDB Gremlin query results into a NetworkX MultiDiGraph.

    Parameters
    ----------
    nodes : list of pyorient.otypes.OrientRecord
        OrientDB node query results.
    edges : list of pyorient.otypes.OrientRecord
        OrientDB edge query results.
    force_rid : bool
        If True, always use the OrientDB RID as the node identifier in the
        returned graph. Otherwise, use 'id' property as the
        node identifier if it is present.

    Results
    -------
    g : networkx.MultiDiGraph
        Constructed multigraph containing query results. The OrientDB class of each
        node and edge is stored in the 'class' attribute
        of the corresponding nodes and edges in the result `g`.
    """

    # XXX what should happen if a node/edge in OrientDB contains a 'class' attribute?
    g = nx.MultiDiGraph()
    rid_to_id = {}
    for i, node in enumerate(nodes):
        # Don't let function alter the original records:
        if deepcopy:
            tmp = copy.deepcopy(node.oRecordData)
        else:
            tmp = node.oRecordData
        props = {}
        for k, v in tmp.items():
            if isinstance(v, pyorient.otypes.OrientBinaryObject):
                continue
            if isinstance(k, str) and k.startswith('_'):
                continue
            if isinstance(v, pyorient.otypes.OrientRecordLink):
                props[k] = get_hash(v)
            elif (isinstance(v,list) and v and
                       isinstance(v[0], pyorient.otypes.OrientRecordLink)):
                props[k] = [get_hash(n) for n in v]
            else:
                props[k] = v
        # props_keys = list(props.keys())
        # for k in props_keys:
        #
        #     # Discard binary objects:
        #     if isinstance(props[k], pyorient.otypes.OrientBinaryObject):
        #         del props[k]
        #
        #     # Replace record links with their corresponding RIDs:
        #     #elif isinstance(props[k], pyorient.otypes.OrientRecordLink):
        #     #    props[k] = props[k].get_hash()
        #
        #     # Remove record links
        #     elif isinstance(props[k], pyorient.otypes.OrientRecordLink):
        #         del props[k]
        #
        #     # Remove list of links
        #     elif (isinstance(props[k],list) and props[k] and
        #           isinstance(props[k][0], pyorient.otypes.OrientRecordLink)):
        #         del props[k]
        #
        #     # Remove properties whose name is a string that starts with '_'; they
        #     # are for special OrientDB purposes:
        #     elif isinstance(k, str) and k.startswith('_'):
        #         del props[k]

        # Save the OrientDB class:
        props['class'] = node._class

        # If the node has an 'id' attribute, use that as the NetworkX node
        # identifier:
        if 'id' in props and not force_rid:
            id = props['id']
            del props['id']
        else:
            id = props.get('rid', node._rid)

        g.add_node(id, **props)

        rid_to_id[props.get('rid', node._rid)] = id

    for edge in edges:
        # Don't let function alter the original records:
        if deepcopy:
            tmp = copy.deepcopy(edge.oRecordData)
        else:
            tmp = edge.oRecordData
        in_rid = tmp['in'].get_hash()
        out_rid = tmp['out'].get_hash()
        # del props['in']
        # del props['out']
        props = {k: v for k, v in tmp.items() if k not in ['in', 'out']}

        # Save the OrientDB class:
        props['class'] = edge._class
        g.add_edge(rid_to_id[out_rid], rid_to_id[in_rid],
                   **props)
    return g

def orient_to_nx(client, node_query='', edge_query='', force_rid=False):
    """
    Query OrientDB and return results as a NetworkX MultiDiGraph.

    Parameters
    ----------
    client : pyorient.orient.OrientDB
        OrientDB interface.
    node_query : str
        Gremlin query that returns a collection of nodes.
    edge_query : str
        Gremlin query that returns a collection of edges.
    force_rid : bool
        If True, always use the OrientDB RID as the node identifier in the
        returned graph. Otherwise, use 'id' property as the
        node identifier if it is present.

    Results
    -------
    g : networkx.MultiDiGraph
        Constructed multigraph containing query results. The OrientDB class of each
        node and edge is stored in the 'class' attribute
        of the corresponding nodes and edges in the result `g`.
    """

    if node_query:
        nodes = client.gremlin(node_query)
    else:
        nodes = []
    if edge_query:
        edges = client.gremlin(edge_query)
    else:
        edges = []
    return as_nx(nodes, edges, force_rid)

def nx_to_orient(client, g):
    """
    Converts NetworkX MultiDiGraph to OrientDB graph.

    Parameters
    ----------
    client : pyorient.orient.OrientDB
        OrientDB interface.
    g : networkx.MultiDiGraph
        Graph to convert to OrientDB.

    Notes
    -----
    The 'class' attribute of each node and edge in `g` is assumed to be the
    OrientDB class name to use when creating the corresponding nodes and edges
    in the database. If no 'class' attribute is specified, the node and edge
    class names are assumed to be 'V' and 'E', respectively.

    Node IDs are discarded upon creation of the new graph.
    """

    assert isinstance(g, (nx.DiGraph, nx.MultiDiGraph))

    # This assumes that each OrientDB class has a single cluster:
    N = 10
    id_to_rid = {}
    for chunk in chunks(g.nodes(data=True), N):
        cmd_list = []
        id_list = []
        for i, (id, props) in enumerate(chunk):
            # Remove class name from properties inserted into database (but don't
            # clobber the input graph):
            props = copy.deepcopy(props)
            if props.has_key('class'):
                cls = props['class']
                del props['class']
            else:
                cls = 'V'

            # Save original node ID:
            assert 'id' not in props
            props['id'] = id

            # Add @fieldTypes field to force proper storage of types:
            ft = _find_field_types(props)
            if ft:
                props['@fieldTypes'] = ft

            id_list.append(id)
            cmd_list.append('let $a%s = create vertex %s content %s;' % \
                            (i, cls, json.dumps(byteify(props))))
        cmd = 'begin;'+''.join(cmd_list)+'commit;'+\
              ('return [%s];' % ','.join(['$a%s' % i for i in range(len(chunk))]))
        rec_list = client.batch(cmd)
        for id, r in zip(id_list, rec_list):
            id_to_rid[id] = r._rid

    for chunk in chunks(g.edges(data=True), N):
        cmd_list = []
        for from_id, to_id, props in chunk:
            # Remove class name from properties inserted into database (but don't
            # clobber the input graph):
            props = copy.deepcopy(props)
            if props.has_key('class'):
                cls = props['class']
                del props['class']
            else:
                cls = 'E'

            # Add @fieldTypes field to force proper storage of types:
            ft = _find_field_types(props)
            if ft:
                props['@fieldTypes'] = ft

            cmd_list.append('create edge %s from %s to %s content %s;' % \
                (cls, id_to_rid[from_id], id_to_rid[to_id],
                 json.dumps(byteify(props))))
        cmd = 'begin;'+''.join(cmd_list)+'commit;'
        client.batch(cmd)
