#!/usr/bin/env python

"""
Convert a graph between Pandas and OrientDB.
"""

# Copyright (c) 2015, Lev Givon
# All rights reserved.
# Distributed under the terms of the BSD license:
# http://www.opensource.org/licenses/bsd-license

import copy
import json

import pandas as pd
import pyorient.otypes
from pyorient.utils import get_hash

from .utils import _find_field_types
from ..utils import byteify, chunks

def as_pandas(nodes=[], edges=[], force_rid=False, deepcopy = True):
    """
    Converts OrientDB Gremlin query results into Pandas DataFrame.

    Parameters
    ----------
    nodes : list of pyorient.otypes.OrientRecord
        OrientDB node query results.
    edges : list of pyorient.otypes.OrientRecord
        OrientDB edge query results.
    force_rid : bool
        If True, always use the OrientDB RID as the index value in the
        returned DataFrame of node data. Otherwise, use the 'id' property as the
        index value if it is present.

    Returns
    -------
    df_node, df_edge : pandas.DataFrame
        DataFrame instances containing query results. The OrientDB class of each
        node and edge is stored in the 'class' column of the corresponding
        DataFrame instance.
    """

    props_list = []
    index = []
    rid_to_id = {}
    for node in nodes:
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
        #
        #     # Replace record links with their corresponding RIDs:
        #     #elif isinstance(props[k], pyorient.otypes.OrientRecordLink):
        #     #    props[k] = props[k].get_hash()
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

        # If the node has an 'id' attribute, use that as the index value if
        # force_rid isn't set:
        if 'id' in props and not force_rid:
            id = props['id']
            del props['id']
        else:
            id = props.get('rid', node._rid)
        index.append(id)
        props_list.append(props)

        rid_to_id[props.get('rid', node._rid)] = id
        # rid_to_id[node._rid] = id
    df_node = pd.DataFrame.from_records(props_list)
    df_node.index = pd.Index(data=index, name='id')

    prop_list = []
    for edge in edges:
        # Don't let function alter the original records:
        if deepcopy:
            tmp = copy.deepcopy(edge.oRecordData)
        else:
            tmp = edge.oRecordData

        props = {k:v for k,v in tmp.items()}
        # Convert record IDs to the IDs assigned to the nodes:
        props['in'] = rid_to_id[props['in'].get_hash()]
        props['out'] = rid_to_id[props['out'].get_hash()]

        # Save the OrientDB class:
        props['class'] = edge._class

        prop_list.append(props)

    # Don't preserve the OrientDB edge IDs:
    df_edge = pd.DataFrame.from_records(prop_list)
    df_edge.index.name = 'id'

    return df_node, df_edge

def orient_to_pandas(client, node_query='', edge_query='', force_rid=False):
    """
    Query OrientDB and return results as Pandas DataFrames.

    Parameters
    ----------
    client : pyorient.orient.OrientDB
        OrientDB interface.
    node_query : str
        Gremlin query that returns a collection of nodes.
    edge_query : str
        Gremlin query that returns a collection of edges.
    force_rid : bool
        If True, always use the OrientDB RID as the index value in the
        returned DataFrame of node data. Otherwise, use the 'id' property as the
        index value if it is present.

    Returns
    -------
    df_node, df_edge : pandas.DataFrame
        DataFrame instances containing query results. The OrientDB class of each
        node and edge is stored in the 'class' column of the corresponding
        DataFrame instance.
    """

    if node_query:
        nodes = client.gremlin(node_query)
    else:
        nodes = []
    if edge_query:
        edges = client.gremlin(edge_query)
    else:
        edges = []
    return as_pandas(nodes, edges, force_rid)

def pandas_to_orient(client, df_node, df_edge):
    """
    Loads Pandas DataFrames into OrientDB database.

    Parameters
    ----------
    client : pyorient.orient.OrientDB
        OrientDB interface.
    df_node, df_edge : pandas.DataFrame
        Tables containing the properties of each node and edge to convert.

    Notes
    -----
    Node IDs are discarded upon creation of the new graph
    """

    assert isinstance(df_node, pd.DataFrame)
    assert isinstance(df_edge, pd.DataFrame)

    N = 10
    id_to_rid = {}
    for chunk in chunks(zip(df_node.index, df_node.to_dict('record')), N):
        cmd_list = []
        id_list = []
        for i, (id, props) in enumerate(chunk):
            if 'class' in props:
                cls = props['class']
                del props['class']
            else:
                cls = 'V'

            # Save original ID:
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

    for chunk in chunks(zip(df_edge.index, df_edge.to_dict('record')), N):
        for id, props in chunk:
            from_id = props['out']
            to_id = props['in']
            if 'class' in props:
                cls = props['class']
                del props['class']
            else:
                cls = 'E'
            del props['out']
            del props['in']

            # Add @fieldTypes field to force proper storage of types:
            ft = _find_field_types(props)
            if ft:
                props['@fieldTypes'] = ft

            cmd_list = ['create edge %s from %s to %s content %s;' % \
                        (cls, id_to_rid[from_id], id_to_rid[to_id], json.dumps(props))]
            cmd = 'begin;'+''.join(cmd_list)+'commit;'
            client.batch(cmd)
