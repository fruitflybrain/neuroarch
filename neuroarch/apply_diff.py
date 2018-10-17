#!/usr/bin/env python

"""
Apply graph differences to OrientDB.
"""

# Copyright (c) 2016, Lev Givon
# All rights reserved.
# Distributed under the terms of the BSD license:
# http://www.opensource.org/licenses/bsd-license

from .utils import chunks, is_rid
import time

def apply_node_diff(client, d):
    """
    Applies changes to nodes in OrientDB database.

    Parameters
    ----------
    client : pyorient.orient.OrientDB
        OrientDB interface.
    d : dict
        Output of `neuroarch.diff_nodes()`.

    Notes
    -----
    The input diff dict `d` must be computed given DataFrame instances
    containing RIDs.

    See Also
    --------
    neuroarch.diff
    """

    N = 1000
    rid_list = []
    # Apply mods:
    if d['mod']:
        print('NODE MODS')
        d_mod = {k: v for k,v in d['mod'].items() if (k not in d['add']) and (k not in d['del'])}
        rid_list += _mod_nodes(d_mod, client, N)
        print('Modified records committed to database\n')
            
    # Apply adds:
    if d['add']:
        print('NODE ADDS')
        d_add = {k: v for k,v in d['add'].items() if k not in d['del']}
        rid_list += _add_nodes(d_add, client, N)
        print('New records committed to database\n')
        
            
    # Apply dels:
    if d['del']:
        print('NODE DELS')
        _del_nodes(d['del'], client, N)
        print('Record deletions committed to database\n')
    
    return rid_list



def apply_edge_diff(client, d):
    """
    Applies changes to nodes in OrientDB database.

    Parameters
    ----------
    client : pyorient.orient.OrientDB
        OrientDB interface.
    d : dict
        Output of `neuroarch.diff_nodes()`.

    Notes
    -----
    The input diff dict `d` must be computed given DataFrame instances
    containing RIDs.

    See Also
    --------
    neuroarch.diff
    """

    N = 200
    rid_list = []
    
          
    # Apply adds:
    if d['add']:
        print('EDGE ADDS')
        d_add = {k: v for k,v in d['add'].items() if k not in d['del']}
        rid_list += _add_edges(d_add, client, N)
        print('New records committed to database\n')
        
            
    # Apply dels:
    if d['del']:
        print('EDGE DELS')
        _del_edges(d['del'], client, N)
        print(' Record deletions committed to database\n')
    
    return rid_list

def _add_nodes(d_add, client, N):
    print('d_add', d_add)
    rid_map = {}
    for chunk in chunks(d_add.items(), N):
        cmd_list = []
        vertex_list = []
        vertex = 0
        for k, v in chunk:
            set_cmd = [] 
            for field, val in v.items():
                if field == 'class':
                    class_list = client.command("select classes.name from 0:1")[0].oRecordData['classes']
                    assert (val in class_list), "Assign new nodes to an existing class: \n%s" % ('\n'.join(class_list))
                    node_class = val
                elif (field != 'class') and (str(val).lower() in ('none', 'nan')):
                    continue
                else:
                    set_cmd.append("%s = %s" % (field, val.__repr__()) )
            if set_cmd:
                _set = "SET"
            else:
                _set = ""
            vertex += 1
            cmd_list.append("LET v%s = CREATE VERTEX %s %s %s;\n" % (vertex, node_class, _set, ', '.join(set_cmd)))
            vertex_list.append('$v%s' % vertex)
        
        cmd = "begin;\n" + "".join(cmd_list) + "commit retry 100; return [" + ", ".join(vertex_list) + "];"
        rid_map.update({k: r._rid for r in client.batch(cmd)})
        print('cmd', cmd)
        time.sleep(10)
    return rid_map


def _mod_nodes(d_mod, client, N):
    rid_list = []
    i = 0
    for chunk in chunks(d_mod.items(), N):
        print(i, len(chunk))
        i += 1
        cmd_list = []
        for k, v in chunk: 
            # The node identifier must be a RID because the             
            # 'id' property might not be unique:
            if not is_rid(k):
                raise ValueError('identifiers must be RIDs')
            set_cmd = ["%s = %s" % (field, val.__repr__()) if str(val).lower() not in ('none', 'nan') \
                       else "%s = NULL" % field for field, val in v.items()]
            cmd_list.append("UPDATE %s SET %s;\n" % (k, ", ".join(set_cmd)))
            rid_list.append(k)
        cmd = "begin;\n" + "".join(cmd_list) + "commit retry 100; return ['" + "', '".join(rid_list) + "'];"
        rid_list += client.batch(cmd)[0]
        print('cmd', cmd)
        time.sleep(10)
    return rid_list
    
    
def _del_nodes(d_del, client, N):
    for chunk in chunks(d_del.items(), N):
        cmd_list = []
        for k, v in chunk:
            # The node identifier must be a RID because the             
            # 'id' property might not be unique:
            if not is_rid(k):
                raise ValueError('identifiers must be RIDs')
            cmd_list.append("DELETE VERTEX %s;\n" % k)
        cmd = "begin;\n" + "".join(cmd_list)+"commit retry 100;"
        client.batch(cmd) 
        print('cmd', cmd)
        
def _del_edges(d_del, client, N):
    for chunk in chunks(d_del.items(), N):
        cmd_list = []
        for k, v in chunk:
            out_node, edge_class, in_node = k.split(' ')
            # The node identifier must be a RID because the             
            # 'id' property might not be unique:
            if not (is_rid(in_node) and is_rid(out_node)):
                raise ValueError('identifiers must be RIDs')
            cmd_list.append("DELETE EDGE from %s to %s where @class = %s;\n" % (out_node, in_node, edge_class))
        cmd = "begin;\n" + "".join(cmd_list)+"commit retry 100;"
        client.batch(cmd)
        print('cmd', cmd)
        
        

def _add_edges(d_add, client, N):
    edge_rid_list = []
    for chunk in chunks(d_add.items(), N):
        cmd_list = []
        edge_list = []
        edge = 0
        for k, v in chunk:
            for field, val in v.items():
                if field=='class':
                    class_list = client.command("select classes.name from 0:1")[0].oRecordData['classes']
                    assert (val in class_list), "Assign new edges to an existing class: \n%s" % ('\n'.join(class_list))
                    edge_class = val
                elif field=='in':
                    in_node = val
                elif field=='out':
                    out_node = val
            edge += 1
            cmd_list.append("LET e%s = CREATE EDGE %s FROM %s TO %s;\n" % (edge, edge_class, out_node, in_node))
            edge_list.append('$e%s' % edge)
        cmd = "begin;\n" + "".join(cmd_list) + "commit retry 100; return " + ", ".join(edge_list) + ";"
        edge_rid_list += [(r._class, r._out.get_hash(), r._in.get_hash()) for r in client.batch(cmd)]
        print('cmd', cmd)
    return edge_rid_list
        
