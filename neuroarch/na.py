#!/usr/bin/env python

import collections
import numbers
import pprint
import re
from datetime import datetime
import json
import time
import copy
from tqdm import tqdm

from neuroarch.utils import is_rid, iterable, chunks
from neuroarch.diff import diff_nodes, diff_edges
from neuroarch.apply_diff import apply_node_diff, apply_edge_diff
from neuroarch.query import QueryWrapper, QueryString
import neuroarch.models as models

def update_neuron(graph, neuron, **kwargs):
    """
    Parameters
    ----------
    graph: pyorient.ogm.graph
           Connected database object.
    neuron: models.Neuron or dict
            The neuron to be updated.
            If dict, must be either {'rid': ...} or {'uname': ...}.
    kwargs: fields to be updated.

    Return
    ------
    update_succ: bool
                 Indicate if update was successful.
    """
    if not isinstance(neuron, models.Neuron):
        if 'rid' in neuron:
            q = QueryWrapper.from_rids(graph, neuron['rid'])
            if len(q.nodes):
                neuron_to_update = q.get_as('obj')[0][0]
            if not isinstance(neuron_to_update, models.Neuron):
                print('Error: node with rid {} not a Neuron node'.format(neuron['rid']))
                return False
        elif 'uname' in neuron:
            neuron_to_update = graph.Neurons.query(uname = neuron['uname']).all()
            if len(neuron_to_update) == 0:
                print('Error: No neuron with uname {} in DB'.format(neuron['uname']))
                return False
            elif len(neuron_to_update) > 1:
                print('Error: More than 1 neuron with uname {} in DB'.format(neuron['uname']))
                return False
            neuron_to_update = neuron_to_update[0]
        else:
            print('Error: No current criteria to find name')
            return False
    else:
        neuron_to_update = neuron

    # check if other records need to be updated
    update_chain = False
    if 'uname' in kwargs:
        uname = kwargs['uname']
        # make sure there is no other Neuron node with the uname to be updated
        existing_neuron = graph.Neurons.query(uname = uname).all()
        if len(existing_neuron):
            print('Error: Neuron with uname: already exist in DB'.format(uname))
            return False
        update_chain = True

    if 'name' in kwargs:
        update_chain = True

    # copy original props and update neuron props
    neuron_props = copy.deepcopy(neuron_to_update.get_props())
    for k, v in kwargs.items():
        neuron_props[k] = v
    neuron_to_update.update(**neuron_props)

    # find all related records to update
    if not update_chain:
        return True

    post_syn_nodes = [n for n in neuron_to_update.out('SendsTo') \
                      if isinstance(n, (models.Synapse, models.InferredSynapse))]
    for node in tqdm(post_syn_nodes):
        props = node.get_props()
        update_props = {}
        if 'name' in props:
            pre, post = props['name'].split('--')
            synapse_name = '{}--{}'.format(neuron_props['name'], post)
            update_props['name'] = synapse_name
        if 'uname' in props:
            pre, post = props['uname'].split('--')
            synapse_uname = '{}--{}'.format(neuron_props['uname'], post)
            update_props['uname'] = synapse_uname
        node.update(**update_props)
        data_nodes = node.out('HasData')
        for node1 in data_nodes:
            props = node1.get_props()
            update_props = {}
            if 'name' in props:
                update_props['name'] = synapse_name
            if 'uname' in props:
                update_props['uname'] = synapse_uname
            node1.update(**update_props)

    pre_syn_nodes = [n for n in neuron_to_update.in_('SendsTo') \
                     if isinstance(n, (models.Synapse, models.InferredSynapse))]
    for node in tqdm(pre_syn_nodes):
        props = node.get_props()
        update_props = {}
        if 'name' in props:
            pre, post = props['name'].split('--')
            synapse_name = '{}--{}'.format(pre, neuron_props['name'])
            update_props['name'] = synapse_name
        if 'uname' in props:
            pre, post = props['uname'].split('--')
            synapse_uname = '{}--{}'.format(pre, neuron_props['uname'])
            update_props['uname'] = synapse_uname
        node.update(**update_props)
        data_nodes = node.out('HasData')
        for node1 in data_nodes:
            props = node1.get_props()
            update_props = {}
            if 'name' in props:
                update_props['name'] = synapse_name
            if 'uname' in props:
                update_props['uname'] = synapse_uname
            node1.update(**update_props)

    data_nodes = neuron_to_update.out('HasData')
    for node in data_nodes:
        props = node.get_props()
        update_props = {}
        if 'name' in props:
            update_props['name'] = neuron_props['name']
        if 'uname' in props:
            update_props['uname'] = neuron_props['uname']
        node.update(**update_props)
    return True
