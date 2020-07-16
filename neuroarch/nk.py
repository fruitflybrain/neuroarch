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

def nk_lpu_to_na(g, lpu_name, circuit_attr='circuit',
                 circuit_class='CircuitModel'):
    """
    Transform a graph containing a Neurokernel-executable circuit into
    one that conforms to Neuroarch's data model.

    Parameters
    ----------
    g : networkx.MultiDiGraph
        Graph containing Neurokernel-executable circuit.
    lpu_name : str
        Name of LPU.
    circuit_attr : str
        If a node or edge in `g` contains the `circuit_attr` attribute, the
        value of that attribute is assumed to contain the name of the instance
        of Neuroarch node type `circuit_class`. Otherwise, the nodes added to
        the output graph are connected directly to the LPU node.
    circuit_class : str
        Name of Neuroarch node class to use when creating nodes owned by the LPU
        node that own neurons and synapses.

    Returns
    -------
    g : networkx.MultiDiGraph
        Graph containing Neuroarch-compatible circuit.
    """

    assert isinstance(g, nx.MultiDiGraph)
    assert circuit_attr and circuit_class # shouldn't be empty

    # Find maximum ID in given graph so that we can use it to create new nodes
    # with IDs that don't overlap with those that already exist:
    max_id = 0
    for id in g.nodes():
        if isinstance(id, str):
            if id.isdigit():
                max_id = max(max_id, int(id))
            else:
                raise ValueError('node id must be an integer')
        elif isinstance(id, numbers.Integral):
            max_id = max(max_id, id)
        else:
            raise ValueError('node id must be an integer')
    gen_new_id = itertools.count(max_id+1).next

    # Create LPU and interface nodes and connect the latter to the former via an
    # Owns edge:
    g_new = nx.MultiDiGraph()
    lpu_id = gen_new_id()
    g_new.add_node(lpu_id,
                   **{'name': lpu_name, 'class': 'LPU'})
    int_id = gen_new_id()
    g_new.add_node(int_id,
                   **{'name': 0, 'class': 'Interface'})
    g_new.add_edge(lpu_id, int_id, **{'class': 'Owns'})

    # Transformation:
    # 1. nonpublic neuron node -> neuron node
    # 2. public neuron node -> neuron node with
    #    output edge to output port
    # 3. input port -> input port
    # 4. synapse edge -> synapse node + 2 edges connecting
    #    transformed original input/output nodes
    edges_to_out_ports = {} # edges to new output port nodes:
    for id, data in g.nodes(data=True):

        # Don't clobber the original graph's data:
        data = copy.deepcopy(data)

        # Neurokernel graphs don't contain explicit output port nodes, but
        # Neuroarch's data model does; we therefore have to create extra nodes
        # for neurons that are public to represent output ports:
        if 'public' in data and data['public']:
            new_id = gen_new_id()
            port_data = {'selector': data['selector'],
                         'port_type': 'spike' if data['spiking'] else 'gpot',
                         'port_io': 'out',
                         'class': 'Port'}
            g_new.add_node(new_id, port_data)

            # Since there should only be one connection from a neuron to an
            # output edge, we don't need to store the key in addition to the
            # edge endpoints:
            edges_to_out_ports[(id, new_id)] = {'class': 'SendsTo'}

            # Connect interface node to output port:
            g_new.add_edge(int_id, new_id, **{'class': 'Owns'})

        if 'model' in data:

            # The model attrib of neuron membrane models corresponds to its
            # Neuroarch class name:
            if data['model'] == 'port_in_gpot':
                data['class'] = 'Port'
                data['port_type'] = 'gpot'
                data['port_io'] = 'in'

                # The circuit attrib isn't needed because ports should only be owned by
                # interface node, but we don't delete it in order to facilitate
                # converting the graph back to Neurokernel-compatible format.
            elif data['model'] == 'port_in_spk':
                data['class'] = 'Port'
                data['port_type'] = 'spike'
                data['port_io'] = 'in'

                # The circuit attrib isn't needed because ports should only be owned by
                # interface node, but we don't delete it in order to facilitate
                # converting the graph back to Neurokernel-compatible format.
            elif data['model'] in ['MorrisLecar', 'LeakyIAF']:
                data['class'] = data['model']
            elif data['model'] == 'Photoreceptor':
                data['class'] = 'PhotoreceptorModel'
            else:

                # Ignore other nodes:
                continue

            # Don't need to several attributes that are implicit:
            for a in ['model', 'public', 'spiking']:
                if a in data:
                    del data[a]

            g_new.add_node(id, **data)

            # Connect interface node to output port:
            if data['class'] == 'Port':
                g_new.add_edge(int_id, id, **{'class': 'Owns'})

    # Create synapse nodes for each edge in original graph and connect them to
    # the source/dest neuron/port nodes:
    for from_id, to_id, data in g.edges(data=True):

        # Don't clobber the original graph's data:
        data = copy.deepcopy(data)

        # Convert Neurokernel model to Neuroarch class. Don't need to save the
        # former explicitly because it can be inferred from the types
        # of the connected neurons, i.e., MorrisLecar is graded potential,
        # LeakyIAF is spiking:
        if data['model'] == 'AlphaSynapse':
            data['class'] = 'AlphaSynapse'
        elif data['model'] == 'power_gpot_gpot':
            data['class'] = 'PowerGPotGPot'
        else:
            raise ValueError('unrecognized synapse type')
        del data['model']

        # Don't need to save any NetworkX edge ID either:
        if 'id' in data:
            del data['id']

        new_id = gen_new_id()
        g_new.add_node(new_id, **data)
        g_new.add_edge(from_id, new_id, **{'class': 'SendsTo'})
        g_new.add_edge(new_id, to_id, **{'class': 'SendsTo'})

    # Connect output ports to the neurons that emit data through them:
    for from_id, to_id in edges_to_out_ports:
        g_new.add_edge(from_id, to_id, **{'class': 'SendsTo'})

    circuit_nodes = {}
    circuit_name_to_id = {}
    for id, data in g_new.nodes(data=True):

        # If the specified circuit attribute exists, use it to create a circuit
        # node and connect the neuron/synapse nodes to it:
        if circuit_attr in data:
            # Create new circuit node if the name hasn't been seen yet:
            if data[circuit_attr] not in circuit_name_to_id:
                new_id = gen_new_id()
                circuit_name_to_id[data[circuit_attr]] = new_id
                g_new.add_node(new_id, **{'name': data[circuit_attr],
                                                  'class': circuit_class})

                # Connect the LPU node to the new circuit node:
                g_new.add_edge(lpu_id, new_id, **{'class': 'Owns'})

            # Connect circuit node to current node:
            g_new.add_edge(circuit_name_to_id[data[circuit_attr]], id,
                           **{'class': 'Owns'})

        # Otherwise, connect the neurons/synapses to the LPU node directly:
        else:
            g_new.add_edge(lpu_id, id,
                           **{'class': 'Owns'})

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

    # Transformation:
    # 1. neuron node with no output edge to output port ->
    #    nonpublic neuron node
    # 2. neuron node with output edge to output port ->
    #    public neuron node
    # 3. input port -> input port
    # 4. synapse node -> synapse edge between preceding and
    #    succeeding transformed nodes
    g_new = nx.MultiDiGraph()
    synapses = {}
    synapse_keys = {}
    for id, data in g.nodes(data=True):

        # Don't clobber the original graph's data:
        data = copy.deepcopy(data)

        # Neuron models:
        if data['class'] in ['MorrisLecar', 'LeakyIAF',
                             'PhotoreceptorModel']:
            if data['class'] == 'PhotoreceptorModel':
                data['model'] = str('Photoreceptor')
            else:
                data['model'] = str(data['class'])

            # Find any output ports connected to the neuron model node:
            out_port_ids = [i for i in g.successors(id)
                            if g.nodes[i]['class'] == 'Port'
                            and g.nodes[i]['port_io'] == 'out']

            # A neuron is public if it is connected to an output port:
            if out_port_ids:

                # There should only be one output port per neuron, if
                # any (fan-out should be handled by the pattern):
                assert len(out_port_ids) == 1

                # Save the port's selector in the neuron node:
                data['selector'] = g.nodes[out_port_ids[0]]['selector']
                data['public'] = True
            else:
                data['public'] = False

            if data['class'] in ['MorrisLecar', 'PhotoreceptorModel']:
                data['spiking'] = False
            elif data['class'] == 'LeakyIAF':
                data['spiking'] = True

            # Not needed by Neurokernel:
            del data['class']

        # Ports:
        elif data['class'] == 'Port':
            if data['port_io'] == 'in':
                data['model'] = u'port_in_spk' if data['port_type'] == 'spike' \
                                else u'port_in_gpot'
                data['spiking'] = True if data['port_type'] == 'spike' else False
                data['public'] = False
                data['extern'] = False

                # Not needed by Neurokernel:
                del data['class']
                del data['port_type']
                del data['port_io']
            elif data['port_io'] == 'out':

                # Since output ports are represented by marking neuron nodes as
                # public, we don't recreate the port nodes in the new graph:
                continue

        # Synapses:
        elif data['class'] in ['AlphaSynapse', 'PowerGPotGPot']:

            # Convert Neuroarch class into Neurokernel model:
            if data['class'] == 'AlphaSynapse':
                data['model'] = u'AlphaSynapse'
            elif data['class'] == 'PowerGPotGPot':
                data['model'] = u'power_gpot_gpot'

            # Neurokernel synapse classes correspond to the following:
            # 0 = spike -> spike
            # 1 = spike -> gpot
            # 2 = gpot -> spike
            # 3 = gpot -> gpot

            # Find predecessors and successors of g.node[id] connected by
            # SendsTo edges; these two calls rely upon the fact that there can
            # be no multiple edges between any two endpoints in a
            # Neuroarch-compatible graph:
            pred_sendsto = \
                filter(lambda n: g.get_edge_data(n, id)[0]['class'] == 'SendsTo',
                       g.predecessors(id))
            succ_sendsto = \
                filter(lambda n: g.get_edge_data(id, n)[0]['class'] == 'SendsTo',
                       g.successors(id))
            if len(pred_sendsto) != 1 or len(succ_sendsto) != 1:
                raise ValueError('incorrectly connected synapse node')
            else:
                from_id = pred_sendsto[0]
                to_id = succ_sendsto[0]
                from_id_spike = (g.nodes[from_id]['class'] == 'LeakyIAF' or \
                                 (g.nodes[from_id]['class'] == 'Port' and \
                                  g.nodes[from_id]['port_type'] == 'spike'))
                to_id_spike = (g.nodes[to_id]['class'] == 'LeakyIAF' or \
                               (g.nodes[to_id]['class'] == 'Port' and \
                                g.nodes[to_id]['port_type'] == 'spike'))
                if from_id_spike and to_id_spike:
                    data['class'] = 0
                elif from_id_spike and not to_id_spike:
                    data['class'] = 1
                elif not from_id_spike and to_id_spike:
                    data['class'] = 2
                else:
                    data['class'] = 3

                # Add counter for new (from_id, to_id) pair to generate keys to
                # distinguish multiple synapses between the same endpoints:
                if (from_id, to_id) not in synapse_keys:
                    synapse_keys[(from_id, to_id)] = itertools.count()

                # Save nodes connected to synapse node so that an edge can be
                # created in the new graph:
                synapses[(from_id, to_id,
                          synapse_keys[(from_id, to_id)].next())] = data

                # Since synapse nodes are converted into edges in the
                # Neurokernel-compatible graph, don't recreate the current node
                # in the new graph if it is a synapse model:
                continue

        # Don't recreate non-neuron nodes in new graph:
        else:
            continue

        g_new.add_node(id, **data)

    # Create synapse edges:
    for (from_id, to_id, key), data in synapses.items():
        g_new.add_edge(from_id, to_id, **data)

    return g_new

def nk_pat_to_na(g, pat_name):
    """
    Transform a graph containing a Neurokernel-compatible pattern into
    one that conforms to Neuroarch's data model.

    Parameters
    ----------
    g : networkx.MultiDiGraph
        Graph containing Neurokernel-compatible pattern.
    pat_name : str
        Name of pattern.

    Returns
    -------
    result : networkx.MultiDiGraph
        Neuroarch-compliant graph of pattern.
    """

    assert isinstance(g, nx.MultiDiGraph)

    g_new = nx.MultiDiGraph()

    # Create pattern node:
    gen_new_id = itertools.count().next
    pat_id = gen_new_id()
    g_new.add_node(pat_id, **{'class': 'Pattern', 'name': pat_name})

    int_ids = {}
    sel_to_id = {}
    for i in [0, 1]:

        # Set each interface node's name to the number of the interface:
        int_ids[i] = gen_new_id()

        # Create interface nodes and connect pattern node to them:
        g_new.add_node(int_ids[i], **{'class': 'Interface', 'name': i})
        g_new.add_edge(pat_id, int_ids[i], **{'class': 'Owns'})

    # Create port nodes:
    for n, data in g.nodes(data=True):
        data = copy.deepcopy(data)

        id = gen_new_id()
        sel_to_id[n] = id
        g_new.add_node(id, **{'class': 'Port',
                                      'selector': n,
                                      'port_io': data['io'],
                                      'port_type': data['type']})
        g_new.add_edge(int_ids[int(data['interface'])], id,
                       **{'class': 'Owns'})

    # Create connections between ports in the two interfaces:
    for from_sel, to_sel in g.edges():
        g_new.add_edge(sel_to_id[from_sel], sel_to_id[to_sel],
                       **{'class': 'SendsTo'})

    return g_new

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

def na_lpu_to_nk_new(g):
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
#         if data['class'] in ['OmmatidiumModel', 'CartridgeModel', 'ColumnModel',
#                              'Interface', 'LPU']:
#             continue
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
