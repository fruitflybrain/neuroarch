import pdb

PORT_IN_GPOT = 'port_in_gpot'
PORT_IN_SPK = 'port_in_spk'

def lpu_db_parser(q):
    """
    neuroArch LPU specification parser.
    
    
    Parameters
    ----------
         q : query_result

    Returns
    -------
        n_dict : dict of dict of neuron
    Each key of `n_dict` is the name of a neuron model; the values
    are dicts that map each attribute name to a list that contains the
    attribute values for each neuron.
    s_dict : dict of dict of synapse
    Each key of `s_dict` is the name of a synapse model; the values are
    dicts that map each attribute name to a list that contains the
    attribute values for each each neuron.
    
    Example
    -------
    >>> n_dict = {'LeakyIAF': {'Vr': [0.5, 0.6], 'Vt': [0.3, 0.2]},
    'MorrisLecar': {'V1': [0.15, 0.16], 'Vt': [0.13, 0.27]}}

    Notes
    -----
        All neurons must have the following attributes; any additional 
    attributes for a specific neuron model must be provided 
    for all neurons of that model type:

        1. spiking - True if the neuron emits spikes, False if it emits graded
    potentials.
    2. model - model identifier string, e.g., 'LeakyIAF', 'MorrisLecar'
    3. public - True if the neuron emits output exposed to other LPUS.
    4. extern - True if the neuron can receive external input from a file.

        All synapses must have the following attributes:

        1. class - int indicating connection class of synapse; it may assume the
    following values:

            0. spike to spike synapse
    1. spike to graded potential synapse
    2. graded potential to spike synapse
    3. graded potential to graded potential synapse
    2. model - model identifier string, e.g., 'AlphaSynapse'
    3. conductance - True if the synapse emits conductance values, False if
    it emits current values.
    4. reverse - If the `conductance` attribute is True, this attribute
    should be set to the reverse potential.

        TODO
    ----
        Input data should be validated.
    """

        
        # parse neuron data
    n_dict = {}
    neurons = [ (x._rid, x.oRecordData) for x in \
                q._client.gremlin("g.v('%s').out.has('node_type','neuron')" % (q._rid))]
    
        
        # sort based on id (id is first converted to an integer)
    # this is done so that consecutive neurons of the same type 
    # in the constructed LPU is the same in neurokernel
    neurons.sort(cmp=neuron_cmp_str)
    rid_id_map = {}
    rid_model_id_map = {}
    for id, (rid, neu) in enumerate(neurons):
        try:
            del neu['in_']
        except:
            pass
        try:
            del neu['out_']
        except:
            pass
        try:
            del neu['node_type']
        except:
            pass
        model = neu['model']

        # if an output_port, make sure selector is specified
        if 'public' not in neu.keys():
            neu['public'] = False
        if 'selector' not in neu.keys():
            neu['selector'] = ''
        # if the neuron model does not appear before, add it into n_dict
        if model not in n_dict:
            n_dict[model] = {k:[] for k in neu.keys() + ['id']}
            rid_model_id_map[model] = {}

        # neurons of the same model should have the same attributes
        assert(set(n_dict[model].keys()) == set(neu.keys() + ['id']))

        # add neuron data into the subdictionary of n_dict
        for key in neu.keys():
            n_dict[model][key].append( neu[key] )

        rid_id_map[rid] = id
        rid_model_id_map[model][rid] = len(n_dict[model]['id'])
            
        n_dict[model]['id'].append( int(id) )

    # Process output ports
    out_ports = q._client.gremlin("t = new Table();g.v('%s').out.has('node_type','port').as('x').inE.has('edge_type', 'data').outV.has('node_type','neuron').as('y').table(t).iterate();t.flatten()" % (q._rid))
            
    it = iter(out_ports)
    out_ports = [(x.oRecordData['selector'], y.oRecordData['model'], y._rid) for (x,y) in [(x,it.next()) for x in it]]

    #pdb.set_trace()
    for sel, model, rid in out_ports:
        n_dict[model]['selector'][rid_model_id_map[model][rid]] = sel

    # Process input ports
    in_ports = [(x._rid, x.oRecordData) for x in q._client.gremlin("g.v('%s').out.has('node_type','port').as('x').outE.has('edge_type', 'data').inV.has('node_type','synapse').back('x')" % (q._rid))]
            
    for id, (rid, neu) in enumerate(in_ports):
        try:
            del neu['in_']
        except:
            pass
        try:
            del neu['out_']
        except:
            pass
        try:
            del neu['node_type']
        except:
            pass
        model = neu['model']
        assert('selector' in neu.keys())

        if model == PORT_IN_GPOT:
            neu['spiking'] = False
            neu['public'] = False
        else:
            neu['spiking'] = True
            neu['public'] = False

        # if an output_port, make sure selector is specified
        if 'public' not in neu.keys():
            neu['public'] = False
        if model not in n_dict:
            n_dict[model] = {k:[] for k in neu.keys() + ['id']}
            rid_model_id_map[model] = {}

        # neurons of the same model should have the same attributes
        assert(set(n_dict[model].keys()) == set(neu.keys() + ['id']))

        # add neuron data into the subdictionary of n_dict
        for key in neu.keys():
            n_dict[model][key].append( neu[key] )

        rid_id_map[rid] = id
        rid_model_id_map[model][rid] = len(n_dict[model]['id'])
            
        n_dict[model]['id'].append( int(id) )


        
    # remove duplicate model information
    for val in n_dict.values(): val.pop('model')
    if not n_dict: n_dict = None

    synapses = q._client.gremlin("t = new Table();g.v('%s').out.has('node_type','synapse').as('x').inE.has('edge_type', 'data').outV.has('node_type','neuron').id.as('y').back('x').outE.has('edge_type','data').inV.has('node_type','neuron').id.as('z').table(t).iterate();t.flatten()" % (q._rid)) 
    it = iter(synapses)

    #pdb.set_trace()
    synapses = [ (rid_id_map[it.next().get()], rid_id_map[it.next().get()], x.oRecordData) for x in it]
        
        
    # parse synapse data
    s_dict = {}
    synapses.sort(cmp=synapse_cmp)
    scnt = 0
    for syn in synapses:
        # syn[0/1]: pre-/post-neu id; syn[2]: dict of synaptic data
        model = syn[2]['model']
        syn[2]['id'] = scnt
        # if the synapse model does not appear before, add it into s_dict
        if model not in s_dict:
            s_dict[model] = {k:[] for k in syn[2].keys() + ['pre', 'post']}

        # synapses of the same model should have the same attributes
        assert(set(s_dict[model].keys()) == set(syn[2].keys() + ['pre', 'post']))
        # add synaptic data into the subdictionary of s_dict
        for key in syn[2].keys():
            s_dict[model][key].append(syn[2][key])
        s_dict[model]['pre'].append(syn[0])
        s_dict[model]['post'].append(syn[1])
        scnt += 1
    for val in s_dict.values():
        val.pop('model')
    if not s_dict:
        s_dict = {}
    return n_dict, s_dict
                            

def neuron_cmp_str(x, y):
    if (x[0]) < (y[0]):
        return -1
    elif (x[0]) > (y[0]):
        return 1
    else:
        return 0

def synapse_cmp(x, y):
    if int(x[1]) < int(y[1]):
        return -1
    elif int(x[1]) > int(y[1]):
        return 1
    else:
        return 0
