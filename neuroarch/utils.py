#!/usr/bin/env python

"""
Utility functions.
"""

# Copyright (c) 2015, Lev Givon
# All rights reserved.
# Distributed under the terms of the BSD license:
# http://www.opensource.org/licenses/bsd-license

import itertools
import re
from functools import wraps
import time

import numpy as np
from numpy import iterable

import pyorient
from pyorient.otypes import OrientRecordLink, OrientBinaryObject

def _iterable(x):
    try:
        iter(x)
    except:
        return False
    else:
        return True

def is_rid(rid):
    """
    Returns True if the specified string is a well-formatted OrientDB RID.
    """

    if isinstance(rid, str) and re.search('^\#\d+\:\d+$', rid):
        return True
    else:
        return False

def orientrecord_to_dict(r):
    """
    Convert a pyorient.otypes.OrientRecord instance into a dict.
    """

    def rec(d):
        if isinstance(d, dict):
            out = {}
            for k in d:
                if not isinstance(d[k], OrientBinaryObject):
                    out[k] = rec(d[k])
            return out
        elif _iterable(d) and not isinstance(d, str):
            return d.__class__(map(rec,
                [x for x in d if not isinstance(d, OrientBinaryObject)]))
        elif isinstance(d, OrientRecordLink):
            return d.get_hash()
        elif isinstance(d, OrientBinaryObject):

            # This should never be reached:
            return None
        else:
            return d

    if r.oRecordData:
        out = rec(r.oRecordData)
    else:
        out = {}
    return out

def reconnect_graph(graph):
    """
    Reconnect a pyorient.ogm.Graph instance whose connection has stopped working.
    """

    config = graph.config
    graph.client = pyorient.OrientDB(config.host, config.port)
    graph.client.connect(config.user, config.cred)

    graph.config = config

    if config.initial_drop:
        graph._last_db = self._last_user = self._last_cred = None
        graph.drop()

    db_name = config.db_name
    if db_name:
        graph.open(db_name, config.storage, config.user, config.cred)

    graph.registry = {}
    # Maps property dict from database to added class's property dict
    graph.props_from_db = {}

    graph.scripts = config.scripts or pyorient.Scripts()

def chunks(it, n):
    """
    Generator that returns chunks of size `n` of an iterable `it`.
    """

    it = iter(it)
    while True:
        p = list(itertools.islice(it, n))
        if not p:
            break
        yield p

def get_cluster_ids(client):
    """
    Get cluster IDs associated with each OrientDB class name.
    """

    result = client.query('select name,clusterIds from '
                          '(select expand(classes) from metadata:schema) '
                          'limit -1')
    return {r.oRecordData['name']: r.oRecordData['clusterIds'] \
            for r in result}

def rand_bin_matrix(sh, N, dtype=np.double):
    """
    Generate a rectangular binary matrix with randomly distributed nonzero entries.

    Parameters
    ----------
    sh : tuple
        Shape of generated matrix.
    N : int
        Number of entries to set to 1.
    dtype : dtype
        Generated matrix data type.

    Returns
    -------
    r : numpy.ndarray
        Generated 2D binary array.

    Examples
    --------
    >>> m = rand_bin_matrix((2, 3), 3)
    >>> set(m.flatten()) == set([0, 1])
    True

    """

    result = np.zeros(sh, dtype)
    indices = np.arange(result.size)
    indices = np.random.shuffle(indices)
    result.ravel()[indices[:N]] = 1
    return result

def byteify(input):
    """
    Convert Unicode strings in JSON to byte strings.
    """

    if isinstance(input, dict):
        return {byteify(key): byteify(value) for key, value in input.items()}
    elif isinstance(input, list):
        return [byteify(element) for element in input]
    elif isinstance(input, str):
        return input.encode('utf-8')
    else:
        return input

def class_method_timer(func):
    @wraps(func)
    def wrapper_timer(self, *args, **kwargs):
        debug = False
        if 'debug' in kwargs:
            debug = kwargs.get('debug', False)
        else:
            if getattr(self, 'debug', False):
                debug = True
        if debug:
            start_time = time.time()
        value = func(self, *args, **kwargs)
        if debug:
            end_time = time.time()
            run_time = end_time - start_time
            print("Finished '{}' in {} secs".format(func.__name__, run_time))
        return value
    return wrapper_timer
