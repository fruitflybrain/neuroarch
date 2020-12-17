#!/usr/bin/env python

import collections
import numbers
import pprint
import re
from datetime import datetime
import json
import time
import itertools
import copy
from tqdm import tqdm
import pdb
import os
from warnings import warn

import numpy as np
import networkx as nx
import pandas as pd
from pyorient.ogm import Graph, Config
from pyorient.serializations import OrientSerialization

from neuroarch.utils import is_rid, iterable, chunks
from neuroarch.diff import diff_nodes, diff_edges
from neuroarch.apply_diff import apply_node_diff, apply_edge_diff
from neuroarch.query import QueryWrapper, QueryString, _kwargs
import neuroarch.models as models

special_char = set("*?+\.()[]|{}^$'")

def replace_special_char(text):
    return ''.join(['\\'+s if s in special_char else s for s in text])


def connect(host, db_name, port = 2424, user = 'admin', password = 'admin', initial_drop = False):
    # graph = Graph(Config.from_url(url, user, password, initial_drop))
    graph = Graph(Config('localhost', port, user, password, db_name,
                         'plocal', initial_drop = initial_drop,
                         serialization_type=OrientSerialization.CSV))
    if initial_drop:
        graph.create_all(models.Node.registry)
        graph.create_all(models.Relationship.registry)
    else:
        graph.include(models.Node.registry)
        graph.include(models.Relationship.registry)
    return graph

class NotWriteableError(Exception):
    """NeuroArch not writeable error"""
    pass


class DuplicateNodeError(Exception):
    """NeuroArch got duplicate nodes"""
    pass

class NodeAlreadyExistError(Exception):
    """NeuroArch node with the same property already exsits"""
    pass

class RecordNotFoundError(Exception):
    """Cannot find the data in NeuroArch database"""
    pass

class NodeAlreadyExistWarning(Warning):
    pass

class DuplicateNodeWarning(Warning):
    """NeuroArch got duplicate nodes"""
    pass


relations = {'Neuropil': {'Neuron': 'Owns',
                          'MorphologyData': 'HasData',
                          'Subregion': 'Owns'},
             'Subsystem': {'Neuropil': 'Owns'},
             'Subregion': {'MorphologyData': 'HasData'},
             'Neuron': {'Neuropil': 'ArborizesIn',
                        'Subregion': 'ArborizesIn',
                        'Synapse': 'SendsTo',
                        'InferredSynapse': 'SendsTo',
                        'MorphologyData': 'HasData',
                        'NeurotransmitterData': 'HasData',
                        'GeneticData': 'HasData',
                        'ArborizationData': 'HasData'},
             'Synapse': {'Neuron': 'SendsTo',
                          'MorphologyData': 'HasData',
                          'GeneticData': 'HasData',
                          'ArborizationData': 'HasData'},
             'InferredSynapse': {'Neuron': 'SendsTo',
                                  'MorphologyData': 'HasData',
                                  'GeneticData': 'HasData',
                                  'ArborizationData': 'HasData'},
             'DataSource': {'Neuropil': 'Owns',
                            'Neuron': 'Owns',
                            'Subregion': 'Owns',
                            'Synapse': 'Owns',
                            'InferredSynapse': 'Owns',
                            'MorphologyData': 'Owns',
                            'NeurotransmitterData': 'Owns',
                            'GeneticData': 'Owns',
                            'ArborizationData': 'Owns'}
             }

def _to_var_name(s):
    """
    Remove,hyphens,slashes,whitespace in string so that it can be
    used as an OrientDB variable name.
    """
    r = s.replace("'",'prime')
    table = str.maketrans(dict.fromkeys('.,!?_ -/<>{}[]()+-=*&^%$#@!`~.\|;:"'))
    chars_to_remove = ['.', '!', '?', '_', '-', '/', '>', '<', '(', ')', '+', '-', '*', ',', '?', ':', ';', '"', '[', ']', '{', '}', '=', '^', '%', '$', '#', '@', '!', '`', '~']
    r = r.translate(table)
    if len(r) and r[0].isdigit():
        r = 'a'+r
    return r

class NeuroArch(object):
    def __init__(self, host, db_name, port = 2424, user = 'root', password = 'root', mode = 'r',
                 debug = False):
        """
        Create or connect to a NeuroArch object for database access.

        Parameters
        ----------
        host : str
            IP of the host
        db_name : str
            name of the database to connect to or create
        port : int
            binary port of the OrientDB server
        user : str
            user name to access the database
        password : str
            password to access the database
        mode : str
            'r': read only
            'w': read/write on existing database, if does not exist, one is created.
            'o': read/write and overwrite any of the existing data in the database.
        debug : bool
            Whether the queries are done in debug mode
        """
        if mode == 'r':
            initial_drop = False
            self._allow_write = False
        elif mode == 'o':
            initial_drop = True
            self._allow_write = True
        elif mode == 'w':
            initial_drop = False
            self._allow_write = True
        else:
            raise ValueError("""Database mode must be either read ('r'),
                              write ('w'), or overwrite ('o').""")
        self.graph = connect(host, db_name, port = port,
                             user = user, password = password,
                             initial_drop = initial_drop)
        self._debug = debug
        self._default_DataSource = None
        # self._cache = {'DataSource': {},
        #                'Neuropil': {},
        #                'Neuron': {},
        #                'Synapse': {},
        #                'Subsystem': {},
        #                'Tract': {},
        #                'Subregion': {}
        #                }
        self._cache = {}
        self._check = True

    def get(self, cls, name, data_source, **attr):
        """
        Retrieve an object with name under data_source,
        either from cache or from database.

        Parameters
        ----------
        cls : str
            Type of the Node to be retrieved.
        name : str
            Name to be retrieved
        data_source : neuroarch.models.DataSource or None
            The DataSource under which the unique object will be retrieved
            If None, the searched object is not bound to the DataSource.
        attr : keyword arguments (optional)
            node attributes using key=value, currently not implemented

        Returns
        -------
        obj : neuroarch.models.Node subclass
            The object retrieved
        """
        #ds = self._default_DataSource if data_source is None else data_source
        if data_source is None:
            cache = self._cache.setdefault(cls, {})
        else:
            cache = self._cache.setdefault(data_source._id, {}).setdefault(cls, {})

        try:
            return cache[name]
        except KeyError:
            if cls in ['Neuron', 'Synapse']:
                q = self._find(cls, data_source, uname = name)
            else:
                q = self._find(cls, data_source, name = name)
            if len(q) == 1:
                obj = q.nodes_as_objs[0]
                if data_source is None:
                    tmp = q.owned_by(cls = 'DataSource', cols = '@rid')
                    if len(tmp) == 1:
                        ds_rid = list(tmp.nodes)[0].oRecordData['rid'].get_hash()
                        self.set(cls, name, obj, ds_rid)
                    elif len(tmp) > 1:
                        raise ValueError('unexpected more than 1 DataSource found')
                    else:
                        self.set(cls, name, obj, None)
                else:
                    self.set(cls, name, obj, None)
            elif len(q) > 1:
                raise ValueError('Hit more than one instance of {} with name {} in database.'.format(cls, name))
            else:
                raise RecordNotFoundError('{} {} not found in database.'.format(cls, name))
        return obj

    def set(self, cls, name, value, data_source):
        """
        Set an entry in the local database cache.

        Parameters
        ----------
        cls : str
            The class type of the record, (e.g., 'Neuropil')
        name : str
            The unique name of the node under the data_source.
            It will be used to key the cached item.
        value : models.Node subclasses
            The object for the database record to be cached.
        data_source : models.DataSource
            The DataSource under which name can be uniquely found.
        """
        # if cls not in self._cache:
        #     self._cache[cls] = {}
        # self._cache[cls][name] = value
        #ds = self._default_DataSource if data_source is None else data_source
        if data_source is None:
            self._cache.setdefault(cls, {})[name] = value
        elif isinstance(data_source, models.Node):
            self._cache.setdefault(data_source._id, {}).setdefault(cls, {})[name] = value
        elif isinstance(data_source, str) and data_source.startswith('#'):
            self._cache.setdefault(data_source, {}).setdefault(cls, {})[name] = value
        else:
            raise ValueError('data_source specification unknown.')

    def disable_check(self):
        self._check = False
        print("Disabling database check before write.")

    def enable_check(self):
        self._check = True
        print("Enabling database check before write.")

    def sql_query(self, query_text, edges = False):
        """
        Query NeuroArch database with a SQL query

        Parameters
        ----------
        query_text : str
            SQL Query string. The string will parsed as is, make sure
            that special characters are correctly treated.
        edges : bool
            Indicate whether query should also return all edges in addition
            to the nodes. (default: False).

        Returns
        -------
        q : QueryWrapper
            Result of the query.
        """
        q = QueryWrapper(self.graph, QueryString(query_text, 'sql'),
                         edges = edges, debug = self._debug)
        return q

    def exists(self, cls, **attr):
        """
        Check if data exists in the database.
        Program will search in the cache first, if no hit, will query database.

        Parameters
        ----------
        cls : str
             Node class or classes to retrieve.
        attr : keyword arguments, optional
            node attributes using key=value.

        Returns
        -------
        q : bool
            Indicate if such a node exists.
        """
        query_str = """select from {} where """.format(cls) + \
                    " and ".join(["""{} = {}""".format(
                                  key, """\"{}\"""".format(value) if isinstance(value, str) else value) \
                                  for key, value in attr.items() if value is not None])
        #print(query_str)
        q = self.sql_query(query_str)
        return len(q) > 0

    def exists1(self, cls, **attr):
        """
        Check if data exists in the database.
        Program will search in the cache first, if no hit, will query database.

        Parameters
        ----------
        cls : str
             Node class or classes to retrieve.
        attr : keyword arguments, optional
            node attributes using key = value.

        Returns
        -------
        q : bool
            Indicate if such a node exists.
        """
        nodes = getattr(self.graph, getattr(models, cls).element_plural).query(**attr).all()
        return len(nodes) > 0

    def find(self, cls, **attr):
        """
        Find all instances in the database that meets the criteria.

        Parameters
        ----------
        cls : str
             Node class or classes to retrieve.
        attr : keyword arguments, optional
            node attributes using key=value.

        Returns
        -------
        nodes : list
            Nodes that are found.
        """
        query_str = """select from {} where """.format(cls) + \
                    " and ".join(["""{} = {}""".format(
                                  key, """\"{}\"""".format(value) if isinstance(value, str) else value) \
                                  for key, value in attr.items() if value is not None])
        q = self.sql_query(query_str)
        return q

    def find_objs(self, cls, **attr):
        """
        Find all instances in the database that meets the criteria.

        Parameters
        ----------
        cls : str
             Node class or classes to retrieve.
        attr : keyword arguments, optional
            node attributes using key=value.

        Returns
        -------
        nodes : list
            Nodes that are found.
        """
        nodes = getattr(self.graph, getattr(models, cls).element_plural).query(**attr).all()
        return nodes

    def _find(self, cls, data_source, **attr):
        """
        Find all cls objects under the data_source that match all the attr attributes
        Parameters
        ----------
        cls : str
            Node class or classes to retrieve.
        data_source : neuroarch.models.DataSource
            The DataSource to search from.
        attr : keyword arguments, optional
            Node attributes using key=value.

        Returns
        -------
        nodes : list
            Nodes that are found.
        """
        sub_query = """select from {} where """.format(cls) + \
                    " and ".join(["""{} = {}""".format(
                                  key, """\"{}\"""".format(value) if isinstance(value, str) else value) \
                                  for key, value in attr.items() if value is not None])
        if data_source is None:
            q = self.sql_query(sub_query)
        else:
            q = self.sql_query(
                """ select from ({sub_query}) \
                let $q = (select from (select expand($parent.$parent.current.in('Owns')))
                where @class='DataSource' and @rid = {rid}) \
                where $q.size() = 1""".format(sub_query = sub_query, rid = data_source._id))
        return q

    def _is_in_datasource(self, data_source, obj):
        """
        Check if the obj is owned by the data_source.

        Parameters
        ----------
        data_source : neuroarch.models.DataSource
            The data_source to be searched for
        obj : neuroarch.models.{}
            An instance of NeuroArch OGM class
        """
        q = self.sql_query(
            """ select @rid from (select expand(in(Owns)) from {obj_rid}) \
            where @class = 'DataSource' and @rid = {rid}""".format(
                obj_rid = obj._id, ds_rid = data_source._id))
        return len(q) > 0

    def _database_writeable_check(self):
        if not self._allow_write:
            raise NotWriteableError('NeuroArch not writable, please intiantiate NeuroArch with mode = "w" or mode = "o"')

    def _uniqueness_check(self, cls, unique_in = None, **attr):
        """
        Defines the uniqueness criteria of different types of nodes.
        """
        # under the same datasource, only 1 subsystem, 1 neuropil, 1 tract of the name can exist
        # under the same neuropil, only 1 neuron of the name can exist
        # multiple (collections of) synapses can exist between two neurons
        if cls == 'Species':
            tmp = self.sql_query(
                """select from Species where (name = "{name}" or "{name}" in synonyms) and stage = "{stage}"  and sex = "{sex}" """.format(
                    name = attr['name'], stage = attr['stage'], sex = attr['sex']))
            if len(tmp):
                objs = tmp.nodes_as_objs
                if attr['name'] in [obj.name for obj in objs]:
                    raise NodeAlreadyExistError("""Species {name} at {stage} stage ({sex}) already exists""".format(
                        name = attr['name'], stage = attr['stage'], sex = attr['sex']))
                else:
                    for obj in objs:
                        if attr['name'] in obj.synonyms:
                            raise NodeAlreadyExistError(
                                """Species {name} (as its synonym) at {stage} stage ({sex}) already exists, use name {formalname} instead""".format(
                                name = attr['name'], stage = attr['stage'], sex = attr['sex'], formalname = obj.name))
        elif cls == 'DataSource':
            objs = self.find_objs('DataSource', name=attr['name'], version=attr['version'])
            #if self.exists(cls, name = attr['name'], version = attr['version']):
            if len(objs):
                raise NodeAlreadyExistError("""{} Node with attributes {} already exists""".format(
                                cls, ', '.join(["""{} = {}""".format(key, value) \
                                for key, value in attr.items()])))
        elif cls == 'Neurotransmitter':
            tmp = self.sql_query(
                """select from Neurotransmitter where name = "{name}" or "{name}" in synonyms""".format(
                    name = attr['name']))
            if len(tmp):
                objs = tmp.nodes_as_objs
                if attr['name'] in [obj.name for obj in objs]:
                    raise NodeAlreadyExistError("""Neurotransmitter {name} already exists""".format(
                        name = attr['name']))
                    return objs
                else:
                    for obj in objs:
                        if attr['name'] in obj.synonyms:
                            raise NodeAlreadyExistError(
                                """Neurotransmitter {name} (as its synonym) already exists, use name {formalname} instead""".format(
                                name = attr['name'], formalname = obj.name))
        elif cls in ['Subsystem', 'Neuropil', 'Subregion', 'Tract']:
            # TODO: synonyms are not checked against existing names and synonyms
            if not isinstance(unique_in, models.DataSource):
                raise TypeError('To check the uniqueness of a {} instance, unique_in must be a DataSource object'.format(cls))
            tmp = self.sql_query(
                """select from (select from {cls} where name = "{name}" or "{name}" in synonyms) let $q = (select from (select expand($parent.$parent.current.in('Owns'))) where @class='{ucls}' and @rid = {rid}) where $q.size() = 1""".format(
                    rid = unique_in._id, cls = cls, name = attr['name'], ucls = unique_in.element_type))
            if len(tmp):
                objs = tmp.nodes_as_objs
                if attr['name'] in [obj.name for obj in objs]:
                    raise NodeAlreadyExistError("""{cls} {name} already exists under DataSource {ds} version {version}""".format(
                        cls = cls, name = attr['name'],
                        ds = unique_in.name,
                        version = unique_in.version))
                else:
                    for obj in objs:
                        if attr['name'] in obj.synonyms:
                            raise NodeAlreadyExistError(
                                """{cls} {name} already exists as a synonym of {cls} {formalname} under DataSource {ds} version {version}""".format(
                                cls = cls, name = attr['name'], formalname = obj.name,
                                ds = unique_in.name,
                                version = unique_in.version))
            # Alternatively, try:
            # tmp = self.sql_query(
            #     """select from {cls} where name = "{name}" or "{name}" in synonyms""".format(
            #         cls = cls, name = attr['name']))
            # ds = tmp.owned_by(cls = 'DataSource').has(rid = datasource)
            # if len(ds):
            #     tmp1 = tmp.has(name = attr['name'])
            #     if len(tmp1.owned_by(cls = 'DataSource').has(rid = datasource)):
            #         raise NodeAlreadyExistError("""{cls} {name} already exists under DataSource {ds} version {version}""".format(
            #             cls = cls, name = attr['name'],
            #             ds = datasource.name,
            #             version = datasource.version))
            #     else:
            #         all_synonym_objs = (tmp - tmp1).nodes_as_objs
            #         for obj in objs:
            #             if len(QueryWrapper.from_rids(obj._id).has(cls = 'DataSource').has(rid = datasource)):
            #                 raise NodeAlreadyExistError(
            #                     """{cls} {name} already exists as a synonym of {cls} {formalname} under DataSource {ds} version {version}""".format(
            #                     cls = cls, name = attr['name'], formalname = obj.name,
            #                     ds = datasource.name,
            #                     version = datasource.version))

            # Alternatively 2, try: (will be slow when it has a lot of Owns edges)
            # tmp = sql_query(
            #     """
            #     select from (select expand(out('Owns')[@class = "{cls}"]) from {rid}) where name = "{name}" or "{name}" in synonyms
            #     """
            # )
        # elif cls in ['Subregion']:
        #     if not isinstance(unique_in, models.Neuropil):
        #         raise TypeError('To check the uniqueness of a {} instance, unique_in must be a Neuropil object'.format(cls))
        #     tmp = self.sql_query(
        #         """select from (select from {cls} where name = "{name}" or "{name}" in synonyms) let $q = (select from (select expand($parent.$parent.current.in('Owns'))) where @class='ucls' and @rid = {rid}) where $q.size() = 1""".format(
        #             rid = unique_in._id, cls = cls, name = attr['name'], ucls = unique_in.element_type))
        #     if len(tmp):
        #         objs = tmp.nodes_as_objs
        #         if attr['name'] in [obj.name for obj in objs]:
        #             raise NodeAlreadyExistError("""{cls} {name} already exists under Neuropil {ds}""".format(
        #                 cls = cls, name = attr['name'],
        #                 ds = unique_in.name))
        #         else:
        #             for obj in objs:
        #                 if name in obj.synonyms:
        #                     raise NodeAlreadyExistError(
        #                         """{cls} {name} already exists as a synonym of {cls} {formalname} under Neuropil {ds}""".format(
        #                         cls = cls, name = attr['name'], formalname = obj.name,
        #                         ds = unique_in.name))
        elif cls in ['Neuron']:
            # TODO: synonyms are not checked against existing names and synonyms
            if not isinstance(unique_in, models.DataSource):
                raise TypeError('To check the uniqueness of a {} instance, unique_in must be a DataSource object'.format(cls))
            tmp = self.sql_query(
                """select from (select from {cls} where uname = "{name}") let $q = (select from (select expand($parent.$parent.current.in('Owns'))) where @class='{ucls}' and @rid = {rid}) where $q.size() = 1""".format(
                    rid = unique_in._id, cls = cls, name = attr['name'], ucls = unique_in.element_type))
            if len(tmp):
                objs = tmp.nodes_as_objs
                raise NodeAlreadyExistError("""{cls} {name} already exists under DataSource {ds} version {version}""".format(
                    cls = cls, name = attr['name'],
                    ds = unique_in.name,
                    version = unique_in.version))
        elif cls == 'ArborizationData':
            if not isinstance(unique_in, (models.Neuron, models.Synapse)):
                raise TypeError('To check the uniqueness of a ArborizationData instance, unique_in must be a Neuron or a Synapse object')
            tmp = self.sql_query(
                """select from (select expand(out(HasData)) from {rid}) where @class = 'ArborizationData' """.format(rid = unique_in._id))
            if len(tmp):
                raise NodeAlreadyExistError("""ArborizationData already exists for {node} {uname}. Use NeuroArch.update_{node}_arborization to update the record""".format(
                    node = unique_in.element_type.lower(), uname = unique_in.uname))
        else:
            raise TypeError('Model type not understood.')
        return True

    @property
    def default_DataSource(self):
        if self._default_DataSource is None:
            raise ValueError('Please either specify a DataSource or specify a default DataSource')
        else:
            return self._default_DataSource

    @default_DataSource.setter
    def default_DataSource(self, data_source):
        """
        set default DataSource so they don't need to be passed every time
        a component is created.

        Parameters
        ----------
        data_source : pyorient obj
            containing an pyorient vertex class
        """
        self._default_DataSource = data_source
        print("Setting default DataSource to {} version {}".format(
                            data_source.name,
                            getattr(data_source, 'version', 'not specified')))

    @default_DataSource.deleter
    def default_DataSource(self):
        print("removing default DataSource")
        self._default_DataSource = None

    def add_species(self, name, stage, sex, synonyms = None):
        """
        Add a Species.

        Parameters
        ----------
        name : str
            Name of the species.
        stage : str
            Development stage of the species.
        synonyms : list of str
            Other names used by the species.

        Returns
        -------
        species : models.Species
            The created species record.
        """
        assert isinstance(name, str), 'name must be of str type'
        assert isinstance(stage, str), 'stage must be of str type'
        assert isinstance(sex, str), 'sex must be of str type'
        self._database_writeable_check()
        self._uniqueness_check('Species', name = name, stage = stage, sex = sex)

        if synonyms is None:
            species = self.graph.Species.create(name = name,
                                                stage = stage,
                                                sex = sex)
        else:
            species = self.graph.Species.create(name = name,
                                                stage = stage,
                                                sex = sex,
                                                synonyms = synonyms)
        self._cache['Species'] = species
        return species

    def add_DataSource(self, name, version,
                       url = None, description = None,
                       species = None):
        """
        Add a DataSource.

        Parameters
        ----------
        name : str
            Name of the DataSource.
        version : str
            Version of the Dataset.
        url : str
            Web URL describing the origin of the DataSource
        description : str
            A brief description of the DataSource
        species : dict or models.Species
            The species the added DataSource is for.
            If species is a dict, it must be contain the following keys:
                {'name': str,
                 'stage': str,
                 'synonyms': list of str (optional)
                }

        Returns
        -------
        datasource : models.DataSource
            created DataSource object
        """
        assert isinstance(name, str), 'name must be of str type'
        assert isinstance(version, str), 'version must be of str type'
        self._database_writeable_check()
        self._uniqueness_check('DataSource', name = name, version = version)

        ds_info = {'name': name, 'version': version}
        if isinstance(url, str):
            ds_info['url'] = url
        else:
            if url is not None:
                raise TypeError('url must be of str type')
        if isinstance(description, str):
            ds_info['description'] = description
        else:
            if description is not None:
                raise TypeError('description must be of str type')

        datasource = self.graph.DataSources.create(**ds_info)
        self.set('DataSource', name, datasource, data_source = datasource)

        if species is not None:
            if isinstance(species, models.Species):
                species_obj = species
            elif isinstance(species, dict):
                tmp = self.sql_query(
                    """select from Species where (name = "{name}" or "{name}" in synonyms) and stage = "{stage}" """.format(
                        name = species['name'], stage = species['stage']))
                if len(tmp) == 1:
                    species_obj = tmp.nodes_as_objs[0]
                elif len(tmp) > 1: # most likely will not occur
                    raise ValueError(
                        'Multiple Species nodes with name = {name} and stage = {stage} exists'.format(
                            name = species['name'], stage = species['stage']))
                else: # 0 hit
                    species_obj = self.add_species(
                                        species['name'], species['stage'],
                                        synonyms = species.get('synonyms', None))
            else:
                raise TypeError('Parameter species must be either a str or a Species object.')
            self.graph.Owns.create(species, datasource)
        return datasource

    def add_Subsystem(self, name, synonyms = None,
                      morphology = None, data_source = None):
        """
        Create a Subsystem record and link it to related node types.

        Parameters
        ----------
        name : str
            Name of the subsystem
            (abbreviation is preferred, full name can be given in the synonyms)
        synonyms : list of str
            Synonyms of the subsystem.
        morphology : dict (optional)
            Morphology of the neuropil boundary specified with a triangulated mesh,
            with fields
                'vertices': a single list of float, every 3 entries specify (x,y,z) coordinates.
                'faces': a single list of int, every 3 entries specify samples of vertices.
            Or, specify the file path to a json file that includes the definition of the mesh.
            Or, specify only a url which can be readout later on.
        data_source : neuroarch.models.DataSource (optional)
            The datasource. If not specified, default DataSource will be used.

        Returns
        -------
        neuroarch.models.Subsystem
            Created Subsystem object
        """
        assert isinstance(name, str), 'name must be of str type'
        self._database_writeable_check()
        connect_DataSource = self._default_DataSource if data_source is None else data_source
        self._uniqueness_check('Subsystem', unique_in = connect_DataSource,
                               name = name)

        subsystem_info = {'name': name}
        if isinstance(synonyms, list) and all(isinstance(n, str) for n in synonyms):
            subsystem_info['synonyms'] = synonyms
        else:
            if synonyms is not None:
                raise TypeError('synonyms must be a list of str')

        batch = self.graph.batch()
        node_name = _to_var_name('Subsystem_{}'.format(name))
        batch[node_name] = batch.Subsystems.create(**subsystem_info)

        # Link data_source
        self.link_with_batch(batch, connect_DataSource, batch[:node_name],
                             'Owns')
        subsystem = batch['${}'.format(node_name)]
        batch.commit(20)

        if morphology is not None:
            self.add_morphology(subsystem, morphology, data_source = connect_DataSource)
        self.set('Subsystem', name, subsystem, data_source = connect_DataSource)
        return subsystem

    def add_Neuropil(self, name,
                     synonyms = None,
                     subsystem = None,
                     morphology = None,
                     data_source = None):
        """
        Create a Neuropil record and link it to related node types.

        Parameters
        ----------
        name : str
            Name of the neuropil
            (abbreviation is preferred, full name can be given in the synonyms)
        synonyms : list of str
            Synonyms of the neuropil.
        subsystem : str or neuroarch.models.Subsystem (optional)
            Subsystem that owns the neuropil. Can be specified either by its name
            or the Subsytem object instance.
        morphology : dict (optional)
            Morphology of the neuropil boundary specified with a triangulated mesh,
            with fields
                'vertices': a single list of float, every 3 entries specify (x,y,z) coordinates.
                'faces': a single list of int, every 3 entries specify samples of vertices.
            Or, specify the file path to a json file that includes the definition of the mesh.
            Or, specify only a url which can be readout later on.
        data_source : neuroarch.models.DataSource (optional)
            The datasource. If not specified, default DataSource will be used.

        Returns
        -------
        neuroarch.models.Neuropil
            Created Neuropil object
        """
        assert isinstance(name, str), 'name must be of str type'
        self._database_writeable_check()
        connect_DataSource = self._default_DataSource if data_source is None else data_source
        self._uniqueness_check('Neuropil', unique_in = connect_DataSource,
                               name = name)

        neuropil_info = {'name': name}
        if isinstance(synonyms, list) and all(isinstance(n, str) for n in synonyms):
            neuropil_info['synonyms'] = synonyms
        else:
            if synonyms is not None:
                raise TypeError('synonyms must be a list of str')

        batch = self.graph.batch()
        node_name = _to_var_name('Neuropil_{}'.format(name))
        batch[node_name] = batch.Neuropils.create(**neuropil_info)
        # Link subsystem if specified
        if subsystem is not None:
            if isinstance(subsystem, str):
                subsystem_obj = self.get('Subsystem', subsystem,
                                         connect_DataSource)
                # try:
                #     subsystem_obj = self._cache['Subsystem'][subsystem]
                # except KeyError:
                #     subsystem_objs = self.find_objs('Subsystem', name = subsystem)
                #     if len(subsystem_obj) == 0:
                #         raise ValueError('Subsystem {} not found in database.'.format(subsystem))
                #     else:
                #         subsytem_obj = subsystem_objs[0]
            elif isinstance(subsystem, models.Subsystem):
                if self._is_in_datasource(connect_DataSource, subsystem):
                    subsystem_obj = subsystem
                else:
                    raise ValueError(
                        'Subsystem {} with rid {} to be linked with neuropil is \
                        not in the same datasource {} version {}'.format(
                            subsystem.name, subsystem._id,
                            connect_DataSource.name, connect_DataSource.version))
            self.link_with_batch(batch, subsystem_obj, batch[:node_name],
                                 'Owns')

        # Link data_source
        self.link_with_batch(batch, connect_DataSource, batch[:node_name],
                             'Owns')

        neuropil = batch['${}'.format(node_name)]
        batch.commit(20)

        # Link morphology data separately because they may be too large for
        # batch process
        if morphology is not None:
            self.add_morphology(neuropil, morphology, data_source = connect_DataSource)
        self.set('Neuropil', name, neuropil, data_source = connect_DataSource)
        return neuropil

    def add_Subregion(self, name,
                      synonyms = None,
                      neuropil = None,
                      morphology = None,
                      data_source = None):
        """
        Create a Subregion record and link it to related node types.

        Parameters
        ----------
        name : str
            Name of the subregion
            (abbreviation is preferred, full name can be given in the synonyms)
        synonyms : list of str
            Synonyms of the synonyms.
        neuropil : str or neuroarch.models.Neuropil (optional)
            Neuropil that owns the subregion. Can be specified either by its name
            or the Neuropil object instance.
        morphology : dict (optional)
            Morphology of the neuropil boundary specified with a triangulated mesh,
            with fields
                'vertices': a single list of float, every 3 entries specify (x,y,z) coordinates.
                'faces': a single list of int, every 3 entries specify samples of vertices.
            Or, specify the file path to a json file that includes the definition of the mesh.
            Or, specify only a url which can be readout later on.
        data_source : neuroarch.models.DataSource (optional)
            The datasource. If not specified, default DataSource will be used.

        Returns
        -------
        neuroarch.models.Subregion
            Created Subregion object
        """
        assert isinstance(name, str), 'name must be of str type'
        self._database_writeable_check()
        connect_DataSource = self._default_DataSource if data_source is None else data_source
        self._uniqueness_check('Subregion', unique_in = connect_DataSource,
                               name = name)

        subregion_info = {'name': name}
        if isinstance(synonyms, list) and all(isinstance(n, str) for n in synonyms):
            subregion_info['synonyms'] = synonyms
        else:
            if synonyms is not None:
                raise TypeError('synonyms must be a list of str')

        batch = self.graph.batch()
        node_name = _to_var_name('Subregion_{}'.format(name))
        batch[node_name] = batch.Subregions.create(**subregion_info)

        # Link subsystem if specified
        if neuropil is not None:
            if isinstance(neuropil, str):
                neuropil_obj = self.get('Neuropil', neuropil, connect_DataSource)
                # try:
                #     neuropil_obj = self._cache['Neuropil'][neuropil]
                # except KeyError:
                #     neuropil_objs = self.find_objs('Neuropil', name = neuropil)
                #     if len(neuropil_obj) == 0:
                #         raise ValueError('Neuropil {} not found in database.'.format(neuropil))
                #     else:
                #         neuropil_obj = neuropil_objs[0]
            elif isinstance(neuropil, models.Neuropil):
                if self._is_in_datasource(connect_DataSource, neuropil):
                    neuropil_obj = neuropil
                else:
                    raise ValueError(
                        'Neuropil {} with rid {} to be linked with subregion is \
                        not in the same datasource {} version {}'.format(
                            neuropil.name, neuropil._id,
                            connect_DataSource.name, connect_DataSource.version))
            self.link_with_batch(batch, neuropil_obj, batch[:node_name], 'Owns')

        # Link data_source
        self.link_with_batch(batch, connect_DataSource, batch[:node_name],
                             'Owns')

        subregion = batch['${}'.format(node_name)]
        batch.commit(20)

        # Link morphology data separately because they may be too large for
        # batch process
        if morphology is not None:
            self.add_morphology(subregion, morphology, data_source = connect_DataSource)

        self.set('Subregion', name, subregion, data_source = connect_DataSource)
        return subregion

    def add_Tract(self, name,
                  synonyms = None,
                  morphology = None,
                  data_source = None):
        """
        Create a Subregion record and link it to related node types.

        Parameters
        ----------
        name : str
            Name of the tract
            (abbreviation is preferred, full name can be given in the synonyms)
        synonyms : list of str
            Synonyms of the synonyms.
        morphology : dict (optional)
            Morphology of the neuropil boundary specified with a triangulated mesh,
            with fields
                'vertices': a single list of float, every 3 entries specify (x,y,z) coordinates.
                'faces': a single list of int, every 3 entries specify samples of vertices.
            Or, specify the file path to a json file that includes the definition of the mesh.
            Or, specify only a url which can be readout later on.
        data_source : neuroarch.models.DataSource (optional)
            The datasource. If not specified, default DataSource will be used.

        Returns
        -------
        neuroarch.models.Tract
            Created Tract object
        """
        assert isinstance(name, str), 'name must be of str type'
        self._database_writeable_check()
        connect_DataSource = self._default_DataSource if data_source is None else data_source
        self._uniqueness_check('Tract', unique_in = connect_DataSource,
                               name = name)

        tract_info = {'name': name}
        if isinstance(synonyms, list) and all(isinstance(n, str) for n in synonyms):
            tract_info['synonyms'] = synonyms
        else:
            if synonyms is not None:
                raise TypeError('synonyms must be a list of str')

        batch = self.graph.batch()
        node_name = _to_var_name('Tract_{}'.format(name))
        batch[node_name] = batch.Tracts.create(**tract_info)

        # Link data_source
        self.link_with_batch(batch, connect_DataSource, batch[:node_name],
                             'Owns')

        tract = batch['${}'.format(node_name)]
        batch.commit(20)

        # Link morphology data separately because they may be too large for
        # batch process
        if morphology is not None:
            self.add_morphology(tract, morphology, data_source = connect_DataSource)

        self.set('Tract', name, tract, data_source = connect_DataSource)
        return tract

    def add_Neuron(self, uname,
                   name,
                   referenceId = None,
                   locality = None,
                   synonyms = None,
                   info = None,
                   morphology = None,
                   arborization = None,
                   neurotransmitters = None,
                   neurotransmitters_datasources = None,
                   data_source = None):
        """
        Parameters
        ----------
        uname : str
            A unqiue name assigned to the neuron, must be unique within the DataSource
        name : str
            Name of the neuron, typically the cell type.
        referenceId : str (optional)
            Unique identifier in the original data source
        locality : bool (optional)
            Whether or not the neuron is a local neuron
        synonyms : list of str (optional)
            Synonyms of the neuron
        info : dict (optional)
            Additional information about the neuron, values must be str
        morphology : list of dict (optional)
            Each dict in the list defines a type of morphology of the neuron.
            Must be loaded from a file.
            The dict must include the following key to indicate the type of morphology:
                {'type': 'swc'/'obj'/...}
            Additional keys must be provides, either 'filename' with value
            indicating the file to be read for the morphology,
            or a full definition of the morphology according the schema.
            For swc, required fields are ['sample', 'identifier', 'x', 'y, 'z', 'r', 'parent'].
            More formats pending implementation.
        arborization : list of dict (optional)
            A list of dictionaries define the arborization pattern of
            the neuron in neuropils, subregions, and tracts, if applicable, with
            {'type': 'neuropil' or 'subregion' or 'tract',
             'dendrites': {'EB': 20, 'FB': 2},
             'axons': {'NO': 10, 'MB': 22}}
            Name of the regions must already be present in the database.
        neurotransmitters : str or list of str (optional)
            The neurotransmitter(s) expressed by the neuron
        neurotransmitters_datasources : neuroarch.models.DataSource or list of neuroarch.models.DataSource (optional)
            The datasource of neurotransmitter data.
            If None, all neurotransmitter will have the same datasource of the Neuron.
            If specified, the size of the list must be the same as the size of
            neurotransmitters, and have one to one corresponsdence in the same order.
        data_source : neuroarch.models.DataSource (optional)
            The datasource. If not specified, default DataSource will be used.

        Returns
        -------
        neuron : neuroarch.models.Neuron
            Created Neuron object
        """
        assert isinstance(uname, str), 'uname must be of str type'
        assert isinstance(name, str), 'name must be of str type'
        self._database_writeable_check()
        connect_DataSource = self._default_DataSource if data_source is None else data_source
        self._uniqueness_check('Neuron', unique_in = connect_DataSource,
                               name = uname)
        batch = self.graph.batch()

        neuron_name = _to_var_name(uname)
        neuron_info = {'uname': uname, 'name': name}
        if isinstance(referenceId, str):
            neuron_info['referenceId'] = referenceId
        else:
            if referenceId is not None:
                raise TypeError('referenceId must be of str type')
        if isinstance(locality, bool):
            neuron_info['locality'] = locality
        else:
            if locality is not None:
                raise TypeError('locality must be of bool type')
        if isinstance(synonyms, list) and all(isinstance(a, str) for a in synonyms):
            neuron_info['synonyms'] = synonyms
        else:
            if synonyms is not None:
                raise TypeError('synonyms must be a list of str')
        if isinstance(info, dict) and all(isinstance(v, str) for v in info.values()):
            neuron_info['info'] = info
        else:
            if info is not None:
                raise TypeError('info must be a dict with str values')

        batch[neuron_name] = batch.Neurons.create(**neuron_info)

        self.link_with_batch(batch, connect_DataSource, batch[:neuron_name],
                             'Owns')

        if arborization is not None:
            if not isinstance(arborization, list):
                arborization = [arborization]
            dendrites = {}
            axons = {}
            local_neuron = None
            arb_name = 'arb{}'.format(neuron_name)
            for data in arborization:
                if data['type'] in ['neuropil', 'subregion', 'tract']:
                    arborization_type = data['type'].capitalize()
                    # region_arb = _to_var_name(
                    #     '{}Arb{}'.format(arborization_type, name))
                    if isinstance(data['dendrites'], dict) and \
                            all(isinstance(k, str) and isinstance(v, int) for k, v in data['dendrites'].items()):
                        pass
                    else:
                        raise ValueError('dendrites in the {} arborization data not understood.'.format(data['type']))
                    if isinstance(data['axons'], dict) and \
                            all(isinstance(k, str) and isinstance(v, int) for k, v in data['axons'].items()):
                        pass
                    else:
                        raise ValueError('axons in the {} arborization data not understood.'.format(data['type']))

                    # create the ArborizesIn edge first so the existence of neurpils/subregions/tracts are automatically checked.
                    arborized_regions = {n: [] for n in set(list(data['dendrites'].keys()) + list(data['axons'].keys()))}
                    for n in data['dendrites']:
                        arborized_regions[n].append('s')
                    for n in data['axons']:
                        arborized_regions[n].append('b')
                    for n, v in arborized_regions.items():
                        self.link_with_batch(batch, batch[:neuron_name],
                                             self.get(arborization_type, n, connect_DataSource),
                                             'ArborizesIn',
                                             kind = v,
                                             N_dendrites = data['dendrites'].get(n, 0),
                                             N_axons = data['axons'].get(n, 0))
                    dendrites.update(data['dendrites'])
                    axons.update(data['axons'])
                    if data['type'] == 'neuropil':
                        if len(arborized_regions) == 1:
                            local_neuron = list(arborized_regions.keys())[0]
                else:
                    raise TypeError('Arborization data type of not understood')
            # create the ArborizationData node
            batch[arb_name] = batch.ArborizationDatas.create(name = name, uname = uname,
                                                             dendrites = dendrites,
                                                             axons = axons)
            self.link_with_batch(batch, batch[:neuron_name],
                                 batch[:arb_name], 'HasData')
            self.link_with_batch(batch, connect_DataSource, batch[:arb_name], 'Owns')
            if local_neuron is not None:
                self.link_with_batch(batch,
                                     self.get('Neuropil',
                                              local_neuron,
                                              connect_DataSource),
                                     batch[:neuron_name],
                                     'Owns')

        neuron = batch['${}'.format(neuron_name)]
        batch.commit(20)
        self.set('Neuron', name, neuron, data_source = connect_DataSource)

        if neurotransmitters is not None:
            self.add_neurotransmitter(neuron, neurotransmitters,
                                      data_sources = neurotransmitters_datasources)
        if morphology is not None:
            self.add_morphology(neuron, morphology, data_source = connect_DataSource)
        return neuron

    def add_neurotransmitter(self, neuron, neurotransmitters, data_sources = None):
        """
        Parameters
        ----------
        neuron : neuroarch.models.Neuron subclass
            An instance of Neuron class to which the neurotransmitters will be associated to
        neurotransmitters : str or list of str
            The neurotransmitter(s) expressed by the neuron
        data_sources : neuroarch.models.DataSource or list of neuroarch.models.DataSource (optional)
            The datasource of neurotransmitter data.
            If None, all neurotransmitter will have the same datasource of the Neuron.
            If specified, the size of the list must be the same as the size of
            neurotransmitters, and have one to one corresponsdence in the same order.
        """
        self._database_writeable_check()
        if not isinstance(neurotransmitters, list):
            neurotransmitters = [neurotransmitters]
        if not all(isinstance(a, str) for a in neurotransmitters):
            raise ValueError('neurotransmitters must be a str or a list of str')
        if data_sources is None:
            ntds = [self._default_DataSource]*len(neurotransmitters)
        elif isinstance(data_sources, neuroarch.models.DataSource):
            ntds = [data_sources]
        elif isinstance(data_sources, list) and \
                all(isinstance(ds, neuroarch.models.DataSouce) for nt in data_sources):
            ntds = data_sources
        else:
            raise ValueError('neurotransmitters must be a DataSource or a list of DataSource')
        assert len(ntds) == len(neurotransmitters), \
               'length of data_sources must match that of neurotransmitters'

        batch = self.graph.batch()
        ntds_rids = [ds._id for ds in ntds]
        ntds_unique_rids = list(set(ntds_rids))
        for i, rid in enumerate(ntds_unique_rids):
            ds_index = ntds_rids.index(rid)
            transmitters = [neurotransmitters[i] for i, ds in enumerate(ntds) if ds._id == rid]
            transmitter_node = _to_var_name('Transmitter_{}_{}'.format(neuron.uname, rid))
            batch[transmitter_node] = batch.NeurotransmitterDatas.create(name = neuron.uname, Transmitters = transmitters)
            self.link_with_batch(batch, neuron,
                                 batch[:transmitter_node], 'HasData')
            self.link_with_batch(batch, ntds[ds_index],
                                 batch[:transmitter_node], 'Owns')

        batch.commit(20)

    def add_morphology(self, obj, morphology, data_source = None):
        """
        Add a morphology to a node, e.g., a neuropil, or a neuron.

        Parameters
        ----------
        obj : neuroarch.models.BioNode subclass
            An instance of BioNode class, e.g., Neuropil, Neuron, etc...
            to which the morphology will be associated to
        morphology : list of dict (optional)
            Each dict in the list defines a type of morphology of the neuron.
            Must be loaded from a file.
            The dict must include the following key to indicate the type of morphology:
                {'type': 'swc'/'obj'/...}
            Additional keys must be provides, either 'filename' with value
            indicating the file to be read for the morphology,
            or a full definition of the morphology according the schema.
            For swc, required fields are ['sample', 'identifier', 'x', 'y, 'z', 'r', 'parent'].
            For mesh, requires an obj file or ['faces', 'vertices'] defined as rastered list of a wavefront obj file
        data_source : neuroarch.models.DataSource (optional)
            The datasource. If not specified, default DataSource will be used.
        """
        self._database_writeable_check()
        connect_DataSource = self._default_DataSource if data_source is None else data_source
        if not isinstance(morphology, list):
            morphology = [morphology]
        for i, data in enumerate(morphology):
            content = {'name': obj.name, 'morph_type': data['type']}
            if isinstance(obj, (models.Neuron, models.Synapse)):
                content['uname'] = obj.uname
            if data['type'] == 'swc':
                if 'filename' in data:
                    df = load_swc(data['filename'])
                    x = [round(i, 2) for i in (df['x']*data['scale']).tolist()]
                    y = [round(i, 2) for i in (df['y']*data['scale']).tolist()]
                    z = [round(i, 2) for i in (df['z']*data['scale']).tolist()]
                    r = [round(i, 5) for i in (df['r']*data['scale']).tolist()]
                    parent = df['parent'].tolist()
                    identifier = df['identifier'].tolist()
                    sample = df['sample'].tolist()
                else:
                    x = data['x']
                    y = data['y']
                    z = data['z']
                    r = data['r']
                    parent = data['parent']
                    identifier = data['identifier']
                    sample = data['sample']
                content['x'] = x
                content['y'] = y
                content['z'] = z
                content['r'] = r
                content['parent'] = parent
                content['identifier'] = identifier
                content['sample'] = sample
                morph_obj = self.graph.element_from_record(
                                self.graph.client.command(
                                    'create vertex MorphologyData content {}'.format(
                                    json.dumps(content)))[0])
            elif data['type'] == 'obj':
                morph_obj = self.graph.element_from_record(
                                self.graph.client.command(
                                    'create vertex MorphologyData content {}'.format(
                                    json.dumps(content)))[0])
            elif data['type'] == 'mesh':
                if 'filename' in data:
                    file_type = os.path.splitext(data['filename'])[-1].lower()
                    if file_type == '.json':
                        with open(data['filename'], 'r') as f:
                            mesh_json = json.load(f)
                        faces = mesh_json['faces']
                        vertices = list(np.asarray(mesh_json['vertices']) * data.get('scale', 1.0))
                    elif file_type == '.obj':
                        df = pd.read_csv(data['filename'], sep = ' ',
                                         skip_blank_lines=True,
                                         comment = '#',
                                         index_col = False,
                                         names = ['type', 'x', 'y', 'z'] )
                        vertices = list(itertools.chain.from_iterable(df.loc[df['type'] == 'v'][df.columns[1:]].to_numpy())) * data.get('scale', 1.0)
                        faces = [int(a) for a in itertools.chain.from_iterable(df.loc[df['type'] == 'f'][df.columns[1:]].to_numpy().astype(np.int32))]
                    else:
                        raise ValueError('File type must be .json or .obj')
                else:
                    faces = data['faces']
                    vertices = data['vertices']
                content['faces'] = faces
                content['vertices'] = vertices
                morph_obj = self.graph.element_from_record(
                                self.graph.client.command(
                                    'create vertex MorphologyData content {}'.format(
                                    json.dumps(content)))[0])
            else:
                raise TypeError('Morphology type {} unknown'.format(data['type']))
            self.graph.HasData.create(obj, morph_obj)
            self.graph.Owns.create(data_source, morph_obj)

    def add_neuron_arborization(self, neuron, arborization, data_source = None):
        self._database_writeable_check()
        self._uniqueness_check('ArborizationData', unique_in = neuron)
        connect_DataSource = self._default_DataSource if data_source is None else data_source

        neuron_name = _to_var_name(neuron.uname)
        batch = self.graph.batch()

        if not isinstance(arborization, list):
            arborization = [arborization]
        dendrites = {}
        axons = {}
        local_neuron = None
        arb_name = 'arb{}'.format(neuron_name)
        for data in arborization:
            if data['type'] in ['neuropil', 'subregion', 'tract']:
                arborization_type = data['type'].capitalize()
                # region_arb = _to_var_name(
                #     '{}Arb{}'.format(arborization_type, name))
                if isinstance(data['dendrites'], dict) and \
                        all(isinstance(k, str) and isinstance(v, int) for k, v in data['dendrites'].items()):
                    pass
                else:
                    raise ValueError('dendrites in the {} arborization data not understood.'.format(data['type']))
                if isinstance(data['axons'], dict) and \
                        all(isinstance(k, str) and isinstance(v, int) for k, v in data['axons'].items()):
                    pass
                else:
                    raise ValueError('axons in the {} arborization data not understood.'.format(data['type']))

                # create the ArborizesIn edge first so the existence of neurpils/subregions/tracts are automatically checked.
                arborized_regions = {n: [] for n in set(list(data['dendrites'].keys()) + list(data['axons'].keys()))}
                for n in data['dendrites']:
                    arborized_regions[n].append('s')
                for n in data['axons']:
                    arborized_regions[n].append('b')
                for n, v in arborized_regions.items():
                    self.link_with_batch(batch, neuron,
                                         self.get(arborization_type, n, connect_DataSource),
                                         'ArborizesIn',
                                         kind = v,
                                         N_dendrites = data['dendrites'].get(n, 0),
                                         N_axons = data['axons'].get(n, 0))
                dendrites.update(data['dendrites'])
                axons.update(data['axons'])
                if data['type'] == 'neuropil':
                    if len(arborized_regions) == 1:
                        local_neuron = list(arborized_regions.keys())[0]
            else:
                raise TypeError('Arborization data type of not understood')
        # create the ArborizationData node
        batch[arb_name] = batch.ArborizationDatas.create(name = neuron.name,
                                                         uname = neuron.uname,
                                                         dendrites = dendrites,
                                                         axons = axons)
        self.link_with_batch(batch, neuron,
                             batch[:arb_name], 'HasData')
        self.link_with_batch(batch, connect_DataSource, batch[:arb_name], 'Owns')
        if local_neuron is not None:
            self.link_with_batch(batch,
                                 self.get('Neuropil',
                                          local_neuron,
                                          connect_DataSource),
                                 neuron,
                                 'Owns')

        batch.commit(10)

    def add_Synapse(self, pre_neuron, post_neuron,
                    N = None, NHP = None,
                    morphology = None,
                    arborization = None,
                    data_source = None):
        """
        Add a Synapse from pre_neuron and post_neuron.
        The Synapse is typicall a group of synaptic contact points.

        parameters
        ----------
        pre_neuron : str or models.Neuron
            The neuron that is presynaptic in the synapse.
            If str, must be the uname of the presynaptic neuron.
        post_neuron : str or models.Neuron
            The neuron that is postsynaptic in the synapse.
            If str, must be the uname of the postsynaptic neuron.
        N : int (optional)
            The number of synapses from pre_neuron to the post_neuron.
        NHP : int (optional)
            The number of synapses that can be confirmed with a high probability
        morphology : list of dict (optional)
            Each dict in the list defines a type of morphology of the neuron.
            Must be loaded from a file.
            The dict must include the following key to indicate the type of morphology:
                {'type': 'swc'}
            For swc, required fields are ['sample', 'identifier', 'x', 'y, 'z', 'r', 'parent'].
            For synapses, if both postsynaptic and presynaptic sites are available,
            x, y, z, r must each be a list where the first half indicate the
            locations/radii of postsynaptic sites (on the presynaptic neuron),
            and the second half indicate the locations/radii of the presynaptic
            sites (on the postsynaptic neuron). There should be a one-to-one relation
            between the first half and second half.
            parent must be a list of -1.
        arborization : list of dict (optional)
            A list of dictionaries define the arborization pattern of
            the neuron in neuropils, subregions, and tracts, if applicable, with
            {'type': 'neuropil' or 'subregion' or 'tract',
             'synapses': {'EB': 20, 'FB': 2}}
            Name of the regions must already be present in the database.
        data_source : neuroarch.models.DataSource (optional)
            The datasource. If not specified, default DataSource will be used.

        Returns
        -------
        synapse : models.Synapse
            The created synapse object.
        """
        self._database_writeable_check()
        connect_DataSource = self._default_DataSource if data_source is None else data_source
        # self._uniqueness_check('Synapse', unique_in = connect_DataSource,
                               # name = name)

        if isinstance(pre_neuron, models.Neuron):
            pre_neuron_obj = pre_neuron
            pre_neuron_name = pre_neuron.name
        elif isinstance(pre_neuron, str):
            pre_neuron_name = pre_neuron
            pre_neuron_obj = self.get('Neuron', pre_neuron_name,
                                      connect_DataSource)
        else:
            raise TypeError('Parameter pre_neuron must be either a str or a Neuron object.')

        if isinstance(post_neuron, models.Neuron):
            post_neuron_obj = post_neuron
            post_neuron_name = post_neuron.name
        elif isinstance(post_neuron, str):
            post_neuron_name = post_neuron
            post_neuron_obj = self.get('Neuron', post_neuron_name,
                                       connect_DataSource)
        else:
            raise TypeError('Parameter post_neuron must be either a str or a Neuron object.')

        synapse_uname = '{}--{}'.format(pre_neuron_obj.uname, post_neuron_obj.uname)
        synapse_name = '{}--{}'.format(pre_neuron_obj.name, post_neuron_obj.name)
        synapse_info = {'uname': synapse_uname,
                        'name': synapse_name}
        if N is None:
            if NHP is not None:
                synapse_info['N'] = NHP
                synapse_info['NHP'] = NHP
        else:
            synapse_info['N'] = N
            if NHP is not None:
                synapse_info['NHP'] = NHP
        #pre_conf = np.array(eval(row['pre_confidence']))/1e6
        #post_conf = np.array(eval(row['post_confidence']))/1e6
        #NHP = np.sum(np.logical_and(post_conf>=0.7, pre_conf>=0.7))
        batch = self.graph.batch()
        synapse_obj_name = _to_var_name(synapse_uname)
        batch[synapse_obj_name] = batch.Synapses.create(**synapse_info)

        self.link_with_batch(batch, connect_DataSource, batch[:synapse_obj_name],
                             'Owns')
        self.link_with_batch(batch, pre_neuron_obj, batch[:synapse_obj_name],
                             'SendsTo')
        self.link_with_batch(batch, batch[:synapse_obj_name], post_neuron_obj,
                             'SendsTo')

        if arborization is not None:
            if not isinstance(arborization, list):
                arborization = [arborization]
            synapses = {}
            arb_name = 'arb{}'.format(synapse_obj_name)
            for data in arborization:
                if data['type'] in ['neuropil', 'subregion', 'tract']:
                    arborization_type = data['type'].capitalize()
                    # region_arb = _to_var_name(
                    #     '{}Arb{}'.format(arborization_type, name))
                    if isinstance(data['synapses'], dict) and \
                            all(isinstance(k, str) and isinstance(v, int) for k, v in data['synapses'].items()):
                        pass
                    else:
                        raise ValueError('synapses in the {} distribution data not understood.'.format(data['type']))

                    # check if the regions exists
                    for region in data['synapses']:
                        self.get(arborization_type, region, connect_DataSource)
                    synapses.update(data['synapses'])
                else:
                    raise TypeError('Arborization data type of not understood')
            # create the ArborizationData node
            batch[arb_name] = batch.ArborizationDatas.create(name = synapse_name,
                                                             uname = synapse_uname,
                                                             synapses = synapses)
            self.link_with_batch(batch, batch[:synapse_obj_name],
                                 batch[:arb_name], 'HasData')
            self.link_with_batch(batch, connect_DataSource, batch[:arb_name], 'Owns')
        synapse = batch['${}'.format(synapse_obj_name)]
        batch.commit(20)
        #self.set('Synapse', '{}{}'.format(synapse_name, synapse._id), synapse, data_source = connect_DataSource)

        if morphology is not None:
            self.add_morphology(synapse, morphology, data_source = connect_DataSource)
        return synapse

    def add_synapse_arborization(self, synapse, arborization, data_source = None):
        self._database_writeable_check()
        self._uniqueness_check('ArborizationData', unique_in = synapse)
        connect_DataSource = self._default_DataSource if data_source is None else data_source

        synapse_obj_name = _to_var_name(synapse.uname)
        batch = self.graph.batch()

        if not isinstance(arborization, list):
            arborization = [arborization]
        synapses = {}
        arb_name = 'arb{}'.format(synapse_obj_name)
        for data in arborization:
            if data['type'] in ['neuropil', 'subregion', 'tract']:
                arborization_type = data['type'].capitalize()
                # region_arb = _to_var_name(
                #     '{}Arb{}'.format(arborization_type, name))
                if isinstance(data['synapses'], dict) and \
                        all(isinstance(k, str) and isinstance(v, int) for k, v in data['synapses'].items()):
                    pass
                else:
                    raise ValueError('synapses in the {} distribution data not understood.'.format(data['type']))

                # check if the regions exists
                for region in data['synapses']:
                    self.get(arborization_type, region, connect_DataSource)
                synapses.update(data['synapses'])
            else:
                raise TypeError('Arborization data type of not understood')
        # create the ArborizationData node
        batch[arb_name] = batch.ArborizationDatas.create(name = synapse.name,
                                                         uname = synapse.uname,
                                                         synapses = synapses)
        self.link_with_batch(batch, synapse,
                             batch[:arb_name], 'HasData')
        self.link_with_batch(batch, connect_DataSource, batch[:arb_name], 'Owns')
        batch.commit(10)

    def add_InferredSynapse(self, pre_neuron, post_neuron,
                            N = None, NHP = None,
                            morphology = None,
                            arborization = None,
                            data_source = None):
        """
        Add an InferredSynapse from pre_neuron and post_neuron.
        The Synapse is typicall a group of synaptic contact points.

        parameters
        ----------
        pre_neuron : str or models.Neuron
            The neuron that is presynaptic in the synapse.
            If str, must be the uname of the presynaptic neuron.
        post_neuron : str or models.Neuron
            The neuron that is postsynaptic in the synapse.
            If str, must be the uname of the postsynaptic neuron.
        N : int (optional)
            The number of synapses from pre_neuron to the post_neuron.
        morphology : list of dict (optional)
            Each dict in the list defines a type of morphology of the neuron.
            Must be loaded from a file.
            The dict must include the following key to indicate the type of morphology:
                {'type': 'swc'}
            For swc, required fields are ['sample', 'identifier', 'x', 'y, 'z', 'r', 'parent'].
            For synapses, if both postsynaptic and presynaptic sites are available,
            x, y, z, r must each be a list where the first half indicate the
            locations/radii of postsynaptic sites (on the presynaptic neuron),
            and the second half indicate the locations/radii of the presynaptic
            sites (on the postsynaptic neuron). There should be a one-to-one relation
            between the first half and second half.
            parent must be a list of -1.
        arborization : list of dict (optional)
            A list of dictionaries define the arborization pattern of
            the neuron in neuropils, subregions, and tracts, if applicable, with
            {'type': 'neuropil' or 'subregion' or 'tract',
             'synapses': {'EB': 20, 'FB': 2}}
            Name of the regions must already be present in the database.
        data_source : neuroarch.models.DataSource (optional)
            The datasource. If not specified, default DataSource will be used.

        Returns
        -------
        synapse : models.Synapse
            The created synapse object.
        """
        self._database_writeable_check()
        connect_DataSource = self._default_DataSource if data_source is None else data_source
        # self._uniqueness_check('Synapse', unique_in = connect_DataSource,
                               # name = name)

        if isinstance(pre_neuron, models.Neuron):
            pre_neuron_obj = pre_neuron
            pre_neuron_name = pre_neuron.name
        elif isinstance(pre_neuron, str):
            pre_neuron_name = pre_neuron
            pre_neuron_obj = self.get('Neuron', pre_neuron_name,
                                      connect_DataSource)
        else:
            raise TypeError('Parameter pre_neuron must be either a str or a Neuron object.')

        if isinstance(post_neuron, models.Neuron):
            post_neuron_obj = post_neuron
            post_neuron_name = post_neuron.name
        elif isinstance(post_neuron, str):
            post_neuron_name = post_neuron
            post_neuron_obj = self.get('Neuron', post_neuron_name,
                                       connect_DataSource)
        else:
            raise TypeError('Parameter post_neuron must be either a str or a Neuron object.')

        synapse_uname = '{}--{}'.format(pre_neuron_obj.uname, post_neuron_obj.uname)
        synapse_name = '{}--{}'.format(pre_neuron_obj.name, post_neuron_obj.name)
        synapse_info = {'uname': synapse_uname,
                        'name': synapse_name}
        if N is not None:
            synapse_info['N'] = N
        #pre_conf = np.array(eval(row['pre_confidence']))/1e6
        #post_conf = np.array(eval(row['post_confidence']))/1e6
        #NHP = np.sum(np.logical_and(post_conf>=0.7, pre_conf>=0.7))
        batch = self.graph.batch()
        synapse_obj_name = _to_var_name(synapse_uname)
        batch[synapse_obj_name] = batch.InferredSynapses.create(**synapse_info)

        self.link_with_batch(batch, connect_DataSource, batch[:synapse_obj_name],
                             'Owns')
        self.link_with_batch(batch, pre_neuron_obj, batch[:synapse_obj_name],
                             'SendsTo')
        self.link_with_batch(batch, batch[:synapse_obj_name], post_neuron_obj,
                             'SendsTo')

        if arborization is not None:
            if not isinstance(arborization, list):
                arborization = [arborization]
            synapses = {}
            arb_name = 'arb{}'.format(synapse_obj_name)
            for data in arborization:
                if data['type'] in ['neuropil', 'subregion', 'tract']:
                    arborization_type = data['type'].capitalize()
                    # region_arb = _to_var_name(
                    #     '{}Arb{}'.format(arborization_type, name))
                    if isinstance(data['synapses'], dict) and \
                            all(isinstance(k, str) and isinstance(v, int) for k, v in data['synapses'].items()):
                        pass
                    else:
                        raise ValueError('synapses in the {} distribution data not understood.'.format(data['type']))

                    # check if the regions exists
                    for region in data['synapses']:
                        self.get(arborization_type, region, connect_DataSource)
                    synapses.update(data['synapses'])
                else:
                    raise TypeError('Arborization data type of not understood')
            # create the ArborizationData node
            batch[arb_name] = batch.ArborizationDatas.create(name = synapse_name,
                                                             uname = synapse_uname,
                                                             synapses = synapses)
            self.link_with_batch(batch, batch[:synapse_obj_name],
                                 batch[:arb_name], 'HasData')
            self.link_with_batch(batch, connect_DataSource, batch[:arb_name], 'Owns')
        synapse = batch['${}'.format(synapse_obj_name)]
        batch.commit(20)
        #self.set('Synapse', '{}{}'.format(synapse_name, synapse._id), synapse, data_source = connect_DataSource)

        if morphology is not None:
            self.add_morphology(synapse, morphology, data_source = connect_DataSource)
        return synapse

    def link(self, node1, node2, edge_type = None, **attr):
        """
        To create an edge between node1 and node2 with specified edge type.
        If `edge_type` not given, infer from the types of nodes.
        """
        if edge_type is None:
            edge_type = relations[node1.element_type][node2.element_type]
        edge = getattr(self.graph, edge_type).create(node1, node2, **attr)
        return edge

    def link_with_batch(self, batch, node1, node2, edge_type, **attr):
        """
        """
        batch[:] = getattr(batch, edge_type).create(node1, node2, **attr)


    def _pre_create_check(self, cls, **attr):
        if self._check:
            if self.exists(cls, **attr):
                raise DuplicateNodeError('{} Node with attributes {} already exists'.format(
                                cls, ', '.join(["""{} = {}""".format(key, value) \
                                for key, value in attr.items()])))
        return True

    def create_NeuronModel(self):
        pass

    def remove_Neuron(self):
        pass

    def remove_Synapse(self):
        pass

    def remove_Neuropil(self):
        pass

    def update_Neuron(self):
        pass

    def update_Synapse(self):
        pass

    def update_Neuropil(self):
        pass




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
                neuron_to_update = q.nodes_as_objs[0]
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


def load_swc(file_name):
    """
    Load an SWC file into a DataFrame.
    """

    df = pd.read_csv(file_name, sep = ' ', header=None, comment='#', index_col = False,
                     names=['sample', 'identifier', 'x', 'y', 'z', 'r', 'parent'],
                     skipinitialspace=True)
    return df
