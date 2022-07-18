#!/usr/bin/env python


import re
from datetime import datetime
import json
import itertools
import copy
from tqdm import tqdm
import os
import copy
from packaging import version as pv
import warnings

import numpy as np
import networkx as nx
import pandas as pd
from pyorient.ogm import Graph, Config
from pyorient.serializations import OrientSerialization
from pyorient.ogm.exceptions import NoResultFound

from .query import QueryWrapper, QueryString
from . import models
from . import version as na_version

special_char = set("*?+\.()[]|{}^$'")

rid_pattern = re.compile("#[0-9]+:[0-9]+")

BACKWARD_COMPATIBLE_TO = '0.4.1'


def replace_special_char(text):
    return ''.join(['\\'+s if s in special_char else s for s in text])


def connect(host, db_name, port = 2424, storage = 'plocal', user = 'admin', password = 'admin',
            initial_drop = False, serialization_type = OrientSerialization.Binary,
            new_models = False):
    # graph = Graph(Config.from_url(url, user, password, initial_drop))

    graph = Graph(Config(host, port, user, password, db_name,
                         storage, initial_drop = initial_drop,
                         serialization_type = serialization_type))
    if initial_drop or new_models:
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
    """NeuroArch node with the same property already exsits"""
    pass

class RecordNotFoundWarning(Warning):
    """Warning when record is not found"""
    pass

class DuplicateNodeWarning(Warning):
    """NeuroArch got duplicate nodes"""
    pass

class DataSourceError(Exception):
    """The node is not owned by a DataSource"""
    pass

class VersionMismatchException(Exception):
    pass

class DataInconsistencyWarning(Warning):
    """Potential inconsistenty in the database"""
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
    """
        Create or connect to a NeuroArch object for database access.

        Parameters
        ----------
        db_name : str
            name of the database to connect to or create
        host : str (optional)
            IP of the host
        port : int (optional)
            binary port of the OrientDB server
        user : str (optional)
            user name to access the database
        password : str (optional)
            password to access the database
        mode : str (optional)
            'r': read only
            'w': read/write on existing database, if does not exist, one is created.
            'o': read/write and overwrite any of the existing data in the database.
        storage: str (optional)
            'plocal': using disk-based storage for the database
            'memory': memory storage
        new_models: bool (optional)
            If true, recreate the ogm classes.
        debug : bool (optional)
            Whether the queries are done in debug mode
        serialization_type: str
            Either 'Binary' or 'CSV', specifying the seriailzation strategy of
            pyorient communication with OrientDB server.
        version : str
            If a new database will be created, e.g., mode = 'o' or mode = 'w' and database
            does not exist, then version should be provided.
        maintainer_name: str
            If a new database will be created, e.g., mode = 'o' or mode = 'w' and database
            does not exist, then maintainer of the author name should be provided.
        maintainer_email: str
            Email of the maintainer, should be provided when maintainer_name is needed.
        """
    def __init__(self, db_name, host = 'localhost', port = 2424,
                 user = 'root', password = 'root', mode = 'r',
                 storage = 'plocal',
                 new_models = False, debug = False,
                 serialization_type = 'Binary', version = None,
                 maintainer_name = "", maintainer_email = ""):
        self._mode = mode
        self._db_name = db_name
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        if storage not in ['plocal', 'memory']:
            raise ValueError("storage type can only be either 'plocal' or 'memory'")
        self._storage = storage
        if serialization_type == 'Binary':
            self._serialization_type = OrientSerialization.Binary
        elif serialization_type == 'CSV':
            self._serialization_type = OrientSerialization.CSV
        else:
            self._serialization_type = None
        created_new_database = self.connect(new_models)
        if created_new_database:
            if version is None:
                raise ValueError("Please specify version of this database.")
            self._initialize_database(version,
                                      maintainer_name = maintainer_name,
                                      maintainer_email = maintainer_email)
        else:
            meta = self._get_version()
            if pv.parse(meta['created_by']['min_NeuroArch_version_supported']) > pv.parse(na_version.__version__):
                raise VersionMismatchException("Please upgrade NeuroArch to version {} to operate on this database.".format(
                    meta['created_by']['min_NeuroArch_version_supported']
                )
            )
            if pv.parse(meta['created_by']['min_NeuroArch_version_supported']) < pv.parse(BACKWARD_COMPATIBLE_TO):
                raise VersionMismatchException("The copy of database is obsolete. For public datasets, please download a new copy of the database from https://github.com/FlyBrainLab/datasets")

        self._debug = debug
        self._default_DataSource = None
        self._cache = {}
        self._check = True
        self._owns_write_cache = {}
        self.__neuron_inconsistent_warned = False
        self.__synapse_inconsistent_warned = False

    def connect(self, new_models = False):
        """
        Connect to the database specified during instantiation

        Parameters
        ----------
        new_models: bool
            If model definition in model.py has been changed.

        Returns
        -------
        bool
            Whether a new database is created.
        """
        if self._mode == 'r':
            initial_drop = False
            self._allow_write = False
            if self._serialization_type is None:
                serialization_type = OrientSerialization.Binary
            else:
                serialization_type = self._serialization_type
        elif self._mode == 'o':
            initial_drop = True
            self._allow_write = True
            if self._serialization_type is None:
                serialization_type = OrientSerialization.Binary
            else:
                serialization_type = self._serialization_type
        elif self._mode == 'w':
            initial_drop = False
            self._allow_write = True
            if self._serialization_type is None:
                serialization_type = OrientSerialization.Binary
            else:
                serialization_type = self._serialization_type
        else:
            raise ValueError("""Database mode must be either read ('r'),
                              write ('w'), or overwrite ('o').""")
        self.graph = connect(self._host, self._db_name, port = self._port,
                             user = self._user, password = self._password,
                             storage = self._storage,
                             initial_drop = initial_drop,
                             serialization_type = serialization_type,
                             new_models = new_models)
        
        new_db = self.graph._new_db
        if initial_drop:
            self.reconnect()
        return new_db

    def reconnect(self):
        """Reconnect to the database specified during instantiation"""
        if self._mode == 'r':
            if self._serialization_type is None:
                serialization_type = OrientSerialization.Binary
            else:
                serialization_type = self._serialization_type
        else:
            if self._serialization_type is None:
                serialization_type = OrientSerialization.Binary
            else:
                serialization_type = self._serialization_type
        self._disconnect()
        self.graph = connect(self._host, self._db_name, port = self._port,
                             user = self._user, password = self._password,
                             storage = self._storage,
                             initial_drop = False,
                             serialization_type = serialization_type)
    
    def _disconnect(self):
        self.graph.client._connection._socket.close()

    def __del__(self):
        self._disconnect()
    
    def _initialize_database(self, version, maintainer_name = "", maintainer_email = ""):
        """
        Initialize database by writing essential nodes such as metadata.
        """
        odb_version = self.graph.client.version

        create = {
            "NeuroArch_version": na_version.__version__,
            "min_NeuroArch_version_supported": "0.4.1",
            "OrientDB_version": "{}.{}.{}".format(
                                    odb_version.major,
                                    odb_version.minor,
                                    odb_version.build),
            "created_date": datetime.now().isoformat()
        }
        maintainer = {
            "name": maintainer_name,
            "email": maintainer_email
        }
        self.graph.MetaDatas.create(version = version,
                                    created_by = create,
                                    maintainer = maintainer)

    def _get_version(self):
        try:
            meta = self.graph.MetaDatas.query().one().props
            return meta
        except:
            raise VersionMismatchException("The copy of database is obsolete. For public datasets, please download a new copy of the database from https://github.com/FlyBrainLab/datasets")

    def _get_obj_from_str(self, obj):
        if isinstance(obj, str) and rid_pattern.fullmatch(obj) is not None:
            return QueryWrapper.from_rids(self.graph, obj).node_objs[0]
        else:
            return obj

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
        data_source : models.DataSource or None
            The DataSource under which the unique object will be retrieved
            If None, the searched object is not bound to the DataSource.
        attr : keyword arguments (optional)
            node attributes using key=value, currently not implemented

        Returns
        -------
        obj : models.Node or subclass
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
            if cls in ['Neuron', 'NeuronFragment', 'NeuronAndFragment', 'Synapse', 'InferredSynapse']:
                q = self._find(cls, data_source, uname = name)
            else:
                q = self._find(cls, data_source, name = name)
            if len(q) == 1:
                obj = q.node_objs[0]
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
                raise DuplicateNodeError('Hit more than one instance of {} with name {} in database.'.format(cls, name))
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
        value : models.Node or subclasses
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

    def _add_to_owns_cache(self, cls, owner, child):
        if cls not in self._owns_write_cache:
            self._owns_write_cache[cls] = {}
        if owner._id not in self._owns_write_cache[cls]:
            self._owns_write_cache[cls][owner._id] = []
        self._owns_write_cache[cls][owner._id].append(child)

    def flush_edges(self):
        edges = []
        for cls, v in self._owns_write_cache.items():
            for owner_id, child_list in v.items():
                owner = QueryWrapper.from_rids(self.graph, owner_id).node_objs[0]
                for child in child_list:
                    edges.append([owner, child])
        print('creating Owns edge records, please wait...')
        i = 0
        batch_commited = True
        for owner, child in tqdm(edges):
            if i % 1000 == 0:
                batch_commited = False
                batch = self.graph.batch()
            self.link_with_batch(batch, owner, child, 'Owns')
            if i % 1000 == 999:
                batch.commit(20)
                batch_commited = True
            i += 1
        if not batch_commited:
            batch.commit(20)
        self._owns_write_cache = {}

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
        q : query.QueryWrapper
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
        attr : dict (optional)
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
        attr : dict (optional)
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
        attr : keyword arguments (optional)
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
        attr : dict (optional)
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
        data_source : models.DataSource
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
        data_source : models.DataSource
            The data_source to be searched for
        obj : models.*
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
                objs = tmp.node_objs
                if attr['name'] in [obj.name for obj in objs]:
                    raise NodeAlreadyExistError("""Species {name} at {stage} stage ({sex}) already exists with rid = {rid}""".format(
                        name = attr['name'], stage = attr['stage'], sex = attr['sex'], rid = objs[0]._id))
                else:
                    for obj in objs:
                        if attr['name'] in obj.synonyms:
                            raise NodeAlreadyExistError(
                                """Species {name} (as its synonym) at {stage} stage ({sex}) already exists with rid = {rid}, use name {formalname} instead""".format(
                                name = attr['name'], stage = attr['stage'], sex = attr['sex'], rid = obj._id, formalname = obj.name))
        elif cls == 'DataSource':
            objs = self.find_objs('DataSource', name=attr['name'], version=attr['version'])
            #if self.exists(cls, name = attr['name'], version = attr['version']):
            if len(objs):
                raise NodeAlreadyExistError("""{} Node with attributes {} already exists with rid = {}""".format(
                                cls, ', '.join(["""{} = {}""".format(key, value) \
                                for key, value in attr.items()]), objs[0]._id))
        elif cls == 'Neurotransmitter':
            tmp = self.sql_query(
                """select from Neurotransmitter where name = "{name}" or "{name}" in synonyms""".format(
                    name = attr['name']))
            if len(tmp):
                objs = tmp.node_objs
                if attr['name'] in [obj.name for obj in objs]:
                    raise NodeAlreadyExistError("""Neurotransmitter {name} already exists with rid = {rid}""".format(
                        name = attr['name'], rid = objs[0]._id))
                    return objs
                else:
                    for obj in objs:
                        if attr['name'] in obj.synonyms:
                            raise NodeAlreadyExistError(
                                """Neurotransmitter {name} (as its synonym) already exists with rid = {rid}, use name {formalname} instead""".format(
                                name = attr['name'], rid = obj._id, formalname = obj.name))
        elif cls in ['Subsystem', 'Neuropil', 'Subregion', 'Tract']:
            # TODO: synonyms are not checked against existing names and synonyms
            if not isinstance(unique_in, models.DataSource):
                raise TypeError('To check the uniqueness of a {} instance, unique_in must be a DataSource object'.format(cls))
            tmp = self.sql_query(
                """select from (select from {cls} where name = "{name}" or "{name}" in synonyms) let $q = (select from (select expand($parent.$parent.current.in('Owns'))) where @class='{ucls}' and @rid = {rid}) where $q.size() = 1""".format(
                    rid = unique_in._id, cls = cls, name = attr['name'], ucls = unique_in.element_type))
            if len(tmp):
                objs = tmp.node_objs
                if attr['name'] in [obj.name for obj in objs]:
                    raise NodeAlreadyExistError("""{cls} {name} already exists under DataSource {ds} version {version}, rid = {rid}""".format(
                        cls = cls, name = attr['name'],
                        ds = unique_in.name,
                        version = unique_in.version, rid = objs[0]._id))
                else:
                    for obj in objs:
                        if attr['name'] in obj.synonyms:
                            raise NodeAlreadyExistError(
                                """{cls} {name} already exists as a synonym of {cls} {formalname} under DataSource {ds} version {version}, rid = {rid}""".format(
                                cls = cls, name = attr['name'], formalname = obj.name,
                                ds = unique_in.name,
                                version = unique_in.version, rid = obj._id))
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
            #         all_synonym_objs = (tmp - tmp1).node_objs
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
        #         objs = tmp.node_objs
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
        elif cls in ['Neuron', 'NeuronFragment']:
            # TODO: synonyms are not checked against existing names and synonyms
            if not isinstance(unique_in, models.DataSource):
                raise TypeError('To check the uniqueness of a {} instance, unique_in must be a DataSource object'.format(cls))
            tmp = self.sql_query(
                """select from (select from {cls} where uname = "{name}") let $q = (select from (select expand($parent.$parent.current.in('Owns'))) where @class='{ucls}' and @rid = {rid}) where $q.size() = 1""".format(
                    rid = unique_in._id, cls = cls, name = attr['name'], ucls = unique_in.element_type))
            if len(tmp):
                objs = tmp.node_objs
                raise NodeAlreadyExistError("""{cls} {name} already exists with rid = {rid}, under DataSource {ds} version {version}""".format(
                    cls = cls, name = attr['name'], rid = objs[0]._id,
                    ds = unique_in.name,
                    version = unique_in.version))
        elif cls == 'Circuit':
            if not isinstance(unique_in, models.DataSource):
                raise TypeError('To check the uniqueness of a {} instance, unique_in must be a DataSource object'.format(cls))
            tmp = self.sql_query(
                """select from (select from {cls} where name = "{name}") let $q = (select from (select expand($parent.$parent.current.in('Owns'))) where @class='{ucls}' and @rid = {rid}) where $q.size() = 1""".format(
                    rid = unique_in._id, cls = cls, name = attr['name'], ucls = unique_in.element_type))
            if len(tmp):
                objs = tmp.node_objs
                if attr['name'] in [obj.name for obj in objs]:
                    raise NodeAlreadyExistError("""{cls} {name} already exists under DataSource {ds} version {version}, rid = {rid}""".format(
                        cls = cls, name = attr['name'],
                        ds = unique_in.name,
                        version = unique_in.version, rid = objs[0]._id))
        elif cls == 'ArborizationData':
            if not isinstance(unique_in, (models.Neuron, models.Synapse)):
                raise TypeError('To check the uniqueness of a ArborizationData instance, unique_in must be a Neuron or a Synapse object')
            tmp = self.sql_query(
                """select from (select expand(out(HasData)) from {rid}) where @class = 'ArborizationData' """.format(rid = unique_in._id))
            if len(tmp):
                raise NodeAlreadyExistError("""ArborizationData already exists for {node} {uname} with rid = {rid}. Use NeuroArch.update_{node}_arborization to update the record""".format(
                    node = unique_in.element_type.lower(), rid = tmp.node_objs[0]._id, uname = unique_in.uname))
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

        self._default_DataSource = self._get_obj_from_str(data_source)
        print("Setting default DataSource to {} version {}".format(
                            data_source.name,
                            getattr(data_source, 'version', 'not specified')))

    @default_DataSource.deleter
    def default_DataSource(self):
        print("removing default DataSource")
        self._default_DataSource = None

    def add_Species(self, name, stage, sex, synonyms = None):
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
            species = self._get_obj_from_str(species)
            if isinstance(species, models.Species):
                species_obj = species
            elif isinstance(species, dict):
                tmp = self.sql_query(
                    """select from Species where (name = "{name}" or "{name}" in synonyms) and stage = "{stage}" """.format(
                        name = species['name'], stage = species['stage']))
                if len(tmp) == 1:
                    species_obj = tmp.node_objs[0]
                elif len(tmp) > 1: # most likely will not occur
                    raise ValueError(
                        'Multiple Species nodes with name = {name} and stage = {stage} exists'.format(
                            name = species['name'], stage = species['stage']))
                else: # 0 hit
                    species_obj = self.add_Species(
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
        data_source : models.DataSource (optional)
            The datasource. If not specified, default DataSource will be used.

        Returns
        -------
        models.Subsystem
            Created Subsystem object
        """
        assert isinstance(name, str), 'name must be of str type'
        self._database_writeable_check()
        connect_DataSource = self._default_DataSource if data_source is None \
                                else self._get_obj_from_str(data_source)
        if connect_DataSource is None:
            raise TypeError('Default DataSource is missing.')
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
        subsystem : str or models.Subsystem (optional)
            Subsystem that owns the neuropil. Can be specified either by its name
            or the Subsytem object instance.
        morphology : dict (optional)
            Morphology of the neuropil boundary specified with a triangulated mesh,
            with fields
                'vertices': a single list of float, every 3 entries specify (x,y,z) coordinates.
                'faces': a single list of int, every 3 entries specify samples of vertices.
            Or, specify the file path to a json file that includes the definition of the mesh.
            Or, specify only a url which can be readout later on.
        data_source : models.DataSource (optional)
            The datasource. If not specified, default DataSource will be used.

        Returns
        -------
        models.Neuropil
            Created Neuropil object
        """
        assert isinstance(name, str), 'name must be of str type'
        self._database_writeable_check()
        connect_DataSource = self._default_DataSource if data_source is None \
                                else self._get_obj_from_str(data_source)
        if connect_DataSource is None:
            raise TypeError('Default DataSource is missing.')
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
        neuropil : str or models.Neuropil (optional)
            Neuropil that owns the subregion. Can be specified either by its name
            or the Neuropil object instance.
        morphology : dict (optional)
            Morphology of the neuropil boundary specified with a triangulated mesh,
            with fields
                'vertices': a single list of float, every 3 entries specify (x,y,z) coordinates.
                'faces': a single list of int, every 3 entries specify samples of vertices.
            Or, specify the file path to a json file that includes the definition of the mesh.
            Or, specify only a url which can be readout later on.
        data_source : models.DataSource (optional)
            The datasource. If not specified, default DataSource will be used.

        Returns
        -------
        models.Subregion
            Created Subregion object
        """
        assert isinstance(name, str), 'name must be of str type'
        self._database_writeable_check()
        connect_DataSource = self._default_DataSource if data_source is None \
                                else self._get_obj_from_str(data_source)
        if connect_DataSource is None:
            raise TypeError('Default DataSource is missing.')
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
        data_source : models.DataSource (optional)
            The datasource. If not specified, default DataSource will be used.

        Returns
        -------
        models.Tract
            Created Tract object
        """
        assert isinstance(name, str), 'name must be of str type'
        self._database_writeable_check()
        connect_DataSource = self._default_DataSource if data_source is None \
                                else self._get_obj_from_str(data_source)
        if connect_DataSource is None:
            raise TypeError('Default DataSource is missing.')
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

    def add_Circuit(self, name, circuit_type, neuropil = None, data_source =  None):
        """
        Create a Subregion record and link it to related node types.

        Parameters
        ----------
        name : str
            Name of the circuit
        neuropil : str or models.Neuropil (optional)
            Neuropil that owns the subregion. Can be specified either by its name
            or the Neuropil object instance.
        data_source : models.DataSource (optional)
            The datasource. If not specified, default DataSource will be used.

        Returns
        -------
        models.Circuit
            Created Circuit object
        """
        assert isinstance(name, str), 'name must be of str type'
        self._database_writeable_check()
        connect_DataSource = self._default_DataSource if data_source is None \
                                else self._get_obj_from_str(data_source)
        if connect_DataSource is None:
            raise TypeError('Default DataSource is missing.')
        self._uniqueness_check('Circuit', unique_in = connect_DataSource,
                               name = name)

        circuit_info = {'name': name}
        
        batch = self.graph.batch()
        node_name = _to_var_name('Circuit_{}'.format(name))
        plural = getattr(models, circuit_type).element_plural
        batch[node_name] = getattr(batch, plural).create(**circuit_info)

        # Link subsystem if specified
        if neuropil is not None:
            if isinstance(neuropil, str):
                neuropil_obj = self.get('Neuropil', neuropil, connect_DataSource)
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

        circuit = batch['${}'.format(node_name)]
        batch.commit(20)

        self.set('Circuit', name, circuit, data_source = connect_DataSource)
        return circuit

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
                   data_source = None,
                   circuit = None):
        """
        Create a Neuron Record and link it to the related node types.

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
        neurotransmitters_datasources : models.DataSource or list of models.DataSource (optional)
            The datasource of neurotransmitter data.
            If None, all neurotransmitter will have the same datasource of the Neuron.
            If specified, the size of the list must be the same as the size of
            neurotransmitters, and have one to one corresponsdence in the same order.
        data_source : models.DataSource (optional)
            The datasource. If not specified, default DataSource will be used.

        Returns
        -------
        neuron : models.Neuron
            Created Neuron object
        """
        assert isinstance(uname, str), 'uname must be of str type'
        assert isinstance(name, str), 'name must be of str type'
        self._database_writeable_check()
        connect_DataSource = self._default_DataSource if data_source is None \
                                else self._get_obj_from_str(data_source)
        if connect_DataSource is None:
            raise TypeError('Default DataSource is missing.')
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
        if circuit is not None:
            circuit = self._get_obj_from_str(circuit)
            if not issubclass(type(circuit), models.Circuit):
                raise TypeError('circuit must be a models.Circuit subclass')

        batch[neuron_name] = batch.Neurons.create(**neuron_info)

        if circuit is not None:
            self.link_with_batch(batch, circuit, batch[:neuron_name], 'Owns')
            # a hack now to make nlp work
            self.link_with_batch(batch, batch[:neuron_name], circuit, 'ArborizesIn', kind = ['b','s'])

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
            #self.link_with_batch(batch, connect_DataSource, batch[:arb_name], 'Owns')
            if local_neuron is not None:
                self.link_with_batch(batch,
                                     self.get('Neuropil',
                                              local_neuron,
                                              connect_DataSource),
                                     batch[:neuron_name],
                                     'Owns')
        
        neuron = batch['${}'.format(neuron_name)]
        batch.commit(20)
        self._add_to_owns_cache(connect_DataSource.element_type, connect_DataSource, neuron)
        if not self.__neuron_inconsistent_warned:
            warnings.warn("""Created neuron has not been connected to its DataSource yet. Please execute flush_edges() after adding all Neurons""", category = DataInconsistencyWarning)
            self.__neuron_inconsistent_warned = True

        self.set('Neuron', uname, neuron, data_source = connect_DataSource)
        

        if neurotransmitters is not None:
            self.add_neurotransmitter(neuron, neurotransmitters,
                                      data_sources = neurotransmitters_datasources if neurotransmitters_datasources is not None else data_source)
        if morphology is not None:
            self.add_morphology(neuron, morphology, data_source = connect_DataSource)
        return neuron

    def add_NeuronFragment(self, uname,
                           name,
                           referenceId = None,
                           info = None,
                           morphology = None,
                           arborization = None,
                           data_source = None):
        """
        Create a NeuronFragment Record and link it to the related node types.

        Parameters
        ----------
        uname : str
            A unqiue name assigned to the neuron, must be unique within the DataSource
        name : str
            Name of the neuron, typically the cell type.
        referenceId : str (optional)
            Unique identifier in the original data source
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
        data_source : models.DataSource (optional)
            The datasource. If not specified, default DataSource will be used.

        Returns
        -------
        neuron : models.NeuronFragment
            Created NeuronFragment object
        """
        assert isinstance(uname, str), 'uname must be of str type'
        assert isinstance(name, str), 'name must be of str type'
        self._database_writeable_check()
        connect_DataSource = self._default_DataSource if data_source is None \
                                else self._get_obj_from_str(data_source)
        if connect_DataSource is None:
            raise TypeError('Default DataSource is missing.')
        self._uniqueness_check('NeuronFragment', unique_in = connect_DataSource,
                               name = uname)
        batch = self.graph.batch()

        neuron_name = _to_var_name(uname)
        neuron_info = {'uname': uname, 'name': name}
        if isinstance(referenceId, str):
            neuron_info['referenceId'] = referenceId
        else:
            if referenceId is not None:
                raise TypeError('referenceId must be of str type')
        if isinstance(info, dict) and all(isinstance(v, str) for v in info.values()):
            neuron_info['info'] = info
        else:
            if info is not None:
                raise TypeError('info must be a dict with str values')
        
        batch[neuron_name] = batch.NeuronFragments.create(**neuron_info)

        #self.link_with_batch(batch, connect_DataSource, batch[:neuron_name],
        #                     'Owns')

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
            #self.link_with_batch(batch, connect_DataSource, batch[:arb_name], 'Owns')
            if local_neuron is not None:
                self.link_with_batch(batch,
                                     self.get('Neuropil',
                                              local_neuron,
                                              connect_DataSource),
                                     batch[:neuron_name],
                                     'Owns')
        
        neuron = batch['${}'.format(neuron_name)]
        batch.commit(20)
        self._add_to_owns_cache(connect_DataSource.element_type, connect_DataSource, neuron)
        if not self.__neuron_inconsistent_warned:
            warnings.warn("""Created Neuron has not been connected to its DataSource yet. Please execute flush_edges() after adding all Neurons""", category = DataInconsistencyWarning)
            self.__neuron_inconsistent_warned = True 
        self.set('NeuronFragment', uname, neuron, data_source = connect_DataSource)

        if morphology is not None:
            self.add_morphology(neuron, morphology, data_source = connect_DataSource)
        return neuron

    def add_neurotransmitter(self, neuron, neurotransmitters, data_sources = None):
        """
        Add neurotransmitter data and link it to a neuron.

        Parameters
        ----------
        neuron : models.Neuron or subclass
            An instance of Neuron class to which the neurotransmitters will be associated to
        neurotransmitters : str or list of str
            The neurotransmitter(s) expressed by the neuron
        data_sources : models.DataSource or list of models.DataSource (optional)
            The datasource of neurotransmitter data.
            If None, all neurotransmitter will have the same datasource of the Neuron.
            If specified, the size of the list must be the same as the size of
            neurotransmitters, and have one to one corresponsdence in the same order.
        """
        self._database_writeable_check()
        neuron = self._get_obj_from_str(neuron)
        if not isinstance(neuron, models.Neuron):
            raise ValueError("Input not a models.Neuron object")
        if not isinstance(neurotransmitters, list):
            neurotransmitters = [neurotransmitters]
        if not all(isinstance(a, str) for a in neurotransmitters):
            raise ValueError('neurotransmitters must be a str or a list of str')
        if data_sources is None:
            ntds = [self._default_DataSource]*len(neurotransmitters)
        else:
            data_souces = self._get_obj_from_str(data_sources)
            if isinstance(data_sources, models.DataSource):
                ntds = [data_sources]*len(neurotransmitters)
            elif isinstance(data_sources, list):
                ntds = [self._get_obj_from_str(n) for n in data_sources]
                assert all(isinstance(nt, models.DataSource) for nt in data_sources)
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
        obj : models.BioNode or subclass
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
        data_source : models.DataSource (optional)
            The datasource. If not specified, default DataSource will be used.
        """
        self._database_writeable_check()
        connect_DataSource = self._default_DataSource if data_source is None \
                                else self._get_obj_from_str(data_source)
        if connect_DataSource is None:
            raise TypeError('Default DataSource is missing.')
        obj = self._get_obj_from_str(obj)
        if not isinstance(morphology, list):
            morphology = [morphology]
        for i, data in enumerate(morphology):
            content = {'name': obj.name, 'morph_type': data['type']}
            if isinstance(obj, (models.Neuron, models.Synapse)):
                content['uname'] = obj.uname
            if data['type'] == 'swc':
                has_confidence = False
                if 'filename' in data:
                    df = load_swc(data['filename'])
                    x = [round(i, 2) for i in (df['x']*data['scale']).tolist()]
                    y = [round(i, 2) for i in (df['y']*data['scale']).tolist()]
                    z = [round(i, 2) for i in (df['z']*data['scale']).tolist()]
                    r = [round(i, 5) for i in (df['r']*data['scale']).tolist()]
                    parent = df['parent'].tolist()
                    identifier = df['identifier'].tolist()
                    sample = df['sample'].tolist()
                    if 'confidence' in df:
                        confidence = df['confidence'].tolist()
                        has_confidence = True
                else:
                    x = data['x']
                    y = data['y']
                    z = data['z']
                    r = data['r']
                    parent = data['parent']
                    identifier = data['identifier']
                    sample = data['sample']
                    if 'confidence' in data:
                        confidence = data['confidence']
                        has_confidence = True
                content['x'] = x
                content['y'] = y
                content['z'] = z
                content['r'] = r
                content['parent'] = parent
                content['identifier'] = identifier
                content['sample'] = sample
                if has_confidence:
                    content['confidence'] = confidence
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
            self.link(obj, morph_obj, 'HasData')
            #self.link(connect_DataSource, morph_obj, 'Owns')
            

    def add_neuron_arborization(self, neuron, arborization, data_source = None):
        """
        Add arborization data of a neuron and link it to the neuron.

        Parameters
        ----------
        neuron : models.Neuron or subclass
            An instance of Neuron class to which the arborization data will be associated to.
        arborization : list of dict
            A list of dictionaries define the arborization pattern of
            the neuron in neuropils, subregions, and tracts, if applicable, with
            {'type': 'neuropil' or 'subregion' or 'tract',
             'dendrites': {'EB': 20, 'FB': 2},
             'axons': {'NO': 10, 'MB': 22}}
            Name of the regions must already be present in the database.
        data_source : models.DataSource (optional)
            The datasource. If not specified, default DataSource will be used.
        """
        self._database_writeable_check()
        neuron = self._get_obj_from_str(neuron)
        if not isinstance(neuron, models.Neuron):
            raise ValueError('Input is not a models.Neuron object')
        self._uniqueness_check('ArborizationData', unique_in = neuron)
        connect_DataSource = self._default_DataSource if data_source is None \
                                else self._get_obj_from_str(data_source)
        if connect_DataSource is None:
            raise TypeError('Default DataSource is missing.')

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
        #self.link_with_batch(batch, connect_DataSource, batch[:arb_name], 'Owns')
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
        pre_neuron : str or models.NeuroAndFragment
            The neuron that is presynaptic in the synapse.
            If str, must be the uname or rid of the presynaptic neuron.
        post_neuron : str or models.NeuroAndFragment
            The neuron that is postsynaptic in the synapse.
            If str, must be the uname or rid of the postsynaptic neuron.
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
        data_source : models.DataSource (optional)
            The datasource. If not specified, default DataSource will be used.

        Returns
        -------
        synapse : models.Synapse
            The created synapse object.
        """
        self._database_writeable_check()
        connect_DataSource = self._default_DataSource if data_source is None \
                              else self._get_obj_from_str(data_source)
        if connect_DataSource is None:
            raise TypeError('Default DataSource is missing.')
        # self._uniqueness_check('Synapse', unique_in = connect_DataSource,
                               # name = name)

        pre_neuron = self._get_obj_from_str(pre_neuron)
        post_neuron = self._get_obj_from_str(post_neuron)
        if isinstance(pre_neuron, models.NeuronAndFragment):
            pre_neuron_obj = pre_neuron
            pre_neuron_name = pre_neuron.name
        elif isinstance(pre_neuron, str):
            pre_neuron_name = pre_neuron
            pre_neuron_obj = self.get('NeuronAndFragment', pre_neuron_name,
                                      connect_DataSource)
        else:
            raise TypeError('Parameter pre_neuron must be either a str or a Neuron object.')

        if isinstance(post_neuron, models.NeuronAndFragment):
            post_neuron_obj = post_neuron
            post_neuron_name = post_neuron.name
        elif isinstance(post_neuron, str):
            post_neuron_name = post_neuron
            post_neuron_obj = self.get('NeuronAndFragment', post_neuron_name,
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

        #self.link_with_batch(batch, connect_DataSource, batch[:synapse_obj_name],
        #                     'Owns')
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
            # self.link_with_batch(batch, connect_DataSource, batch[:arb_name], 'Owns')
        synapse = batch['${}'.format(synapse_obj_name)]
        batch.commit(20)
        # datasource -Owns-> synapse is postponed to the cache due to performance issue
        # with orientdb to handle edge insertion into super node.
        self._add_to_owns_cache(connect_DataSource.element_type, connect_DataSource, synapse)
        if not self.__synapse_inconsistent_warned:
            warnings.warn("""Created Synapse has not been connected to its DataSource yet. Please execute flush_edges() after adding all Synapses""", category = DataInconsistencyWarning)
            self.__synapse_inconsistent_warned = True
        #self.set('Synapse', '{}{}'.format(synapse_name, synapse._id), synapse, data_source = connect_DataSource)

        if morphology is not None:
            self.add_morphology(synapse, morphology, data_source = connect_DataSource)
        return synapse

    def add_synapse_arborization(self, synapse, arborization, data_source = None):
        """
        Add data for the distribution of synapses within a Synapse node in Neuropils, Subregions and Tracts.

        Parameters
        ----------
        synapse : neuorarch.models.synapse or subclass
            An instance of Synapse class to which the arborization data will be associated to.
        arborization : list of dict (optional)
            A list of dictionaries define the arborization pattern of
            the neuron in neuropils, subregions, and tracts, if applicable, with
            {'type': 'neuropil' or 'subregion' or 'tract',
             'synapses': {'EB': 20, 'FB': 2}}
            Name of the regions must already be present in the database.
        data_source : models.DataSource (optional)
            The datasource. If not specified, default DataSource will be used.

        """
        self._database_writeable_check()
        synapse = self._get_obj_from_str(synapse)
        if not isinstance(synapse, models.Synapse):
            raise ValueError('Input is not a models.Synapse object')
        self._uniqueness_check('ArborizationData', unique_in = synapse)
        connect_DataSource = self._default_DataSource if data_source is None \
                              else self._get_obj_from_str(data_source)
        if connect_DataSource is None:
            raise TypeError('Default DataSource is missing.')

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
        # self.link_with_batch(batch, connect_DataSource, batch[:arb_name], 'Owns')
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
        data_source : models.DataSource (optional)
            The datasource. If not specified, default DataSource will be used.

        Returns
        -------
        synapse : models.InferredSynapse
            The created synapse object.
        """
        self._database_writeable_check()
        connect_DataSource = self._default_DataSource if data_source is None \
                              else self._get_obj_from_str(data_source)
        if connect_DataSource is None:
            raise TypeError('Default DataSource is missing.')
        # self._uniqueness_check('Synapse', unique_in = connect_DataSource,
                               # name = name)

        pre_neuron = self._get_obj_from_str(pre_neuron)
        post_neuron = self._get_obj_from_str(post_neuron)
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

        # self.link_with_batch(batch, connect_DataSource, batch[:synapse_obj_name],
        #                      'Owns')
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
            # self.link_with_batch(batch, connect_DataSource, batch[:arb_name], 'Owns')
        synapse = batch['${}'.format(synapse_obj_name)]
        batch.commit(20)
        # datasource -Owns-> synapse is postponed to the cache due to performance issue
        # with orientdb to handle edge insertion into super node.
        self._add_to_owns_cache(connect_DataSource.element_type, connect_DataSource, synapse)
        if not self.__synapse_inconsistent_warned:
            warnings.warn("""Created Synapse has not been connected to its DataSource yet. Please execute flush_edges() after adding all Synapses""", category = DataInconsistencyWarning)
            self.__synapse_inconsistent_warned = True
        #self.set('Synapse', '{}{}'.format(synapse_name, synapse._id), synapse, data_source = connect_DataSource)

        if morphology is not None:
            self.add_morphology(synapse, morphology, data_source = connect_DataSource)
        return synapse

    def link(self, node1, node2, edge_type = None, **attr):
        """
        To create an edge between node1 and node2 with specified edge type.

        Parameters
        ----------
        node1 : models.Node or subclass
            The node where the edge starts from.
        node2 : models.Node or subclass
            The node where the edge ends with.
        edge_type : str (optional)
            The name of the type of the edge (relation).
            If `edge_type` not given, infer from the types of nodes.
        attr : dict (optional)
            Attributes of the edge.

        Returns
        -------
        edge : models.Relationship or subclass
            The created edge.
        """
        if edge_type is None:
            edge_type = relations[node1.element_type][node2.element_type]
        edge = getattr(self.graph, edge_type).create(node1, node2, **attr)
        return edge

    def link_with_batch(self, batch, node1, node2, edge_type, **attr):
        """
        To create an edge between node1 and node2 with specified edge type, when node1 and node2 are to be created in batch.

        Parameters
        ----------
        batch : pyorient.ogm.batch.Batch
            A batch object.
        node1 : models.Node or subclass
            The node where the edge starts from.
        node2 : models.Node or subclass
            The node where the edge ends with.
        edge_type : str (optional)
            The name of the type of the edge (relation).
            If `edge_type` not given, infer from the types of nodes.
        attr : dict (optional)
            Attributes of the edge.

        Returns
        -------
        edge : models.Relationship or subclass
            The created edge.
        """
        batch[:] = getattr(batch, edge_type).create(node1, node2, **attr)


    def _pre_create_check(self, cls, **attr):
        if self._check:
            if self.exists(cls, **attr):
                raise DuplicateNodeError('{} Node with attributes {} already exists'.format(
                                cls, ', '.join(["""{} = {}""".format(key, value) \
                                for key, value in attr.items()])))
        return True

    # def add_NeuronModel(self, neuron, model, name, circuit_model = None, **kwargs):
    #     self._database_writeable_check()
    #     neuron = self._get_obj_from_str(neuron)
    #     assert isinstance(neuron, models.Neuron), \
    #            'neuron must be either a Neuron object or its rid'
    #     if circuit_model is not None:
    #         self._uniqueness_check('NeuronModel', unique_in = circuit_model,
    #                                name = name)
    #     model_cls = getattr(models, model)
    #     assert issubclass(model_cls, models.NeuronModel), 'Model must be a str of NeuronModel class name'
    #     model_obj = getattr(self.graph, model_cls.element_plural).create(name = name, **kwargs)
    #     if circuit_model is not None:
    #         self.link(model_obj, neuron, 'Models', version = circuit_model.name)
    #     return model_obj
    #
    # def add_SynapseModel(self, synapse, model, name, circuit_model = None, **kwags):
    #     self._database_writeable_check()
    #     synapse = self._get_obj_from_str(synapse)
    #     assert isinstance(synapse, (models.Synapse, models.InferredSynapse)), \
    #            'synapse must be either a Synapse object or its rid'
    #     if circuit_model is not None:
    #         self._uniqueness_check('SynapseModel', unique_in = circuit_model,
    #                                name = name)
    #     model_cls = getattr(models, model)
    #     assert issubclass(model_cls, models.SynapseModel), 'Model must be a str of SynapseModel class name'
    #     model_obj = getattr(self.graph, model_cls.element_plural).create(name = name, **kwargs)
    #     if circuit_model is not None:
    #         self.link(model_obj, neuron, 'Models', version = circuit_model.name)
    #     return model_obj

    def remove_Neurons(self, neurons, data_source = None, safe = True):
        """
        Remove neurons

        Parameters
        ----------
        neurons: list of models.Neuron or str
            The neurons to be removed from database.
            All synapses, data associated with these neurons will also be removed
        data_source: model.DataSource
            The DataSource from which the neurons will be moved,
            if str (as uname of neurons) are provided to the neurons parameter.
        safe: bool
            If safe is True, will check every item in the neurons list
            if it is owned by the data_source. Otherwise, if models.Neuron is
            provided, will not check.
        """
        self._database_writeable_check()
        connect_DataSource = self._default_DataSource if data_source is None \
                             else self._get_obj_from_str(data_source)
        if connect_DataSource is None:
            raise TypeError('Default DataSource is missing.')

        if not isinstance(neurons, list):
            neurons = [self._get_obj_from_str(neurons)]
        else:
            neurons = [self._get_obj_from_str(neuron) for neuron in neurons]

        neuron_objs = []
        neuron_names = []
        for neuron in neurons:
            if isinstance(neuron, models.Neuron):
                if safe:
                    if not self._is_in_datasource(connect_DataSource, neuron):
                        raise DataSourceError(
                            'Neuron object {} with rid {} does not belong to DataSource {}'.format(
                            neuron.uname, neuron._id, connect_DataSource.name))
                neuron_objs.append(neuron)
            elif isinstance(neuron, str):
                neuron_name = neuron
                try:
                    neuron_obj = self.get('Neuron', neuron_name, connect_DataSource)
                except RecordNotFoundError:
                    warnings.warn('Neuron {} not found in the database, no need to remove'.format(neuron_name),
                                  category = RecordNotFoundWarning)
                except DuplicateNodeError:
                    warnings.warn('Neuron {} found to have more than 1 copy, removing all.',
                                   category = DuplicateNodeWarning)
                    objs = self._find('Neuron', data_source, uname = neuron_name).node_objs
                    neuron_objs.extend(objs)
                except:
                    raise
            else:
                raise TypeError('Parameter neuron must be either a str or a Neuron object.')

        q = QueryWrapper.from_objs(self.graph, neuron_objs)
        post_synapses = outgoing_synapses(q)
        pre_synapses = incoming_synapses(q)
        data = get_data(q)
        any_thing_owned = q.gen_traversal_out(['Owns'], min_depth = 1)
        rids_to_delete = set(
            q.node_rids+post_synapses.node_rids+pre_synapses.node_rids+\
            data.node_rids+any_thing_owned.node_rids
            )
        self._remove_by_rids(rids_to_delete)

    def remove_Synapses(self, synapses, data_source = None, safe = True):
        """
        Remove synapses.

        Parameters
        ----------
        synapses: list of models.Synapse, models.InferredSynapse or str
            The synapses to be removed from database.
            All data associated with these neurons will also be removed
        data_source: model.DataSource (optional)
            The DataSource from which the synapses will be found will be removed,
            if str (as uname of synapses) are provided to the neuron parameter.
        safe: bool (optional)
            If safe is True, will check every item in the neurons list
            if it is owned by the data_source. Otherwise, if models.Neuron is
            provided, will not check.
        """
        self._database_writeable_check()
        connect_DataSource = self._default_DataSource if data_source is None \
                                else self._get_obj_from_str(data_source)
        if connect_DataSource is None:
            raise TypeError('Default DataSource is missing.')

        if not isinstance(synapses, list):
            synapses = [self._get_obj_from_str(synapses)]
        else:
            synapses = [self._get_obj_from_str(synapse) for synapse in synapses]

        synapse_objs = []
        for synapse in synapses:
            if isinstance(synapse, (models.Synapse, models.InferredSynapse)):
                if safe:
                    if not self._is_in_datasource(connect_DataSource, synapse):
                        raise DataSourceError(
                            'Synapse {} is not in datasource {}'.format(
                            synapse.uname, connect_DataSource.name))
                synapse_objs.append(synapse)

            elif isinstance(synapse, str):
                synapse_name = synapse
                try:
                    synapse_obj = self.get('Synapse', synapse_name, connect_DataSource)
                except RecordNotFoundError:
                    try:
                        synapse_obj = self.get('InferredSynapse', synapse_name, connect_DataSource)
                    except RecordNotFoundError:
                        warnings.warn('synapse/InferredSynapse {} not found in the database, no need to remove'.format(synapse_name), category = RecordNotFoundWarning)
                    except DuplicateNodeError:
                        warnings.warn('Synapse {} found to have more than 1 copy, removing all.',
                                      category = DuplicateNodeWarning)
                        objs = self._find('InferredSynapse', data_source, uname = synapse_name).node_objs
                        synapse_objs.extend(objs)
                    except:
                        raise
                except DuplicateNodeError:
                    warnings.warn('Synpase {} found to have more than 1 copy, removing all.',
                                  category =  DuplicateNodeWarning)
                    objs = self._find('Synpase', data_source, uname = synapse_name).node_objs
                    synapse_objs.extend(objs)
                except:
                    raise
            else:
                raise TypeError('Parameter synapse must be either a str or a Synapse or InferredSynapse object.')
        q = QueryWrapper.from_objs(self.graph, synapse_objs)
        data = get_data(q)
        any_thing_owned = q.gen_traversal_out(['Owns'], min_depth = 1)
        rids_to_delete = set(
            q.node_rids+data.node_rids+any_thing_owned.node_rids
            )
        self._remove_by_rids(rids_to_delete)

    def remove_Synapses_between(self, pre_neurons, post_neurons, data_source = None):
        """
        Remove synapses between a list of presynaptic neurons and
        a list of postsynaptic neurons.

        Parameters
        ----------
        pre_neurons : a list of str or models.Neuron
            The presynaptic neurons. If specified by str, can be either the rid or the uname of the neurons.
        post_neurons : a list of str or models.Neuron
            The postsynaptic neurons. If specified by str, can be either the rid or the uname of the neurons.
        data_source: model.DataSource (optional)
            The DataSource from which the pre- and post-synaptic neurons will be found will be queried.
        """
        self._database_writeable_check()
        connect_DataSource = self._default_DataSource if data_source is None \
                                else self._get_obj_from_str(data_source)
        if connect_DataSource is None:
            raise TypeError('Default DataSource is missing.')

        if not isinstance(pre_neurons, list):
            pre_neurons = [self._get_obj_from_str(pre_neurons)]
        else:
            pre_neurons = [self._get_obj_from_str(neuron) for neuron in pre_neurons]
        if not isinstance(post_neurons, list):
            post_neurons = [self._get_obj_from_str(post_neurons)]
        else:
            post_neurons = [self._get_obj_from_str(neuron) for neuron in post_neurons]

        pre_neurons_objs = []
        post_neurons_objs = []
        for neuron in pre_neurons:
            if isinstance(neuron, models.Neuron):
                pre_neuron_objs.append(neuron)
            elif isinstance(neuron, str):
                pre_neuron_objs.append(self.get('Neuron', neuron, connect_DataSource))
            else:
                raise TypeError('Parameter neuron must be either a str or a Neuron object.')
        for neuron in post_neurons:
            if isinstance(neuron, models.Neuron):
                post_neuron_objs.append(neuron)
            elif isinstance(neuron, str):
                post_neuron_objs.append(self.get('Neuron', neuron, connect_DataSource))
            else:
                raise TypeError('Parameter neuron must be either a str or a Neuron object.')

        pre_q = QueryWrapper.from_objs(self.graph, pre_objs)
        post_q = QueryWrapper.from_objs(self.graph, post_neuron_objs)
        synapses = outgoing_synapses(pre_q) & incoming_synapses(post_q)
        self.remove_synapses(synapses.node_objs, safe = False)

    def remove_Neuropil(self):
        pass

    def update_Neuron(self, neuron,
                      uname = None,
                      name = None,
                      referenceId = None,
                      locality = None,
                      synonyms = None,
                      info = None,
                      morphology = None,
                      arborization = None,
                      neurotransmitters = None,
                      neurotransmitters_datasources = None,
                      data_source = None,
                      safe = True):
        """
        Update any property/data of a neuron.

        Parameters
        ----------
        neuron : str or models.Neuron
            The neuron to be updated, speicified either by
            a mdoels.Neuron object, a str for its uname or a str
            starts with '#' as the rid of OrientDB record ID.
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
        neurotransmitters_datasources : models.DataSource or list of models.DataSource (optional)
            The datasource of neurotransmitter data.
            If None, all neurotransmitter will have the same datasource of the Neuron.
            If specified, the size of the list must be the same as the size of
            neurotransmitters, and have one to one corresponsdence in the same order.
        data_source : models.DataSource (optional)
            The datasource. If not specified, default DataSource will be used.
        safe : bool (optional)
            If safe is True, will check every item in the neurons list
            if it is owned by the data_source. Otherwise, if models.Neuron is
            provided, will not check.

        Returns
        -------
        bool
            Whether the update was successful.
        """
        self._database_writeable_check()
        connect_DataSource = self._default_DataSource if data_source is None \
                                else self._get_obj_from_str(data_source)
        if connect_DataSource is None:
            raise TypeError('Default DataSource is missing.')

        neuron = self._get_obj_from_str(neuron)
        if isinstance(neuron, models.Neuron):
            neuron_to_update = neuron
            if safe:
                if not self._is_in_datasource(connect_DataSource, neuron):
                    raise DataSourceError(
                        'The neuron specified {} is not owned by the DataSource {}'.format(
                            neuron.uname, connect_DataSource.name))
        elif isinstance(neuron, str):
            neuron_name = neuron
            neuron_to_update = self.get('Neuron', neuron_name, connect_DataSource)
        else:
            raise TypeError('Parameter neuron must be either a str or a Neuron object.')

        update_chain = False
        neuron_info = copy.deepcopy(neuron_to_update.get_props())
        if isinstance(uname, str):
            if uname != neuron_to_update.uname:
                self._uniqueness_check('Neuron', unique_in = connect_DataSource,
                                       name = uname)
                update_chain = True
                neuron_info['uname'] = uname
        elif uname is not None:
            raise TypeError('uname must be of str type')

        if isinstance(name, str):
            if name != neuron_to_update.name:
                update_chain = True
                neuron_info['name'] = name
        elif name is not None:
            raise TypeError('name must be of str type')

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

        neuron_to_update.update(**neuron_info)

        q_neuron = QueryWrapper.from_objs(self.graph, neuron_to_update)

        if arborization is not None:
            arborization_data = get_data(q_neuron, data_types = 'ArborizationData')
            if len(arborization_data):
                self._remove_by_rids(arborization_data.rids)
            self.add_neuron_arborization(neuron_to_update, arborization, data_source = connect_DataSource)
        else:
            if update_chain:
                arborization_data = get_data(q_neuron, data_types = 'ArborizationData')
                if len(arborization_data):
                    node = arborization_data.node_objs[0]
                    props = node.get_props()
                    update_props = {}
                    if 'name' in props:
                        update_props['name'] = neuron_info['name']
                    if 'uname' in props:
                        update_props['uname'] = neuron_info['uname']
                    node.update(**update_props)

        if neurotransmitters is not None:
            neurotransmitter_data = get_data(q_neuron, data_types = 'NeurotransmitterData')
            if len(neurotransmitter_data):
                self._remove_by_rids(neurotransmitter_data.rids)
            self.add_neurotransmitter(neuron_to_update, neurotramistters, data_source = neurotransmitters_datasources if neurotransmitters_datasources is not None else data_source)
        else:
            if update_chain:
                neurotransmitter_data = get_data(q_neuron, data_types = 'NeurotransmitterData')
                for node in neurotransmitter_data.node_objs:
                    props = node.get_props()
                    update_props = {}
                    if 'name' in props:
                        update_props['name'] = neuron_info['name']
                    if 'uname' in props:
                        update_props['uname'] = neuron_info['uname']
                    node.update(**update_props)

        if morphology is not None:
            if not isinstance(morphology, list):
                morphology = [morphology]
            morphology_types_to_update = [m['type'] for m in morphology]
            morphology_data = get_data(q_neuron, data_types = 'MorphologyData')
            if len(morphology_data):
                nodes_to_remove = [m._id for m in morphology_data.node_objs if m.type in morphology_types_to_update]
                self._remove_by_rids(nodes_to_remove)
            self.add_morphology(neuron_to_update, morphology, data_source = data_source)
        else:
            if update_chain:
                morphology_data = get_data(q_neuron, data_types = 'MorphologyData')
                for node in morphology_data.node_objs:
                    props = node.get_props()
                    update_props = {}
                    if 'name' in props:
                        update_props['name'] = neuron_info['name']
                    if 'uname' in props:
                        update_props['uname'] = neuron_info['uname']
                    node.update(**update_props)
        if not update_chain:
            return True

        pre_synapses = incoming_synapses(q_neuron).node_objs
        for node in tqdm(pre_synapses):
            props = node.get_props()
            update_props = {}
            if 'name' in props:
                pre, post = props['name'].split('--')
                synapse_name = '{}--{}'.format(pre, neuron_info['name'])
                update_props['name'] = synapse_name
            if 'uname' in props:
                pre, post = props['uname'].split('--')
                synapse_uname = '{}--{}'.format(pre, neuron_info['uname'])
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

        post_synapses = outgoing_synapses(q_neuron).node_objs
        for node in tqdm(post_synapses):
            props = node.get_props()
            update_props = {}
            if 'name' in props:
                pre, post = props['name'].split('--')
                synapse_name = '{}--{}'.format(neuron_info['name'], post)
                update_props['name'] = synapse_name
            if 'uname' in props:
                pre, post = props['uname'].split('--')
                synapse_uname = '{}--{}'.format(neuron_info['uname'], post)
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
        return True

    def update_Synapse(self, synapse,
                       N = None, NHP = None,
                       morphology = None,
                       arborization = None,
                       data_source = None):
        """
        Update any property/data of a synapse.

        Parameters
        ----------
        synapse : str or models.Synapse
            The synapse to be updated, speicified either by
            a mdoels.Synapse object, a str for its uname or a str
            starts with '#' as the rid of OrientDB record ID.
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
        data_source : models.DataSource (optional)
            The datasource. If not specified, default DataSource will be used.

        Returns
        -------
        bool
            Whether the update was successful.
        """
        self._database_writeable_check()
        connect_DataSource = self._default_DataSource if data_source is None \
                                else self._get_obj_from_str(data_source)
        if connect_DataSource is None:
            raise TypeError('Default DataSource is missing.')

        synapse = self._get_obj_from_str(synapse)
        if isinstance(synapse, [models.Synapse, models.InferredSynapse]):
            synapse_to_update = synapse
            if safe:
                if not self._is_in_datasource(connect_DataSource, synapse):
                    raise DataSourceError(
                        'The synapse specified {} is not owned by the DataSource {}'.format(
                            synapse.uname, connect_DataSource.name))
        elif isinstance(synapse, str):
            synapse_name = synapse
            try:
                synapse_to_update = self.get('Synapse', synapse_name, connect_DataSource)
            except RecordNotFoundError:
                synapse_to_update = self.get('InferredSynapse', synapse_name, connect_DataSource)
        else:
            raise TypeError('Parameter synapse must be either a str or a Synapse object.')

        synapse_info = copy.deepcopy(synapse_to_update.get_props())
        if isinstance(N, int):
            synapse_info['N'] = N
        elif N is not None:
            raise TypeError('N must be of integer type.')

        if isinstance(NHP, int):
            if NHP > synapse_info["N"]:
                raise ValueError('NHP cannot be greater than N')
            synapse_info['NHP'] = NHP
        elif NHP is not None:
            raise TypeError('NHP must be of integer type.')

        synapse_to_update.update(**synapse_info)

        q_synapse = QueryWrapper.from_objs(self.graph, synapse_to_update)

        if arborization is not None:
            arborization_data = get_data(q_synapse, data_types = 'ArborizationData')
            if len(arborization_data):
                self._remove_by_rids(arborization_data.rids)
            self.add_synapse_arborization(synapse_to_update, arborization, data_source = data_source)

        if morphology is not None:
            if not isinstance(morphology, list):
                morphology = [morphology]
            morphology_types_to_update = [m['type'] for m in morphology]
            morphology_data = get_data(q_synapse, data_types = 'MorphologyData')
            if len(morphology_data):
                nodes_to_remove = [m._id for m in morphology_data.node_objs if m.type in morphology_types_to_update]
                self._remove_by_rids(nodes_to_remove)
            self.add_morphology(synapse_to_update, morphology, data_source = data_source)
        return True

    def update_Neuropil(self):
        pass

    def _remove_by_rids(self, rids):
        """
        Remove a record by rid.

        Should only be used internally to retain the integrity of the database.

        Parameters
        ----------
        rids : list of str
            A list of rids to be removed.
        """
        self._database_writeable_check()
        self.graph.client.command("""delete vertex {}""".format(
                                        ','.join(rids)))

    def export_tags(self, filename):
        """
        Export all the tags to a JSON file.

        Parameters
        ----------
        filename : str
            The name of the JSON file to be export to.
        """
        all_tags = self.sql_query("""select from QueryResult""").node_objs

        query_results = {}
        for tag in all_tags:
            props = tag.get_props()
            results = tag.out()
            records = {}
            try:
                color = props['color'].copy()
                visibility = props['visibility'].copy()
                pinned = props['pinned']
            except KeyError:
                print('tag {} is incomplete and thus not exported'.format(tag.tag))
                continue
            for n in results:
                if isinstance(n, models.Neuron):
                    try:
                        morphology = [n for n in n.out('HasData') if isinstance(n, models.MorphologyData)][0]
                    except IndexError:
                        print('neuron {} not exported in tag {}'.format(n.name if n.uname is None else n.uname, tag.tag))
                        continue
                    records[n.uname] = {'type': 'Neuron',
                                        'referenceId': n.referenceId,
                                        'visible': visibility.pop(morphology._id, True),
                                        'color': color.pop(morphology._id, [1.0, 0., 0.]),
                                        'pinned': morphology._id in pinned}
                elif isinstance(n, (models.Synapse, models.InferredSynapse)):
                    try:
                        morphology = [n for n in n.out('HasData') if isinstance(n, models.MorphologyData)][0]
                    except IndexError:
                        print('synapse {} not exported in tag {}'.format(n.name if n.uname is None else n.uname, tag.tag))
                        continue
                    records[n.uname] = {'type': n.element_type,
                                        'pre': n.in_('SendsTo')[0].referenceId,
                                        'post': n.out('SendsTo')[0].referenceId,
                                        'visible': visibility.pop(morphology._id, True),
                                        'color': color.pop(morphology._id, [1.0, 0., 0.]),
                                        'pinned': morphology._id in pinned}
                else:
                    raise TypeError("type of record not understood: {}".format(n.element_type))

            for n in list(visibility.keys()):
                if n.startswith('#'):
                    visibility.pop(n)
            for n in list(color.keys()):
                if n.startswith('#'):
                    color.pop(n)

            neuropils = {'visibility': visibility, 'color': color}
            query_results[tag.tag] = {'target': props['target'],
                                      'camera': props['camera'],
                                      'records': records,
                                      'neuropils': neuropils}
            settings = props.get('settings', None)
            if settings is not None:
                query_results[tag.tag]['settings'] = settings

        with open(filename, 'w') as f:
            json.dump(query_results, f)

    def import_tags(self, filename):
        """
        Import tags from a JSON file.

        Parameters
        ----------
        filename : str
            The name of the JSON file to import (typically was created by the export_tag)
        """
        self._database_writeable_check()

        with open(filename) as f:
            query_results = json.load(f)

        imported_tags = []
        not_imported = []
        for tag, r in query_results.items():
            print(tag)
            rids_neurons = []
            rids_synapses = []
            visibility = r['neuropils']['visibility']
            color = r['neuropils']['color']
            pinned = []
            try:
                for uname, attr in r['records'].items():
                    if attr['type'] == 'Neuron':
                        if attr['referenceId'] is None:
                            neuron = self.graph.Neurons.query(uname = uname).one()
                        else:
                            neuron = self.graph.Neurons.query(referenceId = attr['referenceId']).one()
                        rids_neurons.append(neuron._id)
                        obj = neuron
                    else: # assuming the rest are synapses
                        if attr['pre'] is None:
                            pre_neuron = self.graph.Neurons.query(uname = uname.split('--')[0]).one()
                        else:
                            pre_neuron = self.graph.Neurons.query(referenceId = attr['pre']).one()
                        if attr['post'] is None:
                            post_neuron = self.graph.Neurons.query(uname = uname.split('--')[1]).one()
                        else:
                            post_neuron = self.graph.Neurons.query(referenceId = attr['post']).one()
                        synapse_rids = list(set(n._id for n in pre_neuron.out('SendsTo')).intersection(set(n._id for n in post_neuron.in_('SendsTo'))))
                        rids_synapses.append(synapse_rids[0])
                        synapse = QueryWrapper.from_rids(self.graph, synapse_rids[0]).node_objs[0]
                        obj = synapse
                    morphology = [n for n in obj.out('HasData') if isinstance(n, models.MorphologyData)][0]
                    visibility[morphology._id] = attr['visible']
                    color[morphology._id] = attr['color']
                    if attr['pinned']:
                        pinned.append(morphology._id)
            except NoResultFound:
                not_imported.append(tag)
                print('Some records are not found in the current database for tag {}, skipping importing'.format(tag))
                print('record not found: {}'.format(uname))
                print('attributes:')
                print(attr)
                continue
            except IndexError:
                not_imported.append(tag)
                print('Some synapse records are not found when importing tag {}, skipping importing'.format(tag))
                print('record not found: {}'.format(uname))
                print('attributes:')
                print(attr)
            kwargs = {'target': r['target'], 'camera': r['camera'],
                      'color': color, 'visibility': visibility, 'pinned': pinned}
            settings = r.get('settings', None)
            if settings is not None:
                kwargs['settings'] = settings
            q = QueryWrapper.from_rids(self.graph, *(rids_neurons+rids_synapses))
            q.tag_query_result_node(tag, True, **kwargs)
            imported_tags.append(tag)
        print('The following tags has been imported:\n{}'.format('\n'.join(imported_tags)))
        print('The following tags were not imported:\n{}'.format('\n'.join(not_imported)))
        return imported_tags, not_imported

    def remove_tag(self, tag_name):
        """
        Remove a tag.

        Parameters
        ----------
        tag_name : str
            The name of the tag to be removed.
        """
        try:
            tag = self.sql_query("""select from QueryResult where tag = "{}" """.format(tag_name)).node_objs[0]
        except IndexError:
            raise ValueError("tag {} does not exist".format(tag_name))

        print('removing tag {} ...'.format(tag_name))
        try:
            for n in tag.out():
                self.graph.client.command("""delete edge from {} to {}""".format(tag._id, n._id))
            for n in tag.in_():
                self.graph.client.command("""delete edge from {} to {}""".format(n._id, tag._id))
            self.graph.client.command("""delete vertex {}""".format(tag._id))
        except:
            raise RuntimeError("tag {} cannot be removed cleanly".format(tag_name))
        return True

    def available_DataSources(self):
        """
        Retrieve all available DataSources.

        Returns
        -------
        dict
            A dictionary with the rids of DataSource nodes as keys and the DataSource properties as values.
        """
        return {n._id: n.get_props() for n in self.find_objs('DataSource')}

    def create_model_from_circuit(self, model_name, model_version, graph,
                                  circuit_diagrams = None, submodules = None):
        """
        Create a model of a circuit.

        Parameters
        ----------
        model_name : str
            The name of the model.
        model_version : str
            The version of the model.
        graph : networkx.DiGraph or dict.
            A graph specifying the models of the circuit. The graph should contain both BioNodes, e.g., Neurons, Synapses, and DesignNodes, e.g., NeuronModels, SynapseModels, linked with a 'models' relationship.
        circuit_diagrams : dict (optional)
            A dict of str specifying the diagram in SVG format.
        js : dict (optional)
            A dict of str specifying the javascript submodules for interactivity.
        
        Returns
        -------
        dict
            A dictionary mapping the original id of model nodes to the rid of the record created.
        """
        if isinstance(graph, nx.DiGraph):
            g = graph
        elif isinstance(graph, dict) and 'nodes' in graph and 'edges' in graph:
            g = nx.MultiDiGraph()
            g.add_nodes_from(graph['nodes'])
            g.add_edges_from(graph['edges'])
        print({rid: v for rid , v in g.nodes(data = True) if 'class' not in v})

        neuron_nodes = [rid for rid, v in g.nodes(data=True) if issubclass(getattr(models, v['class']), models.Neuron)]
        synapse_nodes = [rid for rid, v in g.nodes(data=True) if issubclass(getattr(models, v['class']), models.Synapse)]
        neurons =  QueryWrapper.from_rids(self.graph, *neuron_nodes)
        synapses =  QueryWrapper.from_rids(self.graph, *synapse_nodes)
        neuropils = neurons.traverse_owned_by(cls = 'Neuropil')

        # create LPU
        circuit_model_obj = self.add_ExecutableCircuit(model_name, version = model_version,
                                                       circuit_diagrams = circuit_diagrams,
                                                       submodules = submodules)

        lpus = {}
        for neuropil in neuropils.node_objs:
            lpus[neuropil._id] = self.add_LPU(neuropil, circuit_model_obj, version = model_version)

        neuron_models = {}
        for neuron in neurons.node_objs:
            model = [pre for pre, post, v in g.in_edges(neuron._id, data = True) if v['class'] == 'Models'][0]
            params = copy.deepcopy(g.nodes[model])
            cls = params.pop('class')
            for k in params['params']:
                params['params'][k] = float(params['params'][k])
            for k in params['states']:
                params['states'][k] = float(params['states'][k])
            neuropil = QueryWrapper.from_rids(self.graph, neuron._id).traverse_owned_by(cls = 'Neuropil').node_objs[0]
            neuron_models[neuron._id] = self.add_NeuronModel(neuron, cls,
                                                             lpus[neuropil._id],
                                                             **params)

        synapse_models = {}
        for synapse in synapses.node_objs:
            model = [pre for pre, post, v in g.in_edges(synapse._id, data = True) if v['class'] == 'Models'][0]
            params = copy.deepcopy(g.nodes[model])
            cls = params.pop('class')
            for k in params['params']:
                params['params'][k] = float(params['params'][k])
            for k in params['states']:
                params['states'][k] = float(params['states'][k])
            pre_neuron = [pre for pre, post, v in g.in_edges(synapse._id, data = True) \
                          if v['class'] == 'SendsTo' and \
                             issubclass(getattr(models, g.nodes[pre]['class']), models.Neuron)][0]
            post_neuron = [post for pre, post, v in g.out_edges(synapse._id, data = True) \
                           if v['class'] == 'SendsTo' and \
                             issubclass(getattr(models, g.nodes[post]['class']), models.Neuron)][0]
            post_neuron_neuropil = QueryWrapper.from_rids(self.graph, post_neuron).traverse_owned_by(cls = 'Neuropil').node_objs[0]
            synapse_models[synapse._id] = self.add_SynapseModel(
                synapse, cls,
                neuron_models[pre_neuron],
                neuron_models[post_neuron],
                lpus[post_neuron_neuropil._id],
                **params)

        maps = {}
        for node in neuron_nodes:
            model_rid = [pre for pre, post, v in g.in_edges(node, data = True) if v['class'] == 'Models'][0]
            maps[model_rid] = neuron_models[node]._id
        for node in synapse_nodes:
            model_rid = [pre for pre, post, v in g.in_edges(node, data = True) if v['class'] == 'Models'][0]
            maps[model_rid] = synapse_models[node]._id
        return maps

    def add_ExecutableCircuit(self, name, version = None, circuit_diagrams = None,
                              submodules = None):
        """
        Add an executable circuit.

        Parameters
        ----------
        name : str
            The name of the circuit diagram.
        version : str (optional)
            The version of the circuit diagram
        diagram : str (optional)
            A str specifying the diagram in SVG format.
        js : str (optional)
            A str specifying the javascript submodules for interactivity.

        Returns
        -------
        models.ExecutableCircuit
            The ExecutableCircuit object created.
        """
        self._database_writeable_check()
        assert isinstance(name, str), 'name must be a str'
        circuit_info = {'name': name}
        if version is not None:
            if isinstance(version, str):
                circuit_info['version'] = version
            else:
                raise TypeError('version must be a str')

        obj = self.graph.ExecutableCircuits.create(**circuit_info)
        if circuit_diagrams is not None:
            diagram_obj = self.add_CircuitDiagram(name, circuit_diagrams,
                                                  version = version,
                                                  submodules = submodules)
            self.link(obj, diagram_obj, 'HasData')
        return obj

    def add_CircuitDiagram(self, name, diagrams, version = None, submodules = None):
        """
        Add circuit diagram.

        Parameters
        ----------
        name : str
            The name of the circuit diagram.
        diagrams : dict
            A str specifying the diagram in SVG format.
        version : str (optional)
            The version of the circuit diagram
        js : str (optional)
            A str specifying the javascript submodules for interactivity.
        
        Returns
        -------
        models.CircuitDiagram
            The created CircuitDiagram object.
        """
        self._database_writeable_check()
        assert isinstance(name, str), 'name must be a str'
        assert isinstance(diagrams, dict) and all(isinstance(n, str) for n in diagrams.values()),\
               'name must be a dict of str'
        circuit_info = {'name': name, 'diagrams': diagrams}
        if version is not None:
            if isinstance(version, str):
                circuit_info['version'] = version
            else:
                raise TypeError('version must be a str')
        if submodules is not None:
            if isinstance(submodules, dict) and all(isinstance(n, str) for n in submodules.values()):
                circuit_info['submodules'] = submodules
            else:
                raise TypeError('javascript must be a dict of str')
        obj = self.graph.CircuitDiagrams.create(**circuit_info)
        return obj

    def add_LPU(self, neuropil, circuit_model, version = None):
        """
        Add an LPU.

        Parameters
        ----------
        neuropil : str or models.neuropil
            The neuropil that the created LPU models.
        circuit_model : models.ExecutableCircuit or str
            The ExecutableCircuit model that should own this LPU.
        version : str (optional)
            The version of the LPU.
        
        Returns
        -------
        models.LPU
            the created LPU node
        """
        self._database_writeable_check()
        neuropil = self._get_obj_from_str(neuropil)
        if not isinstance(neuropil, models.Neuropil):
            raise TypeError('neuropil must be a models.Neuropil instance or its rid')

        lpu_info = {'name': neuropil.name}
        if version is not None:
            if isinstance(version, str):
                lpu_info['version'] = version
            else:
                raise TypeError('version must be a str')

        circuit_model = self._get_obj_from_str(circuit_model)
        if not isinstance(circuit_model, models.ExecutableCircuit):
            raise TypeError('circuit_model must be an ExecutableCircuit')
        lpu_obj = self.graph.LPUs.create(**lpu_info)
        self.link(lpu_obj, neuropil, 'Models')
        self.link(circuit_model, lpu_obj, 'Owns')
        return lpu_obj

    def add_Pattern(self, tract, circuit_model, version = None):
        self._database_writeable_check()
        pass

    def add_NeuronModel(self, neuron, model_cls, lpu, **params):
        """
        Create a NeuronModel node.

        Parameters
        ----------
        neuron : models.Neuron or str
            An Neuron object or its rid (str) that the NeuronModel models.
        model_cls : str
            The subclass of the model
        lpu : neuronarch.models.LPU
            The LPU that owns the neuron model.
        params : dict
            parameters of the neuron model.

        Returns
        -------
        models.NeuronModel
            The created NeuronModel object.
        """
        self._database_writeable_check()
        neuron = self._get_obj_from_str(neuron)
        assert isinstance(neuron, models.Neuron), \
               'neuron must be either a Neuron object or its rid'
        # circuit_model = self._get_obj_from_str(circuit_model)
        # assert isinstance(neuron, models.ExecutableCircuit), \
        #        'neuron must be either an ExecutableCircuit object or its rid'
        # self._uniqueness_check('NeuronModel', unique_in = circuit_model,
        #                        name = neuron.uname)
        model_cls = getattr(models, model_cls)
        assert issubclass(model_cls, models.NeuronModel),\
               'model_cls must be one of the models.NeuronModel subclass'
        lpu = self._get_obj_from_str(lpu)
        if not isinstance(lpu, models.LPU):
            raise TypeError('lpu must be of models.LPU instance')

        neuron_model_obj = getattr(self.graph, model_cls.element_plural).create(
                                        name = neuron.uname, **params)
        self.link(neuron_model_obj, neuron, 'Models')
        self.link(lpu, neuron_model_obj, 'Owns')
        port_obj = self.add_Port(neuron_model_obj, lpu)
        return neuron_model_obj

    def add_Port(self, neuron, lpu, selector = None):
        """
        Create a Port node.

        Parameters
        ----------
        neuron : models.NeuronModel or str
            An Neuron object or its rid (str) that the port connect to.
        lpu : neuronarch.models.LPU
            The LPU that owns the port.
        selector : str (optional)
            The selector of the port. See neurokernel.plsel.Selector.

        Returns
        -------
        models.Port
            The created Port object.
        """
        self._database_writeable_check()
        neuron = self._get_obj_from_str(neuron)
        if not isinstance(neuron, models.NeuronModel):
            raise TypeError('neuorn must be of model.NeuronModel class')

        lpu = self._get_obj_from_str(lpu)
        if not isinstance(lpu, models.LPU):
            raise TypeError('lpu must be of models.LPU instance')
        port_obj = self.graph.Ports.create()
        port_obj.update(selector = '/{}/{}'.format(lpu.name.replace('(','_').replace(')',''), port_obj._id[1:].replace(':', '0')) if selector is None else selector,
                        port_type = 'spike' if neuron.spiking else 'gpot',
                        port_io = 'out')
        self.link(neuron, port_obj, 'SendsTo')
        self.link(lpu, port_obj, 'Owns')
        return port_obj

    def add_SynapseModel(self, synapse, model_cls,
                         pre_neuron,
                         post_neuron,
                         lpu, **params):
        """
        Create a NeuronModel node.

        Parameters
        ----------
        synapse : models.Synapse or str
            A Synapse object or its rid (str) that the SynapseModel models.
        model_cls : str
            The subclass of the model
        pre_neuron : models.Neuron or str
            The Neuron object or its rid (str) that is presynaptic to the synapse.
        post_neuron : models.Neuron or str
            The Neuron object or its rid (str) that is postsynaptic to the synapse.
        lpu : neuronarch.models.LPU
            The LPU that owns the synapse model.
        params : dict
            parameters of the neuron model.

        Returns
        -------
        models.SynapseModel
            The created SynapseModel object.
        """
        # make sure the the link from post to synapse is also added for synapses like NMDA
        self._database_writeable_check()
        synapse = self._get_obj_from_str(synapse)
        assert isinstance(synapse, (models.Synapse, models.InferredSynapse)), \
                   'synapse must be either a Synapse object or its rid'

        model_cls = getattr(models, model_cls)
        assert issubclass(model_cls, models.SynapseModel),\
               'model_cls must be one of the models.SynapseModel subclass'
        pre_neuron = self._get_obj_from_str(pre_neuron)
        if not issubclass(type(pre_neuron), models.NeuronModel):
            raise TypeError('pre_neuron must be models.NeuronModel type')

        post_neuron = self._get_obj_from_str(post_neuron)
        if not issubclass(type(pre_neuron), models.NeuronModel):
            raise TypeError('post_neuron must be models.NeuronModel type')

        pre_lpu = QueryWrapper.from_rids(self.graph, pre_neuron._id).owned_by(cls = 'LPU').node_objs[0]
        post_lpu = lpu
        #post_lpu = QueryWrapper.from_rids(self.graph, post_neuron._id).owned_by(cls = 'LPU').node_objs[0]

        synapse_model_obj = getattr(self.graph, model_cls.element_plural).create(name = synapse.uname, **params)
        self.link(synapse_model_obj, synapse, 'Models')
        self.link(lpu, synapse_model_obj, 'Owns')

        if pre_lpu._id == post_lpu._id:
            self.link(pre_neuron, synapse_model_obj, edge_type = 'SendsTo', variable = synapse_model_obj.link_pre)
            self.link(synapse_model_obj, post_neuron, edge_type = 'SendsTo')
            if synapse_model_obj.link_post is not None:
                self.link(post_neuron, synapse_model_obj, edge_type = 'SendsTo', variable = synapse_model_obj.link_post)
        else:
            self.link(synapse_model_obj, post_neuron, edge_type = 'SendsTo')
            if synapse_model_obj.link_post is not None:
                self.link(post_neuron, synapse_model_obj, edge_type = 'SendsTo', variable = synapse_model_obj.link_post)
            #check if there is a connected port through Pattern to a port in post_lpu
            # if has
            # link(port_obj, synapse_model_obj, edge_type = 'SendsTo', variable = synapse_model_obj.link_pre)
            # else
            # port_obj = self.add_Port()
            # find pattern or create pattern
            # link(port_obj, synapse_model_obj, edge_type = 'SendsTo', variable = synapse_model_obj.link_pre)
            # link pre_port -> Pattern in port, Pattern in port to Pattern out port, Pattern out port to port_obj
        return synapse_model_obj

def outgoing_synapses(q, N = None, rel='>',include_inferred=True):
    """
    Get all the outgoing synapses from neurons in a QueryWrapper object.

    Parameters
    ----------
    q: query.QueryWrapper
        The query to search for outgoing synapses
    N: int or None
        Filter for number of synapses (default: None, equivalent to 0)
    rel: str
        Ralation operator to the number of synapses when applying the filter.
        (default: '>')
    include_inferred: bool
        Whether to include InferredSynapses (default: True)

    Returns
    -------
    query.QueryWrapper
        containing the outgoing synapses.

    Example
    -------
    db = NeuroArch('hemibrain')
    neurons = db.sql_query("select from Neuron where name like 'EPG' ")
    q = outgoing_synapses(neurons)
    q.node_objs
    """
    synapse_classes = ['Synapse', 'InferredSynapse'] if include_inferred else 'Synapse'
    if N:
        return q.gen_traversal_out(['SendsTo', synapse_classes, {'N':(rel,N)}], min_depth=1)
    else:
        return q.gen_traversal_out(['SendsTo', synapse_classes], min_depth=1)


def incoming_synapses(q, N=None, rel='>',include_inferred=True):
    """
    Get all the input synapses to neurons in a QueryWrapper object.

    Parameters
    ----------
    q: query.QueryWrapper
        The query to search for outgoing synapses
    N: int or None
        Filter for number of synapses (default: None, equivalent to 0)
    rel: str
        Ralation operator to the number of synapses when applying the filter.
        (default: '>')
    include_inferred: bool
        Whether to include InferredSynapses (default: True)

    Returns
    -------
    query.QueryWrapper
        containing the input synapses.

    Example
    -------
    db = NeuroArch('hemibrain')
    neurons = db.sql_query("select from Neuron where name like 'EPG' ")
    q = incoming_synapses(neurons)
    q.node_objs
    """
    synapse_classes = ['Synapse', 'InferredSynapse'] if include_inferred else 'Synapse'
    if N:
        return q.gen_traversal_in(['SendsTo', synapse_classes, {'N':(rel,N)}], min_depth=1)
    else:
        return q.gen_traversal_in(['SendsTo', synapse_classes], min_depth=1)

def get_data(q, data_types = None):
    """
    Get all the data associated with a QueryWrapper object.

    Parameters
    ----------
    q: query.QueryWrapper
        The query to search for outgoing synapses
    data_types: list or None
        The types of data to be retrieved (default: None, equivalent to all data types)

    Returns
    -------
    query.QueryWrapper
        containing the data.

    Example
    -------
    db = NeuroArch('hemibrain')
    neurons = db.sql_query("select from Neuron where name like 'EPG' ")
    q = get_data(neurons, 'MorphologyData')
    q.node_objs
    """
    if data_types is None or len(data_types) == 0:
        data = q.gen_travesal_out(['HasData'], min_depth=1)
    else:
        if not isinstance(data_types, list):
            data_types = [data_types]
        for data_type in data_types:
            if data_type not in models.Data_Types:
                raise('Data type not understood: {}'.format(data_type))
        data = q.gen_traversal_out(['HasData', data_types], min_depth = 1)
    return data

def load_swc(file_name):
    """
    Load an SWC file into a DataFrame.

    Parameters
    ----------
    filename : str
        The name of the SWC file.

    Returns
    -------
    pandas.DataFrame
        a dataframe having the fields sample, identifier, x, y, z, r and parent.
    """
    df = pd.read_csv(file_name, sep = ' ', header=None, comment='#', index_col = False,
                     names=['sample', 'identifier', 'x', 'y', 'z', 'r', 'parent'],
                     skipinitialspace=True)
    return df
