#!/usr/bin/env python

"""
Query wrapper with support for operators.
"""

# Copyright (c) 2015, Lev Givon
# All rights reserved.
# Distributed under the terms of the BSD license:
# http://www.opensource.org/licenses/bsd-license
import collections
import numbers
import pprint
import re
from datetime import datetime
import json
import time
import sys
from warnings import warn

from pyorient.ogm import Config, Graph
from pyorient.exceptions import PyOrientCommandException
from pyorient.utils import get_hash

from neuroarch.utils import is_rid, iterable, chunks, class_method_timer
from neuroarch.diff import diff_nodes, diff_edges
from neuroarch.apply_diff import apply_node_diff, apply_edge_diff

class UnsupportedQueryLanguage(Exception):
    """
    Unsupported query language.
    """
    pass

class UnsupportedView(Exception):
    """
    Unsupported view type.
    """
    pass

QueryString = collections.namedtuple('QueryString', ['str', 'lang'])

from neuroarch.conv import nx,pd

class QueryWrapper(object):
    """
    Gremlin/OrientDB SQL query wrapper.

    This class encapsulates Gremlin and OrientDB SQL queries that return nodes with
    functionality that permits the query to be composed with other queries using
    various operators. The query mechanism can also extract edges between the
    requested nodes.

    Parameters
    ----------
    graph : pyorient.ogm.Graph
        Graph object.
    query : QueryString, tuple, or list
        If a QueryString, contains a single node query. If a tuple/list, must contain
        an operator followed by two operands, which in turn may either be
        tuples/lists formatted as previously described or QueryString instances.
    init_nodes : set or dict
        Node records with which to initialize the query cache. If dict, the keys
        are the record IDs and the values the records.
    execute : bool
        Whether to execute the query immediately upon instantiation.
    executed : bool
        Whether the query has been executed.
    edges : bool
        If True, extract edges between requested nodes.
    debug : bool
        Run in debug mode. If true, will print execution time.
    """

    @class_method_timer
    def __init__(self, graph, query, init_nodes=set(), execute=True, executed = False,
                 edges=False, debug = False):
        assert isinstance(graph, Graph)
        self._graph = graph

        if isinstance(query, QueryString):
            self._query = query
        else:
            assert (isinstance(query, list) or isinstance(query, tuple)) and \
                len(query) == 3
            self._query = query
        if isinstance(init_nodes, dict):
            self._nodes = init_nodes
        else:
            self._nodes = self._records_to_dict(init_nodes)
        self._edges = {}
        self._executed = executed
        self._edge_executed = False
        self.debug = debug
        if execute:
            self.execute(edges)

    def __len__(self):
        if not self._executed:
            self.execute()
        return len(self._nodes)

    def __iter__(self):
        return self.node_objs.__iter__()

    @classmethod
    def empty_query(cls, graph, debug = False):
        return cls(graph, QueryString('select from #-1:0', 'sql'),
                   executed = True, execute = False, debug = debug)

    @classmethod
    def from_objs(cls, graph, objs, debug = False):
        """
        Construct a QueryWrapper given a list of OGM objects

        Parameters
        ----------
        graph : pyorient.ogm.Graph
            Graph object.
        obj : list
            List of orientdb objs

        Returns
        -------
        result : neuroarch.query.QueryWrapper
            QueryWrapper instance.
        """
        if len(objs) == 0:
            return cls.empty_query(graph, debug = debug)
            # return cls(graph,
            #            QueryString(str="""select from DataSource where name = "uiyth" """,
            #                        lang = 'sql'),
            #            init_nodes = set(), executed = True, debug = debug)

        rids = list(map(lambda x: x._id, objs)).__repr__().replace("'","")
        return cls(graph, QueryString(str='select from '+rids,
                                      lang='sql'),
                   debug = debug)

    @classmethod
    def from_tag(self, graph,tag, debug = False):
        """
        """
        obj = graph.QueryResults.query(tag=tag).all()
        if not obj:
            return {}
        QueryResultNode = QueryWrapper.from_objs(graph,obj)
        output = {}
        output['metadata'] = list(QueryResultNode.get_as('df')[0].to_dict(orient='index').values())[0]
        # for i in range(len(output['metadata']['pinned'])):
        #     output['metadata']['pinned'][i] = '#' + ":".join(map(str,output['metadata']['pinned'][i][1:]))
        output['qw'] = QueryResultNode.gen_traversal_out(['HasQueryResults'],min_depth=1)
        return output

    @classmethod
    def from_rids(cls, graph, *rid_list, **kwargs):
        """
        Construct a QueryWrapper given a list of OrientDB RIDs.

        Parameters
        ----------
        graph : pyorient.ogm.Graph
            Graph object.
        rid_list : list
            List of RIDs expressed as strings, e.g., '#12:0'.

        Returns
        -------
        result : neuroarch.query.QueryWrapper
            QueryWrapper instance.
        """

        if len(rid_list) == 0:
            return cls.empty_query(graph, debug = kwargs.get('debug', False))
            # return cls(graph,
            #            QueryString(str="""select from DataSource where name = "uiyth" """,
            #                        lang = 'sql'),
            #            init_nodes = set(), executed = True,
            #            debug = kwargs.get('debug', False))
        assert all([is_rid(rid) for rid in rid_list])
        return cls(graph, QueryString(str='select from [%s]' % ','.join(rid_list),
                                      lang='sql'),
                   debug = kwargs.get('debug', False))

    @classmethod
    def from_elements(cls, graph, *obj_list, **kwargs):
        """
        Construct a QueryWrapper given a list of pyorient OGM objects.

        Parameters
        ----------
        graph : pyorient.ogm.Graph
            Graph object.
        obj_list : list of neuroarch.models.Node or neuroarch.models.Relationship
            List of pyorient OGM objects.

        Returns
        -------
        result : neuroarch.query.QueryWrapper
            QueryWrapper instance.
        """
        if len(obj_list) == 0:
            return cls.empty_query(graph, debug = kwargs.get('debug', False))
            # return cls(graph,
            #            QueryString(str="""select from DataSource where name = "uiyth" """,
            #                        lang = 'sql'),
            #            init_nodes = set(), executed = True,
            #            debug = kwargs.get('debug', False))
        return cls(graph, QueryString(str='select from [%s]' % \
                                      ','.join([obj._id for obj in obj_list]),
                                      lang='sql'),
                   debug = kwargs.get('debug', False))

    @classmethod
    def _records_to_dict(cls, records):
        return {r._rid: r for r in records}

    @classmethod
    def _records_to_list(cls, records):
        return [r._rid for r in records]

    @class_method_timer
    def get_as(self, as_type='df', force_rid=False, edges = True, edge_class = '', deepcopy = False):
        """
        Return view of query results.

        Parameters
        ----------
        as_type : {'df', 'nx', 'obj'}
            If 'df', return results as a tuple of Pandas DataFrame instances.
            If 'nx', return a single NetworkX MultiDiGraph instance.
            If 'obj', return a tuple of pyorient OGM object lists.
        force_rid : bool
            If True, always use the OrientDB RID as the node identifier or index
            in the returned graph or DataFrame. Otherwise, use 'id' property.
            Ignored if `as_type` == 'obj'.

        Returns
        -------
        result : tuple or networkx.MultiDiGraph
            Query results in specified format.
        """

        if edges:
            self.execute_edges(edge_class = edge_class)

        if as_type=='df':
            return pd.as_pandas(self.nodes, self.edges, force_rid, deepcopy = deepcopy)
        if as_type=='nx':
            tmp = nx.as_nx(self.nodes, self.edges, force_rid, deepcopy = deepcopy)
            return tmp
        if as_type=='obj':
            return self._graph.elements_from_records(self.nodes),\
                self._graph.elements_from_records(self.edges)
        raise UnsupportedView(as_type)

    @class_method_timer
    def get_nodes(self, as_type = 'obj', force_rid = False, deepcopy = False):
        """
        Return view of nodes in query results.

        Parameters
        ----------
        as_type : {'df', 'obj'}
            If 'df', return nodes as a Pandas DataFrame instances.
            If 'obj', return a iist of pyorient OGM objects.
        force_rid : bool
            If True, always use the OrientDB RID as the node identifier or index
            in the returned graph or DataFrame. Otherwise, use 'id' property.
            Ignored if `as_type` == 'obj'.

        Returns
        -------
        result :
            Nodes in the query results in specified format.
        """
        if as_type=='obj':
            return self._graph.elements_from_records(self.nodes)
        elif as_type=='df':
            return pd.as_pandas(nodes = self.nodes, force_rid = force_rid,
                                deepcopy = deepcopy)[0]
        raise UnsupportedView(as_type)

    @property
    def executed(self):
        """
        Whether the query has been executed.
        """

        return self._executed

    @property
    def query_string(self):
        """
        The encapsulated query string(s).
        """

        return self._query

    @property
    def node_rids(self):
        if not self._executed:
            self.execute()
        return list(set(self._nodes.keys()))

    @property
    def nodes(self):
        """
        Set of node records retrieved by query.
        """

        if not self._executed:
            self.execute()
        return set(self._nodes.values())

    @property
    def nodes_as_list(self):
        """
        List of node records retrieved by query.
        """

        if not self._executed:
            self.execute()
        return list(self._nodes.values())

    @property
    def node_objs(self):
        """
        List of pyorient objects corresponding to node records.
        This property do not retrieve again data from the database.
        """
        return self._graph.elements_from_records(self.nodes)

    @property
    def nodes_as_objs(self):
        """
        List of pyorient objects corresponding to node records retrieved by query.
        This is different from QueryWrapper.node_objs in that this property retrieves
        the nodes from database, whereas QueryWrapper.node_objs uses the records stored in self.nodes.
        """
        if not self._executed:
            self.execute()
        return list(map(self._graph.get_element, self._nodes))

    @property
    def edges(self):
        """
        Set of edge records between nodes retrieved by query.
        """

        if not self._executed:
            self.execute()
        return set(self._edges.values())

    @property
    def edges_as_list(self):
        """
        List of edge records between nodes retrieved by query.
        """

        self.execute(edges = True)
        return list(self._edges.values())

    @property
    def edges_as_objs(self):
        """
        List of pyorient objects corresponding to edge records between nodes retrieved by query.
        """

        self.execute(edges = True)
        return list(map(self._graph.get_element, self._edges))

    def __nonzero__(self):
        return True if self._nodes else False

    def __repr__(self):
        return ('QueryWrapper\n------------\n'
                'Nodes: [%i]\n'
                'Edges: [%i]\n') % (len(self._nodes), len(self._edges))

    def _execute_query(self, q):
        """
        Return results of a single OrientDB query string.
        """

        assert isinstance(q, QueryString)
        if q.lang == 'gremlin':
            result = self._graph.client.gremlin(q.str)
        elif q.lang == 'sql':
            result = self._graph.client.command(q.str)
        else:
            raise UnsupportedQueryLanguage(q.lang)
        return result

    def _edge_query_from_node_query(self, node_query, edge_class=''):
        """
        Return query string that can retrieve edges between nodes selected by specified query string.

        Parameters
        ----------
        node_query : QueryString
            Query that returns node.
        edge_class : str or iterable of str
            Name of edge class with which to restrict the constructed edge
            query. Multiple classes may be specified. If no classes are
            specified, nodes of any class are retrieved.

        Returns
        -------
        edge_query : QueryString
            Query that retrieves edges between nodes selected by `node_query`.
        """

        assert isinstance(node_query, QueryString)
        if not edge_class:
            edge_class_str = ''
        elif isinstance(edge_class, str):
            edge_class_str = '"%s"' % str(edge_class)
        elif iterable(edge_class):
            edge_class_str = ','.join([str(c) for c in edge_class])
        else:
            raise ValueError('invalid edge class')
        if node_query.lang == 'gremlin':
            s = 'x='+'.'.join((node_query, 'outE(%s).toList();' % edge_class_str))+\
                'y='+'.'.join((node_query, 'inE(%s).toList();' % edge_class_str))+\
                'x.intersect(y)'
            return QueryString(str=s, lang='gremlin')
        elif node_query.lang == 'sql':
            s = ("""select expand($c) let $a = (select expand(oute(%s)) from (%s)),"""
                 """$b = (select expand(ine(%s)) from (%s)),"""
                 """$c = intersect($a,$b)""") % (str(edge_class_str),
                                             str(node_query), str(edge_class_str),
                                             str(node_query))
            return QueryString(str=s, lang='sql')
        else:
            raise UnsupportedQueryLanguage(node_query.lang)

    def _edge_query_from_node_rids(self, node_rids, edge_class=''):
        """
        Return query string that can retrieve edges between specified node IDs.

        Parameters
        ----------
        node_rids : list of str
            List of node IDs.
        edge_class : str or iterable of str
            Name of edge class with which to restrict the constructed edge
            query. Multiple classes may be specified. If no classes are specified,
            nodes of any class are retrieved.

        Returns
        -------
        edge_query : QueryString
            Query that retrieves edges between nodes in `node_rids`.
        """

        if len(node_rids) == 0:
            return QueryString(str="""select from DataSource where name = "uiyth" """,
                                   lang = 'sql')
        if not edge_class:
            edge_class_str = ''
        elif isinstance(edge_class, str):
            edge_class_str = '"%s"' % edge_class
        elif iterable(edge_class):
            edge_class_str = ','.join([str(c) for c in edge_class])
        else:
            raise ValueError('invalid edge class')
        assert iterable(node_rids)
        rid_str = ','.join(['%s' % rid for rid in node_rids])
        s = ("""select expand($c) let $a = (select expand(oute(%s)) from [%s]),"""
             """$b = (select expand(ine(%s)) from [%s]),"""
             """$c = intersect($a,$b)""") % (str(edge_class_str), str(rid_str),
                                         str(edge_class_str), str(rid_str))
        return QueryString(str=s, lang='sql')

    def _connect_to_query_result_node(self, retries=1):
        """
        Create a new `query_result` node and connect it to all of the cached
        nodes of the query via `query_owns` edges.
        """
        # FIXME
        if not self._result:
            return
        batch = self._graph.batch()
        batch['query_result_node'] = batch.query_results.create(query=self._query)
        for i, rid in enumerate(self._nodes):
            batch[str(i)] = batch.query_owns.create(batch['query_result_node'],
                                                    self._graph.get_element(rid))
        batch.commit(retries)



    @staticmethod
    def _dict_intersection(a, b):
        return {k:a[k] for k in set(a).intersection(b)}

    @staticmethod
    def _dict_union(a, b):
        c = a.copy()
        c.update(b)
        return c

    @staticmethod
    def _dict_difference(a, b):
        return {k:a[k] for k in set(a).difference(b)}

    @staticmethod
    def _dict_symmetric_difference(a, b):
        return {k:(a[k] if k in a else b[k]) for k in \
                set(a).symmetric_difference(b)}

    @classmethod
    def multi_traverse_owned_by_toplevel(cls, queryObjList):
        if isinstance(queryObjList, cls):
            queryObjList=list(queryObjList)
        assert isinstance(queryObjList, list)
        for idx, obj in enumerate(queryObjList):
            assert isinstance(obj, cls)
            if idx==0:
                 queryObj=obj
            else:
                queryObj=queryObj+obj
        return queryObj.traverse_owned_by_get_toplevel()

    @class_method_timer
    def execute(self, edges=False, force=False, connect=False):
        """
        Execute the query.

        Parameters
        ----------
        edges : bool
            If True, retrieve edges between nodes as well as nodes.
        force : bool
            Execute the query even if node/edge results have already been
            cached.
        connect : bool
            Create new `query_results` node and connect it to the nodes
            returned by the query.
        """

        def e(q):
            if isinstance(q, QueryString):
                result = self._records_to_dict(self._execute_query(q))
            else:
                if len(q) == 3:
                    r1 = e(q[1])
                    r2 = e(q[2])

                    if q[0] in ['*', '&']:
                        result = self._dict_intersection(r1, r2)
                    elif q[0] in ['+', '|']:
                        result = self._dict_union(r1, r2)
                    elif q[0] == '-':
                        result = self._dict_difference(r1, r2)
                    elif q[0] == '^':
                        result = self._dict_symmetric_difference(r1, r2)
                    else:
                        raise ValueError('unrecognized operator')
                else:
                    raise ValueError('problematic query entry')
            return result

        # Don't execute query if results have already been cached:
        if self._executed and not force:
            if self._edge_executed:
                return
            else:
                if edges:
                    self._edges = \
                        self._records_to_dict(self._execute_query(self._edge_query_from_node_rids(self._nodes.keys())))
                    self._edge_executed = True
                else:
                    return
        else:
            self._nodes = e(self._query)

            if edges:
                if not self._edge_executed:
                    self._edges = \
                        self._records_to_dict(self._execute_query(self._edge_query_from_node_rids(self._nodes.keys())))
                    self._edge_executed = True
            if connect:
                self._connect_to_query_result_node(1)
            self._executed = True

    @class_method_timer
    def execute_edges(self, edge_class = ''):
        if self._executed:
            if self._edge_executed:
                return
            else:
                self._edges = \
                    self._records_to_dict(self._execute_query(
                        self._edge_query_from_node_rids(self._nodes.keys(), edge_class = edge_class)))
                self._edge_executed = True
        else:
            self.execute(edges = True)

    def clear(self):
        """
        Clear cached query results.
        """

        self._nodes = dict()
        self._edges = dict()
        self._executed = False

        # XXX what should be done with the query_result node in the db?

    @class_method_timer
    def owns(self, levels=1, **kwargs):
        """
        Retrieve all nodes at a specified number of levels below the current query.

        Parameters
        ----------
        levels : int
            Number of ownership edges between the current query and the nodes to
            be retrieved.
        cls : str or iterable
            Node class or classes to retrieve.

        Returns
        -------
        result : QueryWrapper
            Query containing requested nodes.
        """

        return self._own('out', levels, kwargs)

    @class_method_timer
    def owned_by(self, levels=1, **kwargs):
        """
        Retrieve all nodes at a specified number of levels above the current query.

        Parameters
        ----------
        levels : int
            Number of ownership edges between the current query and the nodes to
            be retrieved.
        cls : str or iterable
            Node class or classes to retrieve.

        Returns
        -------
        result : QueryWrapper
            Query containing requested nodes.
        """

        return self._own('in', levels, kwargs)

    def _own(self, direction, levels, kwargs):
        assert isinstance(levels, numbers.Integral) and levels >= 1

        rid_list = self.node_rids
        if len(rid_list) == 0:
            return self.empty_query(self._graph, debug = self.debug)
            # return self.__class__(self._graph, QueryString("""select from DataSource where name = "uiyth" ""","sql"),
            #                       debug = self.debug, edges = self.edges)
        classes, attrs, depth, columns = _kwargs(kwargs)

        relationships = ["""%s('owns')""" % direction]*levels
        query = """select %s from (select expand(%s) from [%s]) %s""" % \
                    (columns, '.'.join(relationships), ", ".join(rid_list), classes)
        '''
        disp_query = "select %s from (select expand(%s) from (%s)) %s" % \
                    (columns, '.'.join(relationships), self._disp_query, classes)
        '''
        disp_query = ''
        return self.__class__(self._graph, QueryString(query,"sql"), debug = self.debug)

    @class_method_timer
    def find_matching_ports_from_selector(self, other):
        try:
            other_df = other.get_as('df')[0][['selector']].to_dict(orient='index').values()
            sels = [list(a.values())[0] for a in other_df if isinstance(list(a.values())[0],str)]
        except KeyError:
            other_df = []
            sels = []
        return self.traverse_owns(selector=sels)

    @class_method_timer
    def traverse_owns(self, **kwargs):
        """
        Retrieve all traversed nodes down to a specified number of levels below the current query.

        Parameters
        ----------
        max_levels : int
            Number of ownership edges between the current query and the nodes to
            be retrieved.
        cls : str or iterable
            Node class or classes to retrieve.

        Returns
        -------
        result : QueryWrapper
            Query containing requested nodes.
        """

        return self._traverse('out', kwargs)

    @class_method_timer
    def traverse_owned_by(self, **kwargs):
        """
        Retrieve all traversed nodes up to a specified number of levels above the current query.

        Parameters
        ----------
        max_levels : int
            Number of ownership edges between the current query and the nodes to
            be retrieved.
        cls : str or iterable
            Node class or classes to retrieve.

        Returns
        -------
        result : QueryWrapper
            Query containing requested nodes.
        """

        return self._traverse('in', kwargs)

    def _traverse(self, direction, kwargs):
        if not 'max_levels' in kwargs:
            kwargs['max_levels']=10

        rid_list = self.node_rids
        if len(rid_list) == 0:
            return self.empty_query(self._graph, debug = self.debug)
            # return self.__class__(self._graph, QueryString("""select from DataSource where name = "uiyth" ""","sql"),
            #                       debug = self.debug, edges = self.edges)

        classes, attrs, depth, columns = _kwargs(kwargs)

        attrs_query = ""
        if attrs and classes:
            attrs_query = " and (" + " and ".join(attrs) + ") "
        elif (not classes) and attrs:
            attrs_query = " where (" + " and ".join(attrs) + ") "


        query = """select %s from (traverse %s('owns') from [%s] %s) %s %s""" %\
                (columns, direction, ", ".join(rid_list), depth, classes, attrs_query)
        '''
        disp_query = "select %s from (traverse %s('owns') from (%s) %s) %s %s" %\
                (columns, direction, self._disp_query, depth, classes, attrs_query)
        '''
        disp_query = ''
        return self.__class__(self._graph, QueryString(query, "sql"),
                              debug = self.debug)

    '''
    def traverse_owned_by_get_toplevel(self):
        """
        Find associated LPU or Pattern that of the nodes encompassed by a query object.

        Returns
        -------
        result : dict
            QueryWrappers broken out by LPU/Pattern containing nodes of the origin query
        """
        toplevel = dict()
        rid_list = self.node_rids

        for c in ('LPU', 'Pattern'):
            obj=self.traverse_owned_by(cls=c, max_levels=10)
            if c not in toplevel:
                toplevel[c] = dict()
            for n in obj.nodes_as_objs:
                query = "select @RID as rid from (traverse out('owns') from [%s] while $depth <= 10)" % (n._id)
                trav_owns = n._graph.client.command(query)
                if trav_owns and isinstance(trav_owns[0].oRecordData['rid'],tuple):
                    obj_rid_list = set(rid_list) & set(['#' + str(record.oRecordData['rid'][1]) + ':' + str(record.oRecordData['rid'][2])
                                                        for record in trav_owns])
                else:
                    obj_rid_list = set(rid_list) & set([record.oRecordData['rid'].get_hash() for record in trav_owns])
                if n.name not in toplevel['LPU']:
                    toplevel[c][n.name]=set()
                toplevel[c][n.name] |= obj_rid_list

        for key, v in toplevel.items():
            for name, rids in v.items():
                toplevel[key][name] = self.from_rids(self._graph, *rids)
        return toplevel
    '''

    @class_method_timer
    def traverse_owned_by_get_toplevel(self):
        """
        Find associated LPU or Pattern that of the nodes encompassed by a query object.

        Returns
        -------
        result : dict
            QueryWrappers broken out by LPU/Pattern containing nodes of the origin query
        """
        toplevel = dict()
        rid_list = self.node_rids

        for c in ('LPU', 'Pattern'):
            obj=self.traverse_owned_by(cls=c, max_levels=10)
            if c not in toplevel:
                toplevel[c] = dict()
            for n in obj.node_objs:
                query = "select expand($c) let $a=(select from (traverse out('owns') from %s while $depth <= 10))" % (n._id)
                query += ", $b=(select from [%s]), $c=intersect($a,$b)" % (", ".join(rid_list))
                toplevel[c][n.name] = self.__class__(self._graph, QueryString(query,'sql'), debug = self.debug)
        return toplevel

    @class_method_timer
    def get_data_rids(self, as_type='df', **kwargs):
        if len(self._nodes) == 0:
            return []

        rid_list = self.node_rids
        classes, attrs, depth, columns = _kwargs(kwargs)
        attrs_query = ""
        if attrs and classes:
            attrs_query = " and (" + " and ".join(attrs) + ") "
        elif (not classes) and attrs:
            attrs_query = " where (" + " and ".join(attrs) + ") "
        elif classes and not attrs:
            attrs_query = ""

        columns = '@rid as rid'
        query = "select %s from (select expand(out('HasData')) from [%s]) %s %s" % \
                (columns, ", ".join(rid_list), classes, attrs_query)

        res = self._graph.client.command(query)
        # if res and isinstance(res[0].oRecordData['rid'],tuple):
        #     res = ['#' + str(record.oRecordData['rid'][1]) + ':' + str(record.oRecordData['rid'][2])
        #            for record in res]
        # else:
        #     res = [record.oRecordData['rid'].get_hash() for record in res]
        res = [record._rid for record in res]
        return res

    @class_method_timer
    def get_data(self, as_type='df', edges = True, deepcopy = True, **kwargs):
        rid_list = self.node_rids
        if len(rid_list) == 0:
            if as_type == 'df':
                return pd.pd.DataFrame()
            elif as_type == 'obj':
                return ([],[])
            elif as_type == 'nx':
                return nx.nx.MultiDiGraph()

        classes, attrs, depth, columns = _kwargs(kwargs)
        attrs_query = ""
        if attrs and classes:
            attrs_query = " and (" + " and ".join(attrs) + ") "
        elif (not classes) and attrs:
            attrs_query = " where (" + " and ".join(attrs) + ") "


        query = "select %s from (select expand(out('HasData')) from [%s]) %s %s" % \
                    (columns, ", ".join(rid_list), classes, attrs_query)

        return QueryWrapper(self._graph, QueryString(query, 'sql'), debug = self.debug).get_as(
                                    as_type, edges = edges, deepcopy = deepcopy)


    @class_method_timer
    def get_data_qw(self, **kwargs):
        rid_list = self.node_rids
        if len(rid_list) == 0:
            return self.empty_query(self._graph, debug = self.debug)

        classes, attrs, depth, columns = _kwargs(kwargs)
        attrs_query = ""
        if attrs and classes:
            attrs_query = " and (" + " and ".join(attrs) + ") "
        elif (not classes) and attrs:
            attrs_query = " where (" + " and ".join(attrs) + ") "


        query = "select %s from (select expand(out('HasData')) from [%s]) %s %s" % \
                    (columns, ", ".join(rid_list), classes, attrs_query)

        return QueryWrapper(self._graph, QueryString(query, 'sql'), debug = self.debug)

    def query(self, **kwargs):
        self.has(**kwargs)

    @class_method_timer
    def has(self, **kwargs):
        if not kwargs:
            return self

        q={}
        rid_list = self.node_rids
        if not len(rid_list):
            return self
        classes, attrs, depth, columns = _kwargs(kwargs)

        q_str = "{var} = (select expand(rid) from (select distinct(traversedvertex(0)) as rid \
                from (traverse out('Models'), out('HasData') from (select from [{rids}] {classes}) \
                while $depth <= 2) {filters}))"
        dq_str = "{var} = (select expand(rid) from (select distinct(traversedvertex(0)) as rid \
                from (traverse out('Models'), out('HasData') from ({disp_query} {classes}) \
                while $depth <= 2) {filters}))"
        dq={}
        if attrs:
            for i, a in enumerate(attrs):
                filters = "where " + a
                var = '$q'+str(i)
                q[var] = q_str.format(var = var, rids = ", ".join(rid_list), classes = classes, filters = filters)
                #dq[var] = dq_str.format(var = var, disp_query = self._disp_query, classes = classes, filters = filters)
        else:
            var = '$q'
            q['$q'] = q_str.format(var = var, rids = ", ".join(rid_list), classes = classes, filters = "")
            #dq['$q'] = dq_str.format(var = var, disp_query = self._disp_query, classes = classes, filters = "")

        query = """select %s from (select expand($a) let %s, $a = intersect(%s))""" % \
                    (columns, ", ".join(list(q.values())), ", ".join(list(q.keys())) )
        '''
        disp_query = "select %s from (select expand($a) let %s, $a = intersect(%s))" % \
                    (columns, ", ".join(dq.values()), ", ".join(dq.keys()) )
        '''
        disp_query = ''
        #print disp_query
        return self.__class__(self._graph, QueryString(query,"sql"), debug = self.debug)


    def _check_tags(self, tag):
        query = "select tag from QueryResult where tag = '%s'" % tag
        #print query
        results = self._graph.client.command(query)
        return self._records_to_list(results)

    @class_method_timer
    def tag_query_result_node(self, tag, permanent_flag, **kwargs):
        """
        Create a new `QueryResult` node and connect it to all of the cached
        nodes of the query via `HasQueryResults` edges.
        """
        # check if tag already exists
        if self._check_tags(tag): return -1
        qr = self._graph.QueryResults.create(tag=tag)
        kwargs['tag'] = tag
        self._graph.client.command('update %s content %s where @rid = %s' % \
                                     (qr.element_type, json.dumps(kwargs), qr._id))

        cmd = ['begin']

        # create tag node
        #set_cmd = ["%s = %s" % (k, v.__repr__()) for k, v in kwargs.items()]
        #if set_cmd:
        #    a = [""]
        #    a.extend(set_cmd)
        #    set_cmd = a
        #let_cmd = "let v = CREATE VERTEX QueryResult SET tag = '%s', permanent = %s, created_timestamp = sysdate() %s" % \
        #            (tag, permanent_flag, ", ".join(set_cmd))
        #cmd.append(let_cmd)

        # create edges from tag node to query nodes
        for i, node in enumerate(self._nodes):
            edge_cmd = "let e%s = CREATE EDGE HasQueryResults FROM %s TO %s" % (i, qr._id, node)
            cmd.append(edge_cmd)

        cmd.append("commit retry 10;\nreturn $v;")
        results = self._graph.client.batch(";\n".join(cmd))
        return 1
        #return self.from_rids(self._graph, *self._records_to_list(results))

    def tag_clean_up(self, older_than):
        cmd = "begin; DELETE VERTEX QueryResults WHERE permanent=False and created_timestamp <= DATE(%s); commit retry 10;" % \
                (older_than)
        results = self._graph.client.batch(cmd)

    def _get_node_num(self, node_name, node_class):
        col_name = re.sub('[#0-9]', '', node_name)
        query = "select max(name.replace('%s', '').asInteger()) from %s where name like '%s%%'" % \
                (col_name, node_class, col_name)
        res = self._graph.client.command(query)
        return res[0].oRecordData['max']

    def _get_subgraphs(self, edge_types=None, max_levels=10, max_node_cls=None, cls=None):
        return self._get_graphs('out', edge_types, max_levels, max_node_cls, cls)

    def _get_supgraphs(self, edge_types=None, max_levels=10, max_node_cls=None, cls=None):
        return self._get_graphs('in', edge_types, max_levels, max_node_cls, cls)

    def _get_graphs(self, direction, edge_types=None, max_levels=10, max_node_cls=None, cls=None):
        rid_list = self.node_rids
        class_list = list(self._graph.registry.keys())

        assert isinstance(max_levels, numbers.Integral) and max_levels >= 0

        if cls:
            assert cls in class_list
            cls = _list_repr(cls)
            where_classes = "where @class in %s" % cls
        else:
            where_classes = ""

        if edge_types:
            edge_types = _list_repr(edge_types)
            assert all(e in class_list for e in edge_types)
            relationships = ["%s('%s')" % (direction, e) for e in edge_types]
        else:
            relationships = ["in()"]

        if max_node_cls:
            assert max_node_cls in class_list
            while_classes = "and @class <> '%s'" % max_node_cls
        else:
            while_classes = ""

        query = "select from (traverse %s from [%s] while $depth<=%s %s) %s" % \
                (", ".join(relationships), ", ".join(rid_list), max_levels, while_classes, where_classes)
        disp_query = ''
        '''
        disp_query = "select from (traverse %s from (%s) while $depth<=%s %s) %s" % \
                (", ".join(relationships), self._disp_query, max_levels, while_classes, where_classes)
        '''

        return self.__class__(self._graph, QueryString(query, "sql"), debug = self.debug)

    def _get_in_edges(self, rid, edge_types):
        in_relationships = ["in('%s') as %s" % (e, e) for e in edge_types]
        if isinstance(rid, str):
            rids = [rid]
        query = "select @RID as rid, %s from %s " % (", ".join(in_relationships), ", ".join(rids))


        records = self._graph.client.command(query)
        in_edges = dict()
        for r in records:
            # rid_hash = r.oRecordData['rid'].get_hash()
            rid_hash = r._rid
            if rid_hash not in in_edges:
                in_edges[rid_hash] = dict()
            for edge in edge_types:
                in_edges[rid_hash][edge] = [e.get_hash() for e in r.oRecordData[edge]]

        return in_edges

    def _in_edges_command(self, to_from_records, edge_types):
        edge_in = "let f{num} = CREATE EDGE {edge_type} from {from_nodes} to {to_node}"
        in_cmd = []

        for i, (to_rid, from_rid) in enumerate(to_from_records):
            in_edges = self._get_in_edges(to_rid, edge_types)
            for n, edge_type in enumerate(edge_types):
                from_nodes = in_edges[to_rid][edge_type]
                if from_nodes:
                    in_cmd.append(edge_in.format(to_node = to_rid, from_nodes = from_rid,
                                                  num = len(to_from_records) * i + n , edge_type = edge_type))
        return in_cmd

    def _rename_nodes(self, func, rid_list, cmd):
        max_node_num = dict()

        # rename new top-level nodes
        for i, rid in enumerate(rid_list):
            node = self._nodes[rid]
            old_name = node.oRecordData['name']
            node_name = re.sub('[#0-9]', '', old_name)

            if node_name not in max_node_num:
                max_node_num[node_name] = self._get_node_num(old_name, node._class)
            assert func(max_node_num[node_name]) > max_node_num[node_name], 'Node name must not already exist'

            max_node_num[node_name] = func(max_node_num[node_name])
            new_name = node_name + str(max_node_num[node_name])
            cmd = cmd.replace(old_name, new_name)
        return cmd

    def _copy_graph_command(self, edge_types=None):
        node_cmd, node_map = self._copy_node_command(commit_stmt=False)
        edge_cmd = self._copy_edge_command(node_map, edge_types=None, commit_stmt=False)

        cmd = 'begin;\n' + "".join(node_cmd) + "".join(edge_cmd) + ";\ncommit retry 100;\nreturn [%s];" % (", ".join(list(node_map.values())))
        return cmd, node_map


    def _copy_node_command(self, commit_stmt=False, N=20):
        node_map_full = dict()
        cmd_list = []
        num = 0

        # batch commands for nodes
        for chunk in chunks(self._nodes.items(), N):
            node_map = dict()
            cmd = []

            for rid, node in chunk:
                set_cmd = ["%s = %s" % (k, v.__repr__()) for k, v in node.oRecordData.items() \
                           if isinstance(v, (str, numbers.Number))]
                let_cmd = "let v%s = CREATE VERTEX %s SET " % (num, node._class)
                node_map[rid] = '$v%s' % num
                cmd.append(let_cmd + ", ".join(set_cmd))
                num += 1

            cmd = ";\n".join(cmd) + ";\n"

            if commit_stmt:
                cmd += "commit retry 100;\nreturn [%s];" % (", ".join(list(node_map.values())))
            cmd_list.append(cmd)
            node_map_full.update(node_map)

        return cmd_list, node_map_full

    def _copy_edge_command(self, node_map, edge_types=None, commit_stmt=False, N=20):
        cmd_list = []
        num = 0

        # batch commands for edges, given node variables
        for chunk in chunks(self.edges, N):
            cmd = []
            for edge in chunk:
                edge_cmd = "let e%s = CREATE EDGE %s from %s to %s" % \
                                    (num, edge._class, node_map[edge._out.__str__()], node_map[edge._in.__str__()])
                if edge_types:
                    if edge._class in edge_types:
                        cmd.append(edge_cmd)
                else:
                    cmd.append(edge_cmd)
                num += 1

            cmd = ";\n".join(cmd) + ";"

            if commit_stmt:
                cmd += "\ncommit retry 100;"
            cmd_list.append(cmd)

        return cmd_list

    def copy_models(self, func, in_flag, copies=1000):
        edge_types = ['Owns', 'SendsTo', 'HasData']
        rid_list = self.node_rids
        data = self._get_subgraphs(edge_types)
        results = list()

        for c in range(copies):
            cmd, node_map = data._copy_graph_command(edge_types)
            cmd  = self._rename_nodes(func, rid_list, cmd)

            # create in edges to subgraph if in_flag=True
            if in_flag:
                in_cmd = self._in_edges_command(zip(rid_list, [node_map[r] for r in rid_list]), edge_types)
                cmd = cmd.replace(";\ncommit", ";\n" + ";\n".join(in_cmd) + ";\ncommit")
            results += self._graph.client.batch(cmd)
            #print cmd + '\nNew records committed to database'

        return self.from_rids(self._graph, *self._records_to_list(results))

    @staticmethod
    def _remove_edges_by_node(df, del_nodes):
        del_filter = (~df['in'].isin(del_nodes)) & (~df['out'].isin(del_nodes))
        return df[del_filter]


    def _diff(self, new_df_nodes, new_df_edges, full_replace, node_map={}):
        old_df_nodes, old_df_edges = self.get_as()

        if node_map:
            old_df_nodes.index = [node_map[r] for r in old_df_nodes.index]
            new_df_nodes.index = [node_map[r] for r in new_df_nodes.index]
            for direction in ('in', 'out'):
                old_df_edges[direction] = old_df_edges[direction].map(lambda x: node_map[x])
                new_df_edges[direction] = new_df_edges[direction].map(lambda x: node_map[x])

        d_n = diff_nodes(old_df_nodes, new_df_nodes, full_replace)

        # filter out edges related to nodes that will be deleted
        del_nodes = list(d_n['del'].keys())
        old_df_edges_filter = self._remove_edges_by_node(old_df_edges, del_nodes)
        new_df_edges_filter = self._remove_edges_by_node(new_df_edges, del_nodes)

        d_e = diff_edges(old_df_edges_filter, new_df_edges_filter, full_replace)
        rid_list = apply_node_diff(self._graph.client, d_n)
        edge_rid_list = apply_edge_diff(self._graph.client, d_e)
        return rid_list

    def diff_save(self, new_df_nodes, new_df_edges, full_replace=False):
        rid_list = self._diff(new_df_nodes, new_df_edges, full_replace)
        return self.from_rids(self._graph, *rid_list)

    def diff_save_as(self, new_df_nodes, new_df_edges, **kwargs):
        class_list = list(self._graph.registry.keys())

        if 'max_levels_in' in kwargs:
            assert isinstance(kwargs['max_levels_in'], numbers.Integral) and kwargs['max_levels_in'] >= 0
        else:
            kwargs['max_levels_in'] = 0

        if 'max_levels_out' in kwargs:
            assert isinstance(kwargs['max_levels_out'], numbers.Integral) and kwargs['max_levels_out'] >= 0
        else:
            kwargs['max_levels_out'] = 0

        if 'max_node_cls' in kwargs:
            assert (kwargs['max_node_cls'] in class_list), "Assign new nodes to an existing class: \n%s" % \
                                                                ('\n'.join(class_list))
        else:
            kwargs['max_node_cls'] = None

        # copy graph
        q1 = self._get_supgraphs(max_levels=kwargs['max_levels_in'], max_node_cls=kwargs['max_node_cls'])
        q2 = self._get_subgraphs(max_levels=kwargs['max_levels_out'])
        q = q1 + q2

        # commit copy of nodes to database
        result_rids = []
        node_cmd_list, node_map = self._copy_node_command(commit_stmt=True)
        for node_cmd in node_cmd_list:
            results = self._graph.client.batch("begin;\n" + node_cmd)
            result_rids += [r._rid for r in results]

        # map old nodes to newly created nodes
        node_to_node_map = {k: v for k, v in zip(node_map.keys(), result_rids)}

        # commit copy of edges to database
        edge_cmd_list = self._copy_edge_command(node_to_node_map, commit_stmt=True)
        for edge_cmd in edge_cmd_list:
            self._graph.client.batch("begin;\n" + edge_cmd)

        # take diff, map to new nodes in db
        rid_list = self._diff(new_df_nodes, new_df_edges, full_replace = True, node_map = node_to_node_map)
        res = result_rids+rid_list
        return self.from_rids(self._graph, *res)

    @class_method_timer
    def post_synaptic_neurons(self, N=None, rel='>', include_inferred=True, include_fragments = False, high_prob = False):
        # N represents number of synapses
        #    if N is none, will return all post synaptic neurons
        #    else, it will only return postsynaptic neurons where the number of synapses
        #    satisfy <rel> N. See below for rel
        # rel can be '>'(default),'<','='
        synapse_classes = ['Synapse', 'InferredSynapse'] if include_inferred else 'Synapse'
        neuron_classes = 'NeuronAndFragment' if include_fragments else 'Neuron'
        if N:
            return self.gen_traversal_out(['SendsTo', synapse_classes, {'NHP' if high_prob else 'N':(rel,N)}],['SendsTo',neuron_classes], min_depth=2)
        else:
            if high_prob:
                return self.gen_traversal_out(['SendsTo', synapse_classes, {'NHP':(rel,0)}],['SendsTo', neuron_classes, 'instanceof'], min_depth=2)
            else:
                return self.gen_traversal_out(['SendsTo', synapse_classes],['SendsTo',neuron_classes, 'instanceof'], min_depth=2)

    @class_method_timer
    def _post_synaptic_neurons(self, N=None, rel='>', include_fragments = False, high_prob = False):
        # slower than using post_synaptic_neurons
        neuron_classes = 'NeuronAndFragment' if include_fragments else 'Neuron'
        rids = {n._id: n for n in self.node_objs}
        return QueryWrapper(self._graph, QueryString(
            """select expand(target) from (MATCH {{class: Neuron, where: (@rid in [{rid}])}} -SendsTo-> {{class: Synapse{where}}} -SendsTo-> {{class: {neuron_class}, as: target}} return target)""".format(
                rid = ','.join(list(rids.keys())), neuron_class = neuron_classes, 
                where = ', where: ({} {} {})'.format('NHP' if high_prob else 'N', rel, 0 if N is None else N) if N is not None else ', where: (NHP {} {})'.format(rel, 0 if N is None else N) if high_prob else '',
                NHP = 'NHP' if high_prob else 'N'
            ), 'sql'), debug = self.debug)
        

    @class_method_timer
    def pre_synaptic_neurons(self, N=None, rel='>', include_inferred=True, include_fragments = False, high_prob = False):
        # N represents number of synapses
        #    if N is none, will return all post synaptic neurons
        #    else, it will only return postsynaptic neurons where the number of synapses
        #    satisfy <rel> N. See below for rel
        # rel can be '>'(default),'<','='
        synapse_classes = ['Synapse', 'InferredSynapse'] if include_inferred else 'Synapse'
        neuron_classes = 'NeuronAndFragment' if include_fragments else 'Neuron'
        if N:
            return self.gen_traversal_in(['SendsTo', synapse_classes,{'NHP' if high_prob else 'N':(rel,N)}],['SendsTo',neuron_classes], min_depth=2)
        else:
            if high_prob:
                return self.gen_traversal_in(['SendsTo', synapse_classes, {'NHP':(rel,0)}],['SendsTo',neuron_classes, 'instanceof'], min_depth=2)
            else:
                return self.gen_traversal_in(['SendsTo', synapse_classes],['SendsTo',neuron_classes, 'instanceof'], min_depth=2)

    @class_method_timer
    def _pre_synaptic_neurons(self, N=None, rel='>', include_fragments = False, high_prob = False):
        # slower than using pre_synaptic_neurons
        neuron_classes = 'NeuronAndFragment' if include_fragments else 'Neuron'
        rids = {n._id: n for n in self.node_objs}
        return QueryWrapper(self._graph, QueryString(
            """select expand(source) from (MATCH {{class: Neuron, where: (@rid in [{rid}])}} <-SendsTo- {{class: Synapse{where}}} -SendsTo-> {{class: {neuron_class}, as: source}} return source)""".format(
                rid = ','.join(list(rids.keys())), neuron_class = neuron_classes, 
                where = ', where: ({} {} {})'.format('NHP' if high_prob else 'N', rel, 0 if N is None else N) if N is not None else ', where: (NHP {} {})'.format(rel, 0 if N is None else N) if high_prob else '',
                NHP = 'NHP' if high_prob else 'N'
            ), 'sql'), debug = self.debug)
    
    @class_method_timer
    def pre_synaptic_neurons_with_synapse_count(self, N=None, rel='>', include_inferred=True, include_fragments = False, high_prob = False):
        neuron_classes = 'NeuronAndFragment' if include_fragments else 'Neuron'
        rids = {n._id: n for n in self.node_objs}
        links = self._graph.client.command(
            """MATCH {{class: {neuron_class}, where: (@rid in [{rid}]), as: post}} <-SendsTo- {{class: Synapse, {where} as: syn}} <-SendsTo- {{class: {neuron_class}, as: source}} return post, syn.{NHP} as N, source""".format(
                rid = ','.join(list(rids.keys())), neuron_class = neuron_classes, 
                where = 'where: ({} {} {}),'.format('NHP' if high_prob else 'N', rel, 0 if N is None else N) if N is not None else 'where: (NHP {} {}),'.format(rel, 0 if N is None else N) if high_prob else '',
                NHP = 'NHP' if high_prob else 'N'
            ))
        if include_inferred:
            links.extend(self._graph.client.command(
            """MATCH {{class: {neuron_class}, where: (@rid in [{rid}]), as: post}} <-SendsTo- {{class: InferredSynapse, {where} as: syn}} <-SendsTo- {{class: {neuron_class}, as: source}} return post, syn.{NHP} as N, source""".format(
                rid = ','.join(list(rids.keys())), neuron_class = neuron_classes, 
                where = 'where: ({} {} {}),'.format('NHP' if high_prob else 'N', rel, 0 if N is None else N) if N is not None else 'where: (NHP {} {}),'.format(rel, 0 if N is None else N) if high_prob else '',
                NHP = 'NHP' if high_prob else 'N'
            )))
        out = {k: {} for k in rids}
        pres = set()
        for link in links:
            post = get_hash(link.oRecordData['post'])
            Nsyn = link.oRecordData['N']
            pre = get_hash(link.oRecordData['source'])
            out[post][pre] = Nsyn
            pres.add(pre)
        pre_neurons = {n._id: n for n in self.from_rids(self._graph, *list(pres)).node_objs}

        return [(rids[post], pre_neurons[pre], N) for post, v in out.items() for pre, N in v.items()]
    
    @class_method_timer
    def _pre_synaptic_neurons_with_synapse_count(self, N=None, rel='>', include_inferred=True, high_prob = False):
        # only work with 1 neuron for now
        synapse_classes = ['Synapse', 'InferredSynapse'] if include_inferred else 'Synapse'
        assert len(self.nodes) <= 1
        if N:
            pre_syn = self.gen_traversal_in(['SendsTo', synapse_classes, {'NHP' if high_prob else 'N':(rel,N)}], min_depth=1)
        else:
            if high_prob:
                pre_syn = self.gen_traversal_in(['SendsTo', synapse_classes, {'NHP':(rel,0)}], min_depth=1)
            else:
                pre_syn = self.gen_traversal_in(['SendsTo', synapse_classes], min_depth=1)
        if not len(pre_syn.nodes):
            return []
        pre_neuron_list = []
        synapse_rid_to_N = {s._id: s.NHP if high_prob else s.N for s in pre_syn.get_as('obj', edges = False)[0]}
        synapse_rids = ','.join(self._records_to_list(pre_syn.nodes))
        n_rec=self._graph.client.command("""SELECT $path from (traverse in('SendsTo') FROM [{}] maxdepth 1)""".format(synapse_rids))
        ntos = {n[1]:n[0] for n in [re.findall('\#\d+\:\d+', x.oRecordData['$path']) for x in n_rec] if len(n)==2}
        neuron_rids = list(set(ntos.keys()))
        neurons = self.from_rids(self._graph, *neuron_rids)
        neuron_rid_to_obj = {n._id: n for n in neurons.get_as('obj', edges = False)[0]}

        for neu_id, syn_id in ntos.items():
            pre_neuron_list.append((neuron_rid_to_obj[neu_id], synapse_rid_to_N[syn_id]))
        return pre_neuron_list

    @class_method_timer
    def post_synaptic_neurons_with_synapse_count(self, N=None, rel='>', include_inferred=True, include_fragments = False, high_prob = False):
        neuron_classes = 'NeuronAndFragment' if include_fragments else 'Neuron'
        rids = {n._id: n for n in self.node_objs}
        links = self._graph.client.command(
            """MATCH {{class: {neuron_class}, where: (@rid in [{rid}]), as: pre}} -SendsTo-> {{class: Synapse, {where} as: syn}} -SendsTo-> {{class: {neuron_class}, as: target}} return pre, syn.{NHP} as N, target""".format(
                rid = ','.join(list(rids.keys())), neuron_class = neuron_classes, 
                where = 'where: ({} {} {}),'.format('NHP' if high_prob else 'N', rel, 0 if N is None else N) if N is not None else 'where: (NHP {} {}),'.format(rel, 0 if N is None else N) if high_prob else '',
                NHP = 'NHP' if high_prob else 'N'
            ))
        if include_inferred:
            links.extend(self._graph.client.command(
            """MATCH {{class: {neuron_class}, where: (@rid in [{rid}]), as: pre}} -SendsTo-> {{class: InferredSynapse, {where} as: syn}} -SendsTo-> {{class: {neuron_class}, as: target}} return pre, syn.{NHP} as N, target""".format(
                rid = ','.join(list(rids.keys())), neuron_class = neuron_classes, 
                where = 'where: ({} {} {}),'.format('NHP' if high_prob else 'N', rel, 0 if N is None else N) if N is not None else 'where: (NHP {} {}),'.format(rel, 0 if N is None else N) if high_prob else '',
                NHP = 'NHP' if high_prob else 'N'
            )))
        out = {k: {} for k in rids}
        posts = set()
        for link in links:
            pre = get_hash(link.oRecordData['pre'])
            Nsyn = link.oRecordData['N']
            post = get_hash(link.oRecordData['target'])
            out[pre][post] = Nsyn
            posts.add(post)
        post_neurons = {n._id: n for n in self.from_rids(self._graph, *list(posts)).node_objs}

        return [(rids[pre], post_neurons[post], N) for pre, v in out.items() for post, N in v.items()]
        
    @class_method_timer
    def _post_synaptic_neurons_with_synapse_count(self, N=None, rel='>', include_inferred=True, high_prob = False):
        # only work with 1 neuron for now
        assert len(self.nodes) <= 1
        synapse_classes = ['Synapse', 'InferredSynapse'] if include_inferred else 'Synapse'
        if N:
            post_syn = self.gen_traversal_out(['SendsTo', synapse_classes, {'NHP' if high_prob else 'N':(rel,N)}], min_depth=1)
        else:
            if high_prob:
                post_syn = self.gen_traversal_out(['SendsTo', synapse_classes, {'NHP':(rel,0)}], min_depth=1)
            else:
                post_syn = self.gen_traversal_out(['SendsTo', synapse_classes], min_depth=1)
        if not len(post_syn.nodes):
            return []
        
        post_neuron_list = []
        synapse_rid_to_N = {s._id: s.NHP if high_prob else s.N for s in post_syn.node_objs}
        synapse_rids = ','.join(self._records_to_list(post_syn.nodes))
        n_rec=self._graph.client.command("""SELECT $path from (traverse out('SendsTo') FROM [{}] maxdepth 1) where $depth=1""".format(synapse_rids))
        ntos = {n[1]:n[0] for n in [re.findall('\#\d+\:\d+', x.oRecordData['$path']) for x in n_rec] if len(n)==2}
        neuron_rids = list(set(ntos.keys()))
        neurons = self.from_rids(self._graph, *neuron_rids)
        neuron_rid_to_obj = {n._id: n for n in neurons.node_objs}

        for neu_id, syn_id in ntos.items():
            post_neuron_list.append((neuron_rid_to_obj[neu_id], synapse_rid_to_N[syn_id]))
        return post_neuron_list

    @class_method_timer
    def get_connecting_synapses(self, N=None, rel='>', include_inferred = True, include_fragments = False, high_prob = False):
        neuron_classes = 'NeuronAndFragment' if include_fragments else 'Neuron'
        rids = self.node_rids
        cc = self.__class__(self._graph, QueryString(
            """select expand(syn) from (MATCH {{class: {neuron_classes}, where: (@rid in [{rid}])}} -SendsTo-> {{class: Synapse, {where} as: syn}} -SendsTo-> {{class: {neuron_classes}, where: (@rid in [{rid}])}} return syn)""".format(
                rid = ','.join(rids), neuron_classes = neuron_classes,
                where = 'where: ({} {} {}),'.format('NHP' if high_prob else 'N', rel, 0 if N is None else N) if N is not None else 'where: (NHP {} {}),'.format(rel, 0 if N is None else N) if high_prob else '',
                NHP = 'NHP' if high_prob else 'N'
            ), 'sql'), debug = self.debug)
        if include_inferred:
            cc = cc + self.__class__(self._graph, QueryString(
                """select expand(syn) from (MATCH {{class: {neuron_classes}, where: (@rid in [{rid}])}} -SendsTo-> {{class: InferredSynapse, {where} as: syn}} -SendsTo-> {{class: {neuron_classes}, where: (@rid in [{rid}])}} return syn)""".format(
                    rid = ','.join(rids),  neuron_classes = neuron_classes,
                    where = 'where: ({} {} {}),'.format('NHP' if high_prob else 'N', rel, 0 if N is None else N) if N is not None else 'where: (NHP {} {}),'.format(rel, 0 if N is None else N) if high_prob else '',
                    NHP = 'NHP' if high_prob else 'N'
                ), 'sql'), debug = self.debug)
        return cc

    @class_method_timer
    def _get_connecting_synapses_old(self, N=None, rel='>', include_inferred = True):
        synapse_classes = ['Synapse', 'InferredSynapse'] if include_inferred else 'Synapse'
        if N:
            return self.gen_traversal_out(['SendsTo', synapse_classes, {'N':(rel,N)}], min_depth=1) & \
                self.gen_traversal_in(['SendsTo', synapse_classes, {'N':(rel,N)}], min_depth=1)
        else:
            return self.gen_traversal_out(['SendsTo', synapse_classes], min_depth=1) & \
                self.gen_traversal_in(['SendsTo', synapse_classes], min_depth=1)

    @class_method_timer
    def add_connecting_synapses(self, N=None, rel='>', include_inferred = True, include_fragments = False, high_prob = False):
        return self + self.get_connecting_synapses(N = N, rel = rel, include_inferred = include_inferred, 
                include_fragments = include_fragments, high_prob = high_prob)


    @class_method_timer
    def _add_connecting_synapses_old(self, N=None, rel='>',include_inferred=True):
        if N:
            return self + (self.gen_traversal_out(['SendsTo', synapse_classes, {'N':(rel,N)}], min_depth=1) & \
                self.gen_traversal_in(['SendsTo', synapse_classes, {'N':(rel,N)}], min_depth=1))
        else:
            return self + (self.gen_traversal_out(['SendsTo', synapse_classes], min_depth=1) & \
                self.gen_traversal_in(['SendsTo', synapse_classes], min_depth=1))

    @class_method_timer
    def add_connecting_synapses1(self, N=None, rel='>',include_inferred = True, include_fragments = False, high_prob = False):
        neuron_classes = 'NeuronAndFragment' if include_fragments else 'Neuron'
        rids = {n._id: n for n in self.node_objs}
        links = self._graph.client.command(
            """MATCH {{class: Neuron, where: (@rid in [{rid}])}} -SendsTo-> {{class: Synapse, {where} as: syn}} -SendsTo-> {{class: Neuron, where: (@rid in [{rid}])}} return syn""".format(
                rid = ','.join(list(rids.keys())), neuron_class = neuron_classes, 
                where = 'where: ({} {} {}),'.format('NHP' if high_prob else 'N', rel, 0 if N is None else N) if N is not None else 'where: (NHP {} {}),'.format(rel, 0 if N is None else N) if high_prob else '',
                NHP = 'NHP' if high_prob else 'N'
            ))

        synapse_classes = ['Synapse', 'InferredSynapse'] if include_inferred else 'Synapse'
        if N:
            return self + (self.gen_traversal_out(['SendsTo', synapse_classes, {'N':(rel,N)}], min_depth=1) & \
                self.gen_traversal_in(['SendsTo', synapse_classes, {'N':(rel,N)}], min_depth=1))
        else:
            return self + (self.gen_traversal_out(['SendsTo', synapse_classes], min_depth=1) & \
                self.gen_traversal_in(['SendsTo', synapse_classes], min_depth=1))

    def get_connecting_synapsemodels(self):
        return self.gen_traversal_out(['SendsTo', 'SynapseModel','instanceof'], min_depth=1) & \
                self.gen_traversal_in(['SendsTo', 'SynapseModel','instanceof'], min_depth=1)

    def get_connected_ports(self):
        return self.gen_traversal_out(['SendsTo', 'Port']) + self.gen_traversal_in(['SendsTo', 'Port'])

    @class_method_timer
    def gen_traversal_in(self, *args, **kwargs):
        return self._gen_traversal('in', args, kwargs)

    @class_method_timer
    def gen_traversal_out(self, *args, **kwargs):
        return self._gen_traversal('out', args, kwargs)

    def _gen_traversal(self, direction, args, kwargs):
        '''
            Args must be tuples or list of [edge_types, cls (optional), instanceof_or_cls (optional),
                                            Dict containing parameters to filter the nodes for this particular stage of traversal (optional)]
                  or strings of only edge_types
        '''
        assert len(args)>0
        class_list = list(self._graph.registry.keys())
        rid_list = self.node_rids
        if len(rid_list) == 0:
            return self.empty_query(self._graph, debug = self.debug)
            # return self.__class__(self._graph, QueryString("""select from DataSource where name = "uiyth" ""","sql"),
            #                       debug = self.debug, edges = self.edges)
        q = dict()
        dq = {}
        q['$q0'] = "$q0 = (select from [%s])" % ", ".join(rid_list)
        #dq['$q0'] = "$q0 = (select from (%s))" % self._disp_query

        if 'min_depth' in kwargs:
            assert isinstance(kwargs['min_depth'], numbers.Integral)
            min_depth = kwargs['min_depth']
        else:
            min_depth = 0

        if 'max_depth' in kwargs:
            assert isinstance(kwargs['max_depth'], numbers.Integral)
            max_depth = kwargs['max_depth']
        else:
            max_depth = len(args) + 1

        for t, a in enumerate(args):
            a = _list_repr(a)
            assert len(a) in (1, 2, 3, 4), \
                "Args must be tuples or list of [edge_types, cls (optional), instanceof_or_cls (optional)," +  \
                "Dict containing parameters to filter the nodes for this particular stage of traversal (optional)], or strings of only edge_types"

            arg_dict = {}
            if len(a)==3:
                for v in a[0:min(len(a), 2)]:
                    if isinstance(v, list) or isinstance(v, tuple):
                        assert all(vv in class_list for vv in v), 'Invalid Relationship or Node class'
                    else:
                        assert(v in class_list), 'Invalid Relationship or Node class'
                if a[2]=='instanceof':
                    arg_dict['instanceof'] = a[1]
                elif a[2]=='cls':
                    arg_dict['cls'] = a[1]
                else:
                    arg_dict['cls'] = a[1]
                    if isinstance(a[2],dict): arg_dict.update(a[2])
            elif len(a)==4:
                for v in a[0:min(len(a), 2)]:
                    if isinstance(v, list) or isinstance(v, tuple):
                        assert all(vv in class_list for vv in v), 'Invalid Relationship or Node class'
                    else:
                        assert(v in class_list), 'Invalid Relationship or Node class'
                if a[2]=='instanceof':
                    arg_dict['instanceof'] = a[1]
                else:
                    arg_dict['cls'] = a[1]
                if isinstance(a[3],dict): arg_dict.update(a[3])
            elif len(a) == 2:
                if isinstance(a[1], dict):
                    assert all(v in class_list for v in a[0:min(len(a),1)]), 'Invalid Relationship class'
                    arg_dict.update(a[1])
                else:
                    for v in a[0:min(len(a), 2)]:
                        if isinstance(v, list) or isinstance(v, tuple):
                            assert all(vv in class_list for vv in v), 'Invalid Relationship or Node class'
                        else:
                            assert(v in class_list), 'Invalid Relationship or Node class'
                    arg_dict['cls'] = a[1]

            classes, attrs, depth, columns = _kwargs(arg_dict)

            attrs_query = ""
            if attrs and classes:
                attrs_query = " and (" + " and ".join(attrs) + ") "
            elif (not classes) and attrs:
                attrs_query = " where (" + " and ".join(attrs) + ") "

            #relationships = [direction + "('%s')" % (a if isinstance(a, str) else a[0]) for a in args[:t+1]]
            relationships = [direction + "('%s')" % (x if isinstance(x, str) else x[0]) for x in args[t:t+1]]

            var = '$q' + str(t+1)

            #q[var] = "%s = (select from (select expand(%s) from [%s]) %s)" % \
            #            (var, ".".join(relationships), ", ".join(rid_list), classes)
            q[var] = "%s = (select from (select expand(%s) from %s) %s %s)" % \
                        (var, ".".join(relationships), '$q' + str(t), classes, attrs_query)

            #dq[var] = "%s = (select from (select expand(%s) from (%s)) %s)" % \
            #            (var, ".".join(relationships),self._disp_query, classes)

        # _, _, _, columns = _kwargs(kwargs) #wrong
        query = """ select expand($q) let %s, $q = unionall(%s) """ % \
                (", ".join(list(q.values())), ",".join(list(q.keys())[min_depth:max_depth]))
        disp_query = ""
        #disp_query = "select expand($q) let %s, $q = unionall(%s) " % \
        #        (", ".join(dq.values()[min_depth:max_depth]), ",".join(dq.keys()[min_depth:max_depth]))

        return self.__class__(self._graph, QueryString(query, "sql"), debug = self.debug)

    def path_to_neurons(self, unames, synapse_threshold = 10, max_hops = 2):
        q1 = self.__class__(self._graph, QueryString("""select from Neuron where uname in ["{}"]""".format('","'.join(unames)), "sql"), debug = self.debug)
        return self.path_to(q1)[0]

    def direct_path_to(self, q, synapse_threshold = 10, max_hops = 2):
        return self.path_to(q, synapse_threshold = synapse_threshold,
                            max_hops = max_hops, exclude_path = True)

    def path_to(self, q, synapse_threshold = 10, max_hops = 2,
                exclude_path = True, timeout = 120):
        if max_hops == 0:
            return self.synapses_to(q, synapse_threshold = synapse_threshold)

        from_neurons = self._records_to_list(self.has(cls = 'Neuron').nodes)
        to_neurons = self._records_to_list(q.has(cls = 'Neuron').nodes)
        exclude_rids = set(from_neurons + to_neurons)
        path_rids = set()
        all_paths = []
        used_time = 0
        try:
            for hop in range(1, max_hops+1):
                template = """
                -SendsTo->
                {{class: Neuron, as: hop{hop}, where: (@rid not in [{exclude_rids}])}}
                -SendsTo->
                {{class: Synapse, where: (N >= {synapse_threshold})}}
                """

                query = """select from(
                MATCH
                {{class: Neuron, as: source, where: (@rid in [{from_rids}]) }}
                -SendsTo->
                {{class: Synapse, where: (N >= {synapse_threshold})}}
                """.format(from_rids = ','.join(from_neurons),
                           synapse_threshold = synapse_threshold) \
                + \
                ''.join([
                template.format(hop = i+1,
                                synapse_threshold = synapse_threshold,
                                exclude_rids = ','.join(exclude_rids)) for i in range(hop)
                ]) \
                + \
                """
                -SendsTo->
                {{class: Neuron, as: dest, where: (@rid in [{to_rids}]) }}
                return source, {hops}, dest)
                timeout {timeout}""".format(
                    to_rids = ','.join(to_neurons),
                    hops = ','.join(['hop{}'.format(i+1) for i in range(hop)]),
                    timeout = int((timeout-used_time)*1000))

                start = time.time()
                q =  self._graph.client.command(query)
                used_time += time.time()-start

                paths = [[get_hash(node.oRecordData['source'])] + \
                         [get_hash(node.oRecordData['hop{}'.format(i+1)]) for i in range(hop)] + \
                         [get_hash(node.oRecordData['dest'])] for node in q]
                all_paths.extend(paths)
                #sum([[node.oRecordData['hop{}'.format(i+1)].get_hash() for node in q] for i in range(hop)], [])
                path_rids.update(set(sum([path[1:-1] for path in paths], [])))
                if exclude_path:
                    exclude_rids.update(path_rids)
        except PyOrientCommandException:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            # pdb.set_trace()
            if 'timeout' in exc_value.errors[0]:
                if hop == 1:
                    warn('Execution timeout, no results returned')
                else:
                    warn('Execution timeout, returnning results for maximum of {} hop(s)'.format(hop-1),
                         RuntimeWarning)
            else:
                raise
        return self.from_rids(self._graph, *list(path_rids)), all_paths


    def path_to2(self, q, synapse_threshold = 10, max_hops = 2,
                 exclude_path = True, timeout = 30):
        if max_hops == 0:
            return self.synapses_to(q, synapse_threshold = synapse_threshold)
        from_neurons = self._records_to_list(self.has(cls = 'Neuron').nodes)
        to_neurons = self._records_to_list(q.has(cls = 'Neuron').nodes)
        query = """
        select distince(pp) from
        (MATCH
        {{class: Neuron, where: (@rid in [{from_rids}]) }}
        -SendsTo->{{}}-SendsTo->
        {{ maxdepth: {nhop}, where: ($depth%2 = 1 and @rid not in [{exclude_rids}]), pathAlias: pp}}
        -SendsTo->{{}}-SendsTo->
        {{class: Neuron, where: (@rid in [{to_rids}]) }}
        return pp) timeout {timeout}""".format(from_rids = ','.join(from_neurons),
                   exclude_rids = ','.join(exclude_rids),
                   to_rids = ','.join(to_neurons),
                   nhop = max_hops+1,
                   timeout = timeout * 1000)


    def synapses_to(self, q, synapse_threshold = 10):
        from_neurons = self._records_to_list(self.has(cls = 'Neuron').nodes)
        to_neurons = self._records_to_list(q.has(cls = 'Neuron').nodes)
        q = self._graph.client.command(
        """
        MATCH
        {{class: Neuron, where: (@rid in [{from_rids}]) }}
        -SendsTo->
        {{class: Synapse, as: synapses, where: (N >= {synapse_threshold})}}
        -SendsTo->
        {{class: Neuron, where: (@rid in [{to_rids}]) }}
        return synapses
        """.format(from_rids = ','.join(from_neurons),
                   to_rids = ','.join(to_neurons),
                   synapse_threshold = synapse_threshold)
        )
        # synapse_rids = []
        # for node in q:
        #     data = node.oRecordData['synapses']
        #     if isinstance(data, tuple):
        #         if data[0] == 'OrientRecordLink':
        #             rid = '#{}:{}'.format(data[1], data[2])
        #             synapse_rids.append(rid)
        #     elif isinstance(data, OrientRecordLink):
        #         synapse_rids.append(data.get_hash())
        # synapse_rids = list(set(synapse_rids))
        synapse_rids = list(set([get_hash(node.oRecordData['synapses']) for node in q]))
        return self.from_rids(self._graph, *synapse_rids)

    def export_graph(self, graph_name, as_type='df', stored_as='gpickle', compression=''):
        g = self.get_as(as_type)
        if as_type=='df':
            g[0].to_csv(graph_name+'_nodes.csv', index=True)
            g[1].to_csv(graph_name+'_edges.csv', index=False)
            return 'Saved'
        elif as_type=='nx':
            if stored_as=='gpickle':
                nx.nx.write_gpickle(g, graph_name+'.gpickle'+compression)
                return 'Saved'
            elif stored_as=='gexf':
                nx.nx.write_gexf(g, graph_name+'.gexf'+compression)
                return 'Saved'
            else:
                raise UnsupportedView(stored_as)
        raise UnsupportedView(as_type)

    def import_graph(self, graph_name, as_type='df', stored_as='csv', compression=''):
        client = self._graph.client
        if as_type=='df':
            df_node = pd.pd.read_csv(graph_name+'_nodes.csv', index_col=0)
            df_edge = pd.pd.read_csv(graph_name+'_edges.csv', index_col=False)
            pd.pandas_to_orient(client, df_node, df_edge)
            return True
        elif as_type=='nx':
            if stored_as=='gpickle':
                g = nx.nx.read_gpickle(graph_name+'.gpickle'+compression)
                nx.nx_to_orient(client, g)
                return True
            elif stored_as=='gexf':
                g = nx.nx.read_gexf(graph_name+'.gexf'+compression)
                nx.nx_to_orient(client, g)
                return True
        return False

    @class_method_timer
    def __or__(self, other):
        assert isinstance(other, self.__class__)
        return self.__class__(self._graph, ('|', self._query, other._query),
                              init_nodes=self._dict_union(self._nodes, other._nodes),
                              executed = True,
                              debug = (self.debug | other.debug))#'(%s\n|\n%s)' %(self._disp_query, other._disp_query))

    __add__ = __or__

    @class_method_timer
    def __sub__(self, other):
        assert isinstance(other, self.__class__)
        return self.__class__(self._graph, ('-', self._query, other._query),
                              init_nodes=self._dict_difference(self._nodes, other._nodes),
                              executed = True,
                              debug = (self.debug | other.debug))#'(%s\n-\n%s)' %(self._disp_query, other._disp_query))

    @class_method_timer
    def __and__(self, other):
        assert isinstance(other, self.__class__)
        return self.__class__(self._graph, ('&', self._query, other._query),
                              init_nodes=self._dict_intersection(self._nodes, other._nodes),
                              executed = True,
                              debug = (self.debug | other.debug))#'(%s\n&\n%s)' %(self._disp_query, other._disp_query))

    __mul__ = __and__

    @class_method_timer
    def __xor__(self, other):
        assert isinstance(other, self.__class__)
        return self.__class__(self._graph, ('^', self._query, other._query),
                              init_nodes=self._dict_symmetric_difference(self._nodes, other._nodes),
                              executed = True,
                              debug = (self.debug | other.debug))#'(%s\n^\n%s)' %(self._disp_query, other._disp_query))

    @class_method_timer
    def __ior__(self, other):
        assert isinstance(other, self.__class__)
        self._query = ('|', self._query, other._query)
        self._nodes = self._dict_union(self._nodes, other._nodes)
        #self._disp_query = '(%s\n|\n%s)' %(self._disp_query, other._disp_query)

    __iadd__ = __ior__

    @class_method_timer
    def __isub__(self, other):
        assert isinstance(other, self.__class__)
        self._query = ('-', self._query, other._query)
        self._nodes = self._dict_difference(self._nodes, other._nodes)
        #self._disp_query = '(%s\n-\n%s)' %(self._disp_query, other._disp_query)

    @class_method_timer
    def __iand__(self, other):
        assert isinstance(other, self.__class__)
        self._query = ('&', self._query, other._query)
        self._nodes = self._dict_intersection(self._nodes, other._nodes)
        #self._disp_query = '(%s\n&\n%s)' %(self._disp_query, other._disp_query)

    @class_method_timer
    def __ixor__(self, other):
        assert isinstance(other, self.__class__)
        self._query = ('^', self._query, other._query)
        self._nodes = self._dict_symmetric_difference(self._nodes, other._nodes)
        #self._disp_query = '(%s\n^\n%s)' %(self._disp_query, other._disp_query)

    @class_method_timer
    def __eq__(self, other):
        """
        Check whether nodes returned by queries are equivalent.
        """

        assert isinstance(other, self.__class__)

        # Queries can only be checked for equality after evaluation:
        if not self._executed:
            return False
        if set(self._nodes) == set(other._nodes):
            return True
        else:
            return False

def _q_repr(attr):
    if isinstance(attr, str):
        return "'" + str(attr) + "'"
    else:
        return str(attr)

def _list_repr(attr):
    if not(isinstance(attr, list)):
        if isinstance(attr, (str, numbers.Number)):
            return [attr]
        else:
            return list(attr)
    return attr


def _kwargs(kwargs):
    if 'max_levels' in kwargs:
        assert isinstance(kwargs['max_levels'], numbers.Integral) and kwargs['max_levels'] >= 0

    assert not (('cls' in kwargs) and ('instanceof' in kwargs)), "can't use both cls and instanceof"

    attrs = []
    classes = ""
    columns = ""
    depth = ""

    for k, v in kwargs.items():
        if k=='max_levels':
            depth="while $depth <= %s" %v
        elif k=='instanceof':
            classes = "where @this instanceof '%s'" % v
        else:
            v = _list_repr(v)
            if k=='cls':
                if kwargs[k]:
                    classes = "where @class in %s" % v
            elif k=='cols':
                columns = ", ".join(v)
            elif k=='rid':
                attrs.append("@rid in %s" % v.__repr__().replace("'",""))
            else:
                # if isinstance(k, (tuple, list)) and len(k) > 1:
                #     if len(v) == 1 and isinstance(v[0],(str,bytes)) and len(v[0])>=2 and v[0][:2] == '/r':
                #         attrs.append(""" any(%s) matches "%s" """ % (','.join(k), v[0][2:]))
                #     elif (len(v) ==2 and isinstance(v[0],(str,bytes)) and len(v[0])
                #     and v[0] in ['<','>','=','<=','>=']):
                #         attrs.append("any(%s) %s %s" % (','.join(k),v[0],v[1]))
                #     else:
                #         attrs.append("any(%s) in %s" % (','.join(k), v))
                # else:
                if len(v) == 1 and isinstance(v[0],(str,bytes)) and len(v[0])>=2 and v[0][:2] == '/r':
                    # regex match
                    attrs.append("""%s matches "%s" """ % (k, v[0][2:]))
                elif (len(v) ==2 and isinstance(v[0],(str,bytes)) and len(v[0])
                and v[0] in ['<','>','=','<=','>=']):
                    attrs.append("%s %s %s" % (k,v[0],v[1]))
                else:
                    attrs.append("%s in %s" % (k, v))
    return classes, attrs, depth, columns


if __name__ == '__main__':
    # from networkx import *
    from pyorient.ogm import Config, Graph
    import numpy as np
    from neuroarch.utils import is_rid, iterable
    from neuroarch.query import *
    from neuroarch.diff import diff_nodes, diff_edges
    from neuroarch.apply_diff import apply_node_diff, apply_edge_diff
    import pandas as pd

    from neuroarch.models import Node, Relationship

    from pyorient.ogm.declarative import declarative_node, declarative_relationship

    SchemaNode = declarative_node()
    SchemaRelationship = declarative_relationship()
    graph = Graph(Config.from_url('/na_server', 'root', 'root', initial_drop=False))

    classes_from_schema = graph.build_mapping(SchemaNode, SchemaRelationship, auto_plural=True)

    graph.include(Node.registry)
    graph.include(Relationship.registry)
    print(graph, type(graph))

    q2=QueryWrapper(graph,QueryString("select from CartridgeModel where name in ['cart114']",'sql'))
    print(q2)
    n1 = q2.traverse_owns(max_levels=3) #, cls = 'MorrisLecar')
    n2 = n1.get_as()[0]

    threshold = list(np.arange(0,1.,0.1)) + [np.nan]*(len(n2)-len(np.arange(0,1.,0.1)))
    model = ['a']*(len(n2)-4) + [np.nan]*4
    n2['threshold'] = threshold
    a={'#111:1':{'name': 'a', 'n_outputs': 2, 'class': 'MorrisLecar'}}
    b={'#112:1':{'name': 'b', 'n_outputs': 2, 'class': 'MorrisLecar'}}
    n2 = n2.ix[:]
    n2 = n2.append(pd.DataFrame(a).T)
    n2 = n2.append(pd.DataFrame(b).T)
    n2.drop(['V2'], inplace=True, axis=1)
    q3 = QueryWrapper(graph,QueryString("select from Neuron limit 20",'sql'))
