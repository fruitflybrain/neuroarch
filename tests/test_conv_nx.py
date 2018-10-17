#!/usr/bin/env python

from unittest import main, TestCase
import warnings

import neuroarch.conv.nx
import deepdiff
import pyorient
import networkx as nx

db_name = 'neuroarch_test_db'
username = 'admin'
passwd = 'admin'

class TestConvNetworkX(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = pyorient.OrientDB('localhost', 2424)
        cls.client.connect(username, passwd)
        if cls.client.db_exists(db_name):
            cls.client.db_drop(db_name)
        cls.client.db_create(db_name, pyorient.DB_TYPE_GRAPH,
                             pyorient.STORAGE_TYPE_MEMORY)
        cls.client.db_open(db_name, username, passwd)

    @classmethod
    def tearDownClass(cls):
        cls.client.connect(username, passwd)
        try:
            cls.client.db_drop(db_name)
        except Exception as e:
            warnings.warn('problem cleaning up test database: %s' % e.message)

    def setUp(self):
        cmds = ['create class neuron extends V',
                'create class synapse extends V',
                'create class data extends E']
        for cmd in cmds:
            self.client.command(cmd)

    def tearDown(self):
        cmds = ['delete vertex neuron',
                'delete vertex synapse',
                'delete edge data',
                'drop class neuron',
                'drop class synapse',
                'drop class data']
        for cmd in cmds:
            self.client.command(cmd)

    def _create_nx_graph(self):
        g = nx.MultiDiGraph()
        g.add_node(0, **{'name': 'foo',
                                 'class': 'neuron'})
        g.add_node(1, **{'name': 'bar',
                                 'class': 'neuron'})
        g.add_node(2, **{'name': 'baz',
                                 'class': 'neuron'})
        g.add_node(3, **{'name': 'foo-bar',
                                 'class': 'synapse'})
        g.add_node(4, **{'name': 'foo-baz',
                                 'class': 'synapse'})
        g.add_edge(0, 3, **{'class': 'data'})
        g.add_edge(3, 1, **{'class': 'data'})
        g.add_edge(0, 4, **{'class': 'data'})
        g.add_edge(4, 2, **{'class': 'data'})
        return g

    def _create_orient_graph(self):
        cmd = ("begin;"               
               "let foo = create vertex neuron content {'name': 'foo'};"
               "let bar = create vertex neuron content {'name': 'bar'};"
               "let baz = create vertex neuron content {'name': 'baz'};"
               "let foo_bar = create vertex synapse content {'name': 'foo-bar'};"
               "let foo_baz = create vertex synapse content {'name': 'foo-baz'};"
               "create edge data from $foo to $foo_bar;"
               "create edge data from $foo_bar to $bar;"
               "create edge data from $foo to $foo_baz;"
               "create edge data from $foo_baz to $baz;"
               "commit retry 5;")
        self.client.batch(cmd)

    def test_orient_to_nx(self):
        g_nx = self._create_nx_graph()
        self._create_orient_graph()
        g_orient = neuroarch.conv.nx.orient_to_nx(self.client,
                    'g.V.has("@class", T.in, ["neuron","synapse"])',
                    'g.E.has("@class", "data")')
        m = lambda a, b: False if deepdiff.DeepDiff(a, b) else True
        self.assertTrue(nx.isomorphism.is_isomorphic(g_nx, g_orient,
                                                     node_match=m,
                                                     edge_match=m))
        # We don't check whether the IDs of the two graphs are the same because
        # the original OrientDB graph doesn't contain any 'id' node properties.

    def test_nx_to_orient(self):
        g_nx = self._create_nx_graph()
        neuroarch.conv.nx.nx_to_orient(self.client, g_nx)
        g_orient = neuroarch.conv.nx.orient_to_nx(self.client,
                    'g.V.has("@class", T.in, ["neuron","synapse"])',
                    'g.E.has("@class", "data")')
        m = lambda a, b: False if deepdiff.DeepDiff(a, b) else True
        self.assertTrue(nx.isomorphism.is_isomorphic(g_nx, g_orient,
                                                     node_match=m,
                                                     edge_match=m))
        self.assertSetEqual(set(g_nx.nodes()), set(g_orient.nodes()))

    def test_nx_to_orient_double(self):
        g_nx = nx.MultiDiGraph()
        g_nx.add_node(0, **{'name': 'foo',
                                    'class': 'neuron',
                                    'x': 1/3.0})
        g_nx.add_node(1, **{'name': 'bar',
                                    'class': 'neuron',
                                    'x': 1/7.0})
        g_nx.add_edge(0, 1, **{'class': 'data'})
        neuroarch.conv.nx.nx_to_orient(self.client, g_nx)
        g_orient = neuroarch.conv.nx.orient_to_nx(self.client,
                    'g.V.has("@class", T.in, ["neuron","synapse"])',
                    'g.E.has("@class", "data")')
        m = lambda a, b: False if deepdiff.DeepDiff(a, b) else True
        self.assertTrue(nx.isomorphism.is_isomorphic(g_nx, g_orient,
                                                     node_match=m,
                                                     edge_match=m))
        self.assertSetEqual(set(g_nx.nodes()), set(g_orient.nodes()))
        
if __name__ == '__main__':
    main()
