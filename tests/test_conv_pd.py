from unittest import main, TestCase

import neuroarch.conv.pd
import deepdiff
import pyorient
import pandas as pd

db_name = 'neuroarch_test_db'
username = 'admin'
passwd = 'admin'

match = lambda a, b: False if deepdiff.DeepDiff(a, b) else True

class TestConvPandas(TestCase):    
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

    def _connect_server(self):
        self.client = pyorient.OrientDB('localhost', 2424)
        self.client.connect(username, passwd)

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

    def _create_pandas_graph(self):
        df_node = pd.DataFrame({'name': ['foo', 'bar', 'baz', 
                                         'foo-bar', 'foo-baz'],
                                'class': ['neuron', 'neuron', 'neuron',
                                          'synapse', 'synapse']})
        df_edge = pd.DataFrame({'out': [0, 3, 0, 4],
                                'in': [3, 1, 4, 2],
                                'class': ['data', 'data', 'data', 'data']})
        return df_node, df_edge

    def _create_orient_graph(self):
        cmd = ("begin;"               
               "let foo = create vertex neuron content {'name': 'foo', 'id': 0};"
               "let bar = create vertex neuron content {'name': 'bar', 'id': 1};"
               "let baz = create vertex neuron content {'name': 'baz', 'id': 2};"
               "let foo_bar = create vertex synapse content {'name': 'foo-bar', 'id': 3};"
               "let foo_baz = create vertex synapse content {'name': 'foo-baz', 'id': 4};"
               "create edge data from $foo to $foo_bar;"
               "create edge data from $foo_bar to $bar;"
               "create edge data from $foo to $foo_baz;"
               "create edge data from $foo_baz to $baz;"
               "commit retry 5;")
        self.client.batch(cmd)

    def test_orient_to_pandas(self):
        df_node_pandas, df_edge_pandas = self._create_pandas_graph()
        self._create_orient_graph()
        df_node_orient, df_edge_orient = neuroarch.conv.pd.orient_to_pandas(self.client,
                    'g.V.has("@class", T.in, ["neuron","synapse"])',
                    'g.E.has("@class", "data")')
        self.assertSetEqual(set([tuple(v) for v in df_node_pandas.values]),
                            set([tuple(v) for v in df_node_orient.values]))
        self.assertSetEqual(set([tuple(v) for v in df_edge_pandas.values]),
                            set([tuple(v) for v in df_edge_orient.values]))
        self.assertSetEqual(set(df_node_pandas.index),
                            set(df_node_orient.index))

    def test_pandas_to_orient(self):
        df_node_pandas, df_edge_pandas = self._create_pandas_graph()
        neuroarch.conv.pd.pandas_to_orient(self.client,
                                           df_node_pandas, df_edge_pandas)
        df_node_orient, df_edge_orient = neuroarch.conv.pd.orient_to_pandas(self.client,
                    'g.V.has("@class", T.in, ["neuron","synapse"])',
                    'g.E.has("@class", "data")')
        self.assertSetEqual(set([tuple(v) for v in df_node_pandas.values]),
                            set([tuple(v) for v in df_node_orient.values]))
        self.assertSetEqual(set([tuple(v) for v in df_edge_pandas.values]),
                            set([tuple(v) for v in df_edge_orient.values]))
        self.assertSetEqual(set(df_node_pandas.index),
                            set(df_node_orient.index))

    def test_pandas_to_orient_double(self):
        df_node_pandas = pd.DataFrame({'name': ['foo', 'bar', 'foo-bar'],
                                       'class': ['neuron', 'neuron', 'synapse'],
                                       'x': [1/3.0, 1/4.0, 1.0]})
        df_edge_pandas = pd.DataFrame({'out': [0, 2], 'in': [2, 1],
                                       'class': ['data', 'data']})
        neuroarch.conv.pd.pandas_to_orient(self.client,
                                           df_node_pandas,
                                           df_edge_pandas)
        df_node_orient, df_edge_orient = \
            neuroarch.conv.pd.orient_to_pandas(self.client,
                    'g.V.has("@class", T.in, ["neuron","synapse"])',
                    'g.E.has("@class", "data")')
        self.assertSetEqual(set([tuple(v) for v in df_node_pandas.values]),
                            set([tuple(v) for v in df_node_orient.values]))
        self.assertSetEqual(set([tuple(v) for v in df_edge_pandas.values]),
                            set([tuple(v) for v in df_edge_orient.values]))
        self.assertSetEqual(set(df_node_pandas.index),
                            set(df_node_orient.index))

if __name__ == '__main__':
    main()
