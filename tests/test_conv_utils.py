#!/usr/bin/env python

from unittest import main, TestCase

import neuroarch.conv.utils

import deepdiff
import networkx as nx
import pandas as pd

match = lambda a, b: False if deepdiff.DeepDiff(a, b) else True

class TestConvUtils(TestCase):
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

    def _create_pandas_graph(self):
        df_node = pd.DataFrame({'name': ['foo', 'bar', 'baz', 
                                         'foo-bar', 'foo-baz'],
                                'class': ['neuron', 'neuron', 'neuron',
                                          'synapse', 'synapse']})
        df_edge = pd.DataFrame({'out': [0, 3, 0, 4],
                                'in': [3, 1, 4, 2],
                                'class': ['data', 'data', 'data', 'data']})
        return df_node, df_edge
        
    def test_pandas_to_nx(self):
        df_node, df_edge = self._create_pandas_graph()
        g_nx = self._create_nx_graph()
        g_pandas = neuroarch.conv.utils.pandas_to_nx(df_node, df_edge)
        assert nx.isomorphism.is_isomorphic(g_nx, g_pandas,
                                            node_match=match,
                                            edge_match=match)

    def test_nx_to_pandas(self):
        g_nx = self._create_nx_graph()
        df_node_pandas, df_edge_pandas = self._create_pandas_graph()
        df_node_nx, df_edge_nx = neuroarch.conv.utils.nx_to_pandas(g_nx)
        assert set([tuple(v) for v in df_node_pandas.values]) == \
            set([tuple(v) for v in df_node_nx.values])
        assert set([tuple(v) for v in df_edge_pandas.values]) == \
            set([tuple(v) for v in df_edge_nx.values])

if __name__ == '__main__':
    main()
