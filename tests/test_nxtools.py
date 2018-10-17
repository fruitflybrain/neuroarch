#!/usr/bin/env python

import os
import re
import tempfile
from unittest import TestCase, main

import deepdiff
import networkx as nx

import neuroarch.nxtools as nxtools

class TestNXTools(TestCase):
    def setUp(self):
        self.g_digraph = nx.DiGraph()
        self.g_digraph.add_nodes_from([(0, {'x': 3}),
                                       (1, {'x': 4}),
                                       (2, {'y': 3}),
                                       (3, {'x': 3})])
        self.g_digraph.add_edges_from([(0, 1, {'a': 1}),
                                       (0, 2, {'b': 1}),
                                       (0, 3, {'a': 1}),
                                       (1, 2, {'b': 2})])

        self.g_digraph_str = nx.DiGraph()
        self.g_digraph_str.add_nodes_from([(0, {'x': 'foo'}),
                                           (1, {'x': 'fooo'}),
                                           (2, {'y': 'bar'}),
                                           (3, {'x': 'bar'})])
        self.g_digraph_str.add_edges_from([(0, 1, {'a': 'qux'}),
                                           (0, 2, {'b': 'mux'}),
                                           (0, 3, {'a': 'qux'}),
                                           (1, 2, {'b': 'mux'})])

        self.g_multi = nx.MultiDiGraph()
        self.g_multi.add_nodes_from([(0, {'x': 3}),
                                     (1, {'x': 4}),
                                     (2, {'y': 3}),
                                     (3, {'x': 3})])
        self.g_multi.add_edges_from([(0, 1, {'a': 1}),
                                     (0, 1, {'a': 1}),
                                     (0, 2, {'b': 1}),
                                     (0, 3, {'a': 1}),
                                     (1, 2, {'b': 2})])

        self.g_multi_str = nx.MultiDiGraph()
        self.g_multi_str.add_nodes_from([(0, {'x': 'foo'}),
                                         (1, {'x': 'fooo'}),
                                         (2, {'y': 'bar'}),
                                         (3, {'x': 'bar'})])
        self.g_multi_str.add_edges_from([(0, 1, {'a': 'qux'}),
                                         (0, 1, {'a': 'qux'}),
                                         (0, 2, {'b': 'mux'}),
                                         (0, 3, {'a': 'qux'}),
                                         (1, 2, {'b': 'mux'})])

    def test_nodes_has(self):
        result = nxtools.nodes_has(self.g_digraph, [1, 2], 'x', 4)
        self.assertItemsEqual(result, [1])

    def test_nodes_has_regex(self):
        result = nxtools.nodes_has(self.g_digraph_str, [1, 2], 'x',
                                   'foo.*', re.search)
        self.assertItemsEqual(result, [1])
        
    def test_edges_has_digraph(self):
        result = nxtools.edges_has(self.g_digraph, [(0, 1), (0, 2)], 'a', 1)
        self.assertItemsEqual(result, [(0, 1)])

    def test_edges_has_digraph_regex(self):
        result = nxtools.edges_has(self.g_digraph_str, [(0, 1), (0, 2)], 'a',
                                   'qu.*', re.search)
        self.assertItemsEqual(result, [(0, 1)])

    def test_edges_has_multi(self):
        result = nxtools.edges_has(self.g_multi, [(0, 1, 0), (0, 2, 0)], 'a', 1)
        self.assertItemsEqual(result, [(0, 1, 0)])

    def test_edges_has_multi_regex(self):
        result = nxtools.edges_has(self.g_multi_str, [(0, 1, 0), (0, 2, 0)], 
                                   'a', 'qu.*', re.search)
        self.assertItemsEqual(result, [(0, 1, 0)])

    def test_all_nodes_has(self):
        result = nxtools.all_nodes_has(self.g_digraph, 'x', 3)
        self.assertItemsEqual(result, [0, 3])

    def test_all_nodes_has_regex(self):
        result = nxtools.all_nodes_has(self.g_digraph_str, 'x', 'foo.*', re.search)
        self.assertItemsEqual(result, [0, 1])

    def test_all_edges_has_digraph(self):
        result = nxtools.all_edges_has(self.g_digraph, 'a', 1)
        self.assertItemsEqual(result, [(0, 1), (0, 3)])

    def test_all_edges_has_digraph_regex(self):
        result = nxtools.all_edges_has(self.g_digraph_str, 'a', 'qu.*', re.search)
        self.assertItemsEqual(result, [(0, 1), (0, 3)])

    def test_all_edges_has_multi(self):
        result = nxtools.all_edges_has(self.g_multi, 'a', 1)
        self.assertItemsEqual(result,
                             [(0, 1, 0), (0, 1, 1), (0, 3, 0)])

    def test_all_edges_has_multi_regex(self):
        result = nxtools.all_edges_has(self.g_multi_str, 'a',
                                       'qu.*', re.search)
        self.assertItemsEqual(result,
                             [(0, 1, 0), (0, 1, 1), (0, 3, 0)])

    def test_out_nodes_has(self):
        result = nxtools.out_nodes_has(self.g_digraph, [0], 'x', 3)
        self.assertItemsEqual(result, [3])

    def test_out_nodes_has_regex(self):
        result = nxtools.out_nodes_has(self.g_digraph_str, [0], 'x',
                                       'foo.*', re.search)
        self.assertItemsEqual(result, [1])

    def test_in_nodes_has(self):
        result = nxtools.in_nodes_has(self.g_digraph, [1], 'x', 3)
        self.assertItemsEqual(result, [0])

    def test_in_nodes_has_regex(self):
        result = nxtools.in_nodes_has(self.g_digraph_str, [2], 'x',
                                      '.*ooo', re.search)
        self.assertItemsEqual(result, [1])

    def test_out_edges_has_digraph(self):
        result = nxtools.out_edges_has(self.g_digraph, [0], 'b', 1)
        self.assertItemsEqual(result, [(0, 2)])

    def test_out_edges_has_digraph_regex(self):
        result = nxtools.out_edges_has(self.g_digraph_str, [0],
                                       'b', 'mu.*', re.search)
        self.assertItemsEqual(result, [(0, 2)])

    def test_out_edges_has_multi(self):
        result = nxtools.out_edges_has(self.g_multi, [0], 'b', 1)
        self.assertItemsEqual(result, [(0, 2, 0)])

    def test_out_edges_has_multi_regex(self):
        result = nxtools.out_edges_has(self.g_multi_str, [0], 
                                       'b', 'mu.*', re.search)
        self.assertItemsEqual(result, [(0, 2, 0)])

    def test_in_edges_has_digraph(self):
        result = nxtools.in_edges_has(self.g_digraph, [2], 'b', 1)
        self.assertItemsEqual(result, [(0, 2)])

    def test_in_edges_has_digraph_regex(self):
        result = nxtools.in_edges_has(self.g_digraph_str, [1, 2],
                                      'a', 'qu.*', re.search)
        self.assertItemsEqual(result, [(0, 1)])

    def test_in_edges_has_multi(self):
        result = nxtools.in_edges_has(self.g_multi, [2], 'b', 1)
        self.assertItemsEqual(result, [(0, 2, 0)])

    def test_in_edges_has_multi_regex(self):
        result = nxtools.in_edges_has(self.g_multi_str, [1],
                                      'a', 'qu.*', re.search)
        self.assertItemsEqual(result, [(0, 1, 0), (0, 1, 1)])
                             
    def test_find_nonmatching_dict_pairs(self):
        assert nxtools.find_nonmatching_dict_pairs([{1: 2}], [{1: 2}]) == ([], [])        
        assert nxtools.find_nonmatching_dict_pairs([{1: 2}, {3: 4}],
                                           [{1: 2}, {3: 4}]) == ([], [])
        assert nxtools.find_nonmatching_dict_pairs([{1: 2}, {3: 4}],
                                           [{1: 2}, {3: 5}]) == ([{3: 4}], [{3: 5}])
        assert nxtools.find_nonmatching_dict_pairs([{1: 2}, {1: 2}, {3: 4}],
                                  [{1: 2}, {1: 2}, {3: 5}]) == ([{3: 4}], [{3: 5}])
        assert nxtools.find_nonmatching_dict_pairs([{1: 2}, {1: 2}, {3: 4}, {3: 4}],
                                           [{1: 2}, {1: 2}, {3: 5}, {3: 5}]) == \
            ([{3: 4}, {3: 4}], [{3: 5}, {3: 5}])            
        assert nxtools.find_nonmatching_dict_pairs([{1: 2}, {1: 2}, {3: 4}, {3: 5}],
                                  [{1: 2}, {1: 2}, {3: 5}, {3: 5}]) == \
            ([{3: 4}], [{3: 5}])

    def test_is_isomorphic_attr(self):
        g0 = nx.MultiDiGraph()
        g0.add_node(0, **{'name': 'foo',
                                  'node_type': 'neuron'})
        g0.add_node(1, **{'name': 'bar',
                                  'node_type': 'neuron'})
        g0.add_node(2, **{'name': 'foo-bar',
                                  'node_type': 'synapse'})
        g0.add_edge(0, 2, **{'edge_type': 'data'})
        g0.add_edge(2, 1, **{'edge_type': 'data'})

        g1 = nx.MultiDiGraph()
        g1.add_node('a', **{'name': 'foo',
                                    'node_type': 'neuron'})
        g1.add_node('b', **{'name': 'bar',
                                    'node_type': 'neuron'})
        g1.add_node('c', **{'name': 'foo-bar',
                                    'node_type': 'synapse'})
        g1.add_edge('a', 'c', **{'edge_type': 'data'})
        g1.add_edge('c', 'b', **{'edge_type': 'data'})

        g2 = nx.MultiDiGraph()
        g2.add_node('a', **{'name': 'foo',
                                    'node_type': 'neuron'})
        g2.add_node('b', **{'name': 'bar',
                                    'node_type': 'neuron'})
        g2.add_node('c', **{'name': 'foo-bar',
                                    'node_type': 'synapse'})
        g2.add_edge('a', 'c', **{'edge_type': 'xxxx'})
        g2.add_edge('c', 'b', **{'edge_type': 'data'})

        assert nxtools.is_isomorphic_attr(g0, g1)
        assert not nxtools.is_isomorphic_attr(g0, g2)

    def test_iso_attr_diff_multidigraph_no_multiedges(self):
        g0 = nx.MultiDiGraph()
        g0.add_node(0, **{'name': 'foo',
                                  'node_type': 'neuron'})
        g0.add_node(1, **{'name': 'bar',
                                  'node_type': 'neuron'})
        g0.add_node(2, **{'name': 'foo-bar',
                                  'node_type': 'synapse'})
        g0.add_edge(0, 2, **{'edge_type': 'data'})
        g0.add_edge(2, 1, **{'edge_type': 'data'})

        g1 = nx.MultiDiGraph()
        g1.add_node('a', **{'name': 'foo',
                                    'node_type': 'neuron'})
        g1.add_node('b', **{'name': 'bar',
                                    'node_type': 'neuron'})
        g1.add_node('c', **{'name': 'foo-bar',
                                    'node_type': 'synapse'})
        g1.add_edge('a', 'c', **{'edge_type': 'data'})
        g1.add_edge('c', 'b', **{'edge_type': 'data'})

        node_diff, edge_diff = nxtools.iso_attr_diff(g0, g1)
        self.assertDictEqual(node_diff, {})
        self.assertDictEqual(edge_diff, {})

    def test_iso_attr_diff_multidigraph_multiedges(self):
        g0 = nx.MultiDiGraph()
        g0.add_node(0, **{'name': 'foo',
                                  'node_type': 'neuron'})
        g0.add_node(1, **{'name': 'bar',
                                  'node_type': 'neuron'})
        g0.add_node(2, **{'name': 'foo-bar',
                                  'node_type': 'synapse'})
        g0.add_edge(0, 2, **{'edge_type': 'data'})
        g0.add_edge(0, 2, **{'edge_type': 'data'})
        g0.add_edge(2, 1, **{'edge_type': 'data'})

        g1 = nx.MultiDiGraph()
        g1.add_node('a', **{'name': 'foo',
                                    'node_type': 'neuron'})
        g1.add_node('b', **{'name': 'bar',
                                    'node_type': 'neuron'})
        g1.add_node('c', **{'name': 'foo-bar',
                                    'node_type': 'synapse'})
        g1.add_edge('a', 'c', **{'edge_type': 'data'})
        g1.add_edge('a', 'c', **{'edge_type': 'data'})
        g1.add_edge('c', 'b', **{'edge_type': 'data'})

        g2 = nx.MultiDiGraph()
        g2.add_node('a', **{'name': 'foo',
                                    'node_type': 'neuron'})
        g2.add_node('b', **{'name': 'bar',
                                    'node_type': 'neuron'})
        g2.add_node('c', **{'name': 'foo-bar',
                                    'node_type': 'synapse'})
        g2.add_edge('a', 'c', **{'edge_type': 'data'})
        g2.add_edge('a', 'c', **{'edge_type': 'xxxx'})
        g2.add_edge('c', 'b', **{'edge_type': 'data'})

        node_diff, edge_diff = nxtools.iso_attr_diff(g0, g1)
        self.assertDictEqual(node_diff, {})
        self.assertDictEqual(edge_diff, {})

        node_diff, edge_diff = nxtools.iso_attr_diff(g0, g2)

        self.assertDictEqual(node_diff, {})
        self.assertDictEqual(edge_diff, 
            {((0, 2, 0), ('a', 'c', 1)): 
             {'values_changed': ["root['edge_type']: 'data' ===> 'xxxx'"]}})

    def _attr_match(self, a, b):
        return False if deepdiff.DeepDiff(a, b) else True

    def test_read_gexf_digraph(self):
        f = tempfile.mktemp()
        g_orig = nx.DiGraph()
        g_orig.add_nodes_from(['a', 'b'])
        g_orig.add_edge('a', 'b', **{'x': 1})
        nx.write_gexf(g_orig, f)
        g_new = nxtools.read_gexf(f)
        os.unlink(f)

        self.assertTrue(nx.isomorphism.is_isomorphic(g_orig, g_new, 
                                                     self._attr_match, 
                                                     self._attr_match))

    def test_read_gexf_multidigraph(self):
        f = tempfile.mktemp()
        g_orig = nx.MultiDiGraph()
        g_orig.add_nodes_from(['a', 'b'])
        g_orig.add_edge('a', 'b', **{'x': 1})
        g_orig.add_edge('a', 'b', **{'x': 2})
        nx.write_gexf(g_orig, f)
        g_new = nxtools.read_gexf(f)
        os.unlink(f)

        self.assertTrue(nx.isomorphism.is_isomorphic(g_orig, g_new, 
                                                     self._attr_match, 
                                                     self._attr_match))

if __name__ == '__main__':
    main()
