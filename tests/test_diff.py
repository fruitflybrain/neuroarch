#!/usr/bin/env python

from unittest import TestCase, main

import pandas as pd

from neuroarch.diff import diff_nodes

class TestDiff(TestCase):
    def test_mod_row(self):
        df1 = pd.DataFrame({'a': [1,2,3], 'b': [4,5,6]}, 
                           index=['w','x','y'])
        df2 = pd.DataFrame({'a': [1,2,9], 'b': [4,5,6]},
                           index=['w','x','y'])
        self.assertDictEqual(diff_nodes(df1, df2),
                {'add': {},
                 'del': {}, 
                 'mod': {'y': {'a': 9, 'b': 6}}})

    def test_mod_row_int_ind(self):
        df1 = pd.DataFrame({'a': [1,2,3], 'b': [4,5,6]}, 
                           index=[0,1,2])
        df2 = pd.DataFrame({'a': [1,2,9], 'b': [4,5,6]},
                           index=[0,1,2])
        self.assertDictEqual(diff_nodes(df1, df2),
                {'add': {},
                 'del': {}, 
                 'mod': {2: {'a': 9, 'b': 6}}})

    def test_add_row(self):
        df1 = pd.DataFrame({'a': [1,2,3], 'b': [4,5,6]}, 
                           index=['w','x','y'])
        df2 = pd.DataFrame({'a': [1,2,3,4], 'b': [4,5,6,7]},
                           index=['w','x','y','z'])
        self.assertDictEqual(diff_nodes(df1, df2),
                {'add': {'z': {'a': 4, 'b': 7}},
                 'del': {}, 
                 'mod': {}})

    def test_add_row_int_ind(self):
        df1 = pd.DataFrame({'a': [1,2,3], 'b': [4,5,6]}, 
                           index=[0,1,2])
        df2 = pd.DataFrame({'a': [1,2,3,4], 'b': [4,5,6,7]},
                           index=[0,1,2,3])
        self.assertDictEqual(diff_nodes(df1, df2),
                {'add': {3: {'a': 4, 'b': 7}},
                 'del': {}, 
                 'mod': {}})

    def test_del_row(self):
        df1 = pd.DataFrame({'a': [1,2,3], 'b': [4,5,6]}, 
                           index=['w','x','y'])
        df2 = pd.DataFrame({'a': [1,3], 'b': [4,6]},
                           index=['w','y'])
        self.assertDictEqual(diff_nodes(df1, df2),
                {'add': {},
                 'del': {'x': None},
                 'mod': {}})

    def test_ren_row(self):
        df1 = pd.DataFrame({'a': [1,2,3], 'b': [4,5,6]}, 
                           index=['w','x','y'])
        df2 = pd.DataFrame({'a': [1,2,3], 'b': [4,5,6]},
                           index=['w','x','z'])
        self.assertDictEqual(diff_nodes(df1, df2),
                {'add': {'z': {'a': 3, 'b': 6}},
                 'del': {'y': None},
                 'mod': {}})

    def test_ren_row_int_ind(self):
        df1 = pd.DataFrame({'a': [1,2,3], 'b': [4,5,6]}, 
                           index=[0,1,2])
        df2 = pd.DataFrame({'a': [1,2,3], 'b': [4,5,6]},
                           index=[0,1,3])
        self.assertDictEqual(diff_nodes(df1, df2),
                {'add': {3: {'a': 3, 'b': 6}},
                 'del': {2: None},
                 'mod': {}})

    def test_add_col(self):
        df1 = pd.DataFrame({'a': [1,2,3], 'b': [4,5,6]}, 
                           index=['w','x','y'])
        df2 = pd.DataFrame({'a': [1,2,3], 'b': [4,5,6],
                            'c': ['p','q','r']},
                           index=['w','x','y'])
        self.assertDictEqual(diff_nodes(df1, df2),
            {'add': {},
             'mod': {'w': {'a': 1, 'b': 4, 'c': 'p'},
                     'x': {'a': 2, 'b': 5, 'c': 'q'},
                     'y': {'a': 3, 'b': 6, 'c': 'r'}},
             'del': {}})

    def test_del_col(self):
        df1 = pd.DataFrame({'a': [1,2,3], 'b': [4,5,6]}, 
                           index=['w','x','y'])
        df2 = pd.DataFrame({'a': [1,2,3]},
                           index=['w','x','y'])
        self.assertDictEqual(diff_nodes(df1, df2),
            {'add': {},
             'mod': {'w': {'a': 1}, 'x': {'a': 2}, 'y': {'a': 3}},
             'del': {}})

    def test_ren_col(self):
        df1 = pd.DataFrame({'a': [1,2,3], 'b': [4,5,6]}, 
                           index=['w','x','y'])
        df2 = pd.DataFrame({'a': [1,2,3], 'c': [4,5,6]},
                           index=['w','x','y'])
        self.assertDictEqual(diff_nodes(df1, df2),
            {'add': {},
             'mod': {'w': {'a': 1, 'c': 4},
                     'x': {'a': 2, 'c': 5},
                     'y': {'a': 3, 'c': 6}},
             'del': {}})

    def test_mod_row_add_row(self):
        df1 = pd.DataFrame({'a': [1,2,3], 'b': [4,5,6]}, 
                           index=['w','x','y'])
        df2 = pd.DataFrame({'a': [1,2,9,4], 'b': [4,5,6,7]},
                           index=['w','x','y','z'])
        self.assertDictEqual(diff_nodes(df1, df2),
            {'add': {'z': {'a': 4, 'b': 7}},
             'mod': {'y': {'a': 9, 'b': 6}},
             'del': {}})
        
    def test_mod_row_del_row(self):
        df1 = pd.DataFrame({'a': [1,2,3], 'b': [4,5,6]}, 
                           index=['w','x','y'])
        df2 = pd.DataFrame({'a': [1,9], 'b': [4,6]},
                           index=['w','y'])
        self.assertDictEqual(diff_nodes(df1, df2),
            {'add': {},
             'mod': {'y': {'a': 9, 'b': 6}},
             'del': {'x': None}})

    def test_mod_row_ren_row(self):
        df1 = pd.DataFrame({'a': [1,2,3], 'b': [4,5,6]}, 
                           index=['w','x','y'])
        df2 = pd.DataFrame({'a': [1,9,3], 'b': [4,5,6]},
                           index=['w','x','z'])
        self.assertDictEqual(diff_nodes(df1, df2),
            {'add': {'z': {'a': 3, 'b': 6}},
             'mod': {'x': {'a': 9, 'b': 5}},
             'del': {'y': None}})

    def test_mod_row_add_col(self):
        df1 = pd.DataFrame({'a': [1,2,3], 'b': [4,5,6]}, 
                           index=['w','x','y'])
        df2 = pd.DataFrame({'a': [1,2,9], 'b': [4,5,6], 'c': [7,8,9]},
                           index=['w','x','y'])
        self.assertDictEqual(diff_nodes(df1, df2),
            {'add': {},
             'mod': {'w': {'a': 1, 'b': 4, 'c': 7},
                     'x': {'a': 2, 'b': 5, 'c': 8},
                     'y': {'a': 9, 'b': 6, 'c': 9}},
             'del': {}}) 

    def test_mod_row_del_col(self):
        df1 = pd.DataFrame({'a': [1,2,3], 'b': [4,5,6]}, 
                           index=['w','x','y'])
        df2 = pd.DataFrame({'a': [1,2,4]},
                           index=['w','x','y'])
        self.assertDictEqual(diff_nodes(df1, df2),
            {'add': {},
             'mod': {'w': {'a': 1},
                     'x': {'a': 2},
                     'y': {'a': 4}},
             'del': {}}) 

    def test_mod_row_ren_col(self):
        df1 = pd.DataFrame({'a': [1,2,3], 'b': [4,5,6]}, 
                           index=['w','x','y'])
        df2 = pd.DataFrame({'a': [1,2,3], 'c': [5,5,6]},
                           index=['w','x','y'])
        self.assertDictEqual(diff_nodes(df1, df2),
            {'add': {},
             'mod': {'w': {'a': 1, 'c': 5},
                     'x': {'a': 2, 'c': 5},
                     'y': {'a': 3, 'c': 6}},
             'del': {}}) 

    def test_del_row_add_col(self):
        df1 = pd.DataFrame({'a': [1,2,3], 'b': [4,5,6]}, 
                           index=['w','x','y'])
        df2 = pd.DataFrame({'a': [1,3], 'b': [4,6], 'c': [7,9]},
                           index=['w','y'])
        self.assertDictEqual(diff_nodes(df1, df2),
            {'add': {},
             'mod': {'w': {'a': 1, 'b': 4, 'c': 7},
                     'y': {'a': 3, 'b': 6, 'c': 9}},
             'del': {'x': None}}) 

    def test_del_row_del_col(self):
        df1 = pd.DataFrame({'a': [1,2,3], 'b': [4,5,6], 'c': [7,8,9]}, 
                           index=['w','x','y'])
        df2 = pd.DataFrame({'a': [1,3], 'b': [4,6]},
                           index=['w','y'])
        self.assertDictEqual(diff_nodes(df1, df2),
            {'add': {},
             'mod': {'w': {'a': 1, 'b': 4},
                     'y': {'a': 3, 'b': 6}},
             'del': {'x': None}}) 

    def test_del_row_ren_col(self):
        df1 = pd.DataFrame({'a': [1,2,3], 'b': [4,5,6], 'c': [7,8,9]}, 
                           index=['w','x','y'])
        df2 = pd.DataFrame({'a': [1,3], 'b': [4,6], 'd': [7,9]},
                           index=['w','y'])
        self.assertDictEqual(diff_nodes(df1, df2),
            {'add': {},
             'mod': {'w': {'a': 1, 'b': 4, 'd': 7},
                     'y': {'a': 3, 'b': 6, 'd': 9}},
             'del': {'x': None}}) 

    def test_ren_row_add_col(self):
        df1 = pd.DataFrame({'a': [1,2,3], 'b': [4,5,6]}, 
                           index=['w','x','y'])
        df2 = pd.DataFrame({'a': [1,2,3], 'b': [4,5,6], 'c': [7,8,9]},
                           index=['w','x','z'])
        self.assertDictEqual(diff_nodes(df1, df2),
            {'add': {'z': {'a': 3, 'b': 6, 'c': 9}},
             'mod': {'w': {'a': 1, 'b': 4, 'c': 7},
                     'x': {'a': 2, 'b': 5, 'c': 8}},
             'del': {'y': None}}) 
        
    def test_ren_row_del_col(self):
        df1 = pd.DataFrame({'a': [1,2,3], 'b': [4,5,6], 'c': [7,8,9]}, 
                           index=['w','x','y'])
        df2 = pd.DataFrame({'a': [1,2,3], 'b': [4,5,6]},
                           index=['w','x','z'])
        self.assertDictEqual(diff_nodes(df1, df2),
            {'add': {'z': {'a': 3, 'b': 6}},
             'mod': {'w': {'a': 1, 'b': 4},
                     'x': {'a': 2, 'b': 5}},
             'del': {'y': None}}) 

    def test_ren_row_ren_col(self):
        df1 = pd.DataFrame({'a': [1,2,3], 'b': [4,5,6], 'c': [7,8,9]}, 
                           index=['w','x','y'])
        df2 = pd.DataFrame({'a': [1,2,3], 'b': [4,5,6], 'd': [7,8,9]},
                           index=['w','x','z'])
        self.assertDictEqual(diff_nodes(df1, df2),
            {'add': {'z': {'a': 3, 'b': 6, 'd': 9}},
             'mod': {'w': {'a': 1, 'b': 4, 'd': 7},
                     'x': {'a': 2, 'b': 5, 'd': 8}},
             'del': {'y': None}}) 

if __name__ == '__main__':
    main()
