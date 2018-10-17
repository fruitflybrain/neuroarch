#!/usr/bin/env python

# Copyright (c) 2015, Lev Givon
# All rights reserved.
# Distributed under the terms of the BSD license:
# http://www.opensource.org/licenses/bsd-license

import pickle
import copy
import logging
import sys

import networkx as nx
from pyorient.ogm import Graph, Config

from neuroarch.models import *
from neuroarch.utils import byteify, chunks, get_cluster_ids

import csv

import pandas as pd
import json

from pyorient.serializations import OrientSerialization

def load_swc(file_name):
    """
    Load an SWC file into a DataFrame.
    """

    df = pd.read_csv(file_name, delimiter=' ', header=None, comment='#',
                     names=['sample', 'identifier', 'x', 'y', 'z', 'r', 'parent'],
                     skipinitialspace=True)
    return df

class NTHULoader(object):
    neuropils = {'AL':('AL',['right antennal lobe','antennal lobe','al_r','al','right al']) ,
           'al':('al',['left antennal lobe','antennal lobe','al_l','al','left al']),
           'AMMC':('AMMC',['right antennal mechanosensory and motor center','antennal mechanosensory and motor center','ammc_r','ammc']),
           'ammc':('ammc',['left antennal mechanosensory and motor center','antennal mechanosensory and motor center','ammc_l','ammc']),
           #'CAL',
           #'cal',
           'CCP':('CCP',['right caudalcentral protocerebrum','caudalcentral protocerebrum','ccp_r','ccp','right ccp']),
           'ccp':('ccp',['left caudalcentral protocerebrum','caudalcentral protocerebrum','ccp_l','ccp','left ccp']),
           'CMP':('CMP',['right caudalmedial protocerebrum','caudalmedial protocerebrum','cmp_r','right cmp', 'cmp']),
           'cmp':('cmp',['left caudalmedial protocerebrum','caudalmedial protocerebrum','cmp_l','left cmp', 'cmp']),
           'CVLP':('CVLP',['right caudal ventrolateral protocerebrum','caudal ventrolateral protocerebrum','cvlp_r','right cvlp','cvlp']),
           'cvlp':('cvlp',['left caudal ventrolateral protocerebrum','caudal ventrolateral protocerebrum','cvlp_l','left cvlp','cvlp']),
           'DLP':('DLP',['right dorsolateral protocerebrum','dorsolateral protocerebrum','right dlp','dlp_r','dlp']),
           'dlp':('dlp',['left dorsolateral protocerebrum','dorsolateral protocerebrum','left dlp','dlp_l','dlp']),
           'DMP':('DMP',['right dorsomedial protocerebrum','dorsomedial protocerebrum','right dmp','dmp_r','dmp','icl','right icl','icl_r']),
           'dmp':('dmp',['left dorsomedial protocerebrum','dorsomedial protocerebrum','left dmp','dmp_l','dmp','icl','left icl','icl_l']),
           'EB':('EB',['ellipsoid body','left ellipsoid body','right ellipsoid body','eb_l','eb_r','eb']),
           #'eb',
           'FSPP':('FSPP',['right frontal superpeduncular protocerebrum','frontal superpeduncular protocerebrum','right fspp','fspp_r','fspp']),
           'fspp':('fspp',['left frontal superpeduncular protocerebrum','frontal superpeduncular protocerebrum','left fspp','fspp_l','fspp']),
           'IDFP':('IDFP',['right inferior dorsofrontal protocerebrum','inferior dorsofrontal protocerebrum','right idfp','idfp_r','idfp']),
           'idfp':('idfp',['left inferior dorsofrontal protocerebrum','inferior dorsofrontal protocerebrum','left idfp','idfp_l','idfp']),
           'IDLP':('IDLP',['right inferior dorsolateral protocerebrum','inferior dorsolateral protocerebrum','right idlp','idlp_r','idlp']),
           'idlp':('idlp',['left inferior dorsolateral protocerebrum','inferior dorsolateral protocerebrum','left idlp','idlp_l','idlp']),
           'LAT':('LAT',['right lat','lat_r','lat']),
           'lat':('lat',['left lat','lat_l','lat']),
           'LH':('LH',['right lateral horn','lateral horn','right lh','lh_r','lh']),
           'lh':('lh',['left lateral horn','lateral horn','left lh','lh_l','lh']),
           'LOB':('LOB',['right lobula','lobula','right lob','lob_r','lob','right lo','lo_r','lo']),
           'lob':('lob',['left lobula','lobula','left lob','lob_l','lob','left lo','lo_l','lo']),
           'LOP':('LOP',['right lobula plate','lobula plate','right lop','lop_r','lop']),
           'lop':('lop',['left lobula plate','lobula plate','left lop','lop_l','lop']),
           'MB':('MB',['right mushroom body','mushroom body','right mb','mb_r','mb']),
           'mb':('mb',['left mushroom body','mushroom body','left mb','mb_l','mb']),
           'MED':('MED',['right medulla','medulla','right med','med_r','med','right me','me_r','me']),
           'LAM':('LAM',['right lamina','lamina','right lam','lam_r','lam','right la','la_r','la']),
           'med':('med',['left medulla','medulla','left med','med_l','med','left me','me_l','me']),
           'NOD':('NOD',['right noduli','noduli','right nod','nod_r','nod','right no','no_r','no']),
           'nod':('nod',['left noduli','noduli','left nod','nod_l','nod','left no','no_l','no']),
           'OG':('OG',['right optic glomerulus','optic glomerulus','right og','og_r','og']),
           'og':('og',['right optic glomerulus','optic glomerulus','left og','og_l','og']),
           'OPTU':('OPTU',['right optic tubercle','optic tubercle','right optu','optu_r','optu']),
           'optu':('optu',['left optic tubercle','optic tubercle','left optu','optu_l','optu']),
           'PAN':('PAN',['right proximal antennal protocerebrum','proximal antennal protocerebrum','right pan','pan_r','pan']),
           'pan':('pan',['left proximal antennal protocerebrum','proximal antennal protocerebrum','left pan','pan_l','pan']),
           'PB':('PB',['right protocerebral bridge','protocerebral bridge','right pb','pb_r','pb','pcb','pcb_r','right pcb','left protocerebral bridge','left pb','pb_l','pcb_l','left pcb']),
           #'pb',
           'SDFP':('SDFP',['right superior dorsofrontal protocerebrum','superior dorsofrontal protocerebrum','right sdfp','sdfp_r','sdfp']),
           'sdfp':('sdfp',['left superior dorsofrontal protocerebrum','superior dorsofrontal protocerebrum','left sdfp','sdfp_l','sdfp']),
           'SOG':('SOG',['right subesophageal ganglion','subesophageal ganglion','right sog','sog_r','sog']),
           'sog':('sog',['left subesophageal ganglion','subesophageal ganglion','left sog','sog_l','sog']),
           'SPP':('SPP',['right superpeduncular protocerebrum','superpeduncular protocerebrum','right spp','spp_r','spp']),
           'spp':('spp',['left superpeduncular protocerebrum','superpeduncular protocerebrum','left spp','spp_l','spp']),
           'FB':('FB',['right fanshaped body','fanshaped body','right fb','fb_r','fb','left fanshaped body','left fb','fb_l']),
           #'fb',
           'VLP':('VLP',['right ventrolateral protocerebrum','ventrolateral protocerebrum','right vlp','vlp_r','vlp']),
           'vlp':('vlp',['left ventrolateral protocerebrum','ventrolateral protocerebrum','left vlp','vlp_l','vlp']),
           'VMP':('VMP',['right ventromedial protocerebrum','ventromedial protocerebrum','right vmp','vmp_r','vmp']),
           'unclear':('unclear',['unclear']),
           'vmp':('vmp',['left ventromedial protocerebrum','ventromedial protocerebrum','left vmp','vmp_l','vmp'])}
    
    neurotransmitter_map = {'Cha': 'acetylcholine',
                            'Gad': 'gaba',
                            'Tdc': 'octopamine',
                            'TH': 'dopamine',
                            'Trh': 'serotonin',
                            'VGlut': 'glutamate'}
                            
    def __init__(self, g_orient):
        self.logger = logging.getLogger('vl')
        self.g_orient = g_orient

        # Make sure OrientDB classes exist:
        #self.g_orient.create_all(Node.registry)
        #self.g_orient.create_all(Relationship.registry)

        # Get cluster IDs:
        self.cluster_ids = get_cluster_ids(self.g_orient.client)
        
    def load_neurons(self, file_name,morph_dir):
        ds_fc = self.g_orient.DataSources.query(name='FlyCircuit').first()
        if not ds_fc:
            ds_fc = self.g_orient.DataSources.create(name='FlyCircuit')
        
        with open(file_name, 'rb') as csvfile:
            reader = csv.reader(csvfile, delimiter=';',)
            i = -1
            for neuron in reader:
                i+=1
                print(i)
                # Process a neuron
                # Name Dendrites Axons  Total  Neuropil Locality
                #  0      1        2      3       4        5

                #if neuron[4]=='unclear': continue
                # Check if neuropil exists
                npl = self.g_orient.Neuropils.query(name=NTHULoader.neuropils[neuron[4]][0]).first()
                if not npl:
                    npl = self.g_orient.Neuropils.create(\
                                            name=NTHULoader.neuropils[neuron[4]][0],
                                            synonyms=NTHULoader.neuropils[neuron[4]][1])
                    self.logger.info('created node: {0}({1})'.format(npl.element_type, npl.name))

                locality = True if neuron[5]=='LN' else False
                # Create Neuron Node
                n = self.g_orient.Neurons.create(name=neuron[0], locality=locality)
                self.logger.info('created node: {0}({1})'.format(n.element_type, n.name))
                
                # Create Neurotransmitter Node if required
                nt = None
                neurotransmitter = []
                for key in NTHULoader.neurotransmitter_map:
                    if neuron[0].startswith(key):
                        neurotransmitter.append(NTHULoader.neurotransmitter_map[key])
                if neurotransmitter:
                    nt = self.g_orient.NeurotransmitterDatas.create(name=neuron[0], Transmitters=neurotransmitter)
                    self.logger.info('created node: {0}({1})'.format(nt.element_type, nt.name))
                    
                # Create Arborization Node
                dendrites = {c.split(':')[0]:int(c.split(':')[1]) for c in neuron[1].split(',')}
                axons = {c.split(':')[0]:int(c.split(':')[1]) for c in neuron[2].split(',')}
                arb = self.g_orient.ArborizationDatas.create(name=neuron[0], dendrites=dendrites, axons=axons)
                self.logger.info('created node: {0}({1})'.format(arb.element_type, arb.name))
                
                # Create Morphology Node
                df = load_swc('%s/%s.swc' % (morph_dir, neuron[0]))
                content = byteify(json.loads(df.to_json()))
                content = {}
                content['x'] = df['x'].tolist()
                content['y'] = df['y'].tolist()
                content['z'] = df['z'].tolist()
                content['r'] = df['r'].tolist()
                content['parent'] = df['parent'].tolist()
                content['identifier'] = df['identifier'].tolist()
                content['sample'] = df['sample'].tolist()
            
            
                content.update({'name': neuron[0]})
    
                nm = self.g_orient.client.record_create(self.cluster_ids['MorphologyData'][0],
                                                       {'@morphologydata': content})
                nm = self.g_orient.get_element(nm._rid)
            
                
                # Add content to new node:
                self.g_orient.client.command('update %s content %s' % \
                                         (nm._id, json.dumps(content)))

                
                self.logger.info('created node: {0}({1})'.format(nm.element_type, nm.name))
                
                # Connect nodes
                self.g_orient.Owns.create(npl, n)
                self.g_orient.HasData.create(n, arb)
                self.g_orient.HasData.create(n, nm)
                if nt:
                    self.g_orient.HasData.create(n, nt)
                    self.g_orient.Owns.create(ds_fc, n)
                self.g_orient.Owns.create(ds_fc, nm)
                self.g_orient.Owns.create(ds_fc, arb)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, stream=sys.stdout,
                        format='%(asctime)s %(name)s %(levelname)s %(message)s')
    g_orient = Graph(Config.from_url('/na_server','root', 'root', initial_drop=False,
                                     serialization_type=OrientSerialization.Binary))# set to True to erase the database
    g_orient.create_all(Node.registry)
    g_orient.create_all(Relationship.registry)

    
    vl = NTHULoader(g_orient)

    vl.load_neurons('all_female_neurons.txt','flycircuit1.1')
