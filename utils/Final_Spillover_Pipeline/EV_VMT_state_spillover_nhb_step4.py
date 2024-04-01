#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 17 09:46:14 2023

@author: xiaodanxu
"""

import pandas as pd
import os
import numpy as np
from pandas import read_csv
import matplotlib.pyplot as plt
import seaborn as sns
from pandas.api.types import CategoricalDtype
import scipy.stats as s

plt.style.use('ggplot')

# set working directory
#path_to_prj = '/Users/xiaodanxu/Library/CloudStorage/GoogleDrive-arielinseu@gmail.com/My Drive/GEMS/BILD-AQ/data'
path_to_prj = os.getcwd()
os.chdir(path_to_prj)

# set current state for analysis
selected_state = 'UT'

# path to model inputs
nhb_vmt_generation_file = 'Input/spillover/NHTS_nonhome_VMT_generation_spillover.csv'
nhb_vmt_assignment_file = 'Input/spillover/nonhome_spillover_distribution_factors.csv'
hb_vmt_result_file ='Output/' + selected_state + '/destination_daily_vmt_spillover.csv' 
micro_geotype_lookup_file = 'ccst_geoid_key_tranps_geo_with_imputation.csv'
network_attribute_file = 'Network/combined/network_attributes.csv'
# load inputs files
nhb_vmt_generation_rate = read_csv(nhb_vmt_generation_file)
nhb_vmt_assignment_rate = read_csv(nhb_vmt_assignment_file)
hb_vmt_by_od = read_csv(hb_vmt_result_file)
micro_geotype_label = read_csv(micro_geotype_lookup_file)
network_attribute_df = read_csv(network_attribute_file)

# <codecell>

##### step 1 --- calculate non-home-based total VMT #####

micro_geotype_label_short = micro_geotype_label[['GEOID', 'geotype', 'microtype', 'st_code']]
hb_vmt_by_od_with_label = pd.merge(hb_vmt_by_od, micro_geotype_label_short,
                        left_on = 'destination', 
                        right_on = 'GEOID', how = 'left')
hb_vmt_by_od_with_label = hb_vmt_by_od_with_label.drop(columns =['GEOID'])
hb_vmt_by_od_with_label = pd.merge(hb_vmt_by_od_with_label, 
                                   nhb_vmt_generation_rate,
                                   on = ['geotype', 'microtype'],
                                   how = 'left')
hb_vmt_by_od_with_label.loc[:, 'nhb_VMT'] = \
    hb_vmt_by_od_with_label.loc[:, 'VMT'] * \
        hb_vmt_by_od_with_label.loc[:, 'nhb_fraction_VMT']
print("NHB VMT: ", hb_vmt_by_od_with_label.loc[:, 'nhb_VMT'].sum())

# <codecell>

##### step 2 --- distribute non-home-based VMT #####
network_attribute_short = network_attribute_df[['GEOID', 'lm_all_tract']]
list_of_states = hb_vmt_by_od_with_label.st_code.unique()
distance_bin = [-1, 5, 10, 20, 50, 100, 300]
distance_bin_label = ['bin1', 'bin2', 'bin3', 'bin4', 'bin5', 'bin6']
print("Distribute non-home based VMT to: ", list_of_states)
nhb_vmt_by_tract_out = None

vmt_grouping_var = ['home_GEOID', 'home_geotype', 'home_microtype', 
                    'populationGroupType', 'destination']

# allocate nhb VMT to out-of-state through tracts
for st in list_of_states:
    
    # select nhb VMT by state
    nhb_vmt_selected = \
    hb_vmt_by_od_with_label.loc[hb_vmt_by_od_with_label['st_code'] == st]
    nhb_vmt_selected = \
        nhb_vmt_selected.drop(columns = ['VMT', 'geotype', 'microtype', 
                                         'st_code', 'nhb_fraction_VMT'])
    # print(nhb_vmt_selected.columns)
    print('selected nhb VMT to assign in state ' + st)
    print(nhb_vmt_selected.nhb_VMT.sum())
    
    # vmt_to_check= \
    #     nhb_vmt_selected.groupby(vmt_grouping_var)['nhb_VMT'].mean()
    # print(vmt_to_check.sum())   
    # load distance matrix for selected states
    dist_matrix = \
    read_csv('Network/combined/distance_matrix_by_tracts_' + st + '.csv')
    dist_matrix = pd.merge(dist_matrix,
                           micro_geotype_label_short,
                           left_on = 'destination',
                           right_on = 'GEOID',
                           how = 'left')
    
    dist_matrix = dist_matrix.drop(columns = ['GEOID'])
    dist_matrix.loc[:, 'dist_bin'] = pd.cut(dist_matrix.loc[:, 'distance'],
                                            bins = distance_bin,
                                            labels = distance_bin_label,
                                            ordered = False)
    dist_matrix = dist_matrix.loc[dist_matrix['st_code'] == st]
    dist_matrix = dist_matrix.drop(columns = ['Unnamed: 0'])
    
    # rename columns before join
    dist_matrix = \
        dist_matrix.rename(columns={'origin': 'destination', 
                                    'destination': 'thru_GEOID',
                                    'geotype': 'thru_geotype',
                                    'microtype': 'thru_microtype'})
    nhb_vmt_selected = \
        pd.merge(nhb_vmt_selected, dist_matrix,
                 on = 'destination',
                 how = 'left')
    # vmt_to_check= \
    #     nhb_vmt_selected.groupby(['home_GEOID', 'destination'])['nhb_VMT'].mean()
    # print(vmt_to_check.sum())
    nhb_vmt_selected = \
        pd.merge(nhb_vmt_selected, nhb_vmt_assignment_rate,
                 left_on = ['thru_geotype', 'thru_microtype', 'dist_bin'],
                 right_on = ['geotype', 'microtype', 'dist_bin'],
                 how = 'left')
    nhb_vmt_selected = \
        pd.merge(nhb_vmt_selected, network_attribute_short,
                 left_on = ['thru_GEOID'],
                 right_on = ['GEOID'],
                 how = 'left')
    nhb_vmt_selected = \
        nhb_vmt_selected.drop(columns = ['geotype', 'microtype', 'GEOID'])
    nhb_vmt_selected.loc[:, 'vmt_fraction'] *= \
        nhb_vmt_selected.loc[:, 'lm_all_tract']
    nhb_vmt_selected.loc[:, 'vmt_fraction'] = \
        nhb_vmt_selected.loc[:, 'vmt_fraction'] / \
            nhb_vmt_selected.groupby(vmt_grouping_var)['vmt_fraction'].transform('sum')
    nhb_vmt_selected.loc[:, 'nhb_VMT'] *= \
        nhb_vmt_selected.loc[:, 'vmt_fraction'] 
    nhb_vmt_selected.loc[:, 'nhb_VMT'] = \
        np.round(nhb_vmt_selected.loc[:, 'nhb_VMT'], 0)
    nhb_vmt_selected = nhb_vmt_selected.loc[nhb_vmt_selected['nhb_VMT']> 0]
    print('total nhb VMT after assignment:')
    print(nhb_vmt_selected.loc[:, 'nhb_VMT'].sum())
    print('-------------------------------')
    nhb_vmt_selected = \
        nhb_vmt_selected.drop(columns = ['distance', 'dist_bin', 'vmt_fraction', 'lm_all_tract'])
    nhb_vmt_by_tract_out = pd.concat([nhb_vmt_by_tract_out, nhb_vmt_selected])
    # break
nhb_vmt_by_tract_out.to_csv('Output/' + selected_state + '/nhb_vmt_spillover.csv')

