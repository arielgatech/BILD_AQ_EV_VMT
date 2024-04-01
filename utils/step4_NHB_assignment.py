#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 10 14:50:05 2022

@author: xiaodanxu
"""

import pandas as pd
import os
import numpy as np
from pandas import read_csv
import matplotlib.pyplot as plt
import seaborn as sns

plt.style.use('ggplot')

path_to_prj = '/Users/xiaodanxu/Library/CloudStorage/GoogleDrive-arielinseu@gmail.com/My Drive/GEMS/BILD-AQ/data'
os.chdir(path_to_prj)

selected_state = 'NM'

# load inputs
nhb_vmt_file = 'NHTS_nonhome_VMT_fraction_' + selected_state + '.csv'
nhb_trip_vmt_fraction = read_csv('Input/' + selected_state + '/' + nhb_vmt_file)

VMT_to_dest_file = 'destination_daily_vmt_by_tracts.csv'
VMT_to_destination = read_csv('Output/' + selected_state + '/' + VMT_to_dest_file)

OD_summary_file = 'OD_summary_with_routed_distance.csv'
OD_summary = read_csv('Output/' + selected_state + '/' + OD_summary_file)

ccst_lookup_file = 'ccst_geoid_key_tranps_geo_with_imputation.csv'
ccst_lookup = read_csv(ccst_lookup_file, sep = ',')

# <codecell>

# total non-home VMT linked to each destination
print('total hb VMT is:')
print(VMT_to_destination['VMT'].sum())
nhb_VMT_by_destination = pd.merge(VMT_to_destination, nhb_trip_vmt_fraction,
                                  left_on = ['dest_geotype', 'dest_microtype', 'DistanceBinID'],
                                  right_on = ['o_geotype', 'o_microtype', 'DistanceBinID'], how = 'left')

nhb_VMT_by_destination.loc[:, 'VMT'] *= nhb_VMT_by_destination.loc[:, 'nhb_fraction_VMT']
    
# print(nhb_VMT_by_destination.head(5))  
# print(nhb_VMT_by_destination.loc[:, 'VMT'].sum())
# print(VMT_to_destination.VMT.sum())  


# <codecell>
# assign home tract to nhb VMTs
grouping_var = ['GEOID', 'geotype', 'microtype', 'destination', 'dest_geotype', 'dest_microtype']
nhb_VMT_by_destination = nhb_VMT_by_destination.groupby(grouping_var)[['VMT']].sum()
nhb_VMT_by_destination = nhb_VMT_by_destination.reset_index()
destination_list = nhb_VMT_by_destination.destination.unique()

OD_summary.loc[:, 'home_VMT'] = OD_summary.loc[:, 'distance'] * OD_summary.loc[:, 'TripGeneration']
nhb_VMT_by_home_out = None
prob_cut = 0.01

grouping_var_2 = ['GEOID', 'destination', 'dest_geotype', 'dest_microtype']

dest_chunk = np.array_split(destination_list, 100)
i = 0
for dest in dest_chunk:
    # print('processing trips to destination chunk ' + str(i))
    nhb_VMT = nhb_VMT_by_destination.loc[nhb_VMT_by_destination['destination'].isin(dest)]
    nhb_VMT = pd.merge(nhb_VMT, OD_summary, 
                       on = ['destination', 'dest_geotype', 'dest_microtype'], how = 'left')
    nhb_VMT.loc[:, 'VMT_fraction'] = nhb_VMT.loc[:, "home_VMT"] / \
        nhb_VMT.groupby(grouping_var_2)["home_VMT"].transform("sum") 
    nhb_VMT = nhb_VMT.loc[nhb_VMT['VMT_fraction'] > prob_cut]  
    nhb_VMT.loc[:, 'VMT_fraction'] = nhb_VMT.loc[:, "VMT_fraction"] / \
        nhb_VMT.groupby(grouping_var_2)["VMT_fraction"].transform("sum") 
    nhb_VMT.loc[:, 'VMT'] *= nhb_VMT.loc[:, 'VMT_fraction']
    nhb_VMT = nhb_VMT.groupby(['GEOID', 'geotype', 'microtype', 'home_GEOID', 'home_geotype', 'home_microtype',
       'populationGroupType'])[['VMT']].sum()
    nhb_VMT = nhb_VMT.reset_index()
    nhb_VMT_by_home_out = pd.concat([nhb_VMT_by_home_out, nhb_VMT])
    i += 1
    # break
# <codecell>

nhb_VMT_by_home_out = nhb_VMT_by_home_out.groupby(['GEOID', 'geotype', 'microtype', 'home_GEOID', 'home_geotype', 'home_microtype',
       'populationGroupType'])[['VMT']].sum()

nhb_VMT_by_home_out = nhb_VMT_by_home_out.reset_index()
print('total non-home-based VMT is:')
print(np.round(nhb_VMT_by_home_out['VMT'].sum(),1))
nhb_VMT_by_home_out.to_csv('Output/' + selected_state + '/' + 'nonhome_daily_vmt_by_tracts.csv', index = False)