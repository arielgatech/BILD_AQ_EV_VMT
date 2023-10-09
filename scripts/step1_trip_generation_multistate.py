#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 31 09:10:06 2022

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

selected_state = 'AZ'
# states_in_cd = ['WA', 'OR', 'AK', 'HI']
# load input
NHTS_population_file = 'Input/NHTS_population.csv'
ACS_population_file = 'Input/ACS_household_by_tracts.csv'
ccst_lookup_file = 'ccst_geoid_key_tranps_geo_with_imputation.csv'
state_region_file = 'Input/NHTS_region_state.csv'

# load state-specific inputs
hb_trip_generation_file = 'Input/' + selected_state + \
    '/NHTS_home_based_trips_' + selected_state + '.csv'
# nhb_trip_fraction_file = 'Input/NHTS_nonhome_fraction_CA.csv'
# nhb_trip_generation_file = 'Input/NHTS_nonhome_based_trips_CA.csv'


NHTS_population = read_csv(NHTS_population_file, sep = ',')
ACS_population = read_csv(ACS_population_file, sep = ',')
hb_trip_generation = read_csv(hb_trip_generation_file, sep = ',')
state_region_lookup = read_csv(state_region_file)
# nhb_trip_fraction = read_csv(nhb_trip_fraction_file, sep = ',')
# nhb_trip_generation = read_csv(nhb_trip_generation_file, sep = ',')
ccst_lookup = read_csv(ccst_lookup_file, sep = ',')

# <codecell>

# combine population data
# process ACS population
ccst_lookup_state = ccst_lookup.loc[ccst_lookup['st_code'] == selected_state]

ACS_population_state = pd.merge(ACS_population, ccst_lookup_state, 
                             on = 'GEOID', how = 'inner')
ACS_population_state = pd.melt(ACS_population_state, id_vars=['GEOID', 'geotype', 'microtype'], 
                            value_vars=['low-income', 'medium-income', 'high-income'],
                            var_name='populationGroupType', value_name='ACS_households')

ACS_population_by_inc = ACS_population_state.groupby(['populationGroupType'])[['ACS_households']].sum()
ACS_population_by_inc = ACS_population_by_inc.reset_index()
ACS_population_by_inc.loc[:, 'pop_fraction'] = \
ACS_population_by_inc['ACS_households'] / ACS_population_by_inc['ACS_households'].sum()
ACS_population_by_inc.to_csv('Input/' + selected_state + '/ACS_population_'+ selected_state + '.csv')
# <codecell>
# process NHTS population

# NHTS Population needs to come from multiple states (census division)
region_code = str(state_region_lookup.loc[state_region_lookup['state'] == selected_state, 'region'].tolist()[0])
    
states_in_cd = \
    state_region_lookup.loc[state_region_lookup['region'] == region_code, 'state'].unique()
print(states_in_cd)
NHTS_population_state = NHTS_population.loc[NHTS_population['st_code'].isin(states_in_cd)]
NHTS_population_state = NHTS_population_state.groupby(['geotype', 'microtype', 'populationGroupType'])[['NHTS_households']].sum()
NHTS_population_state = NHTS_population_state.reset_index()
joint_population_state = pd.merge(ACS_population_state, NHTS_population_state, 
                      on = ['geotype', 'microtype', 'populationGroupType'], 
                      how = 'left')

joint_population_state = joint_population_state[['GEOID', 'geotype', 'microtype', 'populationGroupType',
                                           'ACS_households', 'NHTS_households']]
joint_population_state = joint_population_state.fillna(0)
print(joint_population_state.head(5))

# <codecell>

# process home-based trips
trip_generation_state = pd.merge(joint_population_state, hb_trip_generation,
                              on = ['geotype', 'microtype', 'populationGroupType'],
                              how = 'left')

trip_generation_state.loc[:, 'NHTS_trip_rate'] = trip_generation_state.loc[:, 'NHTS_trips'] / trip_generation_state.loc[:, 'NHTS_households']
trip_generation_state.loc[:, 'TripGeneration'] = trip_generation_state.loc[:, 'NHTS_trip_rate'] * trip_generation_state.loc[:, 'ACS_households'] 
trip_generation_state = trip_generation_state.fillna(0)
trip_generation_state = trip_generation_state.loc[trip_generation_state['TripGeneration'] > 0 ]
trip_generation_state.columns = ['GEOID', 'home_geotype', 'home_microtype',
                              'populationGroupType', 'ACS_households', 'NHTS_households',
                              'dest_geotype', 'dest_microtype',
                              'TripType', 'TripPurposeID', 'DistanceBinID', 
                              'NHTS_TripGeneration', 'Rates_per_hh', 'TripGeneration']

trip_generation_state.to_csv('Input/' + selected_state + '/TripGeneration_'+ selected_state + '.csv')
# <codecell>

# process non-home based trips
# trip_generation_ca_home_origin = trip_generation_ca.loc[trip_generation_ca['TripType'] == 'origin']
# agg_trip_generation_ca = trip_generation_ca.groupby(['GEOID', 'home_geotype', 
#                                                      'home_microtype', 'dest_geotype', 
#                                                      'dest_microtype','DistanceBinID'])[['TripGeneration']].sum()

# nhb_trip_generation_ca = pd.merge(trip_generation_ca, nhb_trip_fraction, 
#                                   left_on = ['home_geotype', 'home_microtype', 'dest_geotype', 'dest_microtype','DistanceBinID'],
#                                   right_on = ['geotype', 'microtype', 'o_geotype', 'o_microtype', 'distanceBin'],
#                                   how = 'left')


# nhb_trip_generation_ca.loc[:, 'NHBTripGeneration'] = nhb_trip_generation_ca.loc[:, 'nhb_fraction'] * \
# nhb_trip_generation_ca.loc[:, 'TripGeneration'] 

# nhb_trip_generation_ca = nhb_trip_generation_ca[['GEOID', 'home_geotype', 'home_microtype',
#                               'populationGroupType', 'ACS_households', 'NHTS_households',
#                               'dest_geotype', 'dest_microtype',
#                               'TripType', 'TripPurposeID', 'DistanceBinID', 
#                               'NHTS_TripGeneration', 'Rates_per_hh', 'TripGeneration', 'NHBTripGeneration']]
# print(nhb_trip_generation_ca.TripGeneration.sum())  
# print(nhb_trip_generation_ca.NHBTripGeneration.sum())   


# 
# trip_generation_ca.to_csv('Input/TripGeneration_CA.csv', index = False)
# print(trip_generation_ca.head(5))

