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


path_to_prj = '/Volumes/GoogleDrive/My Drive/GEMS/BILD-AQ/data'
os.chdir(path_to_prj)

# load input
NHTS_population_file = 'Input/NHTS_population.csv'
ACS_population_file = 'Input/ACS_household_by_tracts.csv'
hb_trip_generation_file = 'Input/NHTS_home_based_trips_CA.csv'
nhb_trip_fraction_file = 'Input/NHTS_nonhome_fraction_CA.csv'
nhb_trip_generation_file = 'Input/NHTS_nonhome_based_trips_CA.csv'
ccst_lookup_file = 'ccst_geoid_key_tranps_geo_with_imputation.csv'

NHTS_population = read_csv(NHTS_population_file, sep = ',')
ACS_population = read_csv(ACS_population_file, sep = ',')
hb_trip_generation = read_csv(hb_trip_generation_file, sep = ',')
nhb_trip_fraction = read_csv(nhb_trip_fraction_file, sep = ',')
nhb_trip_generation = read_csv(nhb_trip_generation_file, sep = ',')
ccst_lookup = read_csv(ccst_lookup_file, sep = ',')

# <codecell>

# combine population data
# process ACS population
ccst_lookup_ca = ccst_lookup.loc[ccst_lookup['st_code'] == 'CA']

ACS_population_ca = pd.merge(ACS_population, ccst_lookup_ca, 
                             on = 'GEOID', how = 'inner')
ACS_population_ca = pd.melt(ACS_population_ca, id_vars=['GEOID', 'geotype', 'microtype'], 
                            value_vars=['low-income', 'medium-income', 'high-income'],
                            var_name='populationGroupType', value_name='ACS_households')

# process NHTS population
NHTS_population_ca = NHTS_population.loc[NHTS_population['st_code'] == 'CA']
joint_population_ca = pd.merge(ACS_population_ca, NHTS_population_ca, 
                      on = ['geotype', 'microtype', 'populationGroupType'], 
                      how = 'left')

joint_population_ca = joint_population_ca[['GEOID', 'geotype', 'microtype', 'populationGroupType',
                                           'ACS_households', 'NHTS_households']]
joint_population_ca = joint_population_ca.fillna(0)
print(joint_population_ca.head(5))

# <codecell>

# process home-based trips
trip_generation_ca = pd.merge(joint_population_ca, hb_trip_generation,
                              on = ['geotype', 'microtype', 'populationGroupType'],
                              how = 'left')

trip_generation_ca.loc[:, 'NHTS_trip_rate'] = trip_generation_ca.loc[:, 'NHTS_trips'] / trip_generation_ca.loc[:, 'NHTS_households']
trip_generation_ca.loc[:, 'TripGeneration'] = trip_generation_ca.loc[:, 'NHTS_trip_rate'] * trip_generation_ca.loc[:, 'ACS_households'] 
trip_generation_ca = trip_generation_ca.fillna(0)
trip_generation_ca = trip_generation_ca.loc[trip_generation_ca['TripGeneration'] > 0 ]
trip_generation_ca.columns = ['GEOID', 'home_geotype', 'home_microtype',
                              'populationGroupType', 'ACS_households', 'NHTS_households',
                              'dest_geotype', 'dest_microtype',
                              'TripType', 'TripPurposeID', 'DistanceBinID', 
                              'NHTS_TripGeneration', 'Rates_per_hh', 'TripGeneration']
# <codecell>

# process non-home based trips
# trip_generation_ca_home_origin = trip_generation_ca.loc[trip_generation_ca['TripType'] == 'origin']
# agg_trip_generation_ca = trip_generation_ca.groupby(['GEOID', 'home_geotype', 
#                                                      'home_microtype', 'dest_geotype', 
#                                                      'dest_microtype','DistanceBinID'])[['TripGeneration']].sum()

nhb_trip_generation_ca = pd.merge(trip_generation_ca, nhb_trip_fraction, 
                                  left_on = ['home_geotype', 'home_microtype', 'dest_geotype', 'dest_microtype','DistanceBinID'],
                                  right_on = ['geotype', 'microtype', 'o_geotype', 'o_microtype', 'distanceBin'],
                                  how = 'left')


nhb_trip_generation_ca.loc[:, 'NHBTripGeneration'] = nhb_trip_generation_ca.loc[:, 'nhb_fraction'] * \
nhb_trip_generation_ca.loc[:, 'TripGeneration'] 

nhb_trip_generation_ca = nhb_trip_generation_ca[['GEOID', 'home_geotype', 'home_microtype',
                              'populationGroupType', 'ACS_households', 'NHTS_households',
                              'dest_geotype', 'dest_microtype',
                              'TripType', 'TripPurposeID', 'DistanceBinID', 
                              'NHTS_TripGeneration', 'Rates_per_hh', 'TripGeneration', 'NHBTripGeneration']]
print(nhb_trip_generation_ca.TripGeneration.sum())  
print(nhb_trip_generation_ca.NHBTripGeneration.sum())   
nhb_trip_generation_ca.to_csv('Input/TripGeneration_CA.csv')

# 
# trip_generation_ca.to_csv('Input/TripGeneration_CA.csv', index = False)
# print(trip_generation_ca.head(5))

