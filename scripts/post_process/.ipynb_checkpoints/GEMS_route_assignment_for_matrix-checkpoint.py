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

selected_state = 'CA'
# load inputs
od_trip_file = 'NHTS_car_trips_CA.csv'
od_trips = read_csv('Network/' + selected_state + '/' + od_trip_file, low_memory=False)

# nhb_vmt_file = 'NHTS_nonhome_VMT_fraction_CA.csv'
# nhb_trip_vmt_fraction = read_csv('Input/' + nhb_vmt_file)

list_of_routes = os.listdir('Input/' + selected_state + '/route')

# EV_penetration_file = 'EV_penetration.csv'
# EV_penetration = read_csv('Input/' + EV_penetration_file)

ccst_lookup_file = 'ccst_geoid_key_tranps_geo_with_imputation.csv'
ccst_lookup = read_csv(ccst_lookup_file, sep = ',')

# <codecell>

# check OD data

od_trips['trip_indx'] = od_trips.reset_index().index + 1
output_attrs = ['trip_indx', 'o_geoid', 'd_geoid', 'trpmiles', 'wtperfin']
time_window_lb = 16
time_window_ub = 20

od_trips = od_trips.loc[od_trips['start hour'] >= time_window_lb]
od_trips = od_trips.loc[od_trips['start hour'] < time_window_ub]
cut_off = od_trips['trpmiles'].quantile(0.99)
od_trips = od_trips[output_attrs]
od_trips = od_trips.loc[od_trips['trpmiles'] <= cut_off]
od_trips = od_trips.dropna()
od_trips[['o_geoid', 'd_geoid']] = od_trips[['o_geoid', 'd_geoid']].astype(int)
od_trips['OD'] = od_trips['o_geoid'].astype(str) + '_' + od_trips['d_geoid'].astype(str)
od_trips.to_csv('Output/' + selected_state + '/nhts_od_pairs_PM.csv', index = False)
print(len(od_trips))
print(od_trips.head(5))


# <codecell>

# assign route to trips
meter_to_mile = 0.000621371


distance_bins = [0, 5, 10, 20, 10000]
distance_bin_labels = ['1', '2', '3', '4']

selected_route_combined = None
ccst_lookup_short = ccst_lookup[['GEOID', 'FID']]  

route_attr = ['GEOID', 'o_geoid', 'd_geoid', 'Length', 'distance', 'trip_indx', 'FID']
route_colnames = ['thru_geoid', 'o_geoid', 'd_geoid', 'thru_length', 'distance', 'trip_id', 'thru_fid']
for route in list_of_routes:
    if route == '.DS_Store':
        continue
    print('processing route ' + route)
    route_df = read_csv('Input/' + selected_state + '/route/' + route)
    route_df = route_df.loc[route_df['Length'] > 0]
    route_df = pd.merge(route_df, ccst_lookup_short, on = 'GEOID', how = 'left')
    route_df['destination'] = route_df['destination'].astype(int)
    # print(route_df.columns)
    trip_to_route = pd.merge(od_trips, route_df,
                          left_on = ['o_geoid', 'd_geoid'],
                          right_on = ['source', 'destination'],
                          how = 'inner')
    trip_to_route = trip_to_route[route_attr]
    trip_to_route.columns = route_colnames
    trip_to_route['order'] = trip_to_route.groupby('trip_id').cumcount() + 1
    trip_to_route = pd.merge(trip_to_route, ccst_lookup_short, 
                             left_on = 'o_geoid', right_on = 'GEOID', how = 'left')
    trip_to_route = trip_to_route.rename(columns={"FID": "o_fid"})
    trip_to_route = trip_to_route.drop(columns=['GEOID'])
    
    trip_to_route = pd.merge(trip_to_route, ccst_lookup_short, 
                             left_on = 'd_geoid', right_on = 'GEOID', how = 'left')
    trip_to_route = trip_to_route.rename(columns={"FID": "d_fid"})
    trip_to_route = trip_to_route.drop(columns=['GEOID'])
    trip_to_route.loc[:, 'thru_length_ccst'] = \
        trip_to_route.groupby(['trip_id', 'thru_fid'])['thru_length'].transform('sum')
        
    selected_route_combined = pd.concat([selected_route_combined, trip_to_route])
    
    trip_to_route.loc[:, 'OD'] = trip_to_route['o_geoid'].astype(str) + '_' + trip_to_route['d_geoid'].astype(str)
    unique_ODs = trip_to_route.OD.unique()
    print('ODs being paired ' + str(len(unique_ODs)))
    od_trips = od_trips.loc[~ od_trips['OD'].isin(unique_ODs)] # remove trips with route assigned
    # trip_to_route = trip_to_route.dropna()

  

# <codecell>

for route in list_of_routes:
    if route == '.DS_Store':
        continue
    print('processing route ' + route)
    route_df = read_csv('Input/' + selected_state + '/route/' + route)
    route_df = route_df.rename(columns = {'source': 'destination', 'destination': 'source'})
    route_df = route_df.loc[route_df['Length'] > 0]
    route_df = pd.merge(route_df, ccst_lookup_short, on = 'GEOID', how = 'left')
    route_df['destination'] = route_df['destination'].astype(int)
    # print(route_df.columns)
    trip_to_route = pd.merge(od_trips, route_df,
                          left_on = ['o_geoid', 'd_geoid'],
                          right_on = ['source', 'destination'],
                          how = 'inner')
    trip_to_route = trip_to_route[route_attr]
    trip_to_route.columns = route_colnames
    trip_to_route['order'] = trip_to_route.groupby('trip_id').cumcount() + 1
    trip_to_route = pd.merge(trip_to_route, ccst_lookup_short, 
                             left_on = 'o_geoid', right_on = 'GEOID', how = 'left')
    trip_to_route = trip_to_route.rename(columns={"FID": "o_fid"})
    trip_to_route = trip_to_route.drop(columns=['GEOID'])
    
    trip_to_route = pd.merge(trip_to_route, ccst_lookup_short, 
                             left_on = 'd_geoid', right_on = 'GEOID', how = 'left')
    trip_to_route = trip_to_route.rename(columns={"FID": "d_fid"})
    trip_to_route = trip_to_route.drop(columns=['GEOID'])
    trip_to_route.loc[:, 'thru_length_ccst'] = \
        trip_to_route.groupby(['trip_id', 'thru_fid'])['thru_length'].transform('sum')
    selected_route_combined = pd.concat([selected_route_combined, trip_to_route])
    trip_to_route.loc[:, 'OD'] = trip_to_route['o_geoid'].astype(str) + '_' + trip_to_route['d_geoid'].astype(str)
    unique_ODs = trip_to_route.OD.unique()
    print('ODs being paired ' + str(len(unique_ODs)))
    od_trips = od_trips.loc[~ od_trips['OD'].isin(unique_ODs)] # remove trips with route assigned
    # break



# <codecell>
selected_route_combined.to_csv('Output/' + selected_state + '/nhts_thru_length_order_route_PM.csv', index = False)
# find O-Ds that need impute
# od_trips_impute = od_trips.groupby(['home_GEOID', 'destination', 'OD'])[['VMT']].sum()
# od_trips_impute = od_trips_impute.reset_index()
# print(od_trips_impute.VMT.sum())
# od_trips_impute = od_trips_impute.drop_duplicates(subset = 'OD')
# od_trips_impute = od_trips_impute[od_trips_impute['home_GEOID'] != od_trips_impute['destination']]
# od_trips_impute = od_trips_impute.sort_values(by = 'VMT', ascending = False)
# od_trips_impute.loc[:, 'fraction'] = od_trips_impute.loc[:, 'VMT'] / od_trips_impute.loc[:, 'VMT'].sum()
# od_trips_impute.loc[:, 'fraction'] =od_trips_impute.loc[:, 'fraction'].cumsum()
# od_trips_impute = od_trips_impute[od_trips_impute['fraction'] <= 0.95]
# print(od_trips_impute.VMT.sum())
# od_trips_impute.to_csv('Network/OD_to_impute.csv')

   
