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
od_trip_file = 'OD_home_based_trips_by_tract.csv.zip'
od_trips = read_csv('Output/' + selected_state + '/' + od_trip_file)

# nhb_vmt_file = 'NHTS_nonhome_VMT_fraction_CA.csv'
# nhb_trip_vmt_fraction = read_csv('Input/' + nhb_vmt_file)

list_of_routes = os.listdir('Input/' + selected_state + '/route')

# EV_penetration_file = 'EV_penetration.csv'
# EV_penetration = read_csv('Input/' + EV_penetration_file)

ccst_lookup_file = 'ccst_geoid_key_tranps_geo_with_imputation.csv'
ccst_lookup = read_csv(ccst_lookup_file, sep = ',')

# <codecell>

# check OD data
grouping_var_0 = ['GEOID', 'home_geotype', 'home_microtype',
       'populationGroupType', 'destination',
       'dest_geotype', 'dest_microtype']
od_trips = od_trips.groupby(grouping_var_0).agg({'TripGeneration' : np.sum, 
                                                 'distance_mile' : np.mean})
od_trips = od_trips.reset_index()
od_trips.loc[:, 'VMT'] = od_trips.loc[:, 'distance_mile'] * od_trips.loc[:, 'TripGeneration']
print(od_trips.loc[:, 'TripGeneration'].sum())
print('total VMT before route assignment:')
print(od_trips.loc[:, 'VMT'].sum())
print(len(od_trips))
od_trips = od_trips.rename(columns = {'GEOID': 'home_GEOID', 'distance_mile': 'INRIX_distance'})
# od_trips = od_trips.drop(columns=['distance'])
od_trips['destination'] = od_trips['destination'].astype(int)

od_trips['OD'] = od_trips['home_GEOID'].astype(str) + '_' + od_trips['destination'].astype(str)

ccst_lookup_short = ccst_lookup[['GEOID', 'geotype', 'microtype']]
# <codecell>

# assign route to trips
meter_to_mile = 0.000621371

VMT_to_home_out = None
VMT_to_destination_out = None
OD_summary_out = None

distance_bins = [0, 5, 10, 20, 10000]
distance_bin_labels = ['1', '2', '3', '4']

grouping_var = ['GEOID', 'geotype', 'microtype', 
                'home_GEOID', 'home_geotype', 'home_microtype', 'populationGroupType'] # through tract + home attributes

grouping_var_2 = ['GEOID', 'geotype', 'microtype', 'destination', 
                  'dest_geotype', 'dest_microtype', 'DistanceBinID'] # through tract + home attributes

grouping_var_3 = ['home_GEOID', 'home_geotype', 'home_microtype', 
                  'populationGroupType', 'destination', 'dest_geotype', 'dest_microtype']
    
for route in list_of_routes:
    if route == '.DS_Store':
        continue
    print('processing route ' + route)
    route_df = read_csv('Input/' + selected_state + '/route/' + route)
    # print(len(route_df))
    route_df = route_df.loc[route_df['Length'] > 0]
    route_df = pd.merge(route_df, ccst_lookup_short, on = 'GEOID', how = 'left')
    # print(len(route_df))
    route_df['destination'] = route_df['destination'].astype(int)
    # print(route_df.columns)
    trip_to_route = pd.merge(od_trips, route_df,
                          left_on = ['home_GEOID', 'destination'],
                          right_on = ['source', 'destination'],
                          how = 'inner')
    # print(len(trip_to_route))
    sample_origin_tract = trip_to_route['GEOID'].unique()[0]
    # trip_to_route = trip_to_route.dropna()
    trip_to_route.loc[:, 'distance'] *= meter_to_mile
    trip_to_route.loc[:, 'VMT'] = trip_to_route.loc[:, 'Length'] * trip_to_route.loc[:, 'TripGeneration'] * \
    meter_to_mile
    trip_to_route.loc[:, 'DistanceBinID'] = pd.cut(trip_to_route.loc[:, 'distance'], distance_bins, 
                                                      labels = distance_bin_labels, ordered = False)
    trip_to_route.loc[:, 'DistanceBinID'] = trip_to_route.loc[:, 'DistanceBinID'].astype(str)
    trip_to_route.loc[:, 'DistanceBinID'] = trip_to_route.loc[:, 'DistanceBinID'].astype(int)
    trip_to_route.loc[:, 'VMT'] = np.round(trip_to_route.loc[:, 'VMT'], 0)
    trip_to_route = trip_to_route.loc[trip_to_route['VMT'] > 0]
    # print(len(trip_to_route))
    VMT_to_home = trip_to_route.groupby(grouping_var)[['VMT']].sum()
    VMT_to_home = VMT_to_home.reset_index() 
    # print(len(VMT_to_home))
    # VMT_to_home['VMT'] = np.round(VMT_to_home['VMT'], 0)
    # VMT_to_home = VMT_to_home[VMT_to_home['VMT'] > 0]
    
    VMT_to_home_out = pd.concat([VMT_to_home_out, VMT_to_home])
    

    VMT_to_destination = trip_to_route.groupby(grouping_var_2)[['VMT']].sum()
    VMT_to_destination = VMT_to_destination.reset_index() 
    # print(len(VMT_to_destination))    
    VMT_to_destination_out = pd.concat([VMT_to_destination_out, VMT_to_destination])
    

    OD_summary = trip_to_route.groupby(grouping_var_3)[['INRIX_distance', 'distance', 'TripGeneration']].mean()
    OD_summary = OD_summary.reset_index()
    # print(len(OD_summary))    
    OD_summary.loc[:, 'OD'] = OD_summary['home_GEOID'].astype(str) + '_' + OD_summary['destination'].astype(str)
    
    OD_summary_out = pd.concat([OD_summary_out, OD_summary])
    unique_ODs = OD_summary.OD.unique()
    od_trips = od_trips.loc[~ od_trips['OD'].isin(unique_ODs)] # remove trips with route assigned
    # print('total trip VMT = ' + str(VMT_to_home.loc[:, 'VMT'].sum()))  
    # break
    #                
    # grouping_var = ['GEOID', 'home_GEOID', 'home_geotype', 'home_microtype', # through tract + home attributes
    #                 'populationGroupType',  # demographic group
    #                 'destination', 'dest_geotype', 'dest_microtype', # destination attributes
    #                 'DistanceBinID'] # trip distance bin
    # daily_VMT_by_tract = trip_to_route.groupby(grouping_var)[['VMT']].sum()
    # daily_VMT_by_tract = daily_VMT_by_tract.reset_index()

    # daily_VMT_by_tract = pd.merge(daily_VMT_by_tract, EV_penetration,
    #                           on = 'origin', how = 'left')
    # daily_VMT_by_tract.loc[:, 'EV_VMT'] = daily_VMT_by_tract.loc[:, 'VMT'] * daily_VMT_by_tract.loc[:, 'EV_penetration']
    # daily_VMT_by_tract.loc[:, 'ICEV_VMT'] = daily_VMT_by_tract.loc[:, 'VMT'] - daily_VMT_by_tract.loc[:, 'EV_VMT']
    # daily_VMT_by_tract = daily_VMT_by_tract.loc[:, ['GEOID', 'origin', 'destination', 'TripPurposeID', 'DistanceBinID', 
    #                                             'EV_VMT', 'ICEV_VMT']]
    # sample_daily_VMT_by_tract = daily_VMT_by_tract.loc[daily_VMT_by_tract['origin']==sample_origin_tract]
    # sample_daily_VMT_by_tract.to_csv('Output/sample_daily_vmt_by_tracts.csv', index = False)
    
    # selected_destinations = sample_daily_VMT_by_tract['destination'].unique()
    # sample_od_trips = od_trips.loc[od_trips['origin'] == sample_origin_tract]
    # sample_od_trips = sample_od_trips.loc[sample_od_trips['destination'].isin(selected_destinations)]
    # sample_od_trips.to_csv('Output/sample_OD_by_tracts.csv', index = False)
    
    # sample_route_df = route_df.loc[route_df['source'] == sample_origin_tract]
    # sample_route_df.to_csv('Input/sample_route.csv', index = False)
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
                          left_on = ['home_GEOID', 'destination'],
                          right_on = ['source', 'destination'],
                          how = 'inner')
    sample_origin_tract = trip_to_route['GEOID'].unique()[0]
    # trip_to_route = trip_to_route.dropna()
    trip_to_route.loc[:, 'distance'] *= meter_to_mile
    trip_to_route.loc[:, 'VMT'] = trip_to_route.loc[:, 'Length'] * trip_to_route.loc[:, 'TripGeneration'] * \
    meter_to_mile
    trip_to_route.loc[:, 'DistanceBinID'] = pd.cut(trip_to_route.loc[:, 'distance'], distance_bins, 
                                                      labels = distance_bin_labels, ordered = False)
    trip_to_route.loc[:, 'DistanceBinID'] = trip_to_route.loc[:, 'DistanceBinID'].astype(str)
    trip_to_route.loc[:, 'DistanceBinID'] = trip_to_route.loc[:, 'DistanceBinID'].astype(int)
    trip_to_route.loc[:, 'VMT'] = np.round(trip_to_route.loc[:, 'VMT'], 0)
    trip_to_route = trip_to_route.loc[trip_to_route['VMT'] > 0]

    VMT_to_home = trip_to_route.groupby(grouping_var)[['VMT']].sum()
    VMT_to_home = VMT_to_home.reset_index() 
    # VMT_to_home['VMT'] = np.round(VMT_to_home['VMT'], 0)
    # VMT_to_home = VMT_to_home[VMT_to_home['VMT'] > 0]
    
    VMT_to_home_out = pd.concat([VMT_to_home_out, VMT_to_home])
    

    VMT_to_destination = trip_to_route.groupby(grouping_var_2)[['VMT']].sum()
    VMT_to_destination = VMT_to_destination.reset_index() 
        
    VMT_to_destination_out = pd.concat([VMT_to_destination_out, VMT_to_destination])
    

    OD_summary = trip_to_route.groupby(grouping_var_3)[['INRIX_distance', 'distance', 'TripGeneration']].mean()
    OD_summary = OD_summary.reset_index()
    
    OD_summary.loc[:, 'OD'] = OD_summary['home_GEOID'].astype(str) + '_' + OD_summary['destination'].astype(str)
    
    OD_summary_out = pd.concat([OD_summary_out, OD_summary])
    unique_ODs = OD_summary.OD.unique()
    od_trips = od_trips.loc[~ od_trips['OD'].isin(unique_ODs)] # remove trips with route assigned
    # print('total trip VMT = ' + str(VMT_to_home.loc[:, 'VMT'].sum()))  
    # break


# <codecell>
print('total VMT after route assignment:')
print('total routed VMT is ' + str(VMT_to_home_out['VMT'].sum()))
print('total trips is ' + str(OD_summary_out['TripGeneration'].sum()))
# post-processing data
VMT_to_home_agg = VMT_to_home_out.groupby(grouping_var)[['VMT']].sum()
VMT_to_home_agg = VMT_to_home_agg.reset_index()

VMT_to_dest_agg = VMT_to_destination_out.groupby(grouping_var_2)[['VMT']].sum()
VMT_to_dest_agg = VMT_to_dest_agg.reset_index()

VMT_to_home_agg.to_csv('Output/' + selected_state + '/home_daily_vmt_by_tracts.csv', index = False)
VMT_to_dest_agg.to_csv('Output/' + selected_state + '/destination_daily_vmt_by_tracts.csv', index = False)
OD_summary_out.to_csv('Output/' + selected_state + '/OD_summary_with_routed_distance.csv', index = False)

# <codecell>
# find O-Ds that need impute
od_trips_impute = od_trips.groupby(['home_GEOID', 'destination', 'OD'])[['VMT']].sum()
od_trips_impute = od_trips_impute.reset_index()
print(od_trips_impute.VMT.sum())
od_trips_impute = od_trips_impute.drop_duplicates(subset = 'OD')
od_trips_impute = od_trips_impute[od_trips_impute['home_GEOID'] != od_trips_impute['destination']]
od_trips_impute = od_trips_impute.sort_values(by = 'VMT', ascending = False)
od_trips_impute.loc[:, 'fraction'] = od_trips_impute.loc[:, 'VMT'] / od_trips_impute.loc[:, 'VMT'].sum()
od_trips_impute.loc[:, 'fraction'] =od_trips_impute.loc[:, 'fraction'].cumsum()
od_trips_impute = od_trips_impute[od_trips_impute['fraction'] <= 0.95]
od_trips_impute = od_trips_impute[od_trips_impute['VMT'] > 50]
print(od_trips_impute.VMT.sum())
print('number of OD pairs to impute')
print(len(od_trips_impute.VMT))
od_trips_impute.to_csv('Network/' + selected_state + '/OD_to_impute.csv')

   
