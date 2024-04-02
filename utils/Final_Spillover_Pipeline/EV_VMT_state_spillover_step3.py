#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

@author: xiaodanxu and carlosguirado
"""
####################################################################################
############ step 0 -- set up project environment and load inputs ##################
####################################################################################

# Note: run this file first, then run the route imputation (R) and
# move the output to the 'Input/route/{state}_external' folder

import pandas as pd
import os
import numpy as np
from pandas import read_csv
import matplotlib.pyplot as plt
import seaborn as sns
from pandas.api.types import CategoricalDtype
import scipy.stats as s
import dask.dataframe as dd

plt.style.use('ggplot')

# set working directory
path_to_prj = os.getcwd()
os.chdir(path_to_prj)

# set current state for analysis
selected_state = 'UT'

# trip generation input
ACS_population_file = 'ACS_household_by_tracts.csv'
ccst_lookup_file = 'ccst_geoid_key_tranps_geo_with_imputation.csv'
hb_spillover_file = 'spillover/NHTS_home_based_trip_rate_spillover.csv'
hb_border_frac_file = 'spillover/home_based_border_fraction.csv'

# destination choice input
dist_matrix_file = 'combined/distance_matrix_by_tracts_' + selected_state + '.csv'
employment_file = 'wac_tract_2017.csv'
opportunity_file = 'opportunity_counts_tract.csv'
transit_file = 'modeaccessibility.csv'
dest_choice_file = 'spillover/destination_choice_parameters.csv'

# load trip generation inputs
ACS_population = read_csv('Input/' + ACS_population_file, sep = ',')
hb_spillover_generation = read_csv('Input/' + hb_spillover_file, sep = ',')
hb_spillover_border_fraction = read_csv('Input/' + hb_border_frac_file, sep = ',')
ccst_lookup = read_csv(ccst_lookup_file, sep = ',')

# load destination choice inputs
dist_matrix = read_csv('Network/' + dist_matrix_file, sep = ',')
employment_by_tract = read_csv('Network/' + employment_file, sep = ',')
opportunity_by_tract = read_csv('Network/' + opportunity_file, sep = ',')
transit_by_tract = read_csv('Network/' + transit_file, sep = ',')
dest_choice_param = read_csv('Input/' + dest_choice_file, sep = ',')

# load list of route
list_of_routes = os.listdir('Input/route/' + selected_state + '_external')
routes_dir = 'Input/route/' + selected_state + '_external'
# <codecell>

# load output from step 2:
import zipfile
trip_attrac_with_choice_file = selected_state + '/OD_home_based_trips_spillover.csv.zip'
zf = zipfile.ZipFile('Output/' + trip_attrac_with_choice_file)
trip_attraction_with_choice_out = pd.read_csv(zf.open('OD_home_based_trips_spillover.csv'))

print("Files loaded")
####################################################################################
############ step 3 -- route choice model with shortest-path, not cleaned ##########
####################################################################################

# route choice
od_trips = trip_attraction_with_choice_out.copy()
od_trips.loc[:, 'VMT'] = od_trips.loc[:, 'distance'] * od_trips.loc[:, 'TripGeneration']
print("Trips generated: ", od_trips.loc[:, 'TripGeneration'].sum())
print("VMT: ", od_trips.loc[:, 'VMT'].sum())
#print(len(od_trips))
od_trips = od_trips.rename(columns = {'GEOID': 'home_GEOID', 'distance': 'gc_distance'})
# od_trips = od_trips.drop(columns=['distance'])

od_trips['destination'] = od_trips['destination'].astype(str)
od_trips['home_GEOID'] = od_trips['home_GEOID'].astype(str)

od_trips['OD'] = od_trips['home_GEOID'] + '_' + od_trips['destination']

ccst_lookup_short = ccst_lookup[['GEOID', 'geotype', 'microtype']]

# assign route to trips
meter_to_mile = 0.000621371

grouping_var = ['GEOID', 'geotype', 'microtype',
                 'home_GEOID', 'home_geotype', 'home_microtype', 'populationGroupType'] # through tract + home attributes

grouping_var_2 = ['home_GEOID', 'home_geotype', 'home_microtype', 'populationGroupType', 'destination'] # through tract + home attributes

#file_dir = 'Input/' + selected_state + '/route_external'
#file_dir = list_of_routes

filelist = [file for file in os.listdir(routes_dir) if (file.endswith('.csv'))]
route_df = pd.concat([read_csv(routes_dir + '/' + f) for f in filelist ])

route_df = route_df.loc[route_df['Length'] > 0]
route_df = pd.merge(route_df, ccst_lookup_short, on = 'GEOID', how = 'left')
route_df['destination'] = route_df['destination'].astype(str).str.zfill(11)
route_df['source'] = route_df['source'].astype(str).str.zfill(11)
# print(route_df.columns)
trip_to_route = pd.merge(od_trips, route_df,
                       left_on = ['home_GEOID', 'destination'],
                       right_on = ['source', 'destination'],
                       how = 'inner')
sample_origin_tract = trip_to_route['GEOID'].unique()[0]
trip_to_route = trip_to_route.dropna()
trip_to_route.loc[:, 'distance'] *= meter_to_mile
trip_to_route.loc[:, 'VMT'] = trip_to_route.loc[:, 'Length'] * trip_to_route.loc[:, 'TripGeneration'] * \
meter_to_mile

trip_to_route.loc[:, 'VMT'] = np.round(trip_to_route.loc[:, 'VMT'], 0)
trip_to_route = trip_to_route.loc[trip_to_route['VMT'] > 0]

VMT_to_home = trip_to_route.groupby(grouping_var)[['VMT']].sum()
VMT_to_home = VMT_to_home.reset_index()
VMT_to_home['VMT'] = np.round(VMT_to_home['VMT'], 0)
VMT_to_home = VMT_to_home[VMT_to_home['VMT'] > 0]

#VMT_to_home_out = pd.concat([VMT_to_home_out, VMT_to_home])


VMT_to_destination = trip_to_route.groupby(grouping_var_2)[['VMT']].sum()
VMT_to_destination = VMT_to_destination.reset_index()

#VMT_to_destination_out = pd.concat([VMT_to_destination_out, VMT_to_destination])


OD_summary = trip_to_route.groupby(grouping_var_2)[['gc_distance', 'distance']].mean()
OD_summary = OD_summary.reset_index()

OD_summary.loc[:, 'OD'] = OD_summary['home_GEOID'].astype(str) + '_' + OD_summary['destination'].astype(str)

# # OD_summary_out = pd.concat([OD_summary_out, OD_summary])
unique_ODs = OD_summary.OD.unique()
od_trips = od_trips.loc[~ od_trips['OD'].isin(unique_ODs)] # remove trips with route assigned
print('total trip VMT = ' + str(VMT_to_home.loc[:, 'VMT'].sum()))
# # post-processing data

VMT_to_home.to_csv('Output/' + selected_state + '/home_daily_vmt_spillover.csv', index = False)
VMT_to_destination.to_csv('Output/' + selected_state + '/destination_daily_vmt_spillover.csv', index = False)
OD_summary.to_csv('Output/' + selected_state + '/OD_summary_spillover.csv', index = False)
print("VMT to Home & OD summary CSV files written")


# <codecell>
# # find O-Ds that need impute
od_trips_impute = od_trips.groupby(['home_GEOID', 'destination', 'OD'])[['VMT']].sum()
od_trips_impute = od_trips_impute.reset_index()
#(od_trips_impute.VMT.sum())
od_trips_impute = od_trips_impute.drop_duplicates(subset = 'OD')
od_trips_impute = od_trips_impute[od_trips_impute['home_GEOID'] != od_trips_impute['destination']]
od_trips_impute = od_trips_impute.sort_values(by = 'VMT', ascending = False)
od_trips_impute.loc[:, 'fraction'] = od_trips_impute.loc[:, 'VMT'] / od_trips_impute.loc[:, 'VMT'].sum()
od_trips_impute.loc[:, 'fraction'] =od_trips_impute.loc[:, 'fraction'].cumsum()
od_trips_impute = od_trips_impute[od_trips_impute['fraction'] <= 0.95]
od_trips_impute = od_trips_impute[od_trips_impute['VMT'] > 50]
#print(od_trips_impute.VMT.sum())
### SAVE AS STRING
od_trips_impute['home_GEOID'] = od_trips_impute['home_GEOID'].astype(str)
od_trips_impute['destination'] = od_trips_impute['destination'].astype(str)
od_trips_impute['OD'] = od_trips_impute['OD'].astype(str)
od_trips_impute.to_csv('Network/' + selected_state + '/OD_to_impute_spillover.csv')
print("OD to impute spillover CSV file written")