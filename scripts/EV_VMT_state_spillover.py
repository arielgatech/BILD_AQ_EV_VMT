#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun  5 09:23:56 2023

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


path_to_prj = '/Users/xiaodanxu/Library/CloudStorage/GoogleDrive-arielinseu@gmail.com/My Drive/GEMS/BILD-AQ/data'
os.chdir(path_to_prj)

selected_state = 'CA'

# trip generation input
ACS_population_file = 'ACS_household_by_tracts.csv'
ccst_lookup_file = 'ccst_geoid_key_tranps_geo_with_imputation.csv'
hb_spillover_file = 'spillover/NHTS_home_based_trip_rate_spillover.csv'

# destination choice input
dist_matrix_file = 'combined/distance_matrix_by_tracts_' + selected_state + '.csv'
employment_file = 'wac_tract_2017.csv'
opportunity_file = 'opportunity_counts_tract.csv'
transit_file = 'modeaccessibility.csv'
dest_choice_file = 'spillover/destination_choice_parameters.csv'

# load trip generation inputs
ACS_population = read_csv('Input/' + ACS_population_file, sep = ',')
hb_spillover_generation = read_csv('Input/' + hb_spillover_file, sep = ',')
ccst_lookup = read_csv(ccst_lookup_file, sep = ',')

# load destination choice inputs
dist_matrix = read_csv('Network/' + dist_matrix_file, sep = ',')
employment_by_tract = read_csv('Network/' + employment_file, sep = ',')
opportunity_by_tract = read_csv('Network/' + opportunity_file, sep = ',')
transit_by_tract = read_csv('Network/' + transit_file, sep = ',')
dest_choice_param = read_csv('Input/' + dest_choice_file, sep = ',')

# load list of route 
list_of_routes = os.listdir('Input/' + selected_state + '/route_external')
# <codecell>

# calculate home-based spillover trips
ccst_lookup_state = ccst_lookup.loc[ccst_lookup['st_code'] == selected_state]

ACS_population_state = pd.merge(ACS_population, ccst_lookup_state, 
                             on = 'GEOID', how = 'inner')
ACS_population_state = pd.melt(ACS_population_state, id_vars=['GEOID', 'geotype', 'microtype'], 
                            value_vars=['low-income', 'medium-income', 'high-income'],
                            var_name='populationGroupType', value_name='ACS_households')

ACS_population_by_inc = ACS_population_state.groupby(['geotype', 'microtype', 'populationGroupType'])[['ACS_households']].sum()
ACS_population_by_inc = ACS_population_by_inc.reset_index()
ACS_population_by_inc.loc[:, 'pop_fraction'] = \
ACS_population_by_inc['ACS_households'] / ACS_population_by_inc['ACS_households'].sum()

spillover_trip_generation_state = pd.merge(ACS_population_by_inc, hb_spillover_generation,
                              on = ['geotype', 'microtype', 'populationGroupType'],
                              how = 'left')
spillover_trip_generation_state.loc[:, 'TripGeneration'] = \
    spillover_trip_generation_state.loc[:, 'trip_rate'] * spillover_trip_generation_state.loc[:, 'ACS_households'] 
spillover_trip_generation_state.loc[:, 'TripGeneration'] = \
    np.round(spillover_trip_generation_state.loc[:, 'TripGeneration'], 0)
spillover_trip_generation_state = spillover_trip_generation_state.fillna(0)

print(spillover_trip_generation_state.loc[:, 'TripGeneration'].sum()) 

var_to_keep = ['GEOID', 'geotype', 'microtype', 'populationGroupType',
       'dest_geotype', 'dest_microtype', 'trip_tag',
       'trip_purpose', 'distanceBin', 'TripGeneration']
spillover_trip_generation_state = spillover_trip_generation_state[var_to_keep]
spillover_trip_generation_state = \
    spillover_trip_generation_state.loc[spillover_trip_generation_state['TripGeneration'] > 0]

spillover_trip_generation_state.to_csv('Input/' + selected_state + '/TripGeneration_spillover.csv')    
# <codecell>

# destination choice - data preparation
group_var = ['GEOID', 'geotype', 'microtype', 'populationGroupType',
       'trip_tag','trip_purpose']
spillover_trip_generation_agg = \
    spillover_trip_generation_state.groupby(group_var)[['TripGeneration']].sum()
spillover_trip_generation_agg = \
    spillover_trip_generation_agg.reset_index()
spillover_trip_generation_agg.columns = ['GEOID', 'home_geotype', 
                                         'home_microtype', 'populationGroupType',
                                         'TripType','TripPurposeID', 'TripGeneration']
origin_tracts = spillover_trip_generation_agg.GEOID.unique() 
origin_microtypes = spillover_trip_generation_agg.home_microtype.unique() 
print('total origin tracts ' + str(len(origin_tracts))) 
print('total origin microtypes ' + str(len(origin_microtypes))) 
print('total trip origin = ' + str(spillover_trip_generation_agg.loc[:, ['TripGeneration']].sum()))

# adjustable param
max_radius = 300
sample_size = 10
# power_coeff = 1.5 # square of distance
prob_cut = 0.01 # tunable param, drop destinations with too low probability and rescale the fraction for the rest destinations
params = [1, 0.697, 0, 60.29] # fitted Weibull parameters 

ccst_lookup_short = ccst_lookup[['GEOID', 'st_code']]
ccst_lookup_short.columns = ['destination', 'st_code']  
employment_data_short = employment_by_tract[['trct',  'jobs_total']]
employment_data_short.columns = ['destination', 'jobs_total']  

opportunity_by_tract.loc[:, 'num_edu'] = opportunity_by_tract.loc[:, 'num_schools'] + \
opportunity_by_tract.loc[:, 'num_childcare'] + opportunity_by_tract.loc[:, 'num_jrcollege']

opportunity_by_tract.loc[:, 'num_med'] = opportunity_by_tract.loc[:, 'num_hosp'] + \
opportunity_by_tract.loc[:, 'num_pharm'] + opportunity_by_tract.loc[:, 'num_urgentcare']

opportunity_by_tract.loc[:, 'num_ent'] = opportunity_by_tract.loc[:, 'num_parks']

opportunity_by_tract_short = opportunity_by_tract[['GEOID', 'num_edu', 'num_med', 'num_ent']]  
opportunity_by_tract_short.columns = ['destination', 'edu_total', 'med_total', 'ent_total']

transit_by_tract.loc[:, 'transit'] = 0
criteria = (transit_by_tract['rail'] == 1) | ((transit_by_tract['bus'] == 1))
transit_by_tract.loc[criteria, 'transit'] = 1
transit_by_tract_short = transit_by_tract[['geoid', 'transit']]  
transit_by_tract_short.columns = ['destination', 'with_transit']

sample_dist_matrix = dist_matrix.loc[dist_matrix['origin'].isin(origin_tracts)]

sample_dist_matrix['destination'] = \
    sample_dist_matrix['destination'].astype(int)
sample_dist_matrix = sample_dist_matrix.loc[sample_dist_matrix['distance'] <= max_radius]

# append destination attributes
sample_dist_matrix = pd.merge(sample_dist_matrix, ccst_lookup_short,
                                   on = 'destination', how = 'left')
sample_dist_matrix = pd.merge(sample_dist_matrix, employment_data_short,
                                   on = 'destination', how = 'left')
sample_dist_matrix = pd.merge(sample_dist_matrix, opportunity_by_tract_short,
                                   on = 'destination', how = 'left')
sample_dist_matrix = pd.merge(sample_dist_matrix, transit_by_tract_short,
                                   on = 'destination', how = 'left')
sample_dist_matrix = sample_dist_matrix.loc[sample_dist_matrix['st_code'] != selected_state]
# <codecell>

# destination choice - generate choice set
grouping_var = ['GEOID', 'home_geotype', 'home_microtype',
       'populationGroupType', 'TripType', 'TripPurposeID']

def split_dataframe(df, chunk_size = 1000): 
    chunks = list()
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        chunks.append(df[i*chunk_size:(i+1)*chunk_size])
    return chunks

chunks_of_trips = split_dataframe(spillover_trip_generation_agg, chunk_size = 10000)
trip_attraction = None
trip_attraction_failed = None
i = 0

for chunk in chunks_of_trips:
    print('processing batch ' + str(i))
    # generate destination choice
    spillover_attraction = pd.merge(chunk, sample_dist_matrix,
                                    left_on = 'GEOID', right_on = 'origin', how = 'left')
    failed_trips = spillover_attraction.loc[spillover_attraction['destination'].isna()]
    failed_trips = failed_trips[['GEOID', 'home_geotype', 'home_microtype', 
                                  'populationGroupType', 'TripType', 
                                  'TripPurposeID','TripGeneration']]
    spillover_attraction = spillover_attraction.dropna()
    # chunk_attraction.loc[:, 'importance'] = 1 /((chunk_attraction.loc[:, 'distance'] + 2) ** power_coeff)
    spillover_attraction.loc[:, 'probability'] = s.exponweib.sf(spillover_attraction.loc[:, 'distance'], *params)
    # chunk_attraction.loc[chunk_attraction['importance'] < 0.0001, 'importance'] = 0.0001
    spillover_attraction = spillover_attraction.groupby(grouping_var).sample(n = sample_size, 
                                                                             weights = spillover_attraction['probability'],
                                                                             replace = True, random_state = 1)
    spillover_attraction = spillover_attraction[['GEOID', 'home_geotype', 'home_microtype', 
                                             'populationGroupType', 'TripType', 
                                             'TripPurposeID','TripGeneration', 
                                             'destination', 
                                             'distance', 'jobs_total', 'edu_total', 
                                             'med_total', 'ent_total','with_transit']]
    trip_attraction = pd.concat([trip_attraction, spillover_attraction])
    trip_attraction_failed = pd.concat([trip_attraction_failed, failed_trips])
    i += 1

# <codecell>

# destination choice - generate destination choice
def destination_choice_model(data, param, grouping_var, prob_cut = 0.01):
    data = pd.merge(data, param, on = 'populationGroupType', how = 'left')
    data.loc[:, 'Utility'] = data.loc[:, 'B_distance'] * \
        data.loc[:, 'distance'] + data.loc[:, 'B_size'] * \
        np.log(data.loc[:, 'jobs_total'] + \
               data.loc[:, 'B_edu'] * data.loc[:, 'edu_total'] * data.loc[:, 'school'] + \
               data.loc[:, 'B_ent'] * data.loc[:, 'ent_total'] * data.loc[:, 'leisure']) + \
        data.loc[:, 'B_transit'] * data.loc[:, 'with_transit'] 
    data.loc[:, 'Utility_exp'] = np.exp(data.loc[:, 'Utility']) 
    data.loc[:, 'probability'] = data.loc[:, "Utility_exp"] / \
        data.groupby(grouping_var)["Utility_exp"].transform("sum") 
    
    
    # drop destinations with extremely low probability  
    data = data.loc[data['probability'] > prob_cut]  
    data.loc[:, 'probability'] = data.loc[:, "probability"] / \
        data.groupby(grouping_var)["probability"].transform("sum") 
    return data

trip_attraction.loc[:, 'school'] = 0
trip_attraction.loc[trip_attraction['TripPurposeID'] == 'school', 'school'] = 1

trip_attraction.loc[:, 'leisure'] = 0
trip_attraction.loc[trip_attraction['TripPurposeID'] == 'leisure', 'leisure'] = 1

trip_attraction_with_choice = destination_choice_model(trip_attraction, 
                                                       dest_choice_param, 
                                                       grouping_var, 
                                                       prob_cut)  
trip_attraction_with_choice.loc[:,  'TripGeneration'] *= trip_attraction_with_choice.loc[:,  'probability']
# trip_attraction_with_choice.loc[:,  'NHBTripGeneration'] *= trip_attraction_with_choice.loc[:,  'probability'] 
trip_attraction_with_choice.loc[:,  'TripGeneration'] = np.round(trip_attraction_with_choice.loc[:,  'TripGeneration'], 0)
trip_attraction_with_choice = \
    trip_attraction_with_choice.loc[trip_attraction_with_choice['TripGeneration'] > 0]
print(trip_attraction_with_choice.loc[:,  'TripGeneration'].sum())
trip_attraction_with_choice["distance"].plot(kind="hist", density = True,
                                             weights = trip_attraction_with_choice["TripGeneration"], bins = 50)
plt.show()

output_var = ['GEOID', 'home_geotype', 'home_microtype', 'populationGroupType',
              'TripType', 'TripPurposeID',
              'destination', 'distance', 'TripGeneration']

# process non-home based trips
trip_attraction_with_choice_out = trip_attraction_with_choice[output_var]

trip_attraction_with_choice_out.to_csv('Output/' + selected_state + '/OD_home_based_trips_spillover.csv.zip')

# <codecell>

# route choice
od_trips = trip_attraction_with_choice_out.copy()
od_trips.loc[:, 'VMT'] = od_trips.loc[:, 'distance'] * od_trips.loc[:, 'TripGeneration']
print(od_trips.loc[:, 'TripGeneration'].sum())
print(od_trips.loc[:, 'VMT'].sum())
print(len(od_trips))
od_trips = od_trips.rename(columns = {'GEOID': 'home_GEOID', 'distance': 'gc_distance'})
# od_trips = od_trips.drop(columns=['distance'])
od_trips['destination'] = od_trips['destination'].astype(int)

od_trips['OD'] = od_trips['home_GEOID'].astype(str) + '_' + od_trips['destination'].astype(str)

ccst_lookup_short = ccst_lookup[['GEOID', 'geotype', 'microtype']]

# assign route to trips
meter_to_mile = 0.000621371

grouping_var = ['GEOID', 'geotype', 'microtype', 
                'home_GEOID', 'home_geotype', 'home_microtype', 'populationGroupType'] # through tract + home attributes

grouping_var_2 = ['home_GEOID', 'home_geotype', 'home_microtype', 'populationGroupType', 'destination'] # through tract + home attributes

file_dir = 'Input/' + selected_state + '/route_external'
filelist = [file for file in os.listdir(file_dir) if (file.endswith('.csv'))]
route_df = pd.concat([read_csv(file_dir + '/' + f) for f in filelist ])

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

trip_to_route.loc[:, 'VMT'] = np.round(trip_to_route.loc[:, 'VMT'], 0)
trip_to_route = trip_to_route.loc[trip_to_route['VMT'] > 0]

VMT_to_home = trip_to_route.groupby(grouping_var)[['VMT']].sum()
VMT_to_home = VMT_to_home.reset_index() 
# VMT_to_home['VMT'] = np.round(VMT_to_home['VMT'], 0)
# VMT_to_home = VMT_to_home[VMT_to_home['VMT'] > 0]

# VMT_to_home_out = pd.concat([VMT_to_home_out, VMT_to_home])
    

VMT_to_destination = trip_to_route.groupby(grouping_var_2)[['VMT']].sum()
VMT_to_destination = VMT_to_destination.reset_index() 
    
# VMT_to_destination_out = pd.concat([VMT_to_destination_out, VMT_to_destination])


OD_summary = trip_to_route.groupby(grouping_var_2)[['gc_distance', 'distance']].mean()
OD_summary = OD_summary.reset_index()
    
OD_summary.loc[:, 'OD'] = OD_summary['home_GEOID'].astype(str) + '_' + OD_summary['destination'].astype(str)

# OD_summary_out = pd.concat([OD_summary_out, OD_summary])
unique_ODs = OD_summary.OD.unique()
od_trips = od_trips.loc[~ od_trips['OD'].isin(unique_ODs)] # remove trips with route assigned
print('total trip VMT = ' + str(VMT_to_home.loc[:, 'VMT'].sum()))  
# post-processing data

VMT_to_home.to_csv('Output/' + selected_state + '/home_daily_vmt_spillover.csv', index = False)
VMT_to_destination.to_csv('Output/' + selected_state + '/destination_daily_vmt_spillover.csv', index = False)
OD_summary.to_csv('Output/' + selected_state + '/OD_summary_spillover.csv', index = False)

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
od_trips_impute.to_csv('Network/' + selected_state + '/OD_to_impute_spillover.csv')