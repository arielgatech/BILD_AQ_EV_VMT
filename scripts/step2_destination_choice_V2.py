#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May  9 12:40:38 2022

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

selected_state = 'NM'

# step 0 --- load input
trip_generation_file = 'TripGeneration_' + selected_state +'.csv'
trip_generation = read_csv('Input/' + selected_state + '/' + trip_generation_file, sep = ',')

# nonhome_file = 'NHTS_nonhome_based_trips_CA.csv'
# nonhome_trip_fraction = read_csv('Input/' + nonhome_file, sep = ',')

dist_matrix_file = 'travel_time_skim.csv'
dist_matrix = read_csv('Network/' + selected_state + '/' + dist_matrix_file, sep = ',')
# dist_matrix = dist_matrix.rename(columns = {'Unnamed: 0': 'origin'})

employment_file = 'wac_tract_2017.csv'
employment_by_tract = read_csv('Network/' + employment_file, sep = ',')

opportunity_file = 'opportunity_counts_tract.csv'
opportunity_by_tract = read_csv('Network/' + opportunity_file, sep = ',')

transit_file = 'modeaccessibility.csv'
transit_by_tract = read_csv('Network/' + transit_file, sep = ',')

microtype_lookup_file = 'ccst_geoid_key_tranps_geo_with_imputation.csv'
microtype_lookup = read_csv('Network/' + microtype_lookup_file, sep = ',')


# load destination choice parameters
dest_choice_file = 'destination_choice_parameters.csv'
dest_choice_param = read_csv('Input/' + dest_choice_file, sep = ',')

# <codecell>
# step 1 --- check trip generation & pre-processing
grouping_var = ['GEOID', 'home_geotype', 'home_microtype',
       'populationGroupType', 'dest_geotype', 'dest_microtype', 
       'TripType', 'TripPurposeID']
trip_generation = trip_generation.groupby(grouping_var)[['TripGeneration']].sum()
trip_generation = trip_generation.reset_index()
origin_tracts = trip_generation.GEOID.unique() 
origin_microtypes = trip_generation.home_microtype.unique()   
print('total origin tracts ' + str(len(origin_tracts))) 
print('total origin microtypes ' + str(len(origin_microtypes))) 
print('total trip origin = ' + str(trip_generation.loc[:, ['TripGeneration']].sum()))

# <codecell>
# step 2 --- trip attraction
# sample_destination_tract = trip_route.loc[trip_route['source'].isin(origin_tracts), 'destination'].unique()


# adjustable param
max_radius = 80
sample_size = 10
# power_coeff = 1.5 # square of distance
prob_cut = 0.01 # tunable param, drop destinations with too low probability and rescale the fraction for the rest destinations
params = [1, 1.136, 0, 0.354] # fitted Weibull parameters 

def split_dataframe(df, chunk_size = 10000): 
    chunks = list()
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        chunks.append(df[i*chunk_size:(i+1)*chunk_size])
    return chunks


microtype_lookup_short = microtype_lookup[['GEOID', 'geotype', 'microtype']]
microtype_lookup_short.columns = ['destination', 'geotype', 'microtype']

employment_data_short = employment_by_tract[['trct',  'jobs_total']]
employment_data_short.columns = ['destination', 'jobs_total']  

opportunity_by_tract.loc[:, 'num_edu'] = opportunity_by_tract.loc[:, 'num_schools'] + \
opportunity_by_tract.loc[:, 'num_childcare'] + opportunity_by_tract.loc[:, 'num_jrcollege']
opportunity_by_tract_short = opportunity_by_tract[['GEOID', 'num_edu']]  
opportunity_by_tract_short.columns = ['destination', 'edu_total']

transit_by_tract.loc[:, 'transit'] = 0
criteria = (transit_by_tract['rail'] == 1) | ((transit_by_tract['bus'] == 1))
transit_by_tract.loc[criteria, 'transit'] = 1
transit_by_tract_short = transit_by_tract[['geoid', 'transit']]  
transit_by_tract_short.columns = ['destination', 'with_transit']

sample_dist_matrix = dist_matrix.loc[dist_matrix['origin'].isin(origin_tracts)]
# sample_dist_matrix_long = pd.melt(sample_dist_matrix, id_vars = 'origin', 
#                                   var_name = 'destination', value_name = 'distance')

sample_dist_matrix.loc[:, 'destination'] = sample_dist_matrix.loc[:, 'destination'].astype(int)
sample_dist_matrix = sample_dist_matrix.loc[sample_dist_matrix['distance_mile'] <= max_radius]

# append destination attributes
sample_dist_matrix = pd.merge(sample_dist_matrix, microtype_lookup_short,
                                   on = 'destination', how = 'left')
sample_dist_matrix = pd.merge(sample_dist_matrix, employment_data_short,
                                   on = 'destination', how = 'left')
sample_dist_matrix = pd.merge(sample_dist_matrix, opportunity_by_tract_short,
                                   on = 'destination', how = 'left')
sample_dist_matrix = pd.merge(sample_dist_matrix, transit_by_tract_short,
                                   on = 'destination', how = 'left')

# distance_bins = [0, 1.3, 3, 8, sample_dist_matrix_long['distance'].max()]
# distance_bin_labels = ['short', 'medium', 'long', 'xlong']
# sample_dist_matrix_long.loc[:, 'DistanceBinID'] = pd.cut(sample_dist_matrix_long.loc[:, 'distance'], distance_bins, 
#                                                       labels = distance_bin_labels, ordered = False)
# trip_generation = trip_generation.loc[trip_generation['TripGeneration'] > 0]
# <codecell>


# generate random destination choice sets
chunksize = 10000
# trip_generation = trip_generation.loc[trip_generation['TripGeneration'] > 0]
chunks_of_trips = split_dataframe(trip_generation, chunksize)
trip_attraction = None
trip_attraction_failed = None
i = 0
for chunk in chunks_of_trips:
    print('processing batch ' + str(i))
    chunk_attraction = pd.merge(chunk, sample_dist_matrix,
                                left_on = ['GEOID', 'dest_geotype', 'dest_microtype'],
                                right_on = ['origin', 'geotype', 'microtype'], how = 'left')
    failed_trips = chunk_attraction.loc[chunk_attraction['destination'].isna()]
    failed_trips = failed_trips[['GEOID', 'home_geotype', 'home_microtype', 
                                 'populationGroupType', 'dest_geotype', 
                                 'dest_microtype', 'TripType', 
                                 'TripPurposeID','TripGeneration']]
    chunk_attraction = chunk_attraction.dropna()
    # chunk_attraction.loc[:, 'importance'] = 1 /((chunk_attraction.loc[:, 'distance'] + 2) ** power_coeff)
    chunk_attraction.loc[:, 'probability'] = s.exponweib.sf(chunk_attraction.loc[:, 'travel_time_h'], *params)
    # chunk_attraction.loc[chunk_attraction['importance'] < 0.0001, 'importance'] = 0.0001
    chunk_attraction = chunk_attraction.groupby(grouping_var).sample(n = sample_size, weights = chunk_attraction['probability'],
                                                                     replace = True, random_state = 1)
    chunk_attraction_out = chunk_attraction[['GEOID', 'home_geotype', 'home_microtype', 
                                             'populationGroupType', 'dest_geotype', 
                                             'dest_microtype', 'TripType', 
                                             'TripPurposeID','TripGeneration', 
                                             'destination', 'geotype', 'microtype',
                                             'distance_mile', 'travel_time_h', 
                                             'jobs_total', 'edu_total', 'with_transit']]
    trip_attraction = pd.concat([trip_attraction, chunk_attraction_out])
    trip_attraction_failed = pd.concat([trip_attraction_failed, failed_trips])
    i += 1
    # break
# # don't need this step in final model
# trip_attraction = trip_attraction.loc[trip_attraction['destination'].isin(sample_destination_tract)]
print(trip_attraction.head(5))
print(trip_attraction.columns)
# <codecell>
# apply destination choice model and assign trips to destination


def destination_choice_model(data, param, grouping_var, prob_cut = 0.01):
    # print(data.columns)
    # print(param.columns)
    data = pd.merge(data, param, 
                    on = ['home_geotype', 'populationGroupType'], how = 'left')
    data.loc[:, 'Utility'] = data.loc[:, 'B_time'] * \
        data.loc[:, 'travel_time_h'] + data.loc[:, 'B_distance'] * \
        data.loc[:, 'distance_mile'] + data.loc[:, 'B_size'] * \
        np.log(data.loc[:, 'jobs_total'] + \
               data.loc[:, 'B_edu'] * data.loc[:, 'edu_total'] * data.loc[:, 'school']) + \
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
trip_attraction_with_choice = destination_choice_model(trip_attraction, dest_choice_param, grouping_var, prob_cut)  
trip_attraction_with_choice.loc[:,  'TripGeneration'] *= trip_attraction_with_choice.loc[:,  'probability']
# trip_attraction_with_choice.loc[:,  'NHBTripGeneration'] *= trip_attraction_with_choice.loc[:,  'probability'] 
trip_attraction_with_choice["distance_mile"].plot(kind="hist", density = True,
                                             weights = trip_attraction_with_choice["TripGeneration"], bins = 50)
plt.show()
# <codecell>
# impute destination for failed trips
grouping_var_2 = ['GEOID', 'home_geotype', 'home_microtype', 
                  'populationGroupType','TripType', 'TripPurposeID']
trip_attraction_failed_agg = trip_attraction_failed.groupby(grouping_var_2)[['TripGeneration']].sum()
trip_attraction_failed_agg = trip_attraction_failed_agg.reset_index()
chunk_of_failed_trips = split_dataframe(trip_attraction_failed_agg, chunk_size = 2000)
trip_attraction_failed_out = None
i = 0
for chunk in chunk_of_failed_trips:
    print('processing batch ' + str(i))
    chunk_attraction = pd.merge(chunk, sample_dist_matrix,
                                left_on = ['GEOID'],
                                right_on = ['origin'], how = 'left')
    chunk_attraction = chunk_attraction.dropna()
    # chunk_attraction.loc[:, 'importance'] = 1 /((chunk_attraction.loc[:, 'distance'] + 2) ** power_coeff)
    chunk_attraction.loc[:, 'probability'] = s.exponweib.sf(chunk_attraction.loc[:, 'travel_time_h'], *params)
    # chunk_attraction.loc[chunk_attraction['importance'] < 0.0001, 'importance'] = 0.0001
    chunk_attraction = chunk_attraction.groupby(grouping_var_2).sample(n = sample_size, weights = chunk_attraction['probability'],
                                                                     replace = True, random_state = 1)
    chunk_attraction_out = chunk_attraction[['GEOID', 'home_geotype', 'home_microtype', 
                                             'populationGroupType', 'TripType', 
                                             'TripPurposeID','TripGeneration', 
                                             'destination', 'geotype', 'microtype',
                                             'distance_mile', 'travel_time_h', 
                                             'jobs_total', 'edu_total', 'with_transit']]
    trip_attraction_failed_out = pd.concat([trip_attraction_failed_out, chunk_attraction_out])  
    i += 1
    # break

trip_attraction_failed_out.loc[:, 'school'] = 0
trip_attraction_failed_out.loc[trip_attraction_failed_out['TripPurposeID'] == 'school', 'school'] = 1
trip_attraction_failed_with_choice = destination_choice_model(trip_attraction_failed_out, dest_choice_param, grouping_var_2, prob_cut)  
trip_attraction_failed_with_choice.loc[:,  'TripGeneration'] *= trip_attraction_failed_with_choice.loc[:,  'probability']
# trip_attraction_failed_with_choice.loc[:,  'NHBTripGeneration'] *= trip_attraction_failed_with_choice.loc[:,  'probability'] 
trip_attraction_failed_with_choice["distance_mile"].plot(kind="hist", density = True,
                                             weights = trip_attraction_failed_with_choice["TripGeneration"], bins = 50)
plt.show()
print(trip_attraction_failed_with_choice.head(5))

# <codecell>
output_var = ['GEOID', 'home_geotype', 'home_microtype', 'populationGroupType',
              'TripType', 'TripPurposeID',
              'destination', 'geotype', 'microtype', 'distance_mile', 
              'travel_time_h', 'TripGeneration']

# process non-home based trips
trip_attraction_with_choice_out = trip_attraction_with_choice[output_var]
trip_attraction_failed_with_choice_out = trip_attraction_failed_with_choice[output_var]

home_trip_attraction = pd.concat([trip_attraction_with_choice_out, trip_attraction_failed_with_choice_out])


home_trip_attraction = home_trip_attraction.rename(columns = {'geotype': 'dest_geotype', 'microtype': 'dest_microtype'})
output_dir = 'Output/' + selected_state 
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
home_trip_attraction.to_csv(output_dir + '/OD_home_based_trips_by_tract.csv.zip')

# <codecell>

home_trip_attraction["distance_mile"].plot(kind="hist", density = True, 
                                      weights = home_trip_attraction["TripGeneration"], bins = 50)
plt.xlabel('trip distance (mile)')
plt.ylabel('probability density')
plot_dir = 'Plot/' + selected_state
if not os.path.exists(plot_dir):
    os.makedirs(plot_dir)
plt.savefig(plot_dir + '/car_trip_distance_distribution_GEMS.png', dpi = 200)
# process non-home trips
# nonhome_trip_attraction = home_trip_attraction.groupby(['GEOID', 'home_geotype', 'home_microtype', 'populationGroupType',
#                                                         'destination', 'geotype', 'microtype'])[['NHBTripGeneration']].sum()

# nonhome_trip_attraction = nonhome_trip_attraction.reset_index()
# print(nonhome_trip_attraction.NHBTripGeneration.sum())
# nonhome_trip_attraction.loc[:, 'NHBTripGeneration'] = np.round(nonhome_trip_attraction.loc[:, 'NHBTripGeneration'], 0)
# nonhome_trip_attraction = nonhome_trip_attraction.loc[nonhome_trip_attraction['NHBTripGeneration'] > 0]

# # nonhome_trip_attraction = pd.merge(nonhome_trip_attraction, nonhome_trip_fraction,
# #                                    left_on = ['geotype', 'microtype'])
# print(nonhome_trip_attraction.NHBTripGeneration.sum())
# trip_attraction_with_choice.to_csv('')

# destination_count = trip_attraction.groupby(['GEOID', 'DistanceBinID', 
#                                              'DestinationMicrotypeID'])[['destination']].nunique()
# destination_count.columns = ['dest_count']
# destination_count = destination_count.reset_index()

# dist_order = CategoricalDtype(
#     ['short', 'medium', 'long', 'xlong'], 
#     ordered=True
# )
# destination_count['DistanceBinID'] = destination_count['DistanceBinID'].astype(dist_order)
# destination_count = destination_count.sort_values('DistanceBinID')
# sns.boxplot(x = "DistanceBinID", y = "dest_count", data = destination_count, showfliers = False)
# plt.xlabel('Distance bin')
# plt.ylabel('Count of destination tracts')
# plt.savefig('Plot/destination_pool_by_distance_bin.png', dpi = 200)
# plt.show()

# trip_attraction = pd.merge(trip_attraction, destination_count, 
#                             on = ['GEOID', 'DistanceBinID', 'DestinationMicrotypeID'], 
#                             how = 'left')
# trip_attraction_with_match = trip_attraction.loc[trip_attraction['dest_count'] > 0]
# trip_attraction_no_match = trip_attraction.loc[trip_attraction['dest_count'] == 0]
# trip_attraction_with_match.loc[:, 'trips'] = trip_attraction_with_match.loc[:, 'TripGeneration'] / trip_attraction_with_match.loc[:, 'dest_count']
# trip_attraction_with_match = trip_attraction_with_match.loc[:, ['origin', 'destination', 'TimePeriodID', 
#                                           'TripPurposeID', 'DistanceBinID', 'trips']]

# print('total trip assigned = ' + str(trip_attraction_with_match.loc[:, 'trips'].sum()))


# # <codecell>
# # fill missing values
# trip_attraction_no_match = trip_attraction_no_match.groupby(['OriginMicrotypeID', 'GEOID',
#                                                              'TimePeriodID', 'DistanceBinID', 
#                                                              'TripPurposeID']).agg({'TripGenerationRatePerHour':'sum', 
#                                                                                     'NHTS_population':'mean', 
#                                                                                     'ACS_population':'mean'})
# trip_attraction_no_match.columns = ['TripGenerationRatePerHour', 'NHTS_population', 'ACS_population'] 
# trip_attraction_no_match = trip_attraction_no_match.reset_index()   
# trip_attraction_no_match.loc[:, 'Rates'] = trip_attraction_no_match.loc[:, 'TripGenerationRatePerHour'] / \
#     trip_attraction_no_match.loc[:, 'NHTS_population']
# trip_attraction_no_match.loc[:, 'TripGeneration'] = trip_attraction_no_match.loc[:, 'Rates'] * \
#     trip_attraction_no_match.loc[:, 'ACS_population'] 
# trip_attraction_to_fill = pd.merge(trip_attraction_no_match, sample_dist_matrix_long,
#                             left_on = ['GEOID', 'DistanceBinID'],
#                             right_on = ['origin', 'DistanceBinID'], how = 'left')

# destination_count_to_fill = trip_attraction_to_fill.groupby(['GEOID', 'DistanceBinID'])[['destination']].nunique()
# destination_count_to_fill.columns = ['dest_count']
# destination_count_to_fill = destination_count_to_fill.reset_index()

# trip_attraction_to_fill = pd.merge(trip_attraction_to_fill, destination_count_to_fill, 
#                             on = ['GEOID', 'DistanceBinID'], 
#                             how = 'left')  

# criteria = (trip_attraction_to_fill['dest_count'] == 0)
# trip_attraction_to_fill.loc[criteria, 'origin'] = trip_attraction_to_fill.loc[criteria, 'GEOID'] 
# trip_attraction_to_fill.loc[criteria, 'destination'] = trip_attraction_to_fill.loc[criteria, 'GEOID']   
# trip_attraction_to_fill.loc[criteria, 'DestinationMicrotypeID'] = trip_attraction_to_fill.loc[criteria, 'OriginMicrotypeID']   
# trip_attraction_to_fill.loc[criteria, 'distance'] = 0
# trip_attraction_to_fill.loc[:, 'trips'] = trip_attraction_to_fill.loc[:, 'TripGeneration'] / trip_attraction_to_fill.loc[:, 'dest_count']
# trip_attraction_to_fill.loc[criteria, 'trips'] = trip_attraction_to_fill.loc[criteria, 'TripGeneration']                                                                          

# trip_attraction_to_fill = trip_attraction_to_fill.loc[:, ['origin', 'destination', 'TimePeriodID', 
#                                           'TripPurposeID', 'DistanceBinID', 'trips']]
# trip_attraction_combined = pd.concat([trip_attraction_with_match, trip_attraction_to_fill])

# print('total trip assigned = ' + str(trip_attraction_combined.loc[:, 'trips'].sum()))
# trip_attraction_combined.to_csv('Output/OD_trips_by_tract.csv.zip')

