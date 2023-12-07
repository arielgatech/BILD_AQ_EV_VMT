#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

@author: xiaodanxu and @carlosguirado
"""
####################################################################################
############ step 0 -- set up project environment and load inputs ##################
####################################################################################
# from master_file import state_input


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
ACS_population = read_csv('Input/' + ACS_population_file, sep=',')
hb_spillover_generation = read_csv('Input/' + hb_spillover_file, sep=',')
hb_spillover_border_fraction = read_csv('Input/' + hb_border_frac_file, sep=',')
ccst_lookup = read_csv(ccst_lookup_file, sep=',')

# load destination choice inputs
dist_matrix = read_csv('Network/' + dist_matrix_file, sep=',')
employment_by_tract = read_csv('Network/' + employment_file, sep=',')
opportunity_by_tract = read_csv('Network/' + opportunity_file, sep=',')
transit_by_tract = read_csv('Network/' + transit_file, sep=',')
dest_choice_param = read_csv('Input/' + dest_choice_file, sep=',')


############ Step 0.1 - Assign the closest out-of-state Tract and its distance to every tract in the state

# Create new dataframe
df = pd.read_csv('Network/' + dist_matrix_file)


# Need to flag destinations in the state

state_geoid_dic = {'CA': '06', 'OR' : '41', 'WA':'53', 'CO': '08', 'ID': '16', 'NV':'32', 'MT':'30', 'WY':'56',
                   'UT': '49', 'NM' : '35', 'AZ':'04'}
sel_state_geoid = state_geoid_dic[f'{selected_state}']
geoid_lower=int(sel_state_geoid+'000000000')
geoid_upper=int(sel_state_geoid+'999999999')
# State GEOID is 6. Hence numbers between 6000000000 and 6999999999 are in California
# State GEOID is 16. Hence numbers between 6000000000 and 6999999999 are in IDAHO
df['dest_state'] = np.where((df['destination'] >= geoid_lower) & (df['destination'] <= geoid_upper) , 1, 0)

# Filter out selected_state destinations
df_out=df[df['dest_state']==0]

# Calculate the minimum distance for every origin
# Output: origin tract - minimum distance to out of state tract - tract
min_dist_out = df.loc[df_out.groupby('origin')['distance'].idxmin()]


min_dist_out = min_dist_out.reset_index()
final_df = min_dist_out[['origin','destination','distance']].copy()

# Produce the tract_to_border_distance file
final_df.to_csv('Network/combined/' + selected_state + '_tract_to_border_distance.csv')


# Load the newly produced tracts to border distance matrix
tracts_to_border_matrix = pd.read_csv('Network/combined/' + selected_state + '_tract_to_border_distance.csv')




####################################################################################
############ step 1 -- spillover trip generation (by home tract) ###################
####################################################################################

# step 1.1 - calculate total home-based spillover trips = total hh by tract * trip generation rate per hh

ccst_lookup_state = ccst_lookup.loc[ccst_lookup['st_code'] == selected_state]
# select tract within selected states

ACS_population_state = pd.merge(ACS_population, ccst_lookup_state,
                                on='GEOID', how='inner')
# select ACS households within selected states

ACS_population_state = pd.melt(ACS_population_state,
                               id_vars=['GEOID', 'geotype', 'microtype'],
                               value_vars=['low-income', 'medium-income', 'high-income'],
                               var_name='populationGroupType', value_name='ACS_households')
# format population table

# carlos adding GEOID to keep
ACS_population_by_inc = ACS_population_state.groupby(['geotype', 'microtype', 'populationGroupType'])[
    ['ACS_households']].sum()
ACS_population_by_inc = ACS_population_by_inc.reset_index()
ACS_population_by_inc.loc[:, 'pop_fraction'] = \
    ACS_population_by_inc['ACS_households'] / ACS_population_by_inc['ACS_households'].sum()
# aggregate population

spillover_trip_generation_state = pd.merge(ACS_population_by_inc, hb_spillover_generation,
                                           on=['geotype', 'microtype', 'populationGroupType'],
                                           how='left')
# append trip generation rate

spillover_trip_generation_state.loc[:, 'TripGeneration'] = \
    spillover_trip_generation_state.loc[:, 'trip_rate'] * spillover_trip_generation_state.loc[:, 'ACS_households']
# compute total trip generated by aggregation levels

spillover_trip_generation_state = spillover_trip_generation_state.fillna(0)
# fill missing values with 0 (if any)

print('Total home based spillover trips in ' + selected_state + ' is:')
print(spillover_trip_generation_state.loc[:, 'TripGeneration'].sum())
# 610345.132454572 - total did not change. GEOID added after this edit

### Step 1.2 -- assign spillover trips to near border tracts (within California)#

# tracts_to_border_file = 'CA_tract_to_border_distance.csv' # CHANGE

# ACS_population_file = 'ACS_household_by_tracts.csv'
# ccst_lookup_file = 'ccst_geoid_key_tranps_geo_with_imputation.csv'
# hb_spillover_file = 'spillover/NHTS_home_based_trip_rate_spillover.csv'separate columns pandas groupy sum
# hb_border_frac_file = 'spillover/home_based_border_fraction.csv'

hb_spillover_border_fraction_3bins = hb_spillover_border_fraction

hb_spillover_border_fraction_3bins['dist_bin'] = hb_spillover_border_fraction_3bins['dist_bin'].replace(
    ['bin1', 'bin2'], ['bin3', 'bin3'])
hb_spillover_border_fraction_3bins = hb_spillover_border_fraction_3bins.drop(['Unnamed: 0'], axis=1)

hb_spillover_border_fraction_3bins = \
hb_spillover_border_fraction_3bins.groupby(['populationGroupType', 'dist_bin']).sum()['fraction'].reset_index()

# Check: the total fraction is 1 after the rebinning

for idx, row in tracts_to_border_matrix.iterrows():
    # if row['distance'] <= 5:
    #    tracts_to_border_matrix.at[idx,'dist_bin'] = 'bin1'
    # elif row['distance'] > 5 and row['distance'] <= 10:
    #    tracts_to_border_matrix.at[idx,'dist_bin'] = 'bin2'
    if row['distance'] <= 20:
        tracts_to_border_matrix.at[idx, 'dist_bin'] = 'bin3'
    elif row['distance'] > 20 and row['distance'] <= 50:
        tracts_to_border_matrix.at[idx, 'dist_bin'] = 'bin4'
    elif row['distance'] > 50 and row['distance'] <= 100:
        tracts_to_border_matrix.at[idx, 'dist_bin'] = 'bin5'
    else:
        tracts_to_border_matrix.at[idx, 'dist_bin'] = 'bin6'

tracts_to_border_binned = pd.merge(tracts_to_border_matrix, hb_spillover_border_fraction_3bins,
                                   on='dist_bin',
                                   how='left')

tracts_to_border_binned = tracts_to_border_binned.drop(['Unnamed: 0'], axis=1)

# same total fraction (3.0 approx) - fraction adds up to 1 by income rank (low/medium/high)

tracts_to_border_binned['inc_bin'] = tracts_to_border_binned['populationGroupType'] + "_" + tracts_to_border_binned[
    'dist_bin']
tracts_to_border_binned['count_inc_bin'] = tracts_to_border_binned['inc_bin'].map(
    tracts_to_border_binned['inc_bin'].value_counts())
tracts_to_border_binned['tract_frac'] = tracts_to_border_binned['fraction'] / tracts_to_border_binned['count_inc_bin']

# check
if round(tracts_to_border_binned['tract_frac'].sum(), 5) == 3.0:
    print('The total fraction is correct')
else:
    print('There is an error splitting the trips fraction among tracts')

# the sum of tract_frac is correct

# I am going to assign to all tracts in the state as per the proportions
spillover_trip_generation_state_backup = spillover_trip_generation_state

spillover_trip_generation_state = pd.merge(spillover_trip_generation_state, tracts_to_border_binned,
                                           on='populationGroupType',
                                           how='left')

# tract_frac total: 2.999 - tract frac is correct again

spillover_trip_generation_state['trips_by_tract'] = spillover_trip_generation_state['TripGeneration'] * \
                                                    spillover_trip_generation_state['tract_frac']

# check th total number of trips matches the generation in the previous file
if round(spillover_trip_generation_state['trips_by_tract'].sum(), 2) == round(
        spillover_trip_generation_state_backup['TripGeneration'].sum(), 2):
    print("All of the trips have been distributed. The total number of generated trips remains correct")
else:
    print("Error. Number of trips does not match original. Original: ",
          spillover_trip_generation_state_backup['TripGeneration'].sum(), ". After this stage: ",
          spillover_trip_generation_state['trips_by_tract'].sum())

#### Population checks

# Calculate total population by tract
ACS_population['total_pop'] = ACS_population['low-income'] + ACS_population['medium-income'] + ACS_population[
    'high-income']

# Merge pop data into trips dataset
spillover_trip_generation_state = pd.merge(spillover_trip_generation_state,
                                           ACS_population,
                                           left_on='origin',
                                           right_on='GEOID',
                                           how='left')

# Checks at this point:
# how many trips are assigned to each bin? think about how it compares to the population proportions:
# bin6: 89.7%, bin5: 9.3%, bin4: 0.7%, bin1-3: 0.3%

# Use dask to reduce computation time
df = dd.from_pandas(spillover_trip_generation_state, npartitions=100)

# sum_pop=df.groupby('dist_bin')['total_pop'].sum()
# print(sum_pop.compute())

# spread of trips:

sum_trips_dist = (df.groupby('dist_bin')['trips_by_tract'].sum()).compute()
sum_trips_tot = (df['trips_by_tract'].sum()).compute()
trip_bin_shares = (sum_trips_dist / sum_trips_tot) * 100
print("Share of trips allocated to each distance bin:")
print(trip_bin_shares)

# bin6: 5.6% of trips, bin5: 7.7%, bin4: 19.2% of trips, bin1-3: 67.5% of trips


## Adjusting the weights by population
#### Leaving this commented to discuss with Xiaodan
# Context: at the moment, for every dist_bin & income combination, the trips are assigned equally across all tracts
# We can finesse this by assigning by population

# We can adjust the tract_frac by timing by population_in_tract / population_in_income segment total

# sum_pop=df.groupby('populationGroupType')['total_pop'].sum().to_frame(name='pop_by_income')
# result_sum_pop = sum_pop.compute()

# fetch the population by income group in the dataframe
# ACS_tot_high = result_sum_pop.iloc[0][0]
# ACS_tot_low = result_sum_pop.iloc[1][0]
# ACS_tot_med = result_sum_pop.iloc[2][0]


# df['tract_frac_adj'] = 0
# to adjust by population within each income bracket (low/med/high)
# def pop_adj_partition(partition):
#    for idx, row in partition.iterrows():
#        if row['populationGroupType'] == 'low-income':
#            partition.at[idx,'tract_frac_adj'] = row['tract_frac'] * row['low-income'] / ACS_tot_low
#        elif row['populationGroupType'] == 'medium-income':
#            partition.at[idx, 'tract_frac_adj'] = row['tract_frac'] * row['medium-income'] / ACS_tot_med
#        else:
#            partition.at[idx, 'tract_frac_adj'] = row['tract_frac'] * row['high-income'] / ACS_tot_high
#    return partition

# result=df.map_partitions(pop_adj_partition).compute()
# df['trips_by_tract_adj'] = df['TripGeneration'] * df['tract_frac_adj']

# check th total number of trips matches the generation in the previous file
# if round(spillover_trip_generation_state['trips_by_tract'].sum(),2) == round(spillover_trip_generation_state_backup['TripGeneration'].sum(),2):
#    print("All of the trips have been distributed. The total number of generated trips remains correct")
# else:
#    print("Error. Number of trips does not match original. Original: ", spillover_trip_generation_state_backup['TripGeneration'].sum(), ". After this stage: ", spillover_trip_generation_state['trips_by_tract'].sum())
#
###

# Back to pandas dataframe
spillover_trip_generation_state = df.compute()

print("Summary of trips distributed across state tracts (as origin) by distance-to-border bin: ")
print(spillover_trip_generation_state.groupby('dist_bin')['trips_by_tract'].sum())

print("Total number of trips distributed across state tracts (as origin) by distance-to-border bin: ")
print(spillover_trip_generation_state['trips_by_tract'].sum())

tot_trips_gen = spillover_trip_generation_state['trips_by_tract'].sum()
tot_households = ACS_population_state['ACS_households'].sum()
ratio = tot_trips_gen / tot_households

print("The average number of trips distributed by household is:")
print(ratio)

############################################################
## # Step 1.3 -- Post-processing spillover trip data

# rename some variables to be consistent with rest of code

name_change = {'destination': 'border_tract_out_state',
               'distance': 'distance_to_border_tract',
               'TripGeneration': 'TripGen_Old',
               'trips_by_tract': 'TripGeneration'}  # TripGeneration now has trips per tract

spillover_trip_generation_state = spillover_trip_generation_state.rename(columns=name_change)

var_to_keep = ['GEOID', 'geotype', 'microtype', 'populationGroupType',
               'dest_geotype', 'dest_microtype', 'trip_tag',
               'trip_purpose', 'TripGeneration', 'border_tract_out_state', 'distance_to_border_tract']

spillover_trip_generation_state = spillover_trip_generation_state[var_to_keep]
# # define variable to keep in outputs

spillover_trip_generation_state.loc[:, 'TripGeneration'] = \
    np.round(spillover_trip_generation_state.loc[:, 'TripGeneration'], 0)
spillover_trip_generation_state = \
    spillover_trip_generation_state.loc[spillover_trip_generation_state['TripGeneration'] > 0]
# # round trips and remove zero trip record

spillover_trip_generation_state.to_csv('Input/' + selected_state + '/TripGeneration_spillover.csv')
# # write output
# <codecell>

####################################################################################
############ step 2 -- spillover destination choice ################################
####################################################################################

## step 2.1 -- destination choice - data preparation

group_var = ['GEOID', 'geotype', 'microtype', 'populationGroupType',
             'trip_tag', 'trip_purpose']
spillover_trip_generation_agg = \
    spillover_trip_generation_state.groupby(group_var)[['TripGeneration']].sum()
spillover_trip_generation_agg = \
    spillover_trip_generation_agg.reset_index()
## aggregate data and drop extra aggregation levels (e.g., destination type)

spillover_trip_generation_agg.columns = ['GEOID', 'home_geotype',
                                         'home_microtype', 'populationGroupType',
                                         'TripType', 'TripPurposeID', 'TripGeneration']
## renaming columns -- prevent duplicated colnames during join in the following steps

origin_tracts = spillover_trip_generation_agg.GEOID.unique()
origin_microtypes = spillover_trip_generation_agg.home_microtype.unique()
print('total origin tracts ' + str(len(origin_tracts)))
print('total origin microtypes ' + str(len(origin_microtypes)))
print('total trip origin = ' + str(spillover_trip_generation_agg.loc[:, ['TripGeneration']].sum()))
## sanity check of the spillover results

# # define adjustable params for destination choice
max_radius = 300
sample_size = 10
power_coeff = 1.5  # square of distance
prob_cut = 0.01  # tunable param, drop destinations with too low probability and rescale the fraction for the rest destinations
params = [1, 0.697, 0, 60.29]  # fitted Weibull parameters from NHTS data

## processing other inputs for destimation choice
ccst_lookup_short = ccst_lookup[['GEOID', 'st_code']]
ccst_lookup_short.columns = ['destination', 'st_code']
## load state code lookup table
employment_data_short = employment_by_tract[['trct', 'jobs_total']]
employment_data_short.columns = ['destination', 'jobs_total']
## load total jobs (per work trips)

## load opportunities at destinations (such as school, medical and entertainment)
opportunity_by_tract.loc[:, 'num_edu'] = opportunity_by_tract.loc[:, 'num_schools'] + \
                                         opportunity_by_tract.loc[:, 'num_childcare'] + opportunity_by_tract.loc[:,
                                                                                        'num_jrcollege']
## educational facilities
opportunity_by_tract.loc[:, 'num_med'] = opportunity_by_tract.loc[:, 'num_hosp'] + \
                                         opportunity_by_tract.loc[:, 'num_pharm'] + opportunity_by_tract.loc[:,
                                                                                    'num_urgentcare']
## medical facilities
opportunity_by_tract.loc[:, 'num_ent'] = opportunity_by_tract.loc[:, 'num_parks']
## entertainment facilities (parks)

opportunity_by_tract_short = opportunity_by_tract[['GEOID', 'num_edu', 'num_med', 'num_ent']]
opportunity_by_tract_short.columns = ['destination', 'edu_total', 'med_total', 'ent_total']
## keep essential opportunities needed in model

transit_by_tract.loc[:, 'transit'] = 0
criteria = (transit_by_tract['rail'] == 1) | ((transit_by_tract['bus'] == 1))
transit_by_tract.loc[criteria, 'transit'] = 1
transit_by_tract_short = transit_by_tract[['geoid', 'transit']]
transit_by_tract_short.columns = ['destination', 'with_transit']
## define transit availability (bus + rail)

## process O-D distance skims
sample_dist_matrix = dist_matrix.loc[dist_matrix['origin'].isin(origin_tracts)]

# Using .loc indexing to avoid SettingWithCopyWarning message in console
# sample_dist_matrix.loc[:,'destination'] = sample_dist_matrix['destination'].astype(int)
# Changing data type of 'column_name' from 'int' to 'float'
# sample_dist_matrix['destination'] = sample_dist_matrix['destination'].astype(str)
sample_dist_matrix['destination'] = pd.to_numeric(sample_dist_matrix['destination'])
# sample_dist_matrix = sample_dist_matrix.loc[:, 'destination'].astype(np.int64) #string it

# sample_dist_matrix['destination'] = pd.to_numeric(sample_dist_matrix['destination'], downcast='integer')
sample_dist_matrix = sample_dist_matrix.loc[sample_dist_matrix['distance'] <= max_radius]

# # append destination attributes
sample_dist_matrix = pd.merge(sample_dist_matrix, ccst_lookup_short,
                              on='destination', how='left')
sample_dist_matrix = pd.merge(sample_dist_matrix, employment_data_short,
                              on='destination', how='left')
sample_dist_matrix = pd.merge(sample_dist_matrix, opportunity_by_tract_short,
                              on='destination', how='left')
sample_dist_matrix = pd.merge(sample_dist_matrix, transit_by_tract_short,
                              on='destination', how='left')
sample_dist_matrix = sample_dist_matrix.loc[sample_dist_matrix['st_code'] != selected_state]

# <codecell>

# # step 2.2 --- destination choice - generate choice set
grouping_var = ['GEOID', 'home_geotype', 'home_microtype',
                'populationGroupType', 'TripType', 'TripPurposeID']


# cut large dataset into manageable chunks
def split_dataframe(df, chunk_size=1000):
    chunks = list()
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        chunks.append(df[i * chunk_size:(i + 1) * chunk_size])
    return chunks


chunks_of_trips = split_dataframe(spillover_trip_generation_agg, chunk_size=10000)
trip_attraction = None
trip_attraction_failed = None
i = 0

## loop through chunk of trips
for chunk in chunks_of_trips:
    print('processing batch ' + str(i))
    ## generate destination choice
    spillover_attraction = pd.merge(chunk, sample_dist_matrix,
                                    left_on='GEOID', right_on='origin', how='left')
    ## append distance skim
    failed_trips = spillover_attraction.loc[spillover_attraction['destination'].isna()]
    failed_trips = failed_trips[['GEOID', 'home_geotype', 'home_microtype',
                                 'populationGroupType', 'TripType',
                                 'TripPurposeID', 'TripGeneration']]
    ## check if there are no-matching destinations (ideally should be none)

    spillover_attraction = spillover_attraction.dropna()
    #     # chunk_attraction.loc[:, 'importance'] = 1 /((chunk_attraction.loc[:, 'distance'] + 2) ** power_coeff) # old version
    spillover_attraction.loc[:, 'probability'] = s.exponweib.sf(spillover_attraction.loc[:, 'distance'], *params)
    #     # chunk_attraction.loc[chunk_attraction['importance'] < 0.0001, 'importance'] = 0.0001
    spillover_attraction = spillover_attraction.groupby(grouping_var).sample(n=sample_size,
                                                                             weights=spillover_attraction[
                                                                                 'probability'],
                                                                             replace=True, random_state=1)
    ## sampling destination candidate based on survival function

    spillover_attraction = spillover_attraction[['GEOID', 'home_geotype', 'home_microtype',
                                                 'populationGroupType', 'TripType',
                                                 'TripPurposeID', 'TripGeneration',
                                                 'destination',
                                                 'distance', 'jobs_total', 'edu_total',
                                                 'med_total', 'ent_total', 'with_transit']]
    trip_attraction = pd.concat([trip_attraction, spillover_attraction])
    trip_attraction_failed = pd.concat([trip_attraction_failed, failed_trips])
    i += 1


# <codecell>

# # step 2.3 --- destination choice - generate destination choice

def destination_choice_model(data, param, grouping_var, prob_cut=0.01):
    data = pd.merge(data, param, on='populationGroupType', how='left')
    data.loc[:, 'Utility'] = data.loc[:, 'B_distance'] * \
                             data.loc[:, 'distance'] + data.loc[:, 'B_size'] * \
                             np.log(data.loc[:, 'jobs_total'] + \
                                    data.loc[:, 'B_edu'] * data.loc[:, 'edu_total'] * data.loc[:, 'school'] + \
                                    data.loc[:, 'B_ent'] * data.loc[:, 'ent_total'] * data.loc[:, 'leisure']) + \
                             data.loc[:, 'B_transit'] * data.loc[:, 'with_transit']
    data.loc[:, 'Utility_exp'] = np.exp(data.loc[:, 'Utility'])
    data.loc[:, 'probability'] = data.loc[:, "Utility_exp"] / \
                                 data.groupby(grouping_var)["Utility_exp"].transform("sum")

    #     # drop destinations with extremely low probability
    data = data.loc[data['probability'] > prob_cut]
    data.loc[:, 'probability'] = data.loc[:, "probability"] / \
                                 data.groupby(grouping_var)["probability"].transform("sum")
    return data


# define binary trip purpose for destination chocie model
trip_attraction.loc[:, 'school'] = 0
trip_attraction.loc[trip_attraction['TripPurposeID'] == 'school', 'school'] = 1

trip_attraction.loc[:, 'leisure'] = 0
trip_attraction.loc[trip_attraction['TripPurposeID'] == 'leisure', 'leisure'] = 1

trip_attraction_with_choice = destination_choice_model(trip_attraction,
                                                       dest_choice_param,
                                                       grouping_var,
                                                       prob_cut)
trip_attraction_with_choice.loc[:, 'TripGeneration'] *= trip_attraction_with_choice.loc[:, 'probability']
trip_attraction_with_choice.loc[:, 'TripGeneration'] = np.round(trip_attraction_with_choice.loc[:, 'TripGeneration'], 0)
trip_attraction_with_choice = \
    trip_attraction_with_choice.loc[trip_attraction_with_choice['TripGeneration'] > 0]
print('Trip Attraction with Choice:')
print(trip_attraction_with_choice.loc[:, 'TripGeneration'].sum())
trip_attraction_with_choice["distance"].plot(kind="hist", density=True,
                                             weights=trip_attraction_with_choice["TripGeneration"], bins=50)
plt.show()

## write output
output_var = ['GEOID', 'home_geotype', 'home_microtype', 'populationGroupType',
              'TripType', 'TripPurposeID',
              'destination', 'distance', 'TripGeneration']

trip_attraction_with_choice_out = trip_attraction_with_choice[output_var]
trip_attraction_with_choice_out['destination'] = trip_attraction_with_choice_out['destination'].apply(str)
trip_attraction_with_choice_out.to_csv('Output/' + selected_state + '/OD_home_based_trips_spillover.csv.zip')
print('OD Home Based Trips Spillover CSV file created')
# <codecell>