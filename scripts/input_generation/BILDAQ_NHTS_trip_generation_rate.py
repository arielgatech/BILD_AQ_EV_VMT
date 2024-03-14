#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep  6 15:00:09 2023

@author: xiaodanxu
"""

# set up python environment
import pyreadr
import pandas as pd
from pandas import read_csv
import os
from os import listdir
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import warnings
warnings.filterwarnings('ignore')

os.chdir('/Users/xiaodanxu/Library/CloudStorage/GoogleDrive-arielinseu@gmail.com/My Drive/GEMS/validation')

plt.style.use('ggplot')

# <codecell>
mode_choice_data_dir = 'mode_choice/'
supplement_data_dir = 'nhts_raw/'
output_plot_dir = 'plot/'

# gems_output_dir_2 = 'GEMS_output/LA_local/'
param_dir = 'parameter/'

mode_choice_data_source = 'NHTS/NHTS_tract_mode.split.National.RData'
mode_choice_geotype_id = 'NHTS/nhts_no_ids_1hrtimebins_with_imputation.csv'
mode_choice_microtype_id = 'NHTS/NHTS_tract_od.tr.purpose.National_transgeo.RData'
supplement_data_source = 'trippub.csv'
household_data_source = 'hhpub.csv'

time_period_file = 'TimePeriods_TEMPO.csv'
# CA_houshold_file = 'household_ca.RData'
national_geoid_lookup_file = 'ccst_geoid_key_tranps_geo_with_imputation.csv'
state_region_file = 'NHTS_region_state.csv'
distance_matrix = 'distance_matrix_by_tracts.csv'

nSubBins = 1
mps_to_mph = 2.23694
mode_lookup = {1: 'walk', 2: 'bike', 3: 'auto', 4: 'auto', 5: 'auto', 6: 'auto', 7: 'scooter', 8: 'scooter', 9: 'auto', 10: 'bus', 11: 'bus',
              12: 'bus', 13: 'bus', 14: 'bus', 15: 'rail', 16: 'rail', 17: 'taxi', 18: 'auto', 19: 'other',
              20: 'other', 97: 'other'}

# load NHTS dataset
mode_choice_data = pyreadr.read_r(mode_choice_data_dir + mode_choice_data_source)
mode_choice_data_df = mode_choice_data['mode.split']
print(mode_choice_data_df.columns)
od_microtype_data = pyreadr.read_r(mode_choice_data_dir + mode_choice_microtype_id)
od_microtype_data_df = od_microtype_data['od.tr.purpose']
print(od_microtype_data_df.columns)

mode_choice_additional = pd.read_csv(supplement_data_dir + supplement_data_source, sep = ',')
print(mode_choice_additional.columns)
household_weights = pd.read_csv(supplement_data_dir + household_data_source, sep = ',')
print(household_weights.columns)

mode_choice_geotype_df = pd.read_csv(mode_choice_data_dir + mode_choice_geotype_id, sep = ',')
print(mode_choice_geotype_df.columns)

# <codecell>

# generate NHTS variables
list_of_used_gems_variables = ['HOUSEID', 'PERSONID', 'TDTRPNUM', 'HHFAMINC', 
                               'WHYTRP1S', 'TRPMILES', 'STRTTIME','TRVLCMIN', 'TRIPPURP']

list_of_od_variables = ['HOUSEID', 'PERSONID', 'TDTRPNUM', 'ORIG_COUNTRY',
       'o_geoid', 'DEST_COUNTRY', 'd_geoid', 'o_microtype', 'o_geotype',
       'd_microtype', 'd_geotype']

list_of_hh_variables = ['HOUSEID', 'WTHHFIN', 'HBHUR']

list_of_geotype_variables = ['HOUSEID', 'h_geotype']

def gems_attributes_generator(NHTS_data, distance_bins, distance_bin_labels):
    NHTS_data.loc[:, 'populationGroupType'] = 'high-income'
    NHTS_data.loc[NHTS_data['hhfaminc']<= 5, 'populationGroupType'] = 'low-income'
    NHTS_data.loc[NHTS_data['hhfaminc'].isin([6,7,8]), 'populationGroupType'] = 'medium-income'
    NHTS_data.loc[:, 'trip_purpose'] = 'other'
    NHTS_data.loc[NHTS_data['whytrp1s'] == 1, 'trip_purpose'] = 'home'
    NHTS_data.loc[NHTS_data['whytrp1s'] == 10, 'trip_purpose'] = 'work'
    NHTS_data.loc[NHTS_data['whytrp1s'].isin([40, 50, 80]), 'trip_purpose'] = 'leisure'
    NHTS_data.loc[NHTS_data['whytrp1s'] == 20, 'trip_purpose'] = 'school'
    NHTS_data.loc[NHTS_data['whytrp1s'] == 30, 'trip_purpose'] = 'medical'
#     NHTS_data.loc[:, 'tripPurpose'] = 'nonwork'
#     NHTS_data.loc[NHTS_data['whytrp1s'].isin(work_trips), 'tripPurpose'] = 'work'    
    NHTS_data.loc[:, 'distanceBin'] = pd.cut(NHTS_data.loc[:, 'trpmiles'], distance_bins, 
                                                      labels = distance_bin_labels, ordered = False)
    return(NHTS_data)

def travel_time_calculator(NHTS_data):
    NHTS_data.loc[:, 'start hour'] = NHTS_data.loc[:, 'strttime'] / 100
    NHTS_data.loc[:, 'start hour'] = NHTS_data.loc[:, 'start hour'].astype(int)
    NHTS_data.loc[:, 'start hour'] = NHTS_data.loc[:, 'start hour'] + \
    (NHTS_data.loc[:, 'strttime'] %100) / 60.0

    # NHTS_data.loc[:, 'end hour'] = NHTS_data.loc[:, 'endtime'] / 100
    # NHTS_data.loc[:, 'end hour'] = NHTS_data.loc[:, 'end hour'].astype(int)
    # NHTS_data.loc[:, 'end hour'] = NHTS_data.loc[:, 'end hour'] + \
    # (NHTS_data.loc[:, 'endtime'] %100) / 60.0

    NHTS_data.loc[:, 'travel time'] = NHTS_data.loc[:, 'trvlcmin'] / 60.0
    # NHTS_data.loc[(NHTS_data['travel time'] < 0) & (NHTS_data['trpmiles'] < 1), 'travel time'] = 0
    # NHTS_data.loc[NHTS_data['travel time'] < 0, 'travel time'] += 24
    return(NHTS_data)
    

mode_choice_additional = mode_choice_additional.loc[:, list_of_used_gems_variables]
mode_choice_additional.columns= mode_choice_additional.columns.str.lower()
mode_choice_data_df = pd.merge(mode_choice_data_df, mode_choice_additional, 
                               on = ['houseid', 'personid', 'tdtrpnum'], how = 'left')

mode_choice_geotype_df = mode_choice_geotype_df.loc[:, list_of_geotype_variables]
mode_choice_geotype_df.columns= mode_choice_geotype_df.columns.str.lower()
mode_choice_geotype_df = mode_choice_geotype_df.drop_duplicates(keep = 'first')
mode_choice_data_df = pd.merge(mode_choice_data_df, mode_choice_geotype_df, 
                               on = ['houseid'], how = 'left')

od_variable_df = od_microtype_data_df.loc[:, list_of_od_variables]
od_variable_df.columns= od_variable_df.columns.str.lower()
mode_choice_data_df = pd.merge(mode_choice_data_df, od_variable_df, 
                               on = ['houseid', 'personid', 'tdtrpnum'], how = 'left')

hh_variable_df = household_weights.loc[:, list_of_hh_variables]
hh_variable_df.columns= hh_variable_df.columns.str.lower()
mode_choice_data_df = pd.merge(mode_choice_data_df, hh_variable_df, 
                               on = ['houseid'], how = 'left')

distance_bins = [0, 1.3, 3, 5, 8, 10, 20, mode_choice_data_df['trpmiles'].max()]
distance_bin_labels = ['1_1', '1_2', '1_3', '2_1', '2_2', '3_0', '4_0']

mode_choice_data_df = gems_attributes_generator(mode_choice_data_df, distance_bins, distance_bin_labels)
mode_choice_data_df = travel_time_calculator(mode_choice_data_df)
print(mode_choice_data_df.trip_purpose.unique())

# <codecell>

# change directory to bild aq
os.chdir('/Users/xiaodanxu/Library/CloudStorage/GoogleDrive-arielinseu@gmail.com/My Drive/GEMS/BILD-AQ/data')
input_dir = 'Input/'
network_dir = 'Network/'
plot_dir = 'Plot/'
# generate population
national_geoid_lookup = pd.read_csv(national_geoid_lookup_file, sep = ',')

list_of_population_variables= ['houseid', 'home_geoid', 
                               'populationGroupType',
                              'wthhfin']
population_trips = mode_choice_data_df.loc[:, list_of_population_variables]
population_trips = population_trips.drop_duplicates()
population_trips = pd.merge(population_trips, national_geoid_lookup,
                            left_on = 'home_geoid', right_on = 'GEOID',
                            how = 'left')
population_by_tract = population_trips.groupby(['st_code', 'geotype', 
                               'microtype', 'populationGroupType'])[['wthhfin']].sum()
population_by_tract.columns =['NHTS_households']
population_by_tract = population_by_tract.reset_index()
# population_by_tract.loc[:, 'home_geoid'] = population_by_tract.loc[:, 'home_geoid'].astype(int)
print(len(population_by_tract))
population_by_tract.to_csv(input_dir + 'NHTS_population.csv', index = False)

# <codecell>

# assign trip tag and O-D attributes
mode_choice_data_df.loc[:, 'h_origin'] = 1 * (mode_choice_data_df.loc[:, 'o_geoid'] == mode_choice_data_df.loc[:, 'home_geoid']) + \
0 * (mode_choice_data_df.loc[:, 'o_geoid'] != mode_choice_data_df.loc[:, 'home_geoid'])

mode_choice_data_df.loc[:, 'h_dest'] = 1 * (mode_choice_data_df.loc[:, 'd_geoid'] == mode_choice_data_df.loc[:, 'home_geoid']) + \
0 * (mode_choice_data_df.loc[:, 'd_geoid'] != mode_choice_data_df.loc[:, 'home_geoid'])

mode_choice_data_df.loc[:, 'nhb'] = 1 * (mode_choice_data_df.loc[:, 'trippurp'] == 'NHB') + \
0 * (mode_choice_data_df.loc[:, 'trippurp'] != 'NHB')


mode_choice_data_df.loc[:, 'trip_tag'] = 'nhb'
mode_choice_data_df.loc[mode_choice_data_df['h_origin'] == 1, 'trip_tag'] = 'origin'
mode_choice_data_df.loc[mode_choice_data_df['h_dest'] == 1, 'trip_tag'] = 'dest'
print(mode_choice_data_df.trip_tag.unique())
mode_choice_data_df.loc[:, ['h_origin', 'h_dest', 'nhb', 'trip_tag']].head(10)

# assign tour end micro-geotype ID
mode_choice_data_df.loc[:, 'dest_geotype'] = \
mode_choice_data_df.loc[:, 'o_geotype']
mode_choice_data_df.loc[:, 'dest_microtype'] = \
mode_choice_data_df.loc[:, 'o_microtype']
mode_choice_data_df.loc[:, 'dest_geoid'] = \
mode_choice_data_df.loc[:, 'o_geoid']

mode_choice_data_df.loc[:, 'orig_geotype'] = \
mode_choice_data_df.loc[:, 'd_geotype']
mode_choice_data_df.loc[:, 'orig_microtype'] = \
mode_choice_data_df.loc[:, 'd_microtype']
mode_choice_data_df.loc[:, 'orig_geoid'] = \
mode_choice_data_df.loc[:, 'd_geoid']

criteria1 = mode_choice_data_df['trip_tag'].isin(['origin', 'nhb'])

mode_choice_data_df.loc[criteria1, 'dest_geotype'] = \
mode_choice_data_df.loc[criteria1, 'd_geotype']
mode_choice_data_df.loc[criteria1, 'dest_microtype'] = \
mode_choice_data_df.loc[criteria1, 'd_microtype']
mode_choice_data_df.loc[criteria1, 'dest_geoid'] = \
mode_choice_data_df.loc[criteria1, 'd_geoid']

mode_choice_data_df.loc[criteria1, 'orig_geotype'] = \
mode_choice_data_df.loc[criteria1, 'd_geotype']
mode_choice_data_df.loc[criteria1, 'orig_microtype'] = \
mode_choice_data_df.loc[criteria1, 'd_microtype']
mode_choice_data_df.loc[criteria1, 'orig_geoid'] = \
mode_choice_data_df.loc[criteria1, 'o_geoid']

# <codecell>

# aggregating trip by home micro-geotype, trip purpose, 
# time bin, distance bin and income group

state_region_lookup = read_csv(input_dir + state_region_file)
mode_choice_data_df.loc[:, 'mode'] = mode_choice_data_df.loc[:, 'trptrans'].map(mode_lookup)
car_data_df = mode_choice_data_df.loc[mode_choice_data_df['mode'] == 'auto']
# car_data_df = car_data_df.loc[car_data_df['st_code'].isin(['AK', 'HI', 'OR', 'WA'])]

# plt.ylim([0, 0.03])
# plt.show()
car_data_df = car_data_df[['home_geoid', 'o_geoid', 'd_geoid', 
                           'o_geotype', 'o_microtype', 
                           'dest_geotype','dest_microtype',  
                           'populationGroupType', 'trip_tag', 
                           'start hour', 'travel time', 
                           'trip_purpose', 'trpmiles', 'distanceBin', 'wtperfin']]
car_data_df = pd.merge(car_data_df, national_geoid_lookup,
                            left_on = 'home_geoid', right_on = 'GEOID',
                            how = 'left')
car_data_df.to_csv(input_dir + 'NHTS_car_trips.csv', index = False)

car_data_df_no_loc = car_data_df[['geotype', 'microtype', 
                                  'o_geotype', 'o_microtype', 
                           'dest_geotype','dest_microtype',  
                           'populationGroupType', 'trip_tag', 
                           'start hour', 'travel time', 
                           'trip_purpose', 'trpmiles', 'distanceBin', 'wtperfin']]
car_data_df_no_loc.to_csv(input_dir + 'NHTS_car_trips_no_loc.csv', index = False)

# <codecell>
unique_state = state_region_lookup['state'].unique()
for st in unique_state:
    region_code = str(state_region_lookup.loc[state_region_lookup['state'] == st, 'region'].tolist()[0])
    
    list_of_neighboring_states = \
        state_region_lookup.loc[state_region_lookup['region'] == region_code, 'state'].unique()
    print(region_code, list_of_neighboring_states)
    car_data_df_sel = car_data_df.loc[car_data_df['st_code'].isin(list_of_neighboring_states)]
    home_based_trips = \
        car_data_df_sel.loc[car_data_df_sel['trip_tag'].isin(['origin', 'dest'])]
    nonhome_based_trips = \
        car_data_df_sel.loc[car_data_df_sel['trip_tag'] == 'nhb']
    print(home_based_trips.wtperfin.sum()) # about 75%
    print(nonhome_based_trips.wtperfin.sum()) # about 25%
    
    # assign ditance for non-home trips
    filelist = []
    for item in list_of_neighboring_states:
        filelist.append(network_dir + 'combined/distance_matrix_by_tracts_' + item + '.csv')
    print(filelist)
    distance_matrix = pd.concat([read_csv(f) for f in filelist ])

    list_of_home_tracts = nonhome_based_trips['home_geoid'].unique()
    distance_matrix = \
        distance_matrix.loc[distance_matrix['origin'].isin(list_of_home_tracts)]

    distance_matrix.loc[:, ['origin', 'destination']] = \
        distance_matrix.loc[:, ['origin', 'destination']].astype(int)

  
    nonhome_based_trips = pd.merge(nonhome_based_trips, distance_matrix,
                                   left_on = ['home_geoid', 'o_geoid'],
                                   right_on = ['origin', 'destination'], 
                                   how = 'left')
    
    distance_bins = [0, 1.3, 3, 5, 8, 10, 20, nonhome_based_trips['distance'].max()]
    distance_bin_labels = ['1_1', '1_2', '1_3', '2_1', '2_2', '3_0', '4_0']
    
    nonhome_based_trips.loc[:, 'distanceBin_home'] = pd.cut(nonhome_based_trips.loc[:, 'distance'], distance_bins, 
                                                       labels = distance_bin_labels, ordered = False)
    
    # generating trip rates for home-based trips
    result_dir = input_dir + st
    if not os.path.exists(result_dir):
        os.mkdir(result_dir)
    visual_dir = input_dir + st
    if not os.path.exists(visual_dir):
        os.mkdir(visual_dir)
    grouping_var = ['geotype', 'microtype', 'populationGroupType',
                'dest_geotype','dest_microtype',  
                'trip_tag', 'trip_purpose', 'distanceBin']
                               
    home_based_trips_agg = home_based_trips.groupby(grouping_var)[['wtperfin']].sum()
    home_based_trips_agg.columns = ['NHTS_trips']
    home_based_trips_agg = home_based_trips_agg.reset_index()
    hb_output_file = 'NHTS_home_based_trips_' + st + '.csv'
    home_based_trips_agg.to_csv(os.path.join(result_dir, hb_output_file), index = False)

    # pairing home and non-home based trips to generate VMT fraction
    home_based_trips.loc[:, 'DistanceBinID'] = \
    home_based_trips.loc[:, 'distanceBin'].str.split('_').str[0]
    
    home_var = ['dest_geotype', 'dest_microtype', 'DistanceBinID']
    home_based_trips.loc[:, 'VMT'] = home_based_trips.loc[:, 'trpmiles'] * home_based_trips.loc[:, 'wtperfin']
    home_trips = home_based_trips.groupby(home_var)[['VMT']].sum()
    home_trips.columns = ['home VMT']
    home_trips = home_trips.reset_index()
    # home_trips.head(5)
    
    nonhome_based_trips.loc[:, 'DistanceBinID'] = \
    nonhome_based_trips.loc[:, 'distanceBin_home'].str.split('_').str[0]
    
    nonhome_var = ['o_geotype', 'o_microtype', 'DistanceBinID']
    nonhome_based_trips.loc[:, 'VMT'] = nonhome_based_trips.loc[:, 'trpmiles'] * nonhome_based_trips.loc[:, 'wtperfin']
    nonhome_trips = nonhome_based_trips.groupby(nonhome_var)[['VMT']].sum()
    nonhome_trips.columns = ['nonhome VMT']
    nonhome_trips = nonhome_trips.reset_index()
    # nonhome_trips.head(5)
    
    nonhome_fraction = pd.merge(home_trips, nonhome_trips,
                                left_on = home_var, 
                                right_on = nonhome_var, how = 'left')

    nonhome_fraction = nonhome_fraction.loc[(nonhome_fraction['home VMT'] > 0) & (nonhome_fraction['nonhome VMT'] > 0)]
    print(nonhome_fraction[['home VMT', 'nonhome VMT']].sum())
    nonhome_fraction.loc[:, 'nhb_fraction_VMT'] = nonhome_fraction.loc[:, 'nonhome VMT'] / \
    nonhome_fraction.loc[:, 'home VMT']

    # nonhome_fraction.head(5)
    sns.boxplot(data = nonhome_fraction, x = 'DistanceBinID', y = 'nhb_fraction_VMT', 
                showfliers = False)
    plt.savefig(os.path.join(visual_dir, 'nhb_fraction_by_dist_bin.png'), dpi = 200)
    plt.show()
    nonhome_fraction = nonhome_fraction[['o_geotype', 'o_microtype', 'DistanceBinID', 'nhb_fraction_VMT']]
    nhb_output_file = 'NHTS_nonhome_VMT_fraction_' + st + '.csv'
    nonhome_fraction.to_csv(os.path.join(result_dir, nhb_output_file), index = False)          
    # break

