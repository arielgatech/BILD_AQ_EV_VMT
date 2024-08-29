#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug 24 15:30:58 2023

@author: xiaodanxu
"""

import pandas as pd
import os
from pandas import read_csv
from pandas import read_parquet
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
import numpy as np
import pyarrow
import fastparquet
from datetime import datetime, timezone, timedelta
import scipy.stats as s

warnings.filterwarnings("ignore")

work_dir = '/Users/xiaodanxu/Library/CloudStorage/GoogleDrive-arielinseu@gmail.com/My Drive/GEMS/BILD-AQ/data'
os.chdir(work_dir)

inrix_data_path = '/Volumes/LaCie/INRIX/data_by_state_v2'
# selected_state = 'NV'
plt.style.use('ggplot')
sns.set(font_scale=1.2)  # larger font
meter_to_mile = 0.000621371

# load spatial labels
microtype_file = 'ccst_geoid_key_tranps_geo_with_imputation.csv'
microtype_label = read_csv(microtype_file)

microtype_label_short = microtype_label[['GEOID', 'st_code']]

all_states = microtype_label_short.st_code.unique()

processed_states = ['CA', 'TX', 'WA', 'OR', 'AZ', 'NY', 'IL']
for selected_state in all_states:
    # if selected_state in processed_states:
    #     continue
    print('processing results from ' + selected_state)
# microtype_label_short.head(5)
    os.chdir(work_dir)
    plot_dir = 'Plot/' + selected_state
    if not os.path.exists(plot_dir):
        os.makedirs(plot_dir)
        
    out_dir = 'Network/' + selected_state
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
        
    dist_matrix_file = 'distance_matrix_by_tracts_' + selected_state + '.csv'
    cut_off = 100
    dist_matrix_long = pd.read_csv('Network/combined/' + dist_matrix_file, sep = ',')
    print(len(dist_matrix_long))
    dist_matrix_long = dist_matrix_long.drop(columns = ['Unnamed: 0'])
    dist_matrix_long = pd.merge(dist_matrix_long, microtype_label_short,
                          left_on = 'destination',
                          right_on = 'GEOID', how = 'left')
    
    dist_matrix_long = dist_matrix_long.loc[dist_matrix_long['st_code'] == selected_state]
    print(len(dist_matrix_long))
    dist_matrix_long = dist_matrix_long.loc[dist_matrix_long['distance'] <= cut_off]
    dist_matrix_long = dist_matrix_long.drop(columns = ['GEOID', 'st_code'])
    print(len(dist_matrix_long))
    dist_matrix_long.loc[:, 'origin'] = \
    dist_matrix_long.loc[:, 'origin'].astype(str).str.zfill(11) 
    dist_matrix_long.loc[:, 'destination'] = \
    dist_matrix_long.loc[:, 'destination'].astype(str).str.zfill(11) 
    # dist_matrix_long.head(5)
    
    # <codecell>
    # load INRIX data
    os.chdir(inrix_data_path)
    inrix_file_list = [file for file in os.listdir('state=' + selected_state) if (file.endswith('.parquet'))]
    
    inrix_travel_time = None
    for file in inrix_file_list:
        if file.startswith('._'):
            continue
        print(file)
        data = pd.read_parquet('state=' + selected_state + '/' + file, engine = 'auto')
        inrix_travel_time = pd.concat([inrix_travel_time, data])
        print(len(data))
    
    # post processing INRIX data
    inrix_travel_time = \
    inrix_travel_time.loc[inrix_travel_time['vehicle_weight_class'] == 1] # passenger vehicle
    inrix_travel_time_daily = \
    inrix_travel_time.groupby(['o_GEOID', 'd_GEOID'])[['travel_time_h', 'trip_distance_mile', 'trip_count']].sum()
    inrix_travel_time_daily = inrix_travel_time_daily.reset_index()
    print(len(inrix_travel_time_daily))
    
    # <codecell>
    inrix_travel_time_daily.loc[:, 'avg_speed'] = \
    inrix_travel_time_daily.loc[:, 'trip_distance_mile'] / \
    inrix_travel_time_daily.loc[:, 'travel_time_h']
    
    inrix_travel_time_daily.loc[:, 'distance_mile'] = \
    inrix_travel_time_daily.loc[:, 'trip_distance_mile'] / \
    inrix_travel_time_daily.loc[:, 'trip_count']
    
    inrix_travel_time_daily.loc[:, 'travel_time_h'] = \
    inrix_travel_time_daily.loc[:, 'travel_time_h'] / \
    inrix_travel_time_daily.loc[:, 'trip_count']
    
    inrix_travel_time_daily.loc[inrix_travel_time_daily['avg_speed'] >= 80, 'avg_speed'] = 80
    print(len(inrix_travel_time_daily))
    
    os.chdir(work_dir)
    sample_inrix_travel_time = inrix_travel_time_daily.sample(frac = 0.005)
    sample_inrix_travel_time = sample_inrix_travel_time[sample_inrix_travel_time['distance_mile'] <= 100]
    sns.lmplot(x="distance_mile", y="avg_speed",  
               data=sample_inrix_travel_time, lowess=True, 
               scatter_kws={'alpha':0.1},
               line_kws={'color': 'red'})
    # plt.xlim([0,100])
    plt.xlabel('routed trip distance (mile)')
    plt.ylabel('trip speed (mph)')
    plt.legend(['mean speed', 'raw data'])
    plt.title('speed imputation')
    plt.savefig('Plot/' + selected_state + '/INRIX_speed_imputation.png', 
                dpi = 200, bbox_inches = 'tight')
    plt.show()
    
    # <codecell>
    
    # fit linear model for distance
    dist_and_time_matrix = pd.merge(dist_matrix_long, 
                                    inrix_travel_time_daily, 
                                    left_on = ['origin', 'destination'],
                                    right_on = ['o_GEOID', 'd_GEOID'],
                                    how = 'left')
    print(len(dist_and_time_matrix))
    
    import statsmodels.api as sm
    dist_and_time_matrix_train = dist_and_time_matrix.dropna()
    X = dist_and_time_matrix_train['distance']
    y = dist_and_time_matrix_train['distance_mile']
    X2 = sm.add_constant(X)
    est = sm.OLS(y, X2)
    est2 = est.fit()
    print(est2.summary())
    
    lr_cons = est2.params['const']
    lr_dist = est2.params['distance']
    lr_r2 = est2.rsquared
    print(lr_cons, lr_dist, lr_r2)
    
    # <codecell>
    
    sample_inrix_travel_time_train = dist_and_time_matrix_train.sample(frac = 0.01)
    sns.lmplot(x="distance", y="distance_mile",  
               data = sample_inrix_travel_time_train, 
               scatter_kws={'alpha':0.1},
               line_kws={'color': 'red'})
    plt.xlim([0,100])
    plt.ylim([0,150])
    plt.xlabel('great circle trip distance (mile)')
    plt.ylabel('routed trip distance (mile)')
    lr_cons_short = np.round(lr_cons, 2)
    lr_dist_short = np.round(lr_dist, 2)
    lr_r2_short = np.round(lr_r2, 2)
    title = f"y={lr_cons_short} + {lr_dist_short}x, r2={lr_r2_short}"
    # plt.legend(['y=1.1998x+2.3218', 'raw data'])
    plt.title(title)
    plt.savefig('Plot/' + selected_state + '/INRIX_distance_imputation.png', 
                dpi = 200, bbox_inches = 'tight')
    plt.show()
    
    # <codecell>
    dist_and_time_matrix_train.loc[:, 'distance_bin'] = \
    dist_and_time_matrix_train.loc[:, 'distance_mile'].astype(int)
    mean_speed_lookup = \
    dist_and_time_matrix_train.groupby('distance_bin')[['avg_speed']].mean()
    mean_speed_lookup = mean_speed_lookup.reset_index()
    
    dist_and_time_matrix_to_fill = dist_and_time_matrix[dist_and_time_matrix.isna().any(axis=1)]
    dist_and_time_matrix_to_fill.loc[:, 'distance_mile'] = \
    lr_dist * dist_and_time_matrix_to_fill.loc[:, 'distance'] + lr_cons_short
    
    dist_and_time_matrix_to_fill.loc[:, 'distance_bin'] = \
    dist_and_time_matrix_to_fill.loc[:, 'distance_mile'].astype(int)
    
    dist_and_time_matrix_to_fill = dist_and_time_matrix_to_fill.drop(columns=['avg_speed'])
    dist_and_time_matrix_to_fill = pd.merge(dist_and_time_matrix_to_fill,
                                            mean_speed_lookup,
                                            on = 'distance_bin',
                                            how = 'left')
    
    max_speed = mean_speed_lookup.avg_speed.max()
    print(max_speed)
    dist_and_time_matrix_to_fill.loc[:, 'avg_speed'] = \
    dist_and_time_matrix_to_fill.loc[:, 'avg_speed'].fillna(max_speed)
    dist_and_time_matrix_to_fill.loc[:, 'travel_time_h'] = \
    dist_and_time_matrix_to_fill.loc[:, 'distance_mile'] / \
    dist_and_time_matrix_to_fill.loc[:, 'avg_speed']
    
    # <codecell>
    output_attr = ['origin', 'destination',	'distance',	 'distance_mile',  'travel_time_h', 'avg_speed']
    dist_and_time_matrix_to_fill = dist_and_time_matrix_to_fill[output_attr]
    dist_and_time_matrix_train = dist_and_time_matrix_train[output_attr]
    dist_and_time_matrix_out = pd.concat([dist_and_time_matrix_to_fill, 
                                          dist_and_time_matrix_train])
    print(sum(dist_and_time_matrix_out.isna().any(axis=1)))
    dist_and_time_matrix_out.to_csv('Network/' + selected_state + '/travel_time_skim_' + selected_state + '.csv',
                                    index = False)
    # break