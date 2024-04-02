#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr  1 16:32:53 2024

@author: xiaodanxu
"""

import geopy.distance
import pandas as pd
import numpy as np
import os
import csv
# import shapefile
import matplotlib.pyplot as plt

os.chdir('/Users/xiaodanxu/Library/CloudStorage/GoogleDrive-arielinseu@gmail.com/My Drive/GEMS/BILD-AQ/data')
select_state = 'CA'
file_path = 'Network/combined'
dist_mat_file = 'distance_matrix_by_tracts_' + select_state + '.csv'
df = pd.read_csv(os.path.join(file_path, dist_mat_file))

# <codecell>
# format GEOID
df.loc[:, 'origin'] = df.loc[:, 'origin'].astype(str).str.zfill(11)
df.loc[:, 'destination'] = df.loc[:, 'destination'].astype(str).str.zfill(11)

df.loc[:, 'orig_st'] = df.loc[:, 'origin'].str[:2]
df.loc[:, 'dest_st'] = df.loc[:, 'destination'].str[:2]

# for spillover, select OD belongs to different states
df.loc[:, 'external'] = 0
df.loc[df['orig_st'] != df['dest_st'], 'external'] = 1

df_out = df.loc[df['external'] == 1]

# <codecell>
min_dist_out = df.loc[df_out.groupby('origin')['distance'].idxmin()]
final_df = min_dist_out[['origin','destination','distance']].copy()
output_path = 'Network/' + select_state
final_df.to_csv(os.path.join(output_path,'tract_to_border_distance.csv'))