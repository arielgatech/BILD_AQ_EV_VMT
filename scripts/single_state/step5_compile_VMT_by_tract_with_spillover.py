#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 27 14:19:08 2022

@author: xiaodanxu
"""

from pandas import read_csv
import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import seaborn as sns
import shapely.wkt
import geopandas as gpd
import contextily as cx
from sklearn.metrics import mean_squared_error, r2_score 
import matplotlib

plt.style.use('ggplot')

path_to_prj = '/Users/xiaodanxu/Library/CloudStorage/GoogleDrive-arielinseu@gmail.com/My Drive/GEMS/BILD-AQ/data'
os.chdir(path_to_prj)

selected_state = 'CA'
# load input
home_VMT_file = 'home_daily_vmt_by_tracts.csv'
home_VMT_by_tract = read_csv('Output/' + selected_state + '/' + home_VMT_file, sep = ',')

nonhome_VMT_file = 'nonhome_daily_vmt_by_tracts.csv'
nonhome_VMT_by_tract = read_csv('Output/' + selected_state + '/' + nonhome_VMT_file, sep = ',')

home_spillover_VMT_file = 'home_daily_vmt_spillover.csv'
home_spillover_VMT_by_tract = read_csv('Output/' + selected_state + '/' + home_spillover_VMT_file, sep = ',')

nonhome_spillover_VMT_file = 'nhb_vmt_spillover.csv'
nonhome_spillover_VMT_by_tract = read_csv('Output/' + selected_state + '/' + nonhome_spillover_VMT_file, sep = ',')

state_tract_file = 'census_tracts_2017.geojson'
state_tracts_geojson =  gpd.read_file('Network/combined/' + state_tract_file)

state_HPMS_file = selected_state + '_HPMS_with_GEOID_LANEMILE.geojson'
state_HMPS_geojson = gpd.read_file('Network/' + selected_state + '/' + state_HPMS_file)

# <codecell>
nonhome_spillover_VMT_by_tract = \
    nonhome_spillover_VMT_by_tract.rename(columns = {'thru_GEOID': 'GEOID', 
                                                     'thru_geotype': 'geotype',
                                                     'thru_microtype': 'microtype',
                                                     'nhb_VMT': 'VMT'})
group_var = ['GEOID', 'geotype', 'microtype', 'home_GEOID', 'home_geotype',
       'home_microtype', 'populationGroupType']
nonhome_spillover_VMT_by_tract = \
    nonhome_spillover_VMT_by_tract.groupby(group_var)[['VMT']].sum()   
nonhome_spillover_VMT_by_tract = \
    nonhome_spillover_VMT_by_tract.reset_index()
# <codecell>
VMT_by_tract = pd.concat([home_VMT_by_tract, nonhome_VMT_by_tract, 
                          home_spillover_VMT_by_tract, nonhome_spillover_VMT_by_tract])
VMT_by_tract_home = VMT_by_tract.groupby(['GEOID', 'geotype', 'microtype', 'home_GEOID', 'home_geotype',
       'home_microtype', 'populationGroupType'])[['VMT']].sum()
VMT_by_tract_home = VMT_by_tract.reset_index()

# VMT_by_tract_nhb = pd.concat([home_VMT_by_tract, nonhome_VMT_by_tract, 
#                           home_spillover_VMT_by_tract, nonhome_spillover_VMT_by_tract])
# VMT_by_tract_nonhome = VMT_by_tract_nhb.groupby(['GEOID', 'geotype', 'microtype'])[['VMT']].sum()
# VMT_by_tract_nonhome = VMT_by_tract_nonhome.reset_index()

VMT_by_tract = VMT_by_tract_home.groupby(['GEOID', 'geotype', 'microtype'])[['VMT']].sum()
VMT_by_tract = VMT_by_tract.reset_index()

# <codecell>
# plot VMT with only home-based spillover
state_tracts_geojson.loc[:, 'GEOID'] = state_tracts_geojson.loc[:, 'GEOID'].astype(int)
state_tracts_geojson = state_tracts_geojson.merge(VMT_by_tract, on='GEOID', how='inner')

ax = state_tracts_geojson.plot(figsize = (10,6), column = 'VMT', alpha = 0.6, legend=True,
                            norm=matplotlib.colors.LogNorm(vmin = 1, 
                                                           vmax = state_tracts_geojson.VMT.max()))
cx.add_basemap(ax, crs = 'EPSG:4326', source = cx.providers.Stamen.TonerLite)
plt.title('VMT by tract')
plt.savefig('Plot/' + selected_state + '/BILDAQ_VMT_by_tract_with_all_spillover.png', dpi = 200)

# <codecell>
# plot VMT with all spillover
# VMT_by_tract_nhb = None
# state_tracts_geojson.loc[:, 'GEOID'] = state_tracts_geojson.loc[:, 'GEOID'].astype(int)
# state_tracts_geojson = state_tracts_geojson.merge(VMT_by_tract_nonhome, on='GEOID', how='inner')

# ax = state_tracts_geojson.plot(figsize = (10,6), column = 'VMT', alpha = 0.6, legend=True,
#                             norm=matplotlib.colors.LogNorm(vmin = 1, 
#                                                            vmax = state_tracts_geojson.VMT.max()))
# cx.add_basemap(ax, crs = 'EPSG:4326', source = cx.providers.Stamen.TonerLite)
# plt.title('VMT by tract')
# plt.savefig('Plot/' + selected_state + '/BILDAQ_VMT_by_tract_with_all_spillover.png', dpi = 200)
# <codecell>
meter_to_mile = 0.000621371
# print(CA_HMPS_geojson.columns)
state_HMPS_df = pd.DataFrame(state_HMPS_geojson.drop(columns='geometry'))
state_HMPS_df.loc[:, 'AADT_CAR'] = state_HMPS_df.loc[:, 'AADT'] - state_HMPS_df.loc[:, 'AADT_Singl'] - state_HMPS_df.loc[:, 'AADT_Combi']
state_HMPS_df.loc[:, 'HPMS_VMT'] = state_HMPS_df.loc[:, 'AADT_CAR'] * state_HMPS_df.loc[:, 'Length'] * meter_to_mile
state_HMPS_by_tract = state_HMPS_df.groupby(['GEOID'])[['HPMS_VMT']].sum()
state_HMPS_by_tract = state_HMPS_by_tract.reset_index()
state_HMPS_by_tract['GEOID'] = state_HMPS_by_tract['GEOID'].astype(int)

state_tracts_geojson = state_tracts_geojson.merge(state_HMPS_by_tract, on='GEOID', 
                                                  how='left')

# <codecell>
state_tracts_geojson_in_state = state_tracts_geojson.loc[~state_tracts_geojson['HPMS_VMT'].isna()]
state_tracts_geojson_out_state = state_tracts_geojson.loc[state_tracts_geojson['HPMS_VMT'].isna()]

ax = state_tracts_geojson_in_state.plot(figsize = (10,6), column = 'HPMS_VMT', alpha = 0.6, legend=True,
                            norm=matplotlib.colors.LogNorm(vmin = 1, vmax = state_tracts_geojson.HPMS_VMT.max()))
cx.add_basemap(ax, crs = 'EPSG:4326', source = cx.providers.Stamen.TonerLite)
plt.title('VMT by tract')
plt.savefig('Plot/' + selected_state + '/HPMS_VMT_by_tract.png', dpi = 200)

# <codecell>
# from sklearn.metrics import mean_absolute_percentage_error
print(state_tracts_geojson_in_state[['HPMS_VMT', 'VMT']].sum())
# f, ax = plt.subplots(figsize=(5, 5))
# ax.set(xscale="log", yscale="log")
sns.lmplot(data = state_tracts_geojson_in_state, x="HPMS_VMT", y="VMT",  
           scatter = False, line_kws={'color': 'darkred'})
plt.xlim([10, 2000000])
plt.ylim([10, 2000000])

# plt.xscale('log')
# plt.yscale('log')
plt.xlabel('HPMS VMT by tract')
plt.ylabel('BILD-AQ/GEMS VMT by tract')
plt.savefig('Plot/' + selected_state + '/VMT_comparison_by_tract.png', dpi = 200)


ax = sns.scatterplot(data = state_tracts_geojson_in_state, 
                     x="HPMS_VMT", y="VMT", alpha = 0.2)
plt.xlim([10, 2000000])
plt.ylim([10, 2000000])
diag_line, = ax.plot(ax.get_xlim(), ax.get_ylim(), ls="--", c="0.5")
# plt.xscale('log')
# plt.yscale('log')
plt.xlabel('HPMS VMT by tract')
plt.ylabel('BILD-AQ/GEMS VMT by tract')
plt.savefig('Plot/' + selected_state + '/VMT_comparison_by_tract_with_diagline.png', 
            dpi = 200, bbox_inches = 'tight')

rmse_vmt = mean_squared_error(state_tracts_geojson_in_state['HPMS_VMT'], 
                              state_tracts_geojson_in_state['VMT'], squared = False)
r2_vmt = r2_score(state_tracts_geojson_in_state['HPMS_VMT'], 
                  state_tracts_geojson_in_state['VMT'])

VMT_adj_factor = state_tracts_geojson_in_state['HPMS_VMT'].sum()/ \
                  state_tracts_geojson_in_state['VMT'].sum()
# mape_vmt = mean_absolute_percentage_error(CA_tracts_geojson['HPMS_VMT'], 
#                               CA_tracts_geojson['VMT'])
print('RMSE for tract-level VMT is ' + str(rmse_vmt))
print('R2 for tract-level VMT is ' + str(r2_vmt))
print('mean VMT per tract is ' )
print(state_tracts_geojson['HPMS_VMT'].mean())
# print('MAPE for tract-level VMT is ' + str(mape_vmt))
# <codecell>

# split HMPS VMT by household
grouping_var = ['GEOID', 'geotype', 'microtype']

state_HMPS_by_home_tract = pd.merge(VMT_by_tract_home, state_HMPS_by_tract,
                                 on = 'GEOID', how = 'left')

state_VMT_spillover = state_HMPS_by_home_tract.loc[state_HMPS_by_home_tract['HPMS_VMT'].isna()]
state_HMPS_by_home_tract = state_HMPS_by_home_tract.loc[~state_HMPS_by_home_tract['HPMS_VMT'].isna()]

# this step is moved after merge to only assign in-state VMT
state_HMPS_by_home_tract.loc[:, 'VMT_frac'] = state_HMPS_by_home_tract.loc[:, 'VMT'] / \
    state_HMPS_by_home_tract.groupby(grouping_var)['VMT'].transform("sum")
# <codecell>

# HPMS VMT for in-state travel
state_HMPS_by_home_tract.loc[:, 'HPMS_VMT'] *= state_HMPS_by_home_tract.loc[:, 'VMT_frac']
state_HMPS_by_home_tract = state_HMPS_by_home_tract[['GEOID', 'geotype', 'microtype', 'home_GEOID',
       'home_geotype', 'home_microtype', 'populationGroupType', 'HPMS_VMT']]

state_HMPS_by_home_tract = state_HMPS_by_home_tract.dropna(subset = ['GEOID', 'home_GEOID'])
state_HMPS_by_home_tract[['GEOID', 'home_GEOID']] = state_HMPS_by_home_tract[['GEOID', 'home_GEOID']].astype(int)
state_HMPS_by_home_tract[['GEOID', 'home_GEOID']] = state_HMPS_by_home_tract[['GEOID', 'home_GEOID']].astype(str)
state_HMPS_by_home_tract['GEOID'] = state_HMPS_by_home_tract['GEOID'].str.zfill(11)
state_HMPS_by_home_tract['home_GEOID'] = state_HMPS_by_home_tract['home_GEOID'].str.zfill(11)

state_HMPS_by_home_tract.columns = ['thru_GEOID', 'thru_geotype', 'thru_microtype', 
                                 'home_GEOID', 'home_geotype', 'home_microtype', 'populationGroupType', 'VMT']

# <codecell>

# append out-of-state_travel
state_VMT_spillover.loc[:, 'HPMS_VMT'] = \
    state_VMT_spillover.loc[:, 'VMT'] * VMT_adj_factor

state_VMT_spillover = state_VMT_spillover[['GEOID', 'geotype', 'microtype', 'home_GEOID',
       'home_geotype', 'home_microtype', 'populationGroupType', 'HPMS_VMT']]

state_VMT_spillover = state_VMT_spillover.dropna(subset = ['GEOID', 'home_GEOID'])
state_VMT_spillover[['GEOID', 'home_GEOID']] = state_VMT_spillover[['GEOID', 'home_GEOID']].astype(int)
state_VMT_spillover[['GEOID', 'home_GEOID']] = state_VMT_spillover[['GEOID', 'home_GEOID']].astype(str)
state_VMT_spillover['GEOID'] = state_VMT_spillover['GEOID'].str.zfill(11)
state_VMT_spillover['home_GEOID'] = state_VMT_spillover['home_GEOID'].str.zfill(11)

state_VMT_spillover.columns = ['thru_GEOID', 'thru_geotype', 'thru_microtype', 
                                 'home_GEOID', 'home_geotype', 'home_microtype', 'populationGroupType', 'VMT']


# <codecell>
state_HMPS_by_home_tract = pd.concat([state_HMPS_by_home_tract, state_VMT_spillover])
# sample_HMPS_by_home_tract = state_HMPS_by_home_tract.head(1000)
# sample_HMPS_by_home_tract.to_csv('Output/' + selected_state + '/sample_VMT_by_tract_with_spillover.csv', index = False)
state_HMPS_by_home_tract.to_csv('Output/' + selected_state + '/BILDAQ_VMT_by_tract_with_spillover.csv', index = False)
print(state_HMPS_by_home_tract.head(5))

# <codecell>
sample_HMPS_by_home_tract = state_VMT_spillover.sample(1000)
sample_HMPS_by_home_tract.to_csv('Output/' + selected_state + '/sample_VMT_by_tract_with_spillover.csv', index = False)
# plot final CA results with scaled spillover
state_tract_file = 'census_tracts_2017.geojson'
state_tracts_geojson =  gpd.read_file('Network/combined/' + state_tract_file)

VMT_by_tract = \
    state_HMPS_by_home_tract.groupby(['thru_GEOID', 'thru_geotype', 'thru_microtype'])[['VMT']].sum()
VMT_by_tract = VMT_by_tract.reset_index()
VMT_by_tract.columns = ['GEOID', 'geotype', 'microtype', 'VMT']
# state_tracts_geojson.loc[:, 'GEOID'] = state_tracts_geojson.loc[:, 'GEOID'].astype(int)
state_tracts_geojson = state_tracts_geojson.merge(VMT_by_tract, on='GEOID', how='inner')

ax = state_tracts_geojson.plot(figsize = (10,6), column = 'VMT', alpha = 0.6, legend=True,
                            norm=matplotlib.colors.LogNorm(vmin = 1, 
                                                           vmax = state_tracts_geojson.VMT.max()))
cx.add_basemap(ax, crs = 'EPSG:4326', source = cx.providers.Stamen.TonerLite)
plt.title('VMT by tract')
plt.savefig('Plot/' + selected_state + '/BILDAQ_VMT_by_tract_with_scaled_spillover.png', dpi = 200)

