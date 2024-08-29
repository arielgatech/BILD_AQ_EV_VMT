# load packages
import os
from pandas import read_csv
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import time
import copy
#^ packages needed for GEMS

##########################################################################################
##########         Part 1: set up scenario parameters and load inputs           ##########
##########                                                                      ########## 
##########################################################################################

#work directory is where the I-O data are stored
work_dir = '/Users/xiaodanxu/Library/CloudStorage/GoogleDrive-arielinseu@gmail.com/My Drive/GEMS/BILD-AQ/deliverables'
os.chdir(work_dir)
plt.style.use('ggplot')
sns.set(font_scale=1.4)  # larger font
total_start_time = time.time()


# <codecell>

# define scenario variables
region_code = 'WECC'
analysis_years = [2018, 2030, 2040]
align_scenarios = {
    # 'base-nevi': 'NEVI only', 
    #                'base-base': 'Baseline', 
                   'ira-base': 'Baseline',
                   'ira-nevi': 'NEVI'}

# load all TEMPO data
input_dir = os.path.join('input', region_code, 'TEMPO')
list_of_states = os.listdir(input_dir)

tempo_result = None

to_exlude = ['.DS_Store', 'V1_FEB2024', 'V2_APR2024']
for state in list_of_states:
    if state in to_exlude:
        continue
    print(state)
    
    file_name = state + '-tract-allscens.csv'
    tempo_result_st = read_csv(os.path.join(input_dir, state, file_name))
    tempo_result_st.loc[:, 'State'] = state
    tempo_result_st = \
        tempo_result_st.loc[tempo_result_st['Year'].isin(analysis_years)] 
    #keep TEMPO output only for analysis years 
    tempo_result = pd.concat([tempo_result, tempo_result_st])
# state_name = 'CA'

tempo_result['Scenario'] = tempo_result['Scenario'].astype(str)
print(tempo_result['Scenario'].unique())
tempo_result['Scenario'] = tempo_result['Scenario'].map(align_scenarios)
print(tempo_result['Scenario'].unique())
# print(tempo_result.Scenario.unique())

# <codecell>
# check total vehicle count by year and scenario
tempo_result_summary = pd.pivot_table(tempo_result, values = 'Vehicles', 
                                      index = ['State', 'Year', 'Scenario'], columns = 'Tech',
                                      aggfunc = np.sum)

tempo_result_summary = tempo_result_summary.reset_index()
tempo_result_summary.to_csv('./Output/' + region_code + '/TEMPO_veh_count.csv', index = False)



# <codecell>
# aggregate TEMPO results
scenarios = tempo_result['Scenario'].unique()
navigat_sce_map = {'Baseline': 'baseline', 'NEVI': 'scenario'}
for analysis_year in analysis_years:
    year_start_time = time.time()
    output_name = region_code + '_' + str(analysis_year)
    print(output_name)
    for scenario_name in scenarios:
        print(scenario_name)
        # select TEMPO output under selected scenario
        tempo_result_filtered = copy.deepcopy(tempo_result.loc[tempo_result['Year'] == analysis_year])
        tempo_result_filtered = tempo_result_filtered.loc[tempo_result_filtered['Scenario'] == scenario_name]

        # generate 11-digit GEOID
        tempo_result_filtered.loc[:, 'Region'] = tempo_result_filtered.loc[:, 'Region'].astype(str).str.zfill(5)
        tempo_result_filtered.loc[:, 'tract'] = tempo_result_filtered.loc[:, 'tract'].astype(int)
        tempo_result_filtered.loc[:, 'tract'] = tempo_result_filtered.loc[:, 'tract'].astype(str).str.zfill(6)
        tempo_result_filtered.loc[:, 'home_GEOID'] = tempo_result_filtered.loc[:, 'Region'].astype(str) + \
        tempo_result_filtered.loc[:, 'tract']
        tempo_result_filtered['Vehicles'] = \
            tempo_result_filtered['Vehicles'].astype(int)
        home_veh = tempo_result_filtered.groupby('home_GEOID')[['Vehicles']].sum()
        home_veh = home_veh.reset_index()

        # create list of census tracts with zero vehicle ownership
        home_veh_zero = tempo_result_filtered.groupby(['Region', 'tract', 'home_GEOID'])[['Vehicles']].sum()
        home_veh_zero = home_veh_zero.loc[home_veh_zero['Vehicles'] == 0]
        home_veh_zero = home_veh_zero.reset_index()
        # print(home_veh_zero.head(5))
        home_veh_zero.to_csv('./Output/'+ region_code + '/TEMPO_tract_with_no_veh.csv')

        # generate fleet composition by home tract
        fleet_by_home_tract = pd.pivot_table(tempo_result_filtered, 
                                             values = 'Vehicles', index = ['State','home_GEOID'],
                                             columns = ['Tech'], aggfunc = np.sum)
        list_of_veh_tech = fleet_by_home_tract.columns
        
        fleet_by_home_tract.loc[:, 'Total'] = fleet_by_home_tract.loc[:, list_of_veh_tech].sum(axis = 1)
        # fill census tract with 0 vehicles, assume 1 hypothetical ICEV to ensure VMT can be assigned
        fleet_by_home_tract.loc[fleet_by_home_tract['Total'] == 0, 'ICEV_Gasoline'] = 1
        fleet_by_home_tract.loc[fleet_by_home_tract['Total'] == 0, 'Total'] = 1

        for tech in list_of_veh_tech:
            #print(tech)
            fleet_by_home_tract.loc[:, tech] = \
                fleet_by_home_tract.loc[:, tech] / fleet_by_home_tract.loc[:, 'Total']
        fleet_by_home_tract = fleet_by_home_tract.reset_index()
        print(fleet_by_home_tract.head(5))
        output_path = os.path.join('Output', region_code, 'EV_penetration')
        output_file = 'EV_penetration_' + output_name + '_' + scenario_name + '.csv'
        fleet_by_home_tract.to_csv(os.path.join(output_path, output_file), 
                                   index = False)

        # Add VMT file processes
        VMT_output_by_scenario = None
        for state in list_of_states:
            if state in to_exlude:
                continue
            print(state)
            vmt_input_dir = os.path.join('input', region_code, 'NAVIGAT')
            vmt_file_name = 'VMT_' + state + '_' + str(analysis_year) + '_' + navigat_sce_map[scenario_name] + '.csv'
            vmt_output_by_state = read_csv(os.path.join(vmt_input_dir, state, vmt_file_name))
            vmt_output_by_state.loc[:, 'State'] = state
            VMT_output_by_scenario = pd.concat([VMT_output_by_scenario, vmt_output_by_state])
        vmt_output_path = os.path.join('Output', region_code, 'EV_VMT')
        vmt_output_file = 'raw_VMT_' + output_name + '_' + scenario_name + '.csv'
        VMT_output_by_scenario.to_csv(os.path.join(vmt_output_path, vmt_output_file), 
                                   index = False)        
        



