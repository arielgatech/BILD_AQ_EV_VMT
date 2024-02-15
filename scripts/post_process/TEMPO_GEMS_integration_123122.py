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
sns.set(font_scale=1.2)  # larger font
total_start_time = time.time()

state_name = 'CA'
analysis_years = [2032]

link_to_tempo_output = './Input/' + state_name + '/tract_output_nevi.csv'
link_to_gems_output = './Input/' + state_name + '/BILDAQ_VMT_by_tract_multistate_spillover.csv'
GEMS_output_files = []

# load TEMPO output
tempo_result = read_csv(link_to_tempo_output, sep = ',') #it is a pd dataframe at this point
# print(tempo_result.Scenario.unique())
align_scenarios = {'nevi': 'NEVI', 'base': 'Baseline', 'bend': 'Bookend'}
tempo_result['Scenario'] = tempo_result['Scenario'].astype(str)
tempo_result['Scenario'] = tempo_result['Scenario'].apply(lambda x: align_scenarios[x])
# print(tempo_result.Scenario.unique())

# check total vehicle count by year and scenario
tempo_result_summary = pd.pivot_table(tempo_result, values = 'Vehicles', 
                                      index = ['Year', 'Scenario'], columns = 'Tech2',
                                      aggfunc = np.sum)
# print(tempo_result.columns)
tempo_result_summary = tempo_result_summary.reset_index()
tempo_result_summary.to_csv('./Output/' + state_name + '/TEMPO_veh_count.csv', index = False)


# <codecell>
# load GEMS output and compute VMT by home tract
gems_VMT = read_csv(link_to_gems_output, sep = ',', low_memory = 'False')
# print(gems_VMT.columns)
# print(gems_VMT.head(5))
# formatting GEMS output and check out the data
gems_VMT = gems_VMT.dropna(subset = 'home_GEOID')
# format Census GEOID and add leading zeros
gems_VMT.loc[:, 'home_GEOID'] = gems_VMT.loc[:, 'home_GEOID'].astype(str).str.zfill(11)
gems_VMT.loc[:, 'thru_GEOID'] = gems_VMT.loc[:, 'thru_GEOID'].astype(str).str.zfill(11)
home_VMT = gems_VMT.groupby('home_GEOID')[['VMT']].sum()
home_VMT = home_VMT.reset_index()
# print(home_VMT.head(5))

#need to change to run for each year & change 'base' to '1' for scenario results. 
#so basically need to figure out what code to loop/repeat to get both baseline and scenario results.  
# prepare input and parameters
# analysis_years = list(tempo_result.Year.unique())

scenarios = tempo_result['Scenario'].unique()
# scenarios = ['Baseline', 'NEVI']
#analysis_year = 2018
#scenario_name = 'base' # this variable is currently hard coded, 
# will be ideal to load it from a config file
#output_name = state_name + '_' + scenario_name + '_' + str(analysis_year)
# <codecell>
vmt_by_income = gems_VMT.groupby(['populationGroupType'])[['VMT']].sum()
vmt_by_income = vmt_by_income.reset_index()
vmt_by_income.loc[:, 'fraction'] = vmt_by_income.loc[:, 'VMT']/ vmt_by_income.loc[:, 'VMT'].sum()
vmt_by_income.to_csv('./Output/'+ state_name + '/VMT_by_income.csv')

# <codecell>
for analysis_year in analysis_years:
    year_start_time = time.time()
    output_name = state_name + '_' + str(analysis_year)
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
        home_veh = tempo_result_filtered.groupby('home_GEOID')[['Vehicles']].sum()
        home_veh = home_veh.reset_index()

        # create list of census tracts with zero vehicle ownership
        home_veh_zero = tempo_result_filtered.groupby(['Region', 'tract', 'home_GEOID'])[['Vehicles']].sum()
        home_veh_zero = home_veh_zero.loc[home_veh_zero['Vehicles'] == 0]
        home_veh_zero = home_veh_zero.reset_index()
        # print(home_veh_zero.head(5))
        home_veh_zero.to_csv('./Output/'+ state_name + '/TEMPO_tract_with_no_veh.csv')

        # validation of GEMS results -- if high home VMT correlated with home veh ownership
        home_VMT_vs_veh = pd.merge(home_VMT, home_veh, on = 'home_GEOID', how = 'inner')
        ax = sns.scatterplot(x="Vehicles", y="VMT", data=home_VMT_vs_veh, alpha = 0.3)
        ax.set(yscale="log")
        plt.xlabel('vehicle count from TEMPO')
        plt.ylabel('VMT from GEMS')
        plt.savefig('./Plot/'+ state_name + '/gems_vmt_vs_tempo_veh.png', dpi = 200)
        plt.show()
        plt.close()

        #########################################################################################
        ##########         Part 2: allocate GEMS VMT by TEMPO vehicle composition      ##########
        ##########                                                                     ##########
        #########################################################################################

        # generate fleet composition by home tract
        fleet_by_home_tract = pd.pivot_table(tempo_result_filtered, 
                                             values='Vehicles', index=['home_GEOID'],
                                             columns=['Tech2'])
        list_of_veh_tech = fleet_by_home_tract.columns
        fleet_by_home_tract.loc[:, 'Total'] = fleet_by_home_tract.loc[:, list_of_veh_tech].sum(axis = 1)
        # fill census tract with 0 vehicles, assume 1 hypothetical ICEV to ensure VMT can be assigned
        fleet_by_home_tract.loc[fleet_by_home_tract['Total'] == 0, 'ICEV_Gasoline'] = 1
        fleet_by_home_tract.loc[fleet_by_home_tract['Total'] == 0, 'Total'] = 1

        for tech in list_of_veh_tech:
            #print(tech)
            fleet_by_home_tract.loc[:, tech] = fleet_by_home_tract.loc[:, tech] / fleet_by_home_tract.loc[:, 'Total']
        fleet_by_home_tract = fleet_by_home_tract.reset_index()
        print(fleet_by_home_tract.head(5))
        fleet_by_home_tract.to_csv('./Output/' + state_name + '/EV_penetration/EV_penetration_' + output_name + '_' + scenario_name + '.csv', index = False)
        # combine fleet composition with GEMS VMT, linked by home census tracts
        gems_VMT_with_fleet_composition = pd.merge(gems_VMT, fleet_by_home_tract, 
                                                   on = 'home_GEOID', how = 'inner')
        # assign total through VMT based on fleet composition at home tracts
        for tech in list_of_veh_tech:
            #print(tech)
            gems_VMT_with_fleet_composition.loc[:, tech] = gems_VMT_with_fleet_composition.loc[:, tech] * \
            gems_VMT_with_fleet_composition.loc[:, 'VMT']
        print(gems_VMT_with_fleet_composition.head(5))    

        # aggregate results by through census tracts (home tracts removed to reduce the file size

        list_attr = list_of_veh_tech.tolist()
        list_attr.append('VMT')
        print(list_attr)
        VMT_by_thru_tract_and_veh = gems_VMT_with_fleet_composition.groupby(['thru_GEOID',
                                                                             'thru_geotype',
                                                                             'thru_microtype'])[list_attr].sum()
        VMT_by_thru_tract_and_veh = VMT_by_thru_tract_and_veh.reset_index()

        # writing output VMT by tracts
        VMT_by_thru_tract_and_veh.to_csv('./Output/'+ state_name + '/EV_VMT/raw_VMT_' + output_name + '_' + scenario_name + '.csv', index = False)
        print(VMT_by_thru_tract_and_veh.head(5))

        VMT_by_thru_tract_and_veh['thru_GEOID'] = VMT_by_thru_tract_and_veh['thru_GEOID'].apply(lambda x: x.zfill(11))
        VMT_by_thru_tract_and_veh.rename({'thru_GEOID': 'tract_geoid'}, axis = 'columns', inplace=True)
        #need to sum HEV_Gasoline, ICEV, and (1-0.22)PHEV_25 + (1-0.62)PHEV_50. 
        VMT_by_thru_tract_and_veh['trad_VMT'] = (VMT_by_thru_tract_and_veh['HEV_Gasoline'] +
                                                VMT_by_thru_tract_and_veh['ICEV_Gasoline'] + 
                                                VMT_by_thru_tract_and_veh['ICEV_NG'] + 
                                                0.78*VMT_by_thru_tract_and_veh['PHEV_25']+
                                                0.38*VMT_by_thru_tract_and_veh['PHEV_50'])
        print("check that sums correctly:")
        print(VMT_by_thru_tract_and_veh.head(5))
        if scenario_name == 'Baseline':
            result_base = VMT_by_thru_tract_and_veh[['tract_geoid', 'trad_VMT']]
        elif scenario_name in ['NEVI', 'Bookend']:
            result_scen = VMT_by_thru_tract_and_veh[['tract_geoid', 'trad_VMT']]
            
        del tempo_result_filtered
        del home_veh_zero
        del home_veh
        del gems_VMT_with_fleet_composition
        del VMT_by_thru_tract_and_veh

    result = result_base.merge(result_scen, on = 'tract_geoid', suffixes = ['_base', '_scenario'])
    #PEV accurately describes this output, but the next model expects just 'VMT_base' & '_scenario' as col names
    result.rename({'trad_VMT_base': 'VMT_base', 'trad_VMT_scenario': 'VMT_scenario'},
                   axis='columns', inplace =True)
    # #not sure the below is needed or accurate, but next model might be expecting that col as input.
    # #try without for now
    # result['vehicle_type'] = 'LDT'
    print("check that this and the last check are the same thing, just merged...")
    print(result.head(5))
    GEMS_result_save_name = './Output/'+ state_name + '/VMT_' + output_name + '.csv'
    result.to_csv(GEMS_result_save_name, index=False)
    GEMS_output_files.append(GEMS_result_save_name)
    year_end_time = time.time()
    print('time to run analysis on one year (minutes): ', (year_end_time-year_start_time)/60)
    # break
total_end_time = time.time()
#about one hour to run for baseline & scenario for all years (still need to check output) *** :) 
print('time to run analysis on ALL years (in minutes): ', (total_end_time - total_start_time)/60)

