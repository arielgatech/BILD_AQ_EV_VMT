#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

@author: xiaodanxu and @carlosguirado
"""

# MASTER FILE TO RUN ALL SCRIPTS
# Housekeeping
import runpy
import os.path
from os import path

# Western Interconnection:
#https://github.com/arielgatech/BILD_AQ_EV_VMT/blob/national_pipeline/README.md

### ASK USER TO INPUT STATE (see updated list from emails):

possible_states = ['CA', 'OR', 'WA', 'CO', 'ID', 'NV', 'MT', 'WY', 'UT', 'NM', 'AZ']

state_input = input("Select state: ")

if state_input not in possible_states:
    print('Error! Select one of these: ', possible_states)
else:
    print('Starting pipeline for: ', state_input)
    ###### RUN THE FIRST PART OF THE PIPELINE
    #runpy.run_path("./EV_VMT_state_spillover.py")

##### Check that all required files exist to run Step 1 and 2:

#### general files

# path exists or not

#### state specific paths

paths_list =[]
dist_matrix_file = 'Network/combined/distance_matrix_by_tracts_' + state_input + '.csv'
paths_list.append(dist_matrix_file)

input_routes_file = 'Input/route/' + state_input + '_external'
paths_list.append(input_routes_file)

input_path = 'Input/' + state_input
paths_list.append(input_path)

network_path = 'Network/' + state_input
paths_list.append(network_path)

route_processed_path = 'route/processed/' + state_input
paths_list.append(route_processed_path)
route_processed_final_path = 'route/processed/final/' + state_input
paths_list.append(route_processed_final_path)

#tract_border_file = 'Network/combined/' + state_input + '_tract_to_border_distance.csv'
#paths_list.append(tract_border_file)

#commenting out because this is generated after step 1

# Download this from the GEMS folder
# 'Input/route/ID_external'
# Cannot save file into a non-existent directory: 'Network\ID'


paths_exist =[]
for path in paths_list:
    isExist = os.path.exists(path)
    paths_exist.append(isExist)


# If the state was correct, the length of paths_exist is non-zero
# If it is zero, then pass
if len(paths_exist) == 0:
    pass
else:
    if paths_exist.count(False) ==0:
        print("All files for state: ", state_input, " are located. You can run the pipeline")
    else:
        print("There are ", paths_exist.count(False), "files missing to run pipeline for state: ",state_input,". Check file paths and documentation")
        print(paths_exist)

