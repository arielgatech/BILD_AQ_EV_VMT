# NAVIGAT - NAtional Vehicle Itinerary GenerATor 
**Overview:** a large-scale passenger vehicle mile travelled (VMT) simulator, called NAtional Vehicle Itinerary GenerATor (NAVIGAT), is developed by LBNL researchers to simulate household-owned passenger vehicle movements within the whole U.S. at census tract level resolution. NAVIGAT adopts a modeling framework resembling the traditional travel demand models that are often applied in a regional context, combined with a data-driven approach that estimates model parameters using various national-level data sources. Using NAVIGAT, the vehicle movements can be tracked throughout the network at the census tract level given a technology adoption scenario input also mapped to the census tract level. Outputs from NAVIGAT can support the estimation of common transportation and environmental metrics, such as on-road emission changes, air quality and health impacts on surrounding communities resulting from changes in technology adoption, which might be due to charging infrastructure deployment, adoption incentives, changes in fuel prices or vehicle prices, or other factors.

**Current application:** western interconnection that include: California, Oregon, Washington, Nevada, Montana, Idaho, Wyoming, Colorado, Utah, New Mexico and Arizona.

##  Contact: Xiaodan Xu, Ph.D. (XiaodanXu@lbl.gov)

**Notes:** a streamlined and packaged application of this tool is still under development, The current guide provides a high-level overview on how to run individual modules sequentially, and how to integrate current model with NREL's TEMPO model. If you have any questions, please reach out to the project team for support.

**Input Generation:** for guidance related to all the input generation steps, please refer to this [input guide](input_generation/README.md) for details.

## Part 1: In-state demand generation 

**STEP 1** File: [step1_trip_generation_multistate.py](utils/step1_trip_generation_multistate.py)

Description: Generates in-state home-based trips by home tract. 

Inputs: 
- Trip generation files (ACS pop, CCST lookup, HB trip rate)
- User-specified state

Notes: state has to be changed and file paths have to be checked.

Output:  Home Based Trips by home tract .csv file (for a given state)

**STEP 2:** File: [step2_destination_choice_V2.py](utils/step2_destination_choice_V2.py)

Description: Assign destinations for home-based spillover trips. 

Inputs: 
- Home-based trip output files from step 1
- Destination choice input files (Distance matrix, Employment, Opportunity, Accessibility, Choice parameters)
- User-specified state

Notes: state has to be changed and file paths have to be checked.

Output: OD Home Based Trips .csv file (for a given state)


**STEP 3:** File: [step3_route_assignment_V2.py](utils/step3_route_assignment_V2.py)

Description: Route assignment model with shortest path

Inputs:
- Pre-processed routes (number of available routes depends on whether imputation has been run or not)
- Output from Step 2

Outputs:
- OD with missing routes (.csv file) (see next step)
- Home-Based Daily VMT Spillover (.csv file)
- Destination Daily VMT Spillover (.csv file)
- OD Summary Spillover (.csv file)

**ROUTE IMPUTATION Step 1 ** File: [state_route_imputation-fixed.R](utils/state_route_imputation-fixed.R)

Description:
- Intermediate step that fills missing routes from graph-hopper data
- Re-run step 3 after doing this (it adds more routes that were previously flagged as missing)
- Need to be used if new sets of O-D trips are generated (e.g., re-run destination choice model)

Input:
- Census Tract Shapefile
- OD with missing routes (from Step 3)

Output:
- Imputed routes

**ROUTE IMPUTATION part 2** file: [assign_route_to_tract_imputation.ipynb](util/assign_route_to_tract_imputation.ipynb)

Description:
-Assign imputed route to census tract level, moving the output files to input/{state}/route/*

**STEP 4:** [step4_NHB_assignment.py](utils/step4_NHB_assignment.py)
Description: In-state non-home-based VMT calculation

Inputs:
- Output from steps up to Step 3
- Non-home specific inputs (i.e. non-home VMT generation factors, etc.)

Outputs:
 - NHB VMT (.csv file)


## Part 2: Spillover demand generation

**STEPS 1 and 2:** File: [EV_VMT_state_spillover_steps1_2.py](utils/Final_Spillover_Pipeline/EV_VMT_state_spillover_steps1_2.py)

Description: Generates home-based spillover trips by home tract. Includes:
1. Calculation of nearest out-of-state tract to every state input tract plus distance
2. Calculation of total home-based spillover trips 
3. Assignment of spillover trips to tracts near the border, within the origin state
4. Destination choice

Inputs: 
- Trip generation files (ACS pop, CCST lookup, HB Spillover, HB Border Frac)
- Destination choice input files (Distance matrix, Employment, Opportunity, Accessibility, Choice parameters)
- Tracts-to-border distance matrix (by state)
- User-specified state

Notes: state has to be changed and file paths have to be checked.

Output: OD Home Based Trips Spillover .csv file (for a given state)

**STEP 3:** File: [EV_VMT_state_spillover_step3.py](utils/Final_Spillover_Pipeline/EV_VMT_state_spillover_step3.py)

Description: Route assignment model with shortest path

Inputs:
- Pre-processed routes (number of available routes depends on whether imputation has been run or not)
- Output from Step 2

Outputs:
- OD with missing routes (.csv file) (see next step)
- Home-Based Daily VMT Spillover (.csv file)
- Destination Daily VMT Spillover (.csv file)
- OD Summary Spillover (.csv file)

**ROUTE IMPUTATION P1** File: [state_route_imputation-fixed.R](utils/Final_Spillover_Pipeline/state_route_imputation-fixed.R)

Description:
- Intermediate step that fills missing routes from graph-hopper data
- Re-run step 3 after doing this (it adds more routes that were previously flagged as missing)
- Need to be used if new sets of O-D trips are generated (e.g., re-run destination choice model)

Input:
- Census Tract Shapefile
- OD with missing routes (from Step 3)

Output:
- Imputed routes

**ROUTE IMPUTATION part 2** File: [assign_route_to_tract_imputation.ipynb](util/assign_route_to_tract_imputation.ipynb)

Description:
-Assign imputed route to census tract level, moving the output files to input/{state}/route/*

**STEP 4:** [EV_VMT_state_spillover_nhb_step4.py](utils/Final_Spillover_Pipeline/EV_VMT_state_spillover_nhb_step4.py)

Description: Spillover non-home-based VMT calculation using radius-based method

Inputs:
- Output from steps up to Step 3
- Non-home specific inputs (i.e. non-home VMT generation factors, etc.)

Outputs:
 - NHB VMT spillover (.csv file)
 

## Part 3: VMT calibration
**VMT calibration** file: [step5_compile_VMT_by_tract_with_spillover_multistate.py](utils/step5_compile_VMT_by_tract_with_spillover_multistate.py)

Description: adjust total HB and NHB instate and spillover VMT using HPMS VMT

Inputs: 
- VMT results from steps 3 & 4 from in-state and spillover modules
- processed HPMS network

Outputs:
- Scaled VMT by home, through tracts and income group

## Part 4: Integration with TEMPO

**TEMPO integration** file: [TEMPO_GEMS_integration_123122.py](post_process/TEMPO_GEMS_integration_123122.py)

Description: load scaled VMT from Part 3 and TEMPO output from NREL team, calculate EV VMT penetration at census tract level (by distributing EV VMT adopted at home tracts to through tracts)

Inputs:
-scaled VMT from Part 3 
-TEMPO output from NREL team

Outputs:
- EV adoption rate by home tracts
- EV VMT penetration rate by through tracts