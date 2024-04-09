# Input Generation for BILD-AQ pipeline
Estimate EV VMT penetration as a result of EV adoption

## In-state travel (no spillover)
### step 1 -- ACS population generation
code: [ACS_population_pre_processor.py](ACS_population_pre_processor.py)
* Download household by income data from 2017 ACS 5-year estimates (remove variable description and save as 'ACS2017_income.csv')
https://data.census.gov/table?q=B19001:+HOUSEHOLD+INCOME+IN+THE+PAST+12+MONTHS+(IN+2021+INFLATION-ADJUSTED+DOLLARS)&g=010XX00US$1400000&tid=ACSDT5Y2017.B19001
* Combining households under same income bin under ACS_population_pre_processor.py
* Produce census-tract level household counts by income bins: 'data/Input/ACS_household_by_tracts.csv'
  * Income bin 1: annual income <= 50k
  * Income bin 2: annual income between 50k and 125k
  * Income bin 3: annual income >= 125k
  
### step 2 -- processing NHTS data (run on server)
code: [BILDAQ_NHTS_trip_generation_rate.py](BILDAQ_NHTS_trip_generation_rate.py)
* Produce home-based trip generation rate: 'data/Input/{state}/NHTS_home_based_trips_{state}.csv'
* Produce non-home-based trip generation rate (miles of nhb VMT as a result of per mile of hb VMT): 'data/Input/{state}/NHTS_nonhome_VMT_fraction_{state}.csv'
* Produce car trips by home and O-D census tracts for destination-choice model: 'NHTS_car_trips.csv' (proprietary) 
* Produce weighted NHTS household count by state, : 'data/Input/{state}/NHTS_population.csv' 

### step 3 -- generate US census tract map and great-circle-distance matrix
code: [us_tract_processor.R](us_tract_processor.R)
* Produce great-circle distance matrix between each O-D tract pair within U.S. (dist <= 300 mi to limit # of O-D pairs): 'data/Network/combined/distance_matrix_by_tracts_{state}.csv' 
* Produce U.S. census tract geometry in Geojson format: 'data/Network/combined/census_tracts_2017.geojson'

### step 4 -- INRIX data processing
#### 4.1 -- collecting and filtering raw INRIX data
code: [INRIX_data_processor_cleaned.ipynb](INRIX_data_processor_cleaned.ipynb)
(This code is hand over to NREL and was updated on their end to fit their data pipeline.  The data is proprietary so users will not be able to re-run this part themselves)
* Filtering raw INRIX data to include the subset of the data that are pre-pandemic, during weekday, and performed by light-duty vehicles
* Produce aggregated travel time, distance and speeds by origin-destination tracts by states: 'data/INRIX/state={state}/{serial_name}.parquet
* Produce survival function parameters (probability of trip ended in a destination as a function of travel time to that destination) for each state

#### 4.2 -- travel skim generation using processed INRIX data
code: [BILDAQ_OD_skim_generation_national.py](BILDAQ_OD_skim_generation_national.py)
* Load processed skims from step 4.1 as inputs
* Load distance matrix from step 3 as inputs
* Produce imputed travel time, distance and speeds for all origin-destination tracts within selected states: 'data/Network/{state}/travel_time_skim.csv'

### step 5 -- Graphhopper route processing
#### 5.1 -- clean raw Graphhopper data 
code: [gems_route_processor_external.ipynb](gems_route_processor_external.ipynb)
* Load compressed Graphhopper route data
* Construct the route geometry, and split data by origin state
* Save output data into geojsons: {external_drive}/project_backup/GEMS_ROUTE/route/{internal or external}/*
* internal means destination state is the same as origin state, otherwise external

#### 5.2 -- intersect route data to tracts
code: [assign_route_to_tract.ipynb](assign_route_to_tract.ipynb)
* Load Graph-hopper route data in geojson from step 5.1
* Intersect routes with census tract boundary
* Compute through length in each census tract
* Save output to: 'data/Input/{state}/route/*.csv'

### step 6 -- National-scale destination choice model estimation (run on server)
code: [destination_choice_biogeme_multistates.ipynb](destination_choice_biogeme_multistates.ipynb)
* Load NHTS data from step 2 as input: NHTS_car_trips.csv
* Load following land use, opportunity data from FHWA GEMS project as inputs:
** ccst_geoid_key_tranps_geo_with_imputation.csv
** wac_tract_2017.csv
** opportunity_counts_tract.csv
** modeaccessibility.csv
* Load INRIX travel skims from step 4: data/Network/{state}/travel_time_skim.csv
* Estimate destination choice models by geotype (D & E combined due to lack of observations)
* Writing estimated coefficient to this input: data/Input/destination_choice_parameters.csv

### step 7 -- collecting national-level VMT from HPMS data
code: [process_lane_mile_from_hpms.R](process_lane_mile_from_hpms.R)
* Load 2017 HPMS data downloaded from https://www.fhwa.dot.gov/policyinformation/hpms/shapefiles_2017.cfm
* Load U.S. census tract geometry from step 3: 'data/Network/combined/census_tracts_2017.geojson'
* Intersect HPMS data with 2010 census tract boundary
* Update link lane miles after intersection
* Generate processed HPMS output: 'data/Network/{State}/{State}__HPMS_with_GEOID_LANEMILE.geojson

## Spillover/cross-state travel generation
### step 1 -- processing NHTS data (spillover only) (run on server)
code: [BILD-AQ_NHTS_processor_spillover.ipynb](spillover/BILD-AQ_NHTS_processor_spillover.ipynb)
* Produce home-based spillover trip generation rate: 'data/Input/spillover/NHTS_home_based_trip_rate_spillover.csv'
* Produce non-home-based spillover trip generation rate (miles of nhb VMT as a result of per mile of hb VMT): 'data/Input/spillover/NHTS_nonhome_VMT_generation_spillover.csv'
* Produce car trips by home and O-D census tracts for destination-choice model: 'NHTS_home_based_spillover_trips.csv' (proprietary) 
* Produce non-home-based car trips for NHB factor estimation: 'NHTS_nonhome_OD_spillover.csv' (proprietary) 

### step 2 -- spillover destination choice model estimation (run on server)
code: [destination_choice_biogeme_spillover.ipynb](spillover/destination_choice_biogeme_spillover.ipynb)
* Loading home-based spillover trips from step 1: 'NHTS_home_based_spillover_trips.csv'
* Load following land use, opportunity data from FHWA GEMS project as inputs:
** ccst_geoid_key_tranps_geo_with_imputation.csv
** wac_tract_2017.csv
** opportunity_counts_tract.csv
** modeaccessibility.csv
* Writing estimated coefficient to this input: 'data/Input/spillover/destination_choice_parameters.csv'

### Step 3 -- other spillover allocation factors
#### 3.1 - home-based border fraction
code:[state_spillover_hb_vmt_analysis.ipynb](spillover/state_spillover_hb_vmt_analysis.ipynb)
* Loading home-based spillover trips from step 1: 'NHTS_home_based_spillover_trips.csv'
* Loading tract-to-tract distance matrix from general pipeline, step 3: 'data/Network/combined/distance_matrix_by_tracts_{state}.csv' 
* calculate fraction of spillover trips based on distance bin to nearest out-of-state destination (border fraction): 'data/Input/spillover/home_based_border_fraction.csv'

#### 3.2 - non-home-based radius fraction
code:[state_spillover_nhb_vmt_analysis.ipynb](spillover/state_spillover_nhb_vmt_analysis.ipynb)
* Loading non-home-based spillover trips from step 1: 'NHTS_nonhome_OD_spillover.csv'
* Loading tract-to-tract distance matrix from general pipeline, step 3: 'data/Network/combined/distance_matrix_by_tracts_{state}.csv' 
* Load following land use, opportunity data from FHWA GEMS project as inputs:
** ccst_geoid_key_tranps_geo_with_imputation.csv
* Calculate fraction of non-home-based VMT by distance to out-of-state entry point: 'data/Input/spillover/nonhome_spillover_distribution_factors.csv'

#### Step 4 -- compute distance to border for all home tracts
code:[generate_dist_to_border.py](spillover/generate_dist_to_border.py)
* Loading tract-to-tract distance matrix from general pipeline, step 3: 'data/Network/combined/distance_matrix_by_tracts_{state}.csv' 
* compute distance to nearest out-of-state destination:'data/Network/{state}/tract_to_border_distance.csv'


