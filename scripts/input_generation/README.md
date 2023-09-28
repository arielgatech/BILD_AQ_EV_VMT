# Input Generation for BILD-AQ pipeline
Estimate EV VMT penetration as a result of EV adoption

## In-state travel (no spillover)
### step 1 -- ACS population generation
code: [ACS_population_pre_processor.py](input_generation/ACS_population_pre_processor.py)
* Download household by income data from 2017 ACS 5-year estimates (remove variable description and save as 'ACS2017_income.csv')
https://data.census.gov/table?q=B19001:+HOUSEHOLD+INCOME+IN+THE+PAST+12+MONTHS+(IN+2021+INFLATION-ADJUSTED+DOLLARS)&g=010XX00US$1400000&tid=ACSDT5Y2017.B19001
* Combining households under same income bin under ACS_population_pre_processor.py
* Produce census-tract level household counts by income bins: 'data/Input/ACS_household_by_tracts.csv'
  * Income bin 1: annual income <= 50k
  * Income bin 2: annual income between 50k and 125k
  * Income bin 3: annual income >= 125k
  
### step 2 -- processing NHTS data
code: [BILDAQ_NHTS_trip_generation_rate.py](input_generation/BILDAQ_NHTS_trip_generation_rate.py)
* Produce home-based trip generation rate: 'data/Input/{state}/NHTS_home_based_trips_{state}.csv'
* Produce non-home-based trip generation rate (miles of nhb VMT as a result of per mile of hb VMT): 'data/Input/{state}/NHTS_nonhome_VMT_fraction_{state}.csv'
* Produce car trips by home and O-D census tracts for destination-choice model: 'data/Input/NHTS_car_trips.csv' (proprietary) 
* Produce weighted NHTS household count by state, : 'data/Input/{state}/NHTS_population.csv' 

### step 3 -- generate US census tract map and great-circle-distance matrix
code: [us_tract_processor.R](input_generation/us_tract_processor.R)
* Produce great-circle distance matrix between each O-D tract pair within U.S. (dist <= 300 mi to limit # of O-D pairs): 'data/Network/combined/distance_matrix_by_tracts_{state}.csv' 
* Produce U.S. census tract geometry in Geojson format: 'data/Network/combined/census_tracts_2017.geojson'

### step 4 -- INRIX data processing
#### 4.1 -- collecting and filtering raw INRIX data
code: [INRIX_data_processor_cleaned.ipynb](input_generation/INRIX_data_processor_cleaned.ipynb)
(This code is hand over to NREL and was updated on their end to fit their data pipeline.  The data is proprietary so users will not be able to re-run this part themselves)
* Filtering raw INRIX data to include the subset of the data that are pre-pandemic, during weekday, and performed by light-duty vehicles
* Produce aggregated travel time, distance and speeds by origin-destination tracts by states: 'data/INRIX/state={state}/{serial_name}.parquet
* Produce survival function parameters (probability of trip ended in a destination as a function of travel time to that destination) for each state

#### 4.2 -- travel skim generation using processed INRIX data
code: [BILDAQ_OD_skim_generation_national.py](input_generation/BILDAQ_OD_skim_generation_national.py)
* Load processed skims from step 4.1 as inputs
* Load distance matrix from step 3 as inputs
* Produce imputed travel time, distance and speeds for all origin-destination tracts within selected states: 'data/Network/{state}/travel_time_skim.csv'

## Spillover
