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
* TO BE FILLED
* Produce home-based trip generation rate: 'data/Input/{state}/NHTS_home_based_trips_{state}.csv (**need to re-run for each location**)
* Produce weighted NHTS household count by state, : 'data/Input/{state}/NHTS_population.csv 

## Spillover
