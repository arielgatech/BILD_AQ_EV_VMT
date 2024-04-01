# BILD AQ Documentation

## EV VMT State Spillover

At present, the pipeline is made up of the following files, to be run in order:

**STEP 0** (in preparation): checks the filepaths and required input files are ready.

**STEPS 1 and 2:** File: EV_VMT_state_spillover.py **Note: currently being updated**
Description: Generates spillover trips by home tract. Includes:
1. Calculation of nearest out-of-state tract to every state input tract plus distance
2. Calculation of total home-based spillover trips 
3. Assignment of spillover trips to tracts near the border, within the origin state
4. Destination choice

Inputs: 
- Trip generation files (ACS pop, CCST lookup, HB Spillover, HB Border Frac)
- Destination choice input files (Distance matrix, Employment, Opportunity, Accessibility, Choice parameters)
- Tracts-to-border distance matrix (by state)
- User-specified state (CA/WA/OR)

Notes: state has to be changed and file paths have to be checked.

Output: OD Home Based Trips Spillover .csv file (for a given state)
_______

**STEP 3:** File: EV_VMT_state_spillover_step3.py
Description: Route choice model with shortest path
Inputs:
- Pre-processed routes (number of available routes depends on whether imputation has been run or not)
- Output from Step 2
Outputs:
- OD with missing routes (.csv file) (see next step)
- Home-Based Daily VMT Spillover (.csv file)
- Destination Daily VMT Spillover (.csv file)
- OD Summary Spillover (.csv file)

________

**ROUTE IMPUTATION Step:** - File: state_route_imputation-fixed.R
Notes:
- Iterative process
- Re-run step 3 after doing this (it adds more routes that were previously flagged as missing)
- There is randomness here. Up until the end of Step2, we always get the same result. 
Every time we rerun Step 3 we get something else.

Input:
- Census Tract Shapefile
- OD with missing routes (from Step 3)
Output:
- Imputed routes

**In development**: a .Rmd file in R which, through the use of 'reticulate', merges the R and Python scripts.
_________

**STEP 4:** EV_VMT_state_spillover_nhb_step4.py
Description: Non Home-based VMT calculation

- Similar structure to Step 1

Inputs:
- Output from steps up to Step 3
- Non-home specific inputs (i.e. non-home VMT generation factors, etc.)

Outputs:
 - NHB VMT spillover (.csv file)
__________

