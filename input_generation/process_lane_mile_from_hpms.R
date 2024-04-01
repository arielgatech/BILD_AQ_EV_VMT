library(sf)
library(dplyr)
library(data.table)
library(tidycensus)

# using this key at the first time of using tidycensus
census_api_key("d49f1c9b81751571b083252dfbb8ac14ae8b63b7", install = TRUE, overwrite=TRUE) 
#########
readRenviron("~/.Renviron")

setwd("/Volumes/LaCie/project_backup/GEMS/BILD-AQ/HPMS/")
state = 'WY'
# network_arnold <- fread(paste0(state, '/CA_arnold.csv'), h = T) # this data is not useful!!
#  network_hpms <- fread(paste0(state, '/CA_summary.txt'), h = T)  # this data is not useful too!!

hpms_geometry <- st_read(paste0('HPMS data', '/wyoming2017/wyoming2017.shp'))
crs_hpms <- st_crs(hpms_geometry)
state_tracts = get_acs(
  geography = "tract",
  year = 2017,
  variables = c('B01003_001'),
  state = state,
  geometry = TRUE
)
state_tracts <- st_transform(state_tracts, crs = crs_hpms)
#plot(st_geometry(st_zm(hpms_geometry)))
sf::sf_use_s2(FALSE) 

# assign tracts to each link
hpms_geometry_by_tracts = st_intersection(st_zm(hpms_geometry), state_tracts)
hpms_geometry_by_tracts$Length = st_length(hpms_geometry_by_tracts) # re-generate link length after split network by tracts
hpms_geometry_by_tracts <- hpms_geometry_by_tracts %>% mutate(lanemiles = as.numeric(Through_La * Length / 1609.34)) # compute lane miles
hpms_geometry_by_tracts <- hpms_geometry_by_tracts %>% filter(lanemiles > 0) # remove invalid links
hpms_geometry_by_tracts <- hpms_geometry_by_tracts %>% select(-variable, -estimate, -moe, -Shape_Leng)


hpms_geometry_by_tracts <- hpms_geometry_by_tracts %>% mutate(g_type = st_geometry_type(.))
hpms_geometry_by_tracts <- hpms_geometry_by_tracts %>% filter(g_type %in% c('LINESTRING', 'MULTILINESTRING'))
st_write(hpms_geometry_by_tracts, paste0('output/', state, '_HPMS_with_GEOID_LANEMILE.geojson'))
#plot(hpms_geometry_by_tracts[, 'GEOID'])



