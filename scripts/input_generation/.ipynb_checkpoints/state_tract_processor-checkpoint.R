library(sf)
library(dplyr)
library(tidyr)
library(data.table)
library(tidycensus)
library(stringr)
library(RColorBrewer)
library(ggplot2)
library(sfnetworks)
library(stplanr)
library(sp)

# using this at the first time of using tidycensus
census_api_key("d49f1c9b81751571b083252dfbb8ac14ae8b63b7", install = TRUE, overwrite=TRUE) 
readRenviron("~/.Renviron")

setwd("/Users/xiaodanxu/Library/CloudStorage/GoogleDrive-arielinseu@gmail.com/My Drive/GEMS/BILD-AQ")
state = 'TX'
analysis_year = 2017

### Get census shapefile using tidycensus ###
census_tracts = get_acs(
  geography = "tract",
  year = analysis_year,
  variables = c('B01003_001'),
  state = state,
  geometry = TRUE
)

census_tracts <- census_tracts %>% select(GEOID, NAME) # remove demographic variables
plot(st_geometry(census_tracts))

ccst_geoid_lookup <- data.table::fread('data/ccst_geoid_key_tranps_geo_with_imputation.csv')
ccst_geoid_lookup <- ccst_geoid_lookup %>% mutate(GEOID = str_pad(GEOID, 11, pad = "0"))
census_tracts_with_microtype <- merge(census_tracts, ccst_geoid_lookup, by = 'GEOID', all.x = TRUE)



Pal1 <- rev(brewer.pal(6, "OrRd"))
plot(census_tracts_with_microtype[, 'microtype'], pal = Pal1, lwd = 0.05, border = 'grey')
plot(census_tracts_with_microtype[, 'geotype'], lwd = 0.1, border = 'grey')

census_tracts_with_microtype <- census_tracts_with_microtype %>% drop_na(geotype)
census_tracts_with_microtype <- census_tracts_with_microtype %>% mutate(microtype = as.character(microtype))
p <- ggplot(data = census_tracts_with_microtype, aes(x = microtype, fill=microtype)) +
  geom_bar() +
  scale_fill_brewer(palette='OrRd', direction = -1)
p + facet_wrap(~geotype)

census_tract_centroid <- st_centroid(census_tracts)
plot(st_geometry(census_tract_centroid), col = 'red', add = TRUE)

### compute great circle distance between census tracts ###
centroid_distance <- st_distance(census_tract_centroid, census_tract_centroid) * 0.000621371 # in mile
centroid_distance <- as.data.table(centroid_distance)
colnames(centroid_distance) <- as.character(census_tract_centroid$GEOID)
rownames(centroid_distance) <- as.character(census_tract_centroid$GEOID)


# write output
st_write(census_tracts, paste0('data/Network/', state, '/census_tracts_2017.geojson'))
st_write(census_tract_centroid, paste0('data/Network/', state, '/census_tracts_centroids_2017.geojson'))
write.csv(centroid_distance, paste0('data/Network/', state, '/distance_matrix_by_tracts.csv'))

# census_tract_centroid <- st_read('shapefile/CA_census_tracts_centroids_2010.geojson')
# census_tract_centroid <- census_tract_centroid %>%
#   mutate(lon = st_coordinates(.)[,1],
#         lat = st_coordinates(.)[,2])
# 
# census_tract_centroid <- census_tract_centroid %>% filter (!is.na(lon), !is.na(lat))
# abstract_network <- as_sfnetwork(census_tract_centroid)
# plot(abstract_network) # failed
# 
# node_to_line <- points2line(census_tract_centroid)
# plot(node_to_line)
