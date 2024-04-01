#################################################################################
rm(list = ls())
options(scipen = '10')
list.of.packages <-
  c("dplyr",
    "data.table",
    "sf",
    "mapview",
    "dtplyr",
    "tidyr",
    "parallel",
    "stringr")
new.packages <-
  list.of.packages[!(list.of.packages %in% installed.packages()[, "Package"])]
if (length(new.packages))
  install.packages(new.packages)
lapply(list.of.packages, require, character = TRUE)
#################################################################################
#install_github("f1kidd/fmlogit")
set.seed(0)
path2file <-
  "/Volumes/LaCie/project_backup/GEMS_ROUTE/route/processed"
setwd(path2file)

state_input = 'CO'

list_of_route_files <- list.files(path = state_input, pattern = "*.geojson")
shapefile_link <- paste0(state_input, '/tl_2017_08_tract/tl_2017_08_tract.shp')
state_tract_shapefile <- st_read(shapefile_link)
ccst_lookup <- fread('ccst_geoid_key_tranps_geo_with_imputation.csv')
selected_ccst_lookup <- ccst_lookup %>% filter(st_code == state_input) %>% as_tibble()
selected_geoid <- as.numeric(unique(selected_ccst_lookup$GEOID))

state_tract_shapefile <- state_tract_shapefile %>% mutate(GEOID = as.numeric(GEOID))
state_tract_shapefile <- state_tract_shapefile %>% filter(GEOID %in% selected_geoid)
state_tract_shapefile <- st_transform(state_tract_shapefile, crs = 4326) # convert to route CRS
# plot(st_geometry(state_tract_shapefile))


sf::sf_use_s2(FALSE)

for (file in list_of_route_files){
  print(file)
  file_name = str_split(file, ".geojson")[[1]][1]
  trip_length_by_tracts = NULL
  current_routes <- st_read(paste0(state_input, '/', file))
  factor = (as.numeric(rownames(current_routes))-1) %/% 500 # divide the route into chunks with 500-row per chunk
  split_route <- split(current_routes, factor)
  i = 1
  for (sample_route in split_route){
    print(i)
    current_routes_by_tracts = st_intersection(st_zm(sample_route), state_tract_shapefile)
    current_routes_by_tracts$Length = st_length(current_routes_by_tracts) # re-generate link length in meter after split network by tracts
    current_routes_by_tracts <- current_routes_by_tracts %>% select(source, destination, distance, GEOID, Length)  
    current_routes_by_tracts_df <- current_routes_by_tracts %>% st_drop_geometry()
    trip_length_by_tracts = rbind(trip_length_by_tracts, current_routes_by_tracts_df)
    i = i + 1
    # break
  }
  output_dir = paste0(state_input, '/', file_name, '.csv')
  write.csv(trip_length_by_tracts, output_dir)  

  #break
}
