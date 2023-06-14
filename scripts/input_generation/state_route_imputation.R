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
path2file <-
  "/Users/xiaodanxu/Library/CloudStorage/GoogleDrive-arielinseu@gmail.com/My Drive/GEMS/BILD-AQ"
setwd(path2file)

state_input = 'OR'

state_tract_shapefile <- st_read(paste0('data/Network/', state_input, '/census_tracts_2017.geojson'))
ccst_lookup <- fread('data/ccst_geoid_key_tranps_geo_with_imputation.csv')
selected_ccst_lookup <- ccst_lookup %>% filter(st_code == state_input) %>% as_tibble()
selected_geoid <- as.numeric(unique(selected_ccst_lookup$GEOID))

state_tract_shapefile <- state_tract_shapefile %>% mutate(GEOID = as.numeric(GEOID))
state_tract_shapefile <- state_tract_shapefile %>% filter(GEOID %in% selected_geoid)
state_tract_shapefile <- st_transform(state_tract_shapefile, crs = 4326) # convert to route CRS
# plot(st_geometry(state_tract_shapefile))

tract_centroid <- st_centroid(state_tract_shapefile)
tract_centroid <- tract_centroid %>% select(GEOID)
plot(st_geometry(tract_centroid))

OD_to_impute <- fread(paste0('data/Network/', state_input, '/OD_to_impute.csv'))
#define number of data frames to split into
n <- 100

#split data frame into n equal-sized data frames
OD_chunks = split(OD_to_impute, factor(sort(rank(row.names(OD_to_impute))%%n)))
i = 0
for (chunks in OD_chunks){
  print(i)
  # if (i <= 94){
  #   i = i + 1
  #   next
  # }
  for (row in 1:nrow(chunks)) {
    # if(row < 290){
    #   next
    # }
    # if (row %in% exception){
    #   next
    # }
    
    origin_tract <- as.numeric(chunks$home_GEOID[row])
    dest_tract <- as.numeric(chunks$destination[row])
    origin_node <- tract_centroid %>% filter(GEOID == origin_tract)
    dest_node <- tract_centroid %>% filter(GEOID == dest_tract)
    start_node_coordinate <- st_coordinates(origin_node)
    end_node_coordinate <- st_coordinates(dest_node)
    
    tryCatch(expr = {
      paths_sf <- route_osrm(start_node_coordinate, end_node_coordinate, osrm.profile = "car")
      paths_sf <- st_transform(paths_sf, crs = 4326)
      paths_sf$source <- origin_tract
      paths_sf$destination <- dest_tract
      # paths_sf <- paths_sf$geometry
      # paths_sf <- st_sf(paths_sf)
      # paths_sf$route_id <- route_id
      # paths_sf$origin_faf <- as.numeric(origin_node$FAF)
      # paths_sf$dest_faf <- as.numeric(dest_node$FAF)
      if (row == 1){
        out_route <- paths_sf
      }else{
        out_route <- rbind(out_route, paths_sf)
      }
    },
    error = function(e){
      message('Caught an error!')
      print(e)
    }
    )
    # break
  } 
  st_write(out_route, paste0('data/route/processed/imputed_', state_input, '_route_', i, '.geojson'))
  i = i + 1
  # break
}

