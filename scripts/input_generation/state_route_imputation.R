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

#### set working directory ####
path2file <-
  "/Users/xiaodanxu/Library/CloudStorage/GoogleDrive-arielinseu@gmail.com/My Drive/GEMS/BILD-AQ"
setwd(path2file)

# state_input = 'CA'


#### load input ####
# load census tract shapefile
state_tract_shapefile <- st_read(paste0('data/Network/combined/combined_tracts/combined_tracts.shp'))
# 73,056 census tracts

# load OD with missing routes
OD_to_impute <- fread(paste0('data/Network/combined/OD_to_impute_spillover.csv'))

# generate census tract centroid geometry
state_tract_shapefile <- st_transform(state_tract_shapefile, crs = 4326) # convert to route CRS
tract_centroid <- st_centroid(state_tract_shapefile)
tract_centroid <- tract_centroid %>% select(GEOID)
plot(st_geometry(tract_centroid)) # (optional) show the centroid file

# format census tract ID in OD file
OD_to_impute <- OD_to_impute %>%
  mutate(home_GEOID = str_pad(home_GEOID, 11, pad = "0"), 
         destination = str_pad(destination, 11, pad = "0"))


### split data frame into n equal-sized data frames to generate routes (so that each output geojson file is not too large)
# to Carlos: it will be helpful if you can parallelize this part using 'foreach' and 'doparallel' functions
# useful instructions: https://www.blasbenito.com/post/02_parallelizing_loops_with_r/

#define number of data frames to split into
n <- 100
OD_chunks = split(OD_to_impute, factor(sort(rank(row.names(OD_to_impute))%%n)))

# generate routes for each data frame chunk
i = 0
for (chunks in OD_chunks){
  print(i)
  # if (i <= 94){
  #   i = i + 1
  #   next
  # }
  for (row in 1:nrow(chunks)) { 
    # loop through each O-D pair and query routes using function 'route_osrm' (openstreetmap router)
    # more info about the router: https://docs.ropensci.org/stplanr/reference/route_osrm.html
    origin_tract <- as.character(chunks$home_GEOID[row])
    dest_tract <- as.character(chunks$destination[row])
    origin_node <- tract_centroid %>% filter(GEOID == origin_tract)
    dest_node <- tract_centroid %>% filter(GEOID == dest_tract)
    start_node_coordinate <- st_coordinates(origin_node)
    end_node_coordinate <- st_coordinates(dest_node)
    
    tryCatch(expr = {
      paths_sf <- route_osrm(start_node_coordinate, end_node_coordinate, osrm.profile = "car")
      paths_sf <- st_transform(paths_sf, crs = 4326)
      paths_sf$source <- origin_tract
      paths_sf$destination <- dest_tract
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

