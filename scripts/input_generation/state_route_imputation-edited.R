# Written by Xiaodan Xu and Carlos Guirado

# Clear environment
rm(list = ls())

# Import libraries

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
library(parallel)
library(foreach)
library(doParallel)
library(doSNOW)

#### Time the start of the execution


num_cores  <- parallel::detectCores() # 8 cores. we can use 7 to parallelise and leave 1 for other stuff the computer has to deal with

start_time <- Sys.time()

#### set working directory ####
path2file <-"C:/Users/cguirado/Downloads/drive-download"
setwd(path2file)

state_input = 'CA'


#### load input ####
# load census tract shapefile
state_tract_shapefile_input <- st_read(paste0('Network/combined/combined_tracts/combined_tracts.shp'))
# 73,056 census tracts

# load OD with missing routes
OD_to_impute <- fread(paste0('Network/combined/OD_to_impute_spillover.csv'))

cl <- parallel::makeCluster(num_cores-1)
# #parallel::registerDoParallel(cl)
registerDoSNOW(cl)


### generate census tract centroid geometry
#split the shapefile

split_shapefile <- split(state_tract_shapefile_input, rep(1:ceiling(nrow(state_tract_shapefile_input)/100), each = 100, length.out = nrow(state_tract_shapefile_input)))

iterations <- 730
pb <- txtProgressBar(max = iterations, style = 3)
progress <-function(n) setTxtProgressBar(pb, n)
opts <- list(progress = progress)

foreach(i = 1:length(split_shapefile), .combine = rbind, .options.snow = opts) %dopar% {
  split_shapefile[[i]] <- sf::st_transform(split_shapefile[[i]], crs = 4326)
}

#close(pb)
#stopCluster(cl)

state_tract_shapefile <- do.call(rbind,split_shapefile)
tract_centroid <- st_centroid(state_tract_shapefile)
tract_centroid <- tract_centroid %>% select(GEOID)

#write.table(state_tract_shapefile, file="C:/Users/cguirado/Downloads/drive-download/state_tract_reproj.csv")
#write.table(tract_centroid, file="C:/Users/cguirado/Downloads/drive-download/tract_centroid.csv")

#plot(st_geometry(tract_centroid)) # (optional) show the centroid file

# format census tract ID in OD file
OD_to_impute <- OD_to_impute %>%
  mutate(home_GEOID = str_pad(home_GEOID, 11, pad = "0"), 
         destination = str_pad(destination, 11, pad = "0"))


### split data frame into n equal-sized data frames to generate routes (so that each output geojson file is not too large)

#define number of data frames to split into
n <- 100
OD_chunks = split(OD_to_impute, factor(sort(rank(row.names(OD_to_impute))%%n)))

# Set up parallelisation
registerDoSNOW(cl)

iterations <- 1000
pb <- txtProgressBar(max = iterations, style = 3)
progress <-function(n) setTxtProgressBar(pb, n)
opts <- list(progress = progress)

# generate routes for each data frame chunk
j = 0
foreach(i = 1:length(OD_chunks), .combine = rbind, .options.snow = opts, .packages='dplyr') %dopar% {
  print(j)
  # if (i <= 94){
  #   i = i + 1
  #   next
  # }
  for (row in 1:nrow(OD_chunks[[i]])) { 
    # loop through each O-D pair and query routes using function 'route_osrm' (openstreetmap router)
    # more info about the router: https://docs.ropensci.org/stplanr/reference/route_osrm.html
    origin_tract <- as.character(OD_chunks[[i]]$home_GEOID[row])
    dest_tract <- as.character(OD_chunks[[i]]$destination[row])
    origin_node <- tract_centroid %>% filter(GEOID == origin_tract)
    dest_node <- tract_centroid %>% filter(GEOID == dest_tract)
    start_node_coordinate <- sf::st_coordinates(origin_node)
    end_node_coordinate <- sf::st_coordinates(dest_node)
    
    tryCatch(expr = {
      paths_sf <- stplanr::route_osrm(start_node_coordinate, end_node_coordinate, osrm.profile = "car")
      paths_sf <- sf::st_transform(paths_sf, crs = 4326)
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
  sf::st_write(out_route, paste0('route/processed/imputed_', state_input, '_route_', j, '.geojson'))
  j = j + 1
  # break
}

close(pb)
stopCluster(cl)

### Time the end of the execution
end_time <- Sys.time()

### How long did the code take?
elapsed_time <- end_time - start_time

#Print
print("The code took: ",elapsed_time)

