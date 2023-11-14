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
library(readr)

#### Time the start of the execution


num_cores  <- parallel::detectCores() # 16 cores. we can use 7 to parallelise and leave 1 for other stuff the computer has to deal with

start_time <- Sys.time()

#### set working directory ####
path2file <-"/Users/xiaodanxu/Library/CloudStorage/GoogleDrive-arielinseu@gmail.com/My Drive/GEMS/BILD-AQ/data"
setwd(path2file)

state_input = 'NV'
if_spillover = 0


#### load input ####
# load census tract shapefile
state_tract_shapefile_input <- st_read(paste0('Network/combined/combined_tracts/combined_tracts.shp'))
# 73,056 census tracts

# load OD with missing routes
OD_to_impute <- fread(paste0('Network/', state_input, '/OD_to_impute.csv'))

cl <- parallel::makeCluster(num_cores-4) #if 32 cores: 27 is max as anything above seems to crash. else: num_Cores-1
# #parallel::registerDoParallel(cl)

registerDoSNOW(cl)
#registerDoParallel(cl) # with 31 cores

### generate census tract centroid geometry
#split the shapefile

split_shapefile <- split(state_tract_shapefile_input, rep(1:ceiling(nrow(state_tract_shapefile_input)/100), each = 100, length.out = nrow(state_tract_shapefile_input)))

iterations <- 730
pb <- txtProgressBar(max = iterations, style = 3)
progress <-function(n) setTxtProgressBar(pb, n)
opts <- list(progress = progress)

#foreach(i = 1:length(split_shapefile), .combine = rbind) %dopar% {
#  split_shapefile[[i]] <- sf::st_transform(split_shapefile[[i]], crs = 4326)
#}

foreach(i = 1:length(split_shapefile), .combine = rbind, .options.snow = opts) %dopar% {
  split_shapefile[[i]] <- sf::st_transform(split_shapefile[[i]], crs = 4326)
}

#close(pb)
#stopCluster(cl)

state_tract_shapefile <- do.call(rbind,split_shapefile)
tract_centroid <- st_centroid(state_tract_shapefile)
tract_centroid <- tract_centroid %>% select(GEOID)


### Time the end of the execution
end_time_split <- Sys.time()

### How long did the code take up to here?
elapsed_time_split <- end_time_split - start_time

#write.table(state_tract_shapefile, file="C:/Users/cguirado/Downloads/drive-download/state_tract_reproj.csv")
#write.table(tract_centroid, file="C:/Users/cguirado/Downloads/drive-download/tract_centroid.csv")

#plot(st_geometry(tract_centroid)) # (optional) show the centroid file

# format census tract ID in OD file
OD_to_impute <- OD_to_impute %>%
  mutate(home_GEOID = str_pad(home_GEOID, 11, pad = "0"), 
         destination = str_pad(destination, 11, pad = "0"))

##### FOR TIME COMPLEXITY TESTING ONLY
##### 1000 routes

subset_OD <- OD_to_impute[1:50, ]

### split data frame into n equal-sized data frames to generate routes (so that each output geojson file is not too large)

#define number of data frames to split into
n <- 50
OD_chunks = split(OD_to_impute, factor(sort(rank(row.names(OD_to_impute))%%n)))


iterations <- 100
pb <- txtProgressBar(max = iterations, style = 3)
progress <-function(n) setTxtProgressBar(pb, n)
opts <- list(progress = progress)

library("doFuture")
registerDoFuture()
plan(multisession, workers = 10)

### Time the start of the loop execution
start_time_route_loop <- Sys.time()

# generate routes for each data frame chunk
foreach(i = 1:length(OD_chunks), .packages='sf') %dopar% {
  #j = 0
  for (row in 1:nrow(OD_chunks[[i]])) {
    cat(". Chunk:",i,";route:", row)
    # loop through each O-D pair and query routes using function 'route_osrm' (openstreetmap router)
    # more info about the router: https://docs.ropensci.org/stplanr/reference/route_osrm.html
    origin_tract <- as.character(OD_chunks[[i]]$home_GEOID[row])
    dest_tract <- as.character(OD_chunks[[i]]$destination[row])
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
   
  
  # j = j + 1
  # break
  }
  st_write(out_route, paste0('route/processed/imputed_', state_input, '_route_', i, '.geojson'), append=FALSE)
}

close(pb)
stopCluster(cl)

### Time the end of the execution
end_time_route_loop <- Sys.time()

### Rename the routes, format: 
### This is hard to do within the nested foreach/for loop due to global/local env issues on the package backend

# old_files <- list.files("route/processed", pattern = "*.geojson", full.names = TRUE)
# new_files <- paste0("route/processed/final/imputed_CA_route_",1:length(old_files),".geojson")
# file.copy(from = old_files, to = new_files)
#file.remove(old_files) if removing old files

# library(dplyr)


df <- list.files("route/processed", pattern = "*.geojson", full.names = TRUE) %>% 
  lapply(st_read) %>% 
  bind_rows 
if (if_spillover == 0){
  file_name = paste0('route/processed/final/imputed_', state_input, '_route.geojson')
} else{
  file_name = paste0('route/processed/final/imputed_', state_input, '_route_spillover.geojson')
}

st_write(df, file_name, append=FALSE)
### How long did the code take?
end_time <- Sys.time()
elapsed_time <- end_time - start_time

### How long did the loop take?
elapsed_time_loop <- end_time_route_loop - start_time_route_loop


#Print
cat("The code took: ", elapsed_time, " min in total.")
cat("The route imputation step took: ", elapsed_time_loop, " min.")

