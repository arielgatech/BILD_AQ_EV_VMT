import pandas as pd
import geopandas as gpd
import time
import os
import warnings
from pandas import read_csv

selected_state='UT'

#os.chdir('/Users/xiaodanxu/Library/CloudStorage/GoogleDrive-arielinseu@gmail.com/My Drive/GEMS/BILD-AQ/data')
microtype_lookup = read_csv('ccst_geoid_key_tranps_geo_with_imputation.csv', sep = ',')

census_tract_shapefile = 'Network/combined/census_tracts_2017.geojson'
polygon_gdf = gpd.read_file(census_tract_shapefile)
polygon_gdf = polygon_gdf.to_crs('EPSG:4326')
# polygon_gdf.plot()
from pygris.utils import shift_geometry

polygon_gdf_rescaled = shift_geometry(polygon_gdf)
#polygon_gdf_rescaled.plot()

output_dir = 'route/processed/final/' + selected_state
route_list = [file for file in os.listdir(output_dir) if file.endswith('.geojson')]
#     print(route_list[0:5])
start_time = time.time()
for route in route_list:
    if route.startswith('._'):
        continue
    file_name = route.split('.geojson')[0]
    isExist = os.path.exists(output_dir + '/' + file_name + '.csv')
    if isExist:
        continue
    print('processing route ' + route)
    line_gdf = gpd.read_file(output_dir + '/' + route)
#     sample_line_gdf = line_gdf.head(100)
    # print(start_time)
    line_by_polygon = gpd.overlay(line_gdf, polygon_gdf, how='intersection')

    # compute segment length in meters
    line_by_polygon = line_by_polygon.to_crs("EPSG:3310")
    # in order to get length in meter, the shapefile need to re-projected to a coordinate system in meters (not required in R)
    line_by_polygon.loc[:, 'Length'] = line_by_polygon.loc[:, 'geometry'].length

    line_by_polygon_df = pd.DataFrame(line_by_polygon.drop(columns='geometry'))
    line_by_polygon_df = line_by_polygon_df[['source',	'destination',
                                              'distance', 'GEOID', 'Length']]
#     line_by_polygon_df.columns = []
#     print(len(line_by_polygon))
    line_by_polygon_df.to_csv(output_dir + '/' + file_name + '.csv', index = False)

#     break
end_time = time.time()
total_time = end_time - start_time
print('spatial intersection in Python takes ' + str(total_time) + ' sec')

# Make a copy in 'Input/route/{selected_state}_external'

line_by_polygon_df.to_csv('Input/route/' + selected_state + '_external/' + file_name + '.csv', index = False)

#filelist = [file for file in os.listdir(output_dir) if (file.endswith('.csv'))]
#route_df = pd.concat([read_csv(output_dir + '/' + f) for f in filelist ])
#route_df.to_csv(output_dir + '/' + 'imputed_' + selected_state + '_spillover_route.csv', index = False)