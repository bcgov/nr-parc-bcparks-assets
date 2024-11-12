'''
Publish BCparks Assets data to ArcGIS Online (AGO):
    - reads tables from CityWide Postgres db.
    - cleans and transforms data
    - publishes feature layers to AGO
'''

import warnings
warnings.simplefilter(action='ignore')

import os
import psycopg2
import pandas as pd
import geopandas as gpd
from shapely import wkb

import timeit
start_t = timeit.default_timer() #start time

# Database connection details
host = os.getenv('PG_HOST_CW')
port = os.getenv('PG_PORT_CW')
database = os.getenv('PG_DATABASE_CW')
user = os.getenv('PG_USER_CW')
password = os.getenv('PG_PASSWORD_CW')

try:
    # Connect to PostgreSQL database
    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )

    # Fetch tables and schema names
    sqlTabs= """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'assets'
     """
    
    df_tabs_assets = pd.read_sql(sqlTabs, conn)
    
    tab_names= [
        x for x in df_tabs_assets['table_name'].to_list() 
            if x !='qgis_projects'
    ]

    # Read tables
    tab_dict= {}
    for table_name in tab_names:
        print (f'\nreading table: {table_name}')
        if table_name in ['trails', 'roads']:
            query= f"""
                SELECT 
                    *, 
                    ST_Y(
                        ST_Transform(
                            ST_Centroid(wkb_geometry), 
                            4326
                        )
                    ) AS gis_latitude,
                    ST_X(
                        ST_Transform(
                            ST_Centroid(wkb_geometry), 
                            4326
                        )
                    ) AS gis_longitude
                FROM 
                	assets.{table_name};
                """
                
        else:
            query = f"""
                        SELECT 
                            *, 
                            ST_Y(
                                ST_Transform(
                                    wkb_geometry, 
                                    4326
                                )
                            ) AS gis_latitude,
                            ST_X(
                                ST_Transform(
                                    wkb_geometry, 
                                    4326
                                )
                            ) AS gis_longitude
                        FROM
                        	assets.{table_name};
                    """
                    
        df = pd.read_sql(query, conn)
        df.drop(columns=['wkb_geometry'], inplace=True)
        tab_dict[table_name]= df
        

    
    # Concatenate all DataFrames into one
    df_all = pd.concat(tab_dict.values(), ignore_index=True)

    # Filter asset categories
    cats=[
    'Grounds',
    'Furniture and Amenities',
    'Signs',
    'Water Service',
    'Transportation',
    'Stormwater',
    'Bridges',
    'Structures',
    'Trails',
    'Buildings',
    'Electrical Telcomm Service',
    'Wastewater Service',
    'Water Management',
    'Fuel Storage',
    'Tools']
    df_all= df_all[df['asset_category'].isin(cats)]

    # Keep only points within BC
    lat_min, lat_max = 47, 60
    lon_min, lon_max = -145, -113

    df_all = df_all[
        (df_all['gis_latitude'] >= lat_min) & (df_all['gis_latitude'] <= lat_max) &
        (df_all['gis_longitude'] >= lon_min) & (df_all['gis_longitude'] <= lon_max)
    ]
    


    # Read trails table into a gdf
    print ('\nreading table: trails')
    query_t = "SELECT * FROM assets.trails"
    #df_trl= pd.read_sql(query, conn) 
    gdf_trl = gpd.read_postgis(query_t, conn, geom_col='wkb_geometry')
        

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the connection
    if conn:
        conn.close()



finish_t = timeit.default_timer() #finish time
t_sec = round(finish_t-start_t)
mins = int (t_sec/60)
secs = int (t_sec%60)
print('\nProcessing Completed in {} minutes and {} seconds'.format (mins,secs))
