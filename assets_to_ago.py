'''
Publish BCparks Assets data to ArcGIS Online (AGO):
    - reads tables from CityWide Postgres db.
    - cleans and transforms data
    - publishes feature layers to AGO
'''

import warnings
warnings.simplefilter(action='ignore')

import os
import logging
import psycopg2
from psycopg2 import OperationalError 
from psycopg2 import DatabaseError
import pandas as pd
import geopandas as gpd
#from shapely import wkb

import timeit



class PostgresDBManager:
    def __init__(self, dbname, user, password, host="localhost", port="5432"):
        """
        Initializes PostgresDBManager with database connection parameters.
        """
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connection = None
        self.cursor = None

    def connect(self):
        """Establishes a connection to the PostgreSQL database."""
        try:
            self.connection = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            logging.info("..Postgres connection established successfully.")
            return self.connection
        
        except OperationalError as e:
            logging.error(f"..error connecting to database: {e}")
            self.connection = None
            
    
    def create_cursor(self):
        """Creates a cursor object for executing queries."""
        if self.connection:
            try:
                self.cursor = self.connection.cursor()
                logging.info("..cursor created successfully.")
                
            except DatabaseError as e:
                logging.error(f"..error creating cursor: {e}")
                self.cursor = None
                
        else:
            logging.warning("..no active connection.")

    def disconnect(self):
        """Closes the connection to the PostgreSQL database."""
        if self.connection:
            try:
                self.connection.close()
                logging.info("\nPostgres connection closed.")
                
            except DatabaseError as e:
                logging.error(f"Error closing connection: {e}")
                
            finally:
                self.connection = None
                self.cursor = None
        else:
            logging.warning("..no active database connection to close.")




def read_assets(conn) -> pd.DataFrame:
    """
    Returns a dataframe containing assets point data 
    """
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
    assets_dict= {}
    for table_name in tab_names:
        print (f'..reading table: {table_name}')
        if table_name in ['trails', 'roads']: #centroids
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
        assets_dict[table_name]= df
        
        #concatinate tables data into a signle df
        df = pd.concat(
            assets_dict.values(), 
            ignore_index=True
        )

    return df


def read_trails(conn) -> gpd.GeoDataFrame:
    """
    Returns a geodataframe containing trails line data 
    """
    query = "SELECT * FROM assets.trails"
    gdf = gpd.read_postgis(
        query, 
        conn, 
        geom_col='wkb_geometry'
    )
    
    return gdf


def process_assets (df, latcol, loncol) -> gpd.GeoDataFrame:
    """
    Returns a gdf of clean Assets data
    """
    # Filter asset categories
    logging.info('..filtering Assets Categories')
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
        'Tools'
    ]
    
    df= df[df['asset_category'].isin(cats)]
    
    
    logging.info('..cleaning-up Assets column names')
    ast_cols= {
        'assetid': 'Asset ID', 
        'gisid': 'GIS ID', 
        'park': 'Park',
        'park_subarea': 'Park Subarea', 
        'asset_category': 'Category - Classification', 
        'asset_type': 'Segment - Sub Classification', 
        'description': 'Description', 
        'campsite_number': 'Campsite Number', 
        'name': 'Name', 
        'accessible': 'acs Is Asset Accessible',
        'route_accessible': 'acs Is the Route to the Asset Accessible', 
        'gis_latitude': 'GIS Latitude', 
        'gis_longitude': 'GIS Longitude'
            }
    
    df.rename(
        columns= ast_cols, 
        inplace= True
    )
    
    logging.info('..cleaning-up missing/out-of-range coordinates')
    # remove missing lat/longs
    df = df.dropna(subset=[latcol, loncol])
    
    # keep only points within BC
    lat_min, lat_max = 47, 60
    lon_min, lon_max = -145, -113

    df = df[
        (df[latcol] >= lat_min) & (df[latcol] <= lat_max) &
        (df[loncol] >= lon_min) & (df[loncol] <= lon_max)
    ]
    
    logging.info('..converting the Assets dataset to geodataframe')
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df[loncol], df[latcol]),
        crs="EPSG:4326"
    )
    
    # convert object cols to strings (objects not supported by fiona)
    gdf = gdf.astype(
        {col: 'str' for col in gdf.select_dtypes(include=['object']).columns}
    )  
    
    logging.info(f'..the final Assets dataset has {gdf.shape[0]} rows and {gdf.shape[1]} columns')
    
    return gdf


def process_trails(gdf) -> gpd.GeoDataFrame:
    """
    Returns a gdf of clean trails data
    """
    logging.info('..cleaning-up Trails column names')
    trl_cols= {
        "assetid": "Asset ID", 
        "gisid": "GIS ID",
        "asset_category": "Category - Classification",
        "asset_type": "Asset Type",
        "park": "Park",
        "park_subarea": "Park Subarea",
        "trail_surface": "Trail Surface",
        "length_m": "Length Meters",
        "trail_name": "Trail Name",
        "osmid": "OSM ID",
        "description": "Description",
        "verified_by": "Verified By",
        "accessible": "Is Accessible",
        "route_accessible": "Is Route Accessible",
        "wkb_geometry": "geometry"
            }
    
    gdf.rename(
        columns= trl_cols, 
        inplace= True
    )
    
    #change cols order
    gdf = gdf[trl_cols.values()]
    
    
    # reproject trails gdf to wgs84
    gdf = gdf.set_geometry("geometry")
    
    logging.info('..repojecting Trails coordinates')
    gdf.to_crs(
        crs= 4326,
        inplace= True
    )
    
    # convert object cols to strings (objects not supported by fiona)
    gdf = gdf.astype(
        {col: 'str' for col in gdf.select_dtypes(include=['object']).columns}
    )
    
    logging.info(f'..the final Trails dataset has {gdf.shape[0]} rows and {gdf.shape[1]} columns')
    
    return gdf
    



if __name__ == "__main__":
    start_t = timeit.default_timer() #start time
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    
    try:
        logging.info("Connecting to CityWide database")
        pg= PostgresDBManager(
            host= os.getenv('PG_HOST_CW'),
            port= os.getenv('PG_PORT_CW'),
            dbname= os.getenv('PG_DATABASE_CW'),
            user= os.getenv('PG_USER_CW'),
            password= os.getenv('PG_PASSWORD_CW')
        )
        conn= pg.connect()
        
        logging.info("\nReading Assets (points) data")
        df_ast= read_assets(conn)
        
        logging.info("\nReading Trails (line) data")
        gdf_trl= read_trails(conn)
        
        logging.info("\nProcessing Assets data")
        #assets
        latcol = 'GIS Latitude'
        loncol = 'GIS Longitude'
        gdf_ast= process_assets (df_ast, latcol, loncol)
        
        logging.info("\nProcessing Trails data")
        #trails
        gdf_trl= process_trails(gdf_trl)
        
        
    except Exception as e:
        raise Exception(f"Error occurred: {e}")  
    
    finally: 
        pg.disconnect()
        
    
    
    finish_t = timeit.default_timer() #finish time
    t_sec = round(finish_t-start_t)
    mins = int (t_sec/60)
    secs = int (t_sec%60)
    print('\nProcessing Completed in {} minutes and {} seconds'.format (mins,secs))
            