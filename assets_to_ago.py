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
        Initializes the PostgresDBConnector with database connection parameters.
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
            logging.info("..connection established successfully.")
            return self.connection
        
        except OperationalError as e:
            logging.error(f"..error connecting to database: {e}")
            self.connection = None

    def disconnect(self):
        """Closes the connection to the PostgreSQL database."""
        if self.connection:
            try:
                self.connection.close()
                logging.info("..connection closed.")
                
            except DatabaseError as e:
                logging.error(f"..error closing connection: {e}")
                
            finally:
                self.connection = None
                self.cursor = None
        else:
            logging.warning("..no active database connection to close.")

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
            logging.warning("..no active  connection.")


def read_assets(conn):
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


def read_trails(conn):
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

def process_assets (df, latcol, loncol):
    """
    Returns a gdf of Cleans-up and transforms Assets data
    """
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
    
    df= df[df['asset_category'].isin(cats)]

    # Keep only points within BC
    lat_min, lat_max = 47, 60
    lon_min, lon_max = -145, -113

    df = df[
        (df[latcol] >= lat_min) & (df[latcol] <= lat_max) &
        (df[loncol] >= lon_min) & (df[loncol] <= lon_max)
    ]
    
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df[loncol], df[latcol]),
        crs="EPSG:4326"
    )
    
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
        
        logging.info("\nTransforming data")
        #assets
        latcol = 'gis_latitude'
        loncol = 'gis_latitude'
        gdf_ast= process_assets (df_ast, latcol, loncol)
        
        
    except Exception as e:
        raise Exception(f"Error occurred: {e}")  
    
    finally: 
        pg.disconnect()
        
    
    
    finish_t = timeit.default_timer() #finish time
    t_sec = round(finish_t-start_t)
    mins = int (t_sec/60)
    secs = int (t_sec%60)
    print('\nProcessing Completed in {} minutes and {} seconds'.format (mins,secs))
            