#-------------------------------------------------------------------------------
# Name:        BCParks Assets Data Workflow
#
# Purpose:     This script updates BCparks Assets data in ArcGIS Online (AGO):
#                (1) reads assets tables from CityWide Postgres db
#                (2) cleans and transforms data
#                (3) publishes feature layers to AGO
#              
# Input(s):      (1) CityWide Postgres credentials.
#                (2) AGO credentials.           
#
# Author:      Moez Labiadh - GeoBC
#
# Created:     2024-11-18
# Updated:     2025-05-28
#-------------------------------------------------------------------------------

import warnings
warnings.simplefilter(action='ignore')

import os
import json
import logging

import psycopg2
from psycopg2 import OperationalError 
from psycopg2 import DatabaseError

import pandas as pd
import geopandas as gpd

from io import BytesIO
from datetime import datetime
from arcgis.gis import GIS

import timeit



class PostgresDBManager:
    def __init__(self, dbname, user, password, host, port):
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



class AGOManager:
    def __init__(self, host, username, password):
        """
        Initialize the AGOManager instance 
        """
        self.host = host
        self.username = username
        self.password = password
        self.gis = None 
    
    
    def connect(self):
        """
        Establish a connection to AGO and store the GIS object.
        """
        self.gis = GIS(self.host, self.username, self.password, verify_cert=True)
        if self.gis.users.me:
            logging.info(f'..successfully connected to AGOL as {self.gis.users.me.username}: {self.gis.users.me.userLicenseTypeId}')
            privileges = self.gis.users.me.privileges
            print(f"\nBC Parks Account Privileges:")
            for privilege in sorted(privileges):
                print(f"  {privilege}")
        else:
            logging.error('..connection to AGOL failed.')
            raise ConnectionError("Failed to connect to AGOL.")
    

    def publish_feature_layer_from_geojson(self, geojson_dict, title, geojson_name, item_desc, folder):
        """
        Publishes a GeoJSON dictionary to AGO as a Feature Layer, overwriting if it already exists.
        """
        if not self.gis:
            raise RuntimeError("Not connected to AGOL. Please call connect() first.")

        try:
            # Search for an existing GeoJSON item with the same title
            existing_items = self.gis.content.search(
                f"title:\"{title}\" AND owner:{self.gis.users.me.username}",
                item_type="GeoJson"
            )
            existing_items = [item for item in existing_items if item.title == title]
            
            # Delete the existing GeoJSON item if found
            for item in existing_items:
                if item.type == 'GeoJson':
                    item.delete(force=True)
                    logging.info(f"..existing GeoJSON item '{item.title}' deleted.")

            # Create a new GeoJSON item
            geojson_item_properties = {
                'title': title,
                'type': 'GeoJson',
                'tags': 'BCparks data',
                'description': item_desc,
                'fileName': f'{geojson_name}.geojson'
            }
            geojson_file = BytesIO(json.dumps(geojson_dict).encode('utf-8'))
            new_geojson_item = self.gis.content.add(
                item_properties=geojson_item_properties, data=geojson_file, folder=folder)

            # Publish or overwrite the existing feature layer
            new_geojson_item.publish(overwrite=True)
            logging.info(f"..feature layer '{title}' published successfully.")

        except Exception as e:
            error_message = f"..error publishing/updating feature layer: {str(e)}"
            logging.error(error_message)
            raise RuntimeError(error_message)
    
            
    def disconnect(self):
        """
        Disconnect from AGO by clearing the GIS connection.
        """
        if self.gis:
            username = self.gis.users.me.username
            self.gis = None
            logging.info(f"\nDisconnected from {username} AGOL account.")
        else:
            logging.warning("\nNo active AGOL connection to disconnect.")



def read_assets(conn) -> pd.DataFrame:
    """
    Read Postgres tables and Returns a dataframe containing assets point data 
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
        'Fuel Storage'
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
    
    #change cols order
    df = df[ast_cols.values()]
    
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
    
    gdf = gdf.set_geometry("geometry")
    
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


def gdf_to_geojson(gdf):
    """
    Converts a GeoDataFrame to a GeoJSON-like dictionary.
    Standalone function for pre-processing GeoDataFrames.
    """
    # Clean the GeoDataFrame
    gdf = gdf.fillna('')
    gdf = gdf.replace("None", "")
    
    features = []
    for _, row in gdf.iterrows():
        feature = {
            "type": "Feature",
            "properties": {},
            "geometry": row['geometry'].__geo_interface__
        }
        for column, value in row.items():
            if column != 'geometry':
                if isinstance(value, (datetime, pd.Timestamp)):
                    feature['properties'][column] = value.isoformat() if not pd.isna(value) else ''
                else:
                    feature['properties'][column] = value
        features.append(feature)
    
    geojson_dict = {
        "type": "FeatureCollection",
        "features": features
    }
    return geojson_dict



if __name__ == "__main__":
    start_t = timeit.default_timer() #start time
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    
    #read and process data from postgres
    try:
        logging.info("Connecting to CityWide database")
        PG_HOST_CW= os.getenv('PG_HOST_CW').rstrip()
        PG_PORT_CW= os.getenv('PG_PORT_CW').rstrip()
        PG_DATABASE_CW= os.getenv('PG_DATABASE_CW').rstrip()
        PG_USER_CW= os.getenv('PG_USER_CW').rstrip()
        PG_PASSWORD_CW= os.getenv('PG_PASSWORD_CW').rstrip()

        pg= PostgresDBManager(
            dbname= PG_DATABASE_CW,
            user= PG_USER_CW,
            password= PG_PASSWORD_CW,
            host= PG_HOST_CW,
            port= PG_PORT_CW
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
 
    
    # Pre-convert GeoDataFrames to GeoJSON
    logging.info("\nPre-converting GeoDataFrames to GeoJSON format")
    geojson_assets = None
    geojson_trails = None
    
    if gdf_ast.shape[0] > 0:
        logging.info("..converting Assets to GeoJSON")
        geojson_assets = gdf_to_geojson(gdf_ast)
    else:
        logging.warning("..Assets dataset is empty")
        
    if gdf_trl.shape[0] > 0:
        logging.info("..converting Trails to GeoJSON")
        geojson_trails = gdf_to_geojson(gdf_trl)
    else:
        logging.warning("..Trails dataset is empty")
   
    #publish to multiple AGO accounts

    AGO_HOST = os.getenv('AGO_HOST')

    accounts = [
        {
            "username": os.getenv('AGO_USERNAME_DSS'),
            "password": os.getenv('AGO_PASSWORD_DSS'),
            "label": "DSS",
            "folder": "DSS Protected Areas Resource Catalogue (PARC) - Resource Analysis",
            "asset_title": "PARC_L1G_Park_Asset_Data_Feature_Layer_v2",
            "trail_title": "PARC_L1G_Park_Trail_Data_Feature_Layer_v2"
        },
        {
            "username": os.getenv('AGO_USERNAME_BP'),
            "password": os.getenv('AGO_PASSWORD_BCPARKS'),
            "label": "BC Parks",
            "folder": "AMS Data",
            "asset_title": "PARC_BCParks_Assets_Data",
            "trail_title": "PARC_BCParks_Trails_Data"
        }
    ]

    for acct in accounts:
        try:
            logging.info(f'\nLogging into AGO ({acct["label"]} account)')
            ago = AGOManager(AGO_HOST, acct["username"], acct["password"])
            ago.connect()
            '''
            # Assets - using pre-converted GeoJSON
            logging.info(f'\nPublishing Assets for {acct["label"]}')
            if geojson_assets:
                ago.publish_feature_layer_from_geojson(
                    geojson_assets,
                    title=acct["asset_title"],
                    geojson_name='bcparks_assets_v2',
                    item_desc=f'Point dataset - BCParks assets (updated on {datetime.today():%B %d, %Y})',
                    folder=acct["folder"]
                )
            else:
                logging.error('..Assets dataset is empty. Skipping.')

            # Trails - using pre-converted GeoJSON
            logging.info(f'\nPublishing Trails for {acct["label"]}')
            if geojson_trails:
                ago.publish_feature_layer_from_geojson(
                    geojson_trails,
                    title=acct["trail_title"],
                    geojson_name='bcparks_trails_v2',
                    item_desc=f'Line dataset - BCParks trails (updated on {datetime.today():%B %d, %Y})',
                    folder=acct["folder"]
                )
            else:
                logging.error('..Trails dataset is empty. Skipping.')

        except Exception as e:
            raise Exception(f"Error publishing to {acct['label']} AGO account: {e}")
        '''
        finally:
            ago.disconnect()

        
    finish_t = timeit.default_timer() #finish time
    t_sec = round(finish_t-start_t)
    mins = int (t_sec/60)
    secs = int (t_sec%60)
    print('\nProcessing Completed in {} minutes and {} seconds'.format (mins,secs))
