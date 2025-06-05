import logging
import json
from arcgis.gis import GIS
from io import BytesIO


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
            logging.info(f'..connected to AGOL as {self.gis.users.me.username}: {self.gis.users.me.userLicenseTypeId}')
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