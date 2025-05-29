import warnings
warnings.simplefilter(action='ignore')

import os
import pandas as pd
import geopandas as gpd
import psycopg2
from shapely.geometry import mapping
from shapely.wkb import dumps as wkb_dumps

import folium
from branca.element import Figure, Element
from folium.features import DivIcon

# Load the GeoJSON boundary
gdf = gpd.read_file(r"data\bc.geojson")

# Reproject to EPSG:4326 if not already
if gdf.crs != "EPSG:4326":
    gdf = gdf.to_crs("EPSG:4326")

# Collapse to a single geometry (union) and convert to WKB
bc_geom = gdf.unary_union
bc_geom_wkb = wkb_dumps(bc_geom)  # Returns bytes

# PostgreSQL connection
PG_HOST_CW= os.getenv('PG_HOST_CW').rstrip()
PG_PORT_CW= os.getenv('PG_PORT_CW').rstrip()
PG_DATABASE_CW= os.getenv('PG_DATABASE_CW').rstrip()
PG_USER_CW= os.getenv('PG_USER_CW').rstrip()
PG_PASSWORD_CW= os.getenv('PG_PASSWORD_CW').rstrip()
conn = psycopg2.connect(
    dbname=PG_DATABASE_CW,
    user=PG_USER_CW,
    password=PG_PASSWORD_CW,
    host=PG_HOST_CW,
    port=PG_PORT_CW
)

cursor = conn.cursor()

sqlTabs= """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'assets'
    """

df_tabs_assets = pd.read_sql(sqlTabs, conn)

tab_names= [
    x for x in df_tabs_assets['table_name'].to_list() 
        if x not in ['qgis_projects', 'trails', 'roads']
]

results = {}
for table in tab_names:
    print(f"Processing table: {table}")
    query = f"""
        SELECT
        *,
        ST_X(ST_Transform(wkb_geometry, 4326)) AS longitude,
        ST_Y(ST_Transform(wkb_geometry, 4326)) AS latitude,
        ST_Distance(
            ST_Transform(wkb_geometry, 4326)::geography,
            ST_SetSRID(%s::geometry, 4326)::geography
        ) AS distance_meters
        FROM
        assets.{table}
        WHERE
        NOT ST_Intersects(
            ST_Transform(wkb_geometry, 4326),
            ST_SetSRID(%s::geometry, 4326)
        );
    """
    # Execute query using the WKB geometry
    #cursor.execute(query, (psycopg2.Binary(bc_geom_wkb),))
    df_tab = pd.read_sql_query(query, conn, params=[bc_geom_wkb, bc_geom_wkb])

    results[table] = df_tab
    
# Combine results into a single DataFrame
df = pd.concat(results.values(), ignore_index=True)
df = df[df['distance_meters'] > 50]
df.sort_values(by='distance_meters', ascending=False, inplace=True)
df.drop(columns=['wkb_geometry', 'ogc_fid', 'campsite_number'], inplace=True)
df = df.round(3)



cursor.close()
conn.close()





# --- Color by asset_category ---
cats = df["asset_category"].unique()
palette = ["red","blue","purple","orange","pink","darkred","cadetblue","darkpink", "green"]
color_map = {cat: palette[i % len(palette)] for i, cat in enumerate(cats)}

# --- Build Folium map with no default tiles ---
cent = bc_geom.centroid
m = folium.Map(
    location=[50.897439, -121.868009],
    zoom_start=5,
)

# --- Add Google Satellite basemap ---
folium.TileLayer(
    tiles='http://{s}.google.com/vt/lyrs=s&x={x}&y={y}&z={z}',
    attr='Google Satellite',
    name='Google Satellite',
    max_zoom=20,
    subdomains=['mt0','mt1','mt2','mt3']
).add_to(m)



# BC boundary outline
folium.GeoJson(
    mapping(bc_geom),
    name="BC boundary",
    style_function=lambda f: {"color":"grey","weight":2,"fill":False}
).add_to(m)

# one FeatureGroup per category
groups = {}
for cat in cats:
    fg = folium.FeatureGroup(name=str(cat), show=True)
    groups[cat] = fg
    m.add_child(fg)

for _, row in df.iterrows():
    cat = row["asset_category"]
    popup_html = "".join(
        f"<b>{col}</b>: {row[col]}<br/>" for col in df.columns
    )

    # draw the circle marker
    folium.CircleMarker(
        [row["latitude"], row["longitude"]],
        radius=4,
        color=color_map[cat],
        fill=True,
        fill_opacity=0.7,
        popup=folium.Popup(popup_html, max_width=300)
    ).add_to(groups[cat])

    # add the label using DivIcon
    folium.map.Marker(
        [row["latitude"], row["longitude"]],
        icon=DivIcon(
            icon_size=(150, 36),
            icon_anchor=(0, 0),
            html=f'''
                <div style="
                    font-size: 12px;
                    color: black;
                    text-shadow:
                        -1px -1px 0 white,
                         1px -1px 0 white,
                        -1px  1px 0 white,
                         1px  1px 0 white;
                ">
                    {row["gisid"]}
                </div>
            '''
        )
    ).add_to(groups[cat])

# layer control now lets you switch between Google Satellite, OSM, and your overlays
folium.LayerControl(collapsed=False).add_to(m)

# --- Create Figure first ---
fig = Figure(width="100%", height="100%")
fig.add_child(m)

# --- Floating legend in bottom-right ---
legend_html = """
<div id="legend" style="
     position: fixed;
     bottom: 50px; right: 30px; z-index:1000;
     background-color: rgba(255,255,255,0.9); 
     padding: 10px;
     border-radius: 5px; 
     border: 1px solid grey;
     box-shadow: 0 2px 5px rgba(0,0,0,0.2);
     font-family: Arial, sans-serif;
     font-size: 12px;
">
  <b style="font-size: 14px;">Legend</b><br/>
"""
for cat, col in color_map.items():
    legend_html += f"""
  <div style="
       display: flex;
       align-items: center;
       margin: 5px 0;
  ">
    <div style="
         width: 15px; 
         height: 15px;
         background-color: {col};
         border: 1px solid #333;
         margin-right: 8px;
         border-radius: 50%;
    "></div>
    <span>{cat}</span>
  </div>
"""
legend_html += "</div>"

fig.html.add_child(Element(legend_html))

# --- Scrollable table below map ---
tbl = df.to_html(index=False, classes="table table-striped", border=0)
scroll_div = f"""
<div style="
    max-height:250px; overflow-y:auto;
    width:95%; margin:10px auto;
">
    {tbl}
</div>
"""
fig.html.add_child(Element(
    '<h2 style="text-align:center; font-size:30px;">'
    'Outside-BC Asset Coordinates'
    '</h2>'
))
fig.html.add_child(Element(scroll_div))

output_path = r"outputs\out_of_bc.html"
fig.save(output_path)
