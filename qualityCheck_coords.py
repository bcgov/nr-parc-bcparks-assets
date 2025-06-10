#-------------------------------------------------------------------------------
# Name:        BCParks Assets - Spatial Data Quality Check
#
# Purpose:     This script performs spatial data quality checks on BCParks assets:
#                (1) identifies assets outside the BC boundary
#                (2) saves a html report of the identified assets
#                (3) sends the report via email (not implemented yet - SMTP connection issues) 
#  
# Input(s):      (1) CityWide Postgres credentials.
#                      
# Author:      Moez Labiadh - GeoBC
#
# Created:     2025-06-05
# Updated:     2025-06-05
#-------------------------------------------------------------------------------

import warnings
warnings.simplefilter(action='ignore')

import os
import logging

from db_manager import PostgresDBManager

import pandas as pd
import geopandas as gpd
from shapely.geometry import mapping
from shapely.wkb import dumps as wkb_dumps
from shapely.wkb import loads as wkb_loads

import folium
from branca.element import Figure, Element
from folium.features import DivIcon

import smtplib
from email.message import EmailMessage

from datetime import datetime
import timeit


def read_geojson(geojson_path) -> bytes:
    """
    Reads the BC boundary GeoJSON file and 
    returns a WKB in EPSG:4326
    """
    gdf = gpd.read_file(geojson_path)
    
    if gdf.crs != "EPSG:4326":
        gdf = gdf.to_crs("EPSG:4326")
    
    bc_geom = gdf.unary_union
    
    return wkb_dumps(bc_geom)


def evaluate_assets (bc_geom_wkb, conn) -> pd.DataFrame:
    """
    Returns a df of assets outside the BC boundary.
    """

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
        logging.info (f"...processing table: {table}")
        query = f"""
                SELECT
                    *,
                    ST_X(ST_Transform(wkb_geometry, 4326)) AS longitude,
                    ST_Y(ST_Transform(wkb_geometry, 4326)) AS latitude,
                    ST_Distance(
                        ST_Transform(wkb_geometry, 4326)::geography,
                        ST_SetSRID(%s::geometry, 4326)::geography
                    ) / 1000.0 AS distance_km

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
    df = df[df['distance_km'] > 0.05] # filter out very close points (less than 50 m)
    df.sort_values(by='distance_km', ascending=False, inplace=True)
    df.drop(columns=['wkb_geometry', 'ogc_fid', 'campsite_number'], inplace=True)
    df = df.round(3)

    return df


def build_html_report(bc_geom_wkb, df) -> Figure:
    """
    Builds an HTML report with a Folium map 
    and a scrollable table
    """
    today = datetime.today().strftime("%B %d, %Y")

    # --- CASE: No out-of-BC assets detected ---
    if df.empty:
        report = Figure(width="100%", height="100%")
        report.html.add_child(Element(
            '<h2 style="text-align:center; font-size:25px; font-weight:bold;">'
            'Outside-BC Asset Coordinates'
            '</h2>'
        ))
        report.html.add_child(Element(
            f'<p style="text-align:center; font-size:16px; margin-top:20px;">'
            f'No out-of-BC coordinates detected (as of {today}).'
            '</p>'
        ))
        return report

    # --- Otherwise: build full report ---
    # Color by asset_category
    cats = df["asset_category"].unique()
    palette = ["red","blue","purple","orange","pink",
               "darkred","cadetblue","darkpink","green"]
    color_map = {cat: palette[i % len(palette)] for i, cat in enumerate(cats)}

    # Create Folium map without default tiles
    m = folium.Map(
        location=[50.897439, -121.868009],
        zoom_start=5,
        tiles=None
    )
    map_var = m.get_name()

    # Add basemaps
    folium.TileLayer("OpenStreetMap", name="OpenStreetMap", show=True).add_to(m)
    folium.TileLayer(
        tiles='http://{s}.google.com/vt/lyrs=s&x={x}&y={y}&z={z}',
        attr='Google Satellite',
        name='Google Satellite',
        max_zoom=20,
        subdomains=['mt0','mt1','mt2','mt3'],
        show=False
    ).add_to(m)

    # Draw BC boundary
    geom = wkb_loads(bc_geom_wkb)
    folium.GeoJson(
        mapping(geom),
        name="BC boundary",
        style_function=lambda f: {"color":"grey","weight":2,"fill":False}
    ).add_to(m)

    # FeatureGroup per category
    groups = {}
    for cat in cats:
        fg = folium.FeatureGroup(name=str(cat), show=True)
        groups[cat] = fg
        m.add_child(fg)

    # Collect JS lines for zoom-to
    js_coords = []
    for _, row in df.iterrows():
        cat = row["asset_category"]
        lat, lon, gid = row["latitude"], row["longitude"], row["gisid"]

        # Popup HTML
        popup_html = "".join(f"<b>{col}</b>: {row[col]}<br/>" for col in df.columns)
        popup = folium.Popup(popup_html, max_width=300)

        # Add markers
        folium.CircleMarker(
            [lat, lon],
            radius=4,
            color=color_map[cat],
            fill=True,
            fill_opacity=0.7,
            popup=popup
        ).add_to(groups[cat])

        folium.Marker(
            [lat, lon],
            icon=DivIcon(
                icon_size=(150, 36),
                icon_anchor=(0, 0),
                html=f"""
                    <div style="
                        font-size: 12px;
                        color: black;
                        text-shadow:
                            -1px -1px 0 white,
                            1px -1px 0 white,
                            -1px  1px 0 white,
                            1px  1px 0 white;
                    ">
                        {gid}
                    </div>
                """
            )
        ).add_to(groups[cat])

        js_coords.append(f'coords["{gid}"] = [{lat}, {lon}];')

    folium.LayerControl(collapsed=False).add_to(m)

    # Assemble report
    report = Figure(width="100%", height="100%")
    report.add_child(m)

    # Legend
    legend_html = """
        <div style="
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
            <div style="display:flex; align-items:center; margin:5px 0;">
                <div style="
                    width:15px; height:15px;
                    background-color:{col};
                    border:1px solid #333;
                    margin-right:8px;
                    border-radius:50%;
                "></div>
                <span>{cat}</span>
            </div>
        """
    legend_html += "</div>"
    report.html.add_child(Element(legend_html))

    # Table with clickable gisid
    df_tbl = df.copy()
    df_tbl["gisid"] = df_tbl["gisid"].apply(
        lambda x: f'<a href="#" onclick="zoomTo(\'{x}\')" '
                  f'style="color:blue;text-decoration:underline;">{x}</a>'
    )
    tbl_html = df_tbl.to_html(
        index=False,
        classes="table table-striped",
        border=0,
        escape=False
    )
    scroll_div = f"""
        <div style="max-height:250px; overflow-y:auto; width:95%; margin:10px auto;">
            {tbl_html}
        </div>
    """

    # Title and date
    report.html.add_child(Element(
        '<h2 style="text-align:center; font-size:25px; font-weight:bold;">'
        'Outside-BC Asset Coordinates'
        '</h2>'
    ))
    report.html.add_child(Element(
        f'<h4 style="text-align:center; font-size:15px; margin-top:-10px;">'
        f'(as of {today})'
        '</h4>'
    ))
    report.html.add_child(Element(scroll_div))

    # Inject JS for zoom function
    js_block = "\n        ".join(js_coords)
    js = f"""
    <script>
    var coords = {{}};
        {js_block}
    function zoomTo(gid) {{
        var latlng = coords[gid];
        if (latlng) {{
            {map_var}.setView(latlng, 12);
        }}
    }}
    </script>
    """
    report.html.add_child(Element(js))

    return report


def send_email_report(
    html_report,
    smtp_server,
    smtp_user,
    recipients_list,
    cc_list,
    subject,
    from_addr,
    to_addrs,
    cc_addrs,
    content
) -> None:
    """
    Converts the Folium Figure into HTML bytes and sends it as an email attachment.

    Parameters:
    - html_report       : a Folium Figure object (the report to render)
    - smtp_server       : SMTP server hostname (e.g., "smtp.example.com")
    - smtp_user         : SMTP username (sender’s email address / login)
    - recipients_list   : list of recipient email addresses (to go in “To”)
    - cc_list           : list of CC email addresses
    - subject           : the email subject line
    - from_addr         : the “From:” header (usually same as smtp_user)
    - to_addrs          : the “To:” header (string or comma‐separated list)
    - cc_addrs          : the “Cc:” header (string or comma‐separated list)
    - content           : plain‐text body of the email
    """
    # 1) Render the Folium Figure as HTML, then encode to bytes
    html_str = html_report.render()
    html_bytes = html_str.encode("utf-8")

    # 2) Open SMTP connection
    mailServer = smtplib.SMTP(smtp_server)
    #mailServer.ehlo()
    mailServer.starttls()
    mailServer.ehlo()

    # 3) Build the EmailMessage
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = to_addrs
    msg["Cc"] = cc_addrs
    msg.set_content(content)

    # 4) Attach the HTML report as an .html file
    filename = f"Outside_BC_Assets_{datetime.now().strftime('%Y%m%d')}.html"
    msg.add_attachment(
        html_bytes,
        maintype="text",
        subtype="html",
        filename=filename
    )

    # 5) Send and close
    mailServer.send_message(msg)
    mailServer.quit()
    logging.info("...email with HTML report sent successfully.")



if __name__ == "__main__":
    start_t = timeit.default_timer() #start time
    logging.basicConfig(level=logging.INFO, format='%(message)s')

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
    
        logging.info ('\nReading BC boundary GeoJSON file...')
        bc_geom_wkb = read_geojson("data/bc.geojson")

        logging.info  ('\nEvaluating assets outside BC boundary...')
        df = evaluate_assets (bc_geom_wkb, conn)

    except Exception as e:
        logging.error(f"{e}")
        exit(1)
    
    finally:
        pg.disconnect()

    logging.info  ('\nBuilding HTML report...')
    html_report = build_html_report(bc_geom_wkb, df)
    html_report.save("docs/out_of_bc.html")

    '''
    logging.info('\nSending email...')
    # prepare email parameters
    smtp_server    = os.getenv("SMTP_SERVER")
    smtp_user      = "XXX.XXX@gov.bc.ca"
    recipients_list = []
    cc_list         = []
    subject   = "Outside-BC Asset Coordinates Map"
    from_addr = smtp_user
    to_addrs  = ", ".join(recipients_list)
    cc_addrs  = ", ".join(cc_list)
    content   = """
                Hello,\n\nPlease find attached the Outside-BC Asset Coordinates report.\n
                """


    # send email with the HTML report
    send_email_report(
        html_report=html_report,
        smtp_server=smtp_server,
        smtp_user=smtp_user,
        recipients_list=recipients_list,
        cc_list=cc_list,
        subject=subject,
        from_addr=from_addr,
        to_addrs=to_addrs,
        cc_addrs=cc_addrs,
        content=content
    )
    '''



    finish_t = timeit.default_timer()
    t_sec = round(finish_t - start_t)
    mins, secs = divmod(t_sec, 60)
    logging.info (f'\nProcessing Completed in {mins} minutes and {secs} seconds')