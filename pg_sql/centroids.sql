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
    assets.trails;