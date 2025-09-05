import ee
import datetime

# Initialize with your Google Cloud project linked to Earth Engine
ee.Initialize(project='project-solar-forecast')

dataset = ee.ImageCollection('ECMWF/NRT_FORECAST/IFS/OPER')

def get_daily_radiation(target_date):
    # Convert Python datetime.date → string
    date = ee.Date(str(target_date))
    
    image = (dataset
             .filterDate(date, date.advance(1, 'day'))
             .select('surface_solar_radiation_downwards_sfc')
             .mean())
    
    return image.set('date', str(target_date))

# Define regions of interest (Tokyo, Hokkaido, Tohoku)
regions = {
    "Tokyo": ee.Geometry.Point([139.6917, 35.6895]),        # Tokyo point
    "Hokkaido": ee.Geometry.Rectangle([139.5, 41.0, 146.0, 45.5]),  # Rough bounding box
    "Tohoku": ee.Geometry.Rectangle([139.0, 37.0, 141.5, 40.5])     # Rough bounding box
}

# Example: past 7 days
today = datetime.date.today()
images = [get_daily_radiation(today - datetime.timedelta(days=i)) for i in range(7)]

# # Export statistics (CSV) for each region
# for region_name, geom in regions.items():
#     for img in images:
#         date_str = img.get('date').getInfo()
#         task = ee.batch.Export.table.toDrive(
#             collection=img.reduceRegions(
#                 reducer=ee.Reducer.mean(),
#                 collection=ee.FeatureCollection([ee.Feature(geom)]),
#                 scale=25000  # ECMWF resolution ~0.25° (~25km)
#             ),
#             description=f"ECMWF_solar_{region_name}_{date_str}",
#             fileFormat='CSV'
#         )
#         task.start()
#         print(f"Exporting {region_name} {date_str} to Google Drive...")

# Optional: Export raster GeoTIFF for entire Japan region
japan_bbox = ee.Geometry.Rectangle([128.0, 30.0, 146.0, 46.0])
for img in images:
    date_str = img.get('date').getInfo()
    task = ee.batch.Export.image.toDrive(
        image=img,
        description=f"ECMWF_solar_Japan_{date_str}",
        region=japan_bbox,
        scale=25000,
        fileFormat='GeoTIFF'
    )
    task.start()
    print(f"Exporting raster for Japan {date_str} to Google Drive...")