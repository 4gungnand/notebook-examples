import ee
import datetime
import pandas as pd

# Initialize with your Google Cloud project linked to Earth Engine
ee.Initialize(project='project-solar-forecast')

dataset = ee.ImageCollection('ECMWF/NRT_FORECAST/IFS/OPER')

# Region of interest (Tokyo)
tokyo = ee.Geometry.Point([139.6917, 35.6895])

# Forecast start date (today 00 UTC)
start = datetime.date.today().strftime("%Y-%m-%d")
end = (datetime.date.today() + datetime.timedelta(days=2)).strftime("%Y-%m-%d")  # 2 days ahead

# Filter dataset for forecast run
images = (dataset
          .filterDate(start, end)
          .select('surface_solar_radiation_downwards_sfc')
          .sort('forecast_time'))  # ensure ordered

# Convert to list for iteration
img_list = images.toList(images.size())

rows = []
n = img_list.size().getInfo()

# Loop consecutive pairs (difference → hourly radiation)
for i in range(1, n):
    img_prev = ee.Image(img_list.get(i-1))
    img_curr = ee.Image(img_list.get(i))
    
    # Forecast valid time
    valid_time = img_curr.get('forecast_time').getInfo()
    
    # Radiation increment
    diff = img_curr.subtract(img_prev)
    stats = diff.reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=tokyo,
        scale=25000,
        maxPixels=1e9
    ).getInfo()
    
    value = stats.get('surface_solar_radiation_downwards_sfc', None)
    
    rows.append({
        "forecast_time": valid_time,
        "region": "Tokyo",
        "solar_radiation_J_per_m2": value
    })

df = pd.DataFrame(rows)
df.to_csv("ecmwf_solar_radiation_tokyo_hourly.csv", index=False)

print("✅ Saved hourly forecast to ecmwf_solar_radiation_tokyo_hourly.csv")