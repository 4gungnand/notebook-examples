import openmeteo_requests

import pandas as pd
import requests_cache
from retry_requests import retry

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
url = "https://api.open-meteo.com/v1/forecast"
params = {
    "latitude": [43.432, 35.8188, 35.6895, 35.4338, 37.92, 34.68, 35.04, 33.75, 32.4294],
    "longitude": [142.9347, 139.5714, 139.6917, 140.2797, 139.04, 135.65, 132.27, 133.5, 130.991],
    "hourly": ["global_tilted_irradiance", "shortwave_radiation", "diffuse_radiation", "direct_normal_irradiance"],
    "models": "ecmwf_aifs025_single",
    "timezone": "Asia/Tokyo",
    "forecast_days": 15,
}
responses = openmeteo.weather_api(url, params=params)

# Nama kota/region sesuai urutan koordinat
region_names = [
    "Hokkaido",
    "Tohoku",
    "Tokyo",
    "Chubu",
    "Hokuriku",
    "Kansai",
    "Chugoku",
    "Shikoku",
    "Kyushu"
]

# Gabungkan semua data ke satu dataframe
all_dataframes = []
for idx, response in enumerate(responses):
    print(f"\nCoordinates: {response.Latitude()}°N {response.Longitude()}°E")
    print(f"Elevation: {response.Elevation()} m asl")
    print(f"Timezone: {response.Timezone()}{response.TimezoneAbbreviation()}")
    print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")

    hourly = response.Hourly()
    hourly_global_tilted_irradiance = hourly.Variables(0).ValuesAsNumpy()
    hourly_shortwave_radiation = hourly.Variables(1).ValuesAsNumpy()
    hourly_diffuse_radiation = hourly.Variables(2).ValuesAsNumpy()
    hourly_direct_normal_irradiance = hourly.Variables(3).ValuesAsNumpy()

    hourly_data = {"date": pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    )}

    hourly_data["global_tilted_irradiance"] = hourly_global_tilted_irradiance
    hourly_data["shortwave_radiation"] = hourly_shortwave_radiation
    hourly_data["diffuse_radiation"] = hourly_diffuse_radiation
    hourly_data["direct_normal_irradiance"] = hourly_direct_normal_irradiance

    hourly_dataframe = pd.DataFrame(data=hourly_data)
    hourly_dataframe["region"] = region_names[idx]
    all_dataframes.append(hourly_dataframe)

# Gabungkan semua dataframe dan simpan ke satu CSV
result_df = pd.concat(all_dataframes, ignore_index=True)
result_df.to_csv("hourly_solar_japan_all.csv", index=False)
print("All hourly data saved to hourly_solar_japan_all.csv")
