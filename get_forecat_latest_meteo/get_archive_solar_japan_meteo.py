import openmeteo_requests

import pandas as pd
import requests_cache
from retry_requests import retry

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
url = "https://archive-api.open-meteo.com/v1/archive"
params = {
    "latitude": [43.432, 35.8188, 35.6895, 35.4338, 37.92, 34.68, 35.04, 33.75, 32.4294],
    "longitude": [142.9347, 139.5714, 139.6917, 140.2797, 139.04, 135.65, 132.27, 133.5, 130.991],
    "hourly": ["global_tilted_irradiance", "shortwave_radiation", "diffuse_radiation", "direct_normal_irradiance", "direct_radiation", "terrestrial_radiation", "temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature", "precipitation", "snowfall", "rain", "pressure_msl", "surface_pressure", "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high", "vapour_pressure_deficit", "weather_code", "cloud_cover", "et0_fao_evapotranspiration", "wind_speed_10m", "wind_speed_100m", "wind_direction_10m", "wind_direction_100m", "wind_gusts_10m", "surface_temperature", "runoff", "cape", "total_column_integrated_water_vapour"],
    "timezone": "Asia/Tokyo",
    "start_date": "2024-01-01",
    "end_date": "2024-12-31",
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
    # Process hourly data. The order of variables needs to be the same as requested.
    hourly = response.Hourly()
    hourly_global_tilted_irradiance = hourly.Variables(0).ValuesAsNumpy()
    hourly_shortwave_radiation = hourly.Variables(1).ValuesAsNumpy()
    hourly_diffuse_radiation = hourly.Variables(2).ValuesAsNumpy()
    hourly_direct_normal_irradiance = hourly.Variables(3).ValuesAsNumpy()
    hourly_direct_radiation = hourly.Variables(4).ValuesAsNumpy()
    hourly_terrestrial_radiation = hourly.Variables(5).ValuesAsNumpy()
    hourly_temperature_2m = hourly.Variables(6).ValuesAsNumpy()
    hourly_relative_humidity_2m = hourly.Variables(7).ValuesAsNumpy()
    hourly_dew_point_2m = hourly.Variables(8).ValuesAsNumpy()
    hourly_apparent_temperature = hourly.Variables(9).ValuesAsNumpy()
    hourly_precipitation = hourly.Variables(10).ValuesAsNumpy()
    hourly_snowfall = hourly.Variables(11).ValuesAsNumpy()
    hourly_rain = hourly.Variables(12).ValuesAsNumpy()
    hourly_pressure_msl = hourly.Variables(13).ValuesAsNumpy()
    hourly_surface_pressure = hourly.Variables(14).ValuesAsNumpy()
    hourly_cloud_cover_low = hourly.Variables(15).ValuesAsNumpy()
    hourly_cloud_cover_mid = hourly.Variables(16).ValuesAsNumpy()
    hourly_cloud_cover_high = hourly.Variables(17).ValuesAsNumpy()
    hourly_vapour_pressure_deficit = hourly.Variables(18).ValuesAsNumpy()
    hourly_weather_code = hourly.Variables(19).ValuesAsNumpy()
    hourly_cloud_cover = hourly.Variables(20).ValuesAsNumpy()
    hourly_et0_fao_evapotranspiration = hourly.Variables(21).ValuesAsNumpy()
    hourly_wind_speed_10m = hourly.Variables(22).ValuesAsNumpy()
    hourly_wind_speed_100m = hourly.Variables(23).ValuesAsNumpy()
    hourly_wind_direction_10m = hourly.Variables(24).ValuesAsNumpy()
    hourly_wind_direction_100m = hourly.Variables(25).ValuesAsNumpy()
    hourly_wind_gusts_10m = hourly.Variables(26).ValuesAsNumpy()
    hourly_surface_temperature = hourly.Variables(27).ValuesAsNumpy()
    hourly_runoff = hourly.Variables(28).ValuesAsNumpy()
    hourly_cape = hourly.Variables(29).ValuesAsNumpy()
    hourly_total_column_integrated_water_vapour = hourly.Variables(
        30).ValuesAsNumpy()

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
    hourly_data["direct_radiation"] = hourly_direct_radiation
    hourly_data["terrestrial_radiation"] = hourly_terrestrial_radiation
    hourly_data["temperature_2m"] = hourly_temperature_2m
    hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
    hourly_data["dew_point_2m"] = hourly_dew_point_2m
    hourly_data["apparent_temperature"] = hourly_apparent_temperature
    hourly_data["precipitation"] = hourly_precipitation
    hourly_data["snowfall"] = hourly_snowfall
    hourly_data["rain"] = hourly_rain
    hourly_data["pressure_msl"] = hourly_pressure_msl
    hourly_data["surface_pressure"] = hourly_surface_pressure
    hourly_data["cloud_cover_low"] = hourly_cloud_cover_low
    hourly_data["cloud_cover_mid"] = hourly_cloud_cover_mid
    hourly_data["cloud_cover_high"] = hourly_cloud_cover_high
    hourly_data["vapour_pressure_deficit"] = hourly_vapour_pressure_deficit
    hourly_data["weather_code"] = hourly_weather_code
    hourly_data["cloud_cover"] = hourly_cloud_cover
    hourly_data["et0_fao_evapotranspiration"] = hourly_et0_fao_evapotranspiration
    hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
    hourly_data["wind_speed_100m"] = hourly_wind_speed_100m
    hourly_data["wind_direction_10m"] = hourly_wind_direction_10m
    hourly_data["wind_direction_100m"] = hourly_wind_direction_100m
    hourly_data["wind_gusts_10m"] = hourly_wind_gusts_10m
    hourly_data["surface_temperature"] = hourly_surface_temperature
    hourly_data["runoff"] = hourly_runoff
    hourly_data["cape"] = hourly_cape
    hourly_data["total_column_integrated_water_vapour"] = hourly_total_column_integrated_water_vapour

    hourly_dataframe = pd.DataFrame(data=hourly_data)
    hourly_dataframe["region"] = region_names[idx]
    all_dataframes.append(hourly_dataframe)

# Gabungkan semua dataframe dan simpan ke satu CSV
result_df = pd.concat(all_dataframes, ignore_index=True)
n_params = len(params["hourly"])
n_coordinates = len(params["latitude"])
result_df.to_csv(
    f"open-meteo-1hourStep-{n_params}Params-{n_coordinates}RegionsJapan-{params['start_date']}-{params['end_date']}.csv", index=False)
print(
    f"All hourly data saved to open-meteo-1hourStep-{n_params}Params-{n_coordinates}RegionsJapan-{params['start_date']}-{params['end_date']}.csv")
