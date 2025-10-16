import openmeteo_requests
import pandas as pd
import numpy as np
import requests_cache
from retry_requests import retry
import geocoder
from kafka import KafkaProducer
import json
import time
import argparse

MIN_SLEEPTIME_VAL = 60
MAX_SLEEPTIME_VAL = 600

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda x: x.encode('utf-8'))

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

def get_current_gps_coordinates():
    g = geocoder.ip('me')#this function is used to find the current information using our IP Add
    if g.latlng is not None: #g.latlng tells if the coordiates are found or not
        return g.latlng
    else:
        return None

def get_weather_data(coords):
    
    lat, long = coords
    
    # Make sure all required weather variables are listed here
    # The order of variables in minutely_15 or daily is important to assign them correctly below
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": long,
        "minutely_15": ["temperature_2m", "relative_humidity_2m", "precipitation_probability", "precipitation", "rain", "surface_pressure", "cloud_cover", "wind_speed_10m", "apparent_temperature", "dew_point_2m", "showers", "snowfall", "snow_depth", "weather_code", "pressure_msl", "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high", "visibility", "evapotranspiration", "et0_fao_evapotranspiration", "vapour_pressure_deficit", "wind_speed_80m", "wind_speed_120m", "wind_speed_180m", "wind_direction_10m", "wind_direction_80m", "wind_direction_120m", "wind_direction_180m", "wind_gusts_10m", "temperature_80m", "temperature_120m", "temperature_180m", "soil_temperature_0cm", "soil_temperature_6cm", "soil_temperature_18cm", "soil_moisture_0_to_1cm", "soil_temperature_54cm", "soil_moisture_1_to_3cm", "soil_moisture_3_to_9cm", "soil_moisture_9_to_27cm", "soil_moisture_27_to_81cm"],
        "timezone": "auto",
        "forecast_days": 1,
	    "forecast_minutely_15": 108,
    }
    responses = openmeteo.weather_api(url, params=params)

    # Process responses for different locations in a for loop
    response = responses[0]

    # Process minutely_15 data. The order of variables needs to be the same as requested.
    minutely_15 = response.Minutely15()
    minutely_15_temperature_2m = minutely_15.Variables(0).ValuesAsNumpy()
    minutely_15_relative_humidity_2m = minutely_15.Variables(1).ValuesAsNumpy()
    minutely_15_precipitation_probability = minutely_15.Variables(2).ValuesAsNumpy()
    minutely_15_precipitation = minutely_15.Variables(3).ValuesAsNumpy()
    minutely_15_rain = minutely_15.Variables(4).ValuesAsNumpy()
    minutely_15_surface_pressure = minutely_15.Variables(5).ValuesAsNumpy()
    minutely_15_cloud_cover = minutely_15.Variables(6).ValuesAsNumpy()
    minutely_15_wind_speed_10m = minutely_15.Variables(7).ValuesAsNumpy()
    minutely_15_apparent_temperature = minutely_15.Variables(8).ValuesAsNumpy()
    minutely_15_dew_point_2m = minutely_15.Variables(9).ValuesAsNumpy()
    minutely_15_showers = minutely_15.Variables(10).ValuesAsNumpy()
    minutely_15_snowfall = minutely_15.Variables(11).ValuesAsNumpy()
    minutely_15_snow_depth = minutely_15.Variables(12).ValuesAsNumpy()
    minutely_15_weather_code = minutely_15.Variables(13).ValuesAsNumpy()
    minutely_15_pressure_msl = minutely_15.Variables(14).ValuesAsNumpy()
    minutely_15_cloud_cover_low = minutely_15.Variables(15).ValuesAsNumpy()
    minutely_15_cloud_cover_mid = minutely_15.Variables(16).ValuesAsNumpy()
    minutely_15_cloud_cover_high = minutely_15.Variables(17).ValuesAsNumpy()
    minutely_15_visibility = minutely_15.Variables(18).ValuesAsNumpy()
    minutely_15_evapotranspiration = minutely_15.Variables(19).ValuesAsNumpy()
    minutely_15_et0_fao_evapotranspiration = minutely_15.Variables(20).ValuesAsNumpy()
    minutely_15_vapour_pressure_deficit = minutely_15.Variables(21).ValuesAsNumpy()
    minutely_15_wind_speed_80m = minutely_15.Variables(22).ValuesAsNumpy()
    minutely_15_wind_speed_120m = minutely_15.Variables(23).ValuesAsNumpy()
    minutely_15_wind_speed_180m = minutely_15.Variables(24).ValuesAsNumpy()
    minutely_15_wind_direction_10m = minutely_15.Variables(25).ValuesAsNumpy()
    minutely_15_wind_direction_80m = minutely_15.Variables(26).ValuesAsNumpy()
    minutely_15_wind_direction_120m = minutely_15.Variables(27).ValuesAsNumpy()
    minutely_15_wind_direction_180m = minutely_15.Variables(28).ValuesAsNumpy()
    minutely_15_wind_gusts_10m = minutely_15.Variables(29).ValuesAsNumpy()
    minutely_15_temperature_80m = minutely_15.Variables(30).ValuesAsNumpy()
    minutely_15_temperature_120m = minutely_15.Variables(31).ValuesAsNumpy()
    minutely_15_temperature_180m = minutely_15.Variables(32).ValuesAsNumpy()
    minutely_15_soil_temperature_0cm = minutely_15.Variables(33).ValuesAsNumpy()
    minutely_15_soil_temperature_6cm = minutely_15.Variables(34).ValuesAsNumpy()
    minutely_15_soil_temperature_18cm = minutely_15.Variables(35).ValuesAsNumpy()
    minutely_15_soil_moisture_0_to_1cm = minutely_15.Variables(36).ValuesAsNumpy()
    minutely_15_soil_temperature_54cm = minutely_15.Variables(37).ValuesAsNumpy()
    minutely_15_soil_moisture_1_to_3cm = minutely_15.Variables(38).ValuesAsNumpy()
    minutely_15_soil_moisture_3_to_9cm = minutely_15.Variables(39).ValuesAsNumpy()
    minutely_15_soil_moisture_9_to_27cm = minutely_15.Variables(40).ValuesAsNumpy()
    minutely_15_soil_moisture_27_to_81cm = minutely_15.Variables(41).ValuesAsNumpy()

    minutely_15_data = {"date": pd.date_range(
        start = pd.to_datetime(minutely_15.Time(), unit = "s", utc = True),
        end = pd.to_datetime(minutely_15.TimeEnd(), unit = "s", utc = True),
        freq = pd.Timedelta(seconds = minutely_15.Interval()),
        inclusive = "left"
    )}

    minutely_15_data["temperature_2m"] = minutely_15_temperature_2m
    minutely_15_data["relative_humidity_2m"] = minutely_15_relative_humidity_2m
    minutely_15_data["precipitation_probability"] = minutely_15_precipitation_probability
    minutely_15_data["precipitation"] = minutely_15_precipitation
    minutely_15_data["rain"] = minutely_15_rain
    minutely_15_data["surface_pressure"] = minutely_15_surface_pressure
    minutely_15_data["cloud_cover"] = minutely_15_cloud_cover
    minutely_15_data["wind_speed_10m"] = minutely_15_wind_speed_10m
    minutely_15_data["apparent_temperature"] = minutely_15_apparent_temperature
    minutely_15_data["dew_point_2m"] = minutely_15_dew_point_2m
    minutely_15_data["showers"] = minutely_15_showers
    minutely_15_data["snowfall"] = minutely_15_snowfall
    minutely_15_data["snow_depth"] = minutely_15_snow_depth
    minutely_15_data["weather_code"] = minutely_15_weather_code
    minutely_15_data["pressure_msl"] = minutely_15_pressure_msl
    minutely_15_data["cloud_cover_low"] = minutely_15_cloud_cover_low
    minutely_15_data["cloud_cover_mid"] = minutely_15_cloud_cover_mid
    minutely_15_data["cloud_cover_high"] = minutely_15_cloud_cover_high
    minutely_15_data["visibility"] = minutely_15_visibility
    minutely_15_data["evapotranspiration"] = minutely_15_evapotranspiration
    minutely_15_data["et0_fao_evapotranspiration"] = minutely_15_et0_fao_evapotranspiration
    minutely_15_data["vapour_pressure_deficit"] = minutely_15_vapour_pressure_deficit
    minutely_15_data["wind_speed_80m"] = minutely_15_wind_speed_80m
    minutely_15_data["wind_speed_120m"] = minutely_15_wind_speed_120m
    minutely_15_data["wind_speed_180m"] = minutely_15_wind_speed_180m
    minutely_15_data["wind_direction_10m"] = minutely_15_wind_direction_10m
    minutely_15_data["wind_direction_80m"] = minutely_15_wind_direction_80m
    minutely_15_data["wind_direction_120m"] = minutely_15_wind_direction_120m
    minutely_15_data["wind_direction_180m"] = minutely_15_wind_direction_180m
    minutely_15_data["wind_gusts_10m"] = minutely_15_wind_gusts_10m
    minutely_15_data["temperature_80m"] = minutely_15_temperature_80m
    minutely_15_data["temperature_120m"] = minutely_15_temperature_120m
    minutely_15_data["temperature_180m"] = minutely_15_temperature_180m
    minutely_15_data["soil_temperature_0cm"] = minutely_15_soil_temperature_0cm
    minutely_15_data["soil_temperature_6cm"] = minutely_15_soil_temperature_6cm
    minutely_15_data["soil_temperature_18cm"] = minutely_15_soil_temperature_18cm
    minutely_15_data["soil_moisture_0_to_1cm"] = minutely_15_soil_moisture_0_to_1cm
    minutely_15_data["soil_temperature_54cm"] = minutely_15_soil_temperature_54cm
    minutely_15_data["soil_moisture_1_to_3cm"] = minutely_15_soil_moisture_1_to_3cm
    minutely_15_data["soil_moisture_3_to_9cm"] = minutely_15_soil_moisture_3_to_9cm
    minutely_15_data["soil_moisture_9_to_27cm"] = minutely_15_soil_moisture_9_to_27cm
    minutely_15_data["soil_moisture_27_to_81cm"] = minutely_15_soil_moisture_27_to_81cm

    minutely_15_dataframe = pd.DataFrame(data = minutely_15_data)
    
    return minutely_15_dataframe

if __name__ == "__main__":
    
    argparser = argparse.ArgumentParser(
        prog="producer",
        description="Creates a Kafka producer that periodically calls Open-Meteo API and publishes the responses.",
    )
    
    def range_limited_int_type(arg):
        """ Type function for argparse - an int within some predefined bounds """
        try:
            v = int(arg)
        except ValueError:    
            raise argparse.ArgumentTypeError("Must be an integer number")
        if v < MIN_SLEEPTIME_VAL or v > MAX_SLEEPTIME_VAL:
            raise argparse.ArgumentTypeError("Argument must be < " + str(MAX_SLEEPTIME_VAL) + "and > " + str(MIN_SLEEPTIME_VAL))
        return v
    
    argparser.add_argument(
        '--sleep-time',
        help="Frequency with which the producer publishes an event.",
        nargs=1,
        default=60,
        type=range_limited_int_type,
        dest="sleeptime"
        )
    
    args = argparser.parse_args()
    sleeptime = args.sleeptime
    
    lat, long = get_current_gps_coordinates()
    coords = (lat, long)

    while True:
        weather_data = get_weather_data(coords)
        
        # Set the message value
        value = weather_data.to_json(orient='records', date_format='iso')

        # Set the message key
        key = f"{time.time()}_{lat}_{long}"
        key = str(key).encode()

        # Send the data to Kafka
        producer.send(topic="weather-data", key=key, value=value)

        time.sleep(sleeptime)