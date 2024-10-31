# -*- coding: utf-8 -*-
"""
Created on Thu Jun  13 10:06:44 2024

@author: Robin Fishman
"""
from collections import deque
from copy import deepcopy
from datetime import datetime, timedelta
from dotenv import dotenv_values
from ETArg import ETArg
from ETFetch import ETFetch
from pathlib import Path

import logging
import pandas as pd
import sys

# LOGGING CONFIG
# File handler that allows files to show all log entries
file_log_handler = logging.FileHandler(filename=datetime.now().strftime('logs/main_%Y_%m_%d_%H_%M_%S.log'))

# Stream handler that prints log entries at level WARNING or higher
stdout_log_handler = logging.StreamHandler(stream=sys.stdout)
stdout_log_handler.setLevel(logging.INFO)

logging.basicConfig(level=logging.INFO,
					format='%(asctime)s - %(levelname)s - %(message)s',
					handlers=[file_log_handler, stdout_log_handler])
logger = logging.getLogger(__name__)
# END LOGGING CONFIG

api_key = dotenv_values(".env").get("ET_KEY")
timeseries_endpoint = "https://developer.openet-api.org/raster/timeseries/point"
polygon_timeseries_endpoint = "https://developer.openet-api.org/raster/timeseries/polygon"

forecast_endpoint = "https://developer.openet-api.org/experimental/raster/timeseries/forecasting/seasonal"
polygon_forecast_endpoint = "https://developer.openet-api.org/experimental/raster/timeseries/forecasting/seasonal_polygon"

kern_fields = pd.read_csv("./data/Kern.csv", low_memory=False).set_index("OPENET_ID")
monterey_fields = pd.read_csv("./data/Monterey.csv", low_memory=False).set_index("OPENET_ID")

kern_polygon_fields = pd.read_csv("./data/kern_polygons_large.csv", low_memory=False).set_index('field_id')
monterey_polygon_fields = pd.read_csv("./data/monterey_polygons_large.csv", low_memory=False).set_index('field_id')

data_end_date_reference = "2024-10-07"

def get_historical_data(fields_queue, reference, *, filename, endpoint=timeseries_endpoint, polygon=False):
    et_data = ETFetch(
        deepcopy(fields_queue),
        reference,
        api_key=api_key,  # type: ignore
    )

    timeseries_et = ETArg(
        "actual_et",
        args={
            "endpoint": endpoint,
            "date_range": ["2016-01-01", data_end_date_reference],
            "variable": "ET",
            "reducer": "mean",
        },
    )

    timeseries_eto = ETArg(
        "actual_eto",
        args={
            "endpoint": endpoint,
            "date_range": ["2016-01-01", data_end_date_reference],
            "variable": "ETo",
            "reducer": "mean",
        },
    )

    timeseries_etof = ETArg(
        "actual_etof",
        args={
            "endpoint": endpoint,
            "date_range": ["2016-01-01", data_end_date_reference],
            "variable": "ETof",
            "reducer": "mean",
        },
    )

    if polygon:
        timeseries_et.reducer = "mean"
        timeseries_eto.reducer = "mean"
        timeseries_etof.reducer = "mean"

    et_data.start(
        request_args=[timeseries_et, timeseries_eto, timeseries_etof],
        frequency="daily",
        logger=logger,
        packets=True,
    )
    
    et_data.export(f"data/{filename}.csv")
    et_data.data_table["time"] = pd.to_datetime(et_data.data_table["time"])
    # Climatology compilation
    # Create a column for day of year
    et_data.data_table["doy"] = et_data.data_table["time"].dt.dayofyear
    # Group by field, crop, and doy then calculate the average conditions
    climatology_table = et_data.data_table.groupby(["field_id", "crop", "doy"])[
        ["actual_et", "actual_eto", "actual_etof"]
    ].agg("mean")
    climatology_table.reset_index().to_csv(f"data/{filename}_climatology.csv", index=False)
    # End Climatology
    
    # Year-to-date Averages Compilation
    avgs_table = (
        et_data.data_table.loc[(et_data.data_table["time"].dt.year == 2024), :]
        .groupby(["field_id", "crop"])[["actual_et", "actual_eto", "actual_etof"]]
        .agg("mean")
    )
    avgs_table.reset_index().to_csv(f"data/{filename}_2024_avgs.csv", index=False)
    # End Year-to-date Averages Compilation

def get_forecasts(fields_queue, reference, *, dir, endpoint=forecast_endpoint, polygon=False):
    # Gather predictions at weekly intervals.
    # Forecast begins predictions from the end_range. So to start predictions for Jan 1, set to Dec 31
    forecasting_date = datetime(2024, 8, 5)  # Marker for loop
    end_date = datetime(2024, 10, 7)  # 2 Sep 2024
    interval_delta = timedelta(weeks=1)  # weekly interval

    # Create dir if it doesn't exist
    file_dir = Path(f"data/forecasts/{dir}")
    if file_dir.exists() is False:
        file_dir.mkdir(parents=True)

    logger.info("Getting forecast data.")
    while forecasting_date < end_date:
        process = ETFetch(
            deepcopy(fields_queue),
            reference,
            api_key=api_key,  # type: ignore
        )
        api_date_format = forecasting_date.strftime("%Y-%m-%d")
        filename = f"{file_dir}/{api_date_format}_forecast.csv"
        if Path(filename).exists():
            print(f"{filename} already exists. Moving on..")
            forecasting_date = forecasting_date + interval_delta
            continue

        forecast_et = ETArg(
            "expected_et",
            args={
                "endpoint": endpoint,
                "date_range": ["2016-01-01", api_date_format],
                "variable": "ET",
            },
        )

        forecast_eto = ETArg(
            "expected_eto",
            args={
                "endpoint": endpoint,
                "date_range": ["2016-01-01", api_date_format],
                "variable": "ETo",
            },
        )

        forecast_etof = ETArg(
            "expected_etof",
            args={
                "endpoint": endpoint,
                "date_range": ["2016-01-01", api_date_format],
                "variable": "ETof",
            },
        )

        if polygon:
            forecast_et.reducer = "mean"
            forecast_eto.reducer = "mean"
            forecast_etof.reducer = "mean"

        logger.info(f"Forecasting from {api_date_format}")
        process.start(
            request_args=[forecast_et, forecast_eto, forecast_etof],
            frequency="daily",
            packets=True,
            logger=logger,
        )

        process.export(filename)

        forecasting_date = forecasting_date + interval_delta

def main():
	version_prompt = input('What version of DTW is this?: ')

	kern_queue = deque(kern_fields.index.to_list())
	monterey_queue = deque(monterey_fields.index.to_list())

	# point forecasting
	logger.info("Getting point data for Monterey County")
	# Monterey Data
	get_historical_data(monterey_queue, monterey_fields, filename="monterey_historical")
	get_forecasts(monterey_queue, monterey_fields, dir="/monterey")

	logger.info("Getting point data for Kern County")
	# Kern Data
	get_historical_data(kern_queue, kern_fields, filename="kern_historical")                         
	get_forecasts(kern_queue, kern_fields, dir="/kern")
	
	# polygon forecasting
	monterey_queue = deque(monterey_polygon_fields.index.to_list())
	kern_queue = deque(kern_polygon_fields.index.to_list())
	
	logger.info("Getting polygon data for Monterey County")
	get_forecasts(monterey_queue, monterey_polygon_fields, dir=f"{version_prompt}/polygon/monterey/sampled", endpoint=polygon_forecast_endpoint, polygon=True)
	get_historical_data(monterey_queue, monterey_polygon_fields, filename="monterey_polygon_large_historical", endpoint=polygon_timeseries_endpoint, polygon=True)

	logger.info("Getting polygon data for Kern County")
	get_forecasts(kern_queue, kern_polygon_fields, dir=f"{version_prompt}/polygon/kern/sampled", endpoint=polygon_forecast_endpoint, polygon=True)
	get_historical_data(kern_queue, kern_polygon_fields, filename="kern_polygon_large_historical", endpoint=polygon_timeseries_endpoint, polygon=True)

if __name__ == '__main__':
	main()
