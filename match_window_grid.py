# -*- coding: utf-8 -*-
"""
Created on Mon Oct 11 15:11:24 2024

@author: Robin Fishman
"""

from copy import deepcopy
from datetime import datetime, timedelta
from dotenv import dotenv_values
from ETArg import ETArg
from ETFetch import ETFetch
from pathlib import Path
from Queue import Queue

import logging
import os
import pandas as pd
import sys
import time

# LOGGING CONFIG
# File handler that allows files to show all log entries
file_log_handler = logging.FileHandler(
	filename=datetime.now().strftime(
		f"logs/{os.path.basename(__file__)}_%Y_%m_%d_%H_%M_%S.log"
	)
)

# Stream handler that prints log entries at level WARNING or higher
stdout_log_handler = logging.StreamHandler(stream=sys.stdout)
stdout_log_handler.setLevel(logging.INFO)

logging.basicConfig(
	level=logging.INFO,
	format="%(asctime)s - %(levelname)s - %(message)s",
	handlers=[file_log_handler, stdout_log_handler],
)
logger = logging.getLogger(__name__)
# END LOGGING CONFIG

polygon_forecast_endpoint = (
	"https://developer.openet-api.org/experimental/raster/timeseries/forecasting/seasonal_polygon"
)

api_key = dotenv_values(".env").get("ET_KEY")
kern_polygon_fields = (
	pd.read_csv("./data/kern_polygons.csv", low_memory=False)
	.set_index("OPENET_ID")
	.sample(50)
)
monterey_polygon_fields = (
	pd.read_csv("./data/monterey_polygons.csv", low_memory=False)
	.set_index("OPENET_ID")
	.sample(50)
)

"""
{
  "align": false,
  "date_range": [
	"2016-01-01",
	"2024-10-07"
  ],
  "file_format": "CSV",
  "geometry": [
	-119.7937,
	35.58995,
	-119.7937,
	35.53326,
	-119.71268,
	35.53326,
	-119.71268,
	35.58995
  ],
  "interval": "daily",
  "model": "Ensemble",
  "reducer": "mean",
  "reference_et": "cimis",
  "units": "mm",
  "variable": "ET",
  "match_window": 90,
  "match_variable" : "ndvi"
}'
"""# 60, 90, 180

def get_forecasts(fields_queue, reference, *, dir, endpoint=polygon_forecast_endpoint):
    forecasting_date = datetime(2024, 6, 3)  # Marker for loop
    end_date = datetime(2024, 8, 2)  # 2 Sep 2024
    interval_delta = timedelta(weeks=1)  # weekly interval
    match_windows = [60, 90, 180]

    # Create dir if it doesn't exist
    file_dir = Path(f"data/forecasts/match_sample/{dir}")
    if file_dir.exists() is False:
        file_dir.mkdir(parents=True)

    logger.info("Getting forecast data.")
    while forecasting_date < end_date:
        window_queue = Queue(deepcopy(match_windows))
        while window_queue.is_empty() is False:
            process = ETFetch(
                deepcopy(fields_queue),
                reference,
                api_key=api_key,  # type: ignore
            )
            api_date_format = forecasting_date.strftime("%Y-%m-%d")
            filename = f"{file_dir}/{api_date_format}_window_{window_queue.front()}_forecast.csv"
            if Path(filename).exists():
                print(f'{filename} already exists. Moving on..')
                window_queue.dequeue()
                continue

            forecast_et = ETArg(
                "expected_et",
                args={
                    "endpoint": endpoint,
                    "date_range": ["2016-01-01", api_date_format],
                    "variable": "ET",
                    "reference_et": "cimis",
                    "reducer": "mean",
                    "match_variable": "ndvi",
                    "match_window": window_queue.front(),
                    "align": True
                },
            )

            forecast_eto = ETArg(
                "expected_eto",
                args={
                    "endpoint": endpoint,
                    "date_range": ["2016-01-01", api_date_format],
                    "variable": "ETo",
                    "reference_et": "cimis",
                    "reducer": "mean",
                    "match_variable": "ndvi",
                    "match_window": window_queue.front(),
                    "align": True
                },
            )

            forecast_etof = ETArg(
                "expected_etof",
                args={
                    "endpoint": endpoint,
                    "date_range": ["2016-01-01", api_date_format],
                    "variable": "ETof",
                    "reference_et": "cimis",
                    "reducer": "mean",
                    "match_variable": "ndvi",
                    "match_window": window_queue.front(),
                    "align": True
                },
            )

            logger.info(f"Forecasting from {api_date_format} with match window of {window_queue.front()}")
            process.start(
                request_args=[forecast_et, forecast_eto, forecast_etof],
                frequency="daily",
                packets=True,
                logger=logger,
            )

            window_queue.dequeue()

        process.export(filename)

        forecasting_date = forecasting_date + interval_delta

def main():
	version_prompt = input("What version of DTW is this?: ")
	
	monterey_queue = Queue(monterey_polygon_fields.index.to_list())
	kern_queue = Queue(kern_polygon_fields.index.to_list())

	logger.info("Getting polygon data for Monterey County")
	get_forecasts(
		monterey_queue,
		monterey_polygon_fields,
		dir=f"{version_prompt}/polygon/monterey/sampled",
		endpoint=polygon_forecast_endpoint,
	)
	
	logger.info("Getting polygon data for Kern County")
	get_forecasts(kern_queue, kern_polygon_fields, dir=f"{version_prompt}/polygon/kern/sampled", endpoint=polygon_forecast_endpoint)

if __name__ == '__main__':
    main()
