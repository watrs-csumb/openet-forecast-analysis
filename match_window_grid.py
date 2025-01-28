# -*- coding: utf-8 -*-
"""
Created on Mon Oct 11 15:11:24 2024

@author: Robin Fishman
"""
from collections import deque
from copy import deepcopy
from datetime import datetime, timedelta
from dotenv import dotenv_values
from src import ETArg, ETFetch
from pathlib import Path

import logging
import os
import pandas as pd
import sys

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
polygon_timeseries_endpoint = "https://developer.openet-api.org/raster/timeseries/polygon"

api_key = dotenv_values(".env").get("ET_KEY")
kern_polygon_fields = pd.read_csv("./data/kern_polygons_large.csv", low_memory=False).set_index("field_id")
monterey_polygon_fields = pd.read_csv("./data/monterey_polygons_large.csv", low_memory=False).set_index("field_id")

def get_forecasts(fields_queue, reference, *, dir, endpoint=polygon_forecast_endpoint, align=True, skip_exist=False):
    forecasting_date = datetime(2024, 5, 6)  # Marker for loop
    end_date = datetime(2024, 9, 3) 
    interval_delta = timedelta(weeks=1)  # weekly interval
    match_windows = [60, 90, 180]
    match_variables = ['ndvi', None]

    # Create dir if it doesn't exist
    file_dir = Path(f"data/forecasts/match_sample/{dir}")
    if file_dir.exists() is False:
        file_dir.mkdir(parents=True)

    logger.info("Getting forecast data.")
    while forecasting_date < end_date:
        window_queue = deque(deepcopy(match_windows))
        while (len(window_queue) == 0) is False:
            var_queue = deque(deepcopy(match_variables))
            while (len(var_queue) == 0) is False:
                process = ETFetch(
                    deepcopy(fields_queue),
                    reference,
                    api_key=api_key,  # type: ignore
                )
                api_date_format = forecasting_date.strftime("%Y-%m-%d")
                filename = f"{file_dir}/{api_date_format}_{str(var_queue[0])}_window_{window_queue[0]}_forecast.csv"
                if Path(filename).exists() and skip_exist:
                    print(f'{filename} already exists. Moving on..')
                    var_queue.popleft()
                    continue

                forecast_et = ETArg(
                    "expected_et",
                    args={
                        "endpoint": endpoint,
                        "date_range": ["2016-01-01", api_date_format],
                        "variable": "ET",
                        "reference_et": "cimis",
                        "reducer": "mean",
                        "match_window": window_queue[0],
                        "align": align
                    },
                )
                if var_queue[0]:
                    forecast_et.match_variable = var_queue[0]

                forecast_eto = ETArg(
                    "expected_eto",
                    args={
                        "endpoint": endpoint,
                        "date_range": ["2016-01-01", api_date_format],
                        "variable": "ETo",
                        "reference_et": "cimis",
                        "reducer": "mean",
                        "match_window": window_queue[0],
                        "align": align,
                    },
                )
                if var_queue[0]:
                    forecast_eto.match_variable = var_queue[0]

                forecast_etof = ETArg(
                    "expected_etof",
                    args={
                        "endpoint": endpoint,
                        "date_range": ["2016-01-01", api_date_format],
                        "variable": "ETof",
                        "reference_et": "cimis",
                        "reducer": "mean",
                        "match_window": window_queue[0],
                        "align": align,
                    },
                )
                if var_queue[0]:
                    forecast_etof.match_variable = var_queue[0]

                logger.info(f"Forecasting from {api_date_format} with match_variable {var_queue[0]} and match window of {window_queue[0]}")
                process.start(
                    request_args=[forecast_et, forecast_eto, forecast_etof],
                    frequency="daily",
                    packets=True,
                    logger=logger,
                )
                
                process.export(filename)
                var_queue.popleft()

            window_queue.popleft()

        forecasting_date = forecasting_date + interval_delta

def get_historical(
    fields_queue, reference, *, filename, endpoint=polygon_timeseries_endpoint

):
    et_data = ETFetch(
        deepcopy(fields_queue),
        reference,
        api_key=api_key,  # type: ignore
    )

    timeseries_et = ETArg(
        "actual_et",
        args={
            "endpoint": endpoint,
            "date_range": ["2016-01-01", "2024-09-03"],
            "variable": "ET",
            "reducer": "mean",
        },
    )

    timeseries_eto = ETArg(
        "actual_eto",
        args={
            "endpoint": endpoint,
            "date_range": ["2016-01-01", "2024-09-03"],
            "variable": "ETo",
            "reducer": "mean",
        },
    )

    timeseries_etof = ETArg(
        "actual_etof",
        args={
            "endpoint": endpoint,
            "date_range": ["2016-01-01", "2024-09-03"],
            "variable": "ETof",
            "reducer": "mean",
        },
    )
    
    et_data.start(
        request_args=[timeseries_et, timeseries_eto, timeseries_etof],
        frequency="daily",
        logger=logger,
        packets=True,
    )
    
    et_data.export(f"data/{filename}.csv")

def main():
    version_prompt = input("What version of DTW is this?: ")
    
    monterey_queue = deque(monterey_polygon_fields.index.to_list())
    kern_queue = deque(kern_polygon_fields.index.to_list())

    logger.info("Getting polygon data for Monterey County")
    get_forecasts(
        monterey_queue,
        monterey_polygon_fields,
        dir=f"{version_prompt}/polygon/monterey/sampled",
        endpoint=polygon_forecast_endpoint,
        # align=False,
        skip_exist=False
    )
    get_historical(monterey_queue, monterey_polygon_fields, filename='monterey_window_historical')
    
    logger.info("Getting polygon data for Kern County")
    get_forecasts(
        kern_queue,
        kern_polygon_fields,
        dir=f"{version_prompt}/polygon/kern/sampled",
        endpoint=polygon_forecast_endpoint,
        # align=False,
        skip_exist=False,
    )
    get_historical(kern_queue, kern_polygon_fields, filename='kern_window_historical')

if __name__ == '__main__':
    main()
