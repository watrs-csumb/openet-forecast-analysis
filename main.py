# -*- coding: utf-8 -*-
"""
Created on Thu Jun  13 10:06:44 2024

@author: Robin Fishman
"""
from copy import deepcopy
from datetime import datetime, timedelta
from dotenv import dotenv_values
from ETArg import ETArg
from ETPreprocess import ETPreprocess
from pathlib import Path
from Queue import Queue

import logging
import pandas as pd
import sys

# LOGGING CONFIG
# File handler that allows files to show all log entries
file_log_handler = logging.FileHandler(filename=datetime.now().strftime(f'logs/sample_points_main_%Y_%m_%d_%H_%M_%S.log'))

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
forecast_endpoint = "https://developer.openet-api.org/experimental/raster/timeseries/forecasting/seasonal"

# DataFrame: k(OPENET_ID), v(CROP_2020, .geo)
sample_points_reference = pd.read_csv('sample_points.csv', low_memory=False).set_index('OPENET_ID')
sample_points_queue = Queue(sample_points_reference.index.to_list())

def get_historical_data(fields_queue, reference, *, filename):
	sample_data = ETPreprocess(
		deepcopy(fields_queue), reference, api_key=api_key
	)

	timeseries_et = ETArg(
		"actual_et",
		args={
			"endpoint": timeseries_endpoint,
			"date_range": ["2016-01-01", "2024-07-19"],
			"variable": "ET",
		},
	)

	timeseries_eto = ETArg(
		"actual_eto",
		args={
			"endpoint": timeseries_endpoint,
			"date_range": ["2016-01-01", "2024-07-19"],
			"variable": "ETo",
		},
	)

	timeseries_etof = ETArg(
		"actual_etof",
		args={
			"endpoint": timeseries_endpoint,
			"date_range": ["2016-01-01", "2024-07-19"],
			"variable": "ETof",
		},
	)

	sample_data.start(
		request_args=[timeseries_et, timeseries_eto, timeseries_etof],
		frequency="daily",
		logger=logger,
		packets=True,
	)

	sample_data.export(f"data/{filename}.csv")

def get_forecasts(fields_queue, reference, *, dir):
	# Gather predictions at weekly intervals.
	# Forecast begins predictions from the end_range. So to start predictions for Jan 1, set to Dec 31
	forecasting_date = datetime(2024, 1, 1)  # Marker for loop
	end_date = datetime(2024, 12, 31)  # 31 Dec 2024
	interval_delta = timedelta(weeks=1)  # weekly interval
	logger.info("Getting forecast data.")
	while forecasting_date < end_date:
		process = ETPreprocess(
			deepcopy(fields_queue), reference, api_key=api_key
		)
		api_date_format = forecasting_date.strftime("%Y-%m-%d")
		filename = f"data/forecasts/{dir}/{api_date_format}_forecast.csv"

		forecast_et = ETArg(
			"expected_et",
			args={
				"endpoint": forecast_endpoint,
				"date_range": ["2016-01-01", api_date_format],
				"variable": "ET",
			},
		)

		forecast_eto = ETArg(
			"expected_eto",
			args={
				"endpoint": forecast_endpoint,
				"date_range": ["2016-01-01", api_date_format],
				"variable": "ETo",
			},
		)

		forecast_etof = ETArg(
			"expected_etof",
			args={
				"endpoint": forecast_endpoint,
				"date_range": ["2016-01-01", api_date_format],
				"variable": "ETof",
			},
		)

		logger.info(f"Forecasting from {api_date_format}")
		process.start(
			request_args=[forecast_et, forecast_eto, forecast_etof], frequency="daily"
		)

		process.export(filename)

		forecasting_date = forecasting_date + interval_delta
	merge_forecasts(dir)

def merge_forecasts(dir):
    logger.info(f"Compiling forecasts for {dir}")
    forecasts_table = pd.DataFrame()
    files = Path(f"data/forecasts/{dir}").glob("*.csv")

    for file in files:
        # splits into [$date, 'forecast.csv']
        parts = str(file.name).split("_")
        data = pd.read_csv(file, low_memory=False)
        data["forecasting_date"] = parts[0]
        forecasts_table = pd.concat([data, forecasts_table], ignore_index=True)

    forecasts_table.set_index("forecasting_date", inplace=True)
    forecasts_table.to_csv(f"{dir}_forecast_table.csv")

def main():
    kern_fields = pd.read_csv("./Kern.csv", low_memory=False)
    kern_queue = Queue(kern_fields["OPENET_ID"].to_list())

    monterey_fields = pd.read_csv("/Monterey.csv", low_memory=False)
    monterey_queue = Queue(monterey_fields["OPENET_ID"].to_list())

    logger.info("Getting data for Kern County")
    # Kern Data
    get_historical_data(kern_queue, kern_fields, filename="kern_historical")
    get_forecasts(kern_queue, kern_fields, dir="kern")

    logger.info("Getting data for Monterey County")
    # Monterey Data
    get_historical_data(monterey_queue, monterey_fields, filename="monterey_historical")
    get_forecasts(monterey_queue, monterey_fields, dir="monterey")

if __name__ == '__main__':
	main()