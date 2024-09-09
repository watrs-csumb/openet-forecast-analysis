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

import json
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
forecast_endpoint = "https://developer.openet-api.org/experimental/raster/timeseries/forecasting/seasonal"

kern_fields = pd.read_csv("./data/Kern.csv", low_memory=False).set_index("OPENET_ID")
monterey_fields = pd.read_csv("./data/Monterey.csv", low_memory=False).set_index("OPENET_ID")

def get_historical_data(fields_queue, reference, *, filename):
	sample_data = ETPreprocess(
		deepcopy(fields_queue), reference, api_key=api_key # type: ignore
	)

	timeseries_et = ETArg(
		"actual_et",
		args={
			"endpoint": timeseries_endpoint,
			"date_range": ["2016-01-01", "2024-08-02"],
			"variable": "ET",
		},
	)

	timeseries_eto = ETArg(
		"actual_eto",
		args={
			"endpoint": timeseries_endpoint,
			"date_range": ["2016-01-01", "2024-08-02"],
			"variable": "ETo",
		},
	)

	timeseries_etof = ETArg(
		"actual_etof",
		args={
			"endpoint": timeseries_endpoint,
			"date_range": ["2016-01-01", "2024-08-02"],
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
	end_date = datetime(2024, 8, 1)  # 1 Aug 2024
	interval_delta = timedelta(weeks=1)  # weekly interval
	logger.info("Getting forecast data.")
	while forecasting_date < end_date:
		process = ETPreprocess(
			deepcopy(fields_queue), reference, api_key=api_key # type: ignore
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
			request_args=[forecast_et, forecast_eto, forecast_etof], frequency="daily", packets=True, logger=logger
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

def concat_details():
	cdl_codes = pd.read_csv("cdl_codes.csv", low_memory=False).set_index("Codes")

	kern_points = pd.read_csv("Kern.csv", low_memory=False).set_index("OPENET_ID").rename(index="field_id") # type: ignore
	# Expand .geo column into lon, lat columns
	kern_geo = kern_points[".geo"].apply(lambda x: pd.Series(dict(json.loads(x))))

	monterey_points = pd.read_csv("Monterey.csv", low_memory=False).set_index("OPENET_ID").rename(index="field_id") # type: ignore
	# Expand .geo column into lon, lat columns
	monterey_geo = monterey_points[".geo"].apply(
		lambda x: pd.Series(dict(json.loads(x)))
	)

	kern = pd.read_csv("kern_historical.csv", low_memory=False)
	# Add geo info to the table
	kern.join(kern_geo, how="left", on=["field_id"], validate="many_to_one")

	monterey = pd.read_csv("monterey_historical.csv", low_memory=False)
	# Add geo info to the table
	monterey.join(monterey_geo, how="left", on=["field_id"], validate="many_to_one")

	kern["county"] = "Kern"
	monterey["county"] = "Monterey"

	full_historical = pd.concat([monterey, kern], ignore_index=True)

	# Add CDL info to the table
	full_historical = full_historical.join(
		cdl_codes, how="left", on="crop", validate="many_to_many"
	)

	full_historical.to_csv("kern_monterey_historical.csv", index=False)

def main():
	kern_queue = Queue(kern_fields.index.to_list())
	monterey_queue = Queue(monterey_fields.index.to_list())

	logger.info("Getting data for Monterey County")
	# Monterey Data
	# get_historical_data(monterey_queue, monterey_fields, filename="monterey_historical")
	# get_forecasts(monterey_queue, monterey_fields, dir="monterey")

	logger.info("Getting data for Kern County")
	# Kern Data
	# get_historical_data(kern_queue, kern_fields, filename="kern_historical")                         
	get_forecasts(kern_queue, kern_fields, dir="kern")

if __name__ == '__main__':
	main()
	# concat_details()
