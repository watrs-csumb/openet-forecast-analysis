# -*- coding: utf-8 -*-
"""
Created on Mon Sep 9 18:42:18 2024

@author: Robin Fishman
"""
from copy import deepcopy
from datetime import datetime, timedelta
from dotenv import dotenv_values
from ETArg import ETArg
from ETPreprocess import ETPreprocess
from Queue import Queue

import logging
import os
import pandas as pd
import sys
import time

# LOGGING CONFIG
# File handler that allows files to show all log entries
file_log_handler = logging.FileHandler(
	filename=datetime.now().strftime(f'logs/f{os.path.basename(__file__)}_%Y_%m_%d_%H_%M_%S.log')
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

fret_endpoint = "https://developer.openet-api.org/experimental/raster/timeseries/forecasting/fret"
timeseries_endpoint = "https://developer.openet-api.org/raster/timeseries/point"

api_key = dotenv_values(".env").get("ET_KEY")
kern_fields = pd.read_csv("./data/Kern.csv", low_memory=False).set_index("OPENET_ID")
monterey_fields = pd.read_csv("./data/Monterey.csv", low_memory=False).set_index("OPENET_ID")

def main():
	# Plan is to auto fetch FRET data every 6 days from script start. Script is to run continuously, checking for this time interval each minute.
    check_time = datetime.now()
    check_interval = timedelta(days=6)
    upcoming_check_time = check_time + check_interval
    run_fetch = True

    monterey_queue = Queue(monterey_fields.index.to_list())
    kern_queue = Queue(kern_fields.index.to_list())

    et_arg = ETArg("fret_et", args={"endpoint": fret_endpoint, "variable": "ET"})
    eto_arg = ETArg("fret_eto", args={"endpoint": fret_endpoint, "variable": "ETo"})
    etof_arg = ETArg("fret_etof", args={"endpoint": fret_endpoint, "variable": "ETof"})

    logger.info(f"FRET automation started on: {check_time}")
    try:
        while True:
			# This method performs a do-while loop. Initially running the FRET data fetch, and does so when the run_fetch toggle is True.
            if run_fetch is True:
                export_date_format = check_time.strftime("%Y-%m-%d")
                monterey_fret = ETPreprocess(deepcopy(monterey_queue), monterey_fields, api_key=api_key) # type: ignore
                monterey_fret.start(
					request_args=[et_arg, eto_arg, etof_arg],
					logger=logger,
					packets=True
				)
                monterey_fret.export(f"data/forecasts/fret/monterey_fret_{export_date_format}.csv")

                kern_fret = ETPreprocess(deepcopy(kern_queue), kern_fields, api_key=api_key) # type: ignore
                kern_fret.start(
					request_args=[et_arg, eto_arg, etof_arg],
					logger=logger,
					packets=True
				)
                kern_fret.export(f"data/forecasts/fret/kern_fret_{export_date_format}.csv")
                logger.info(
					f"FRET fetched on: {check_time}. Next check will be on: {upcoming_check_time}"
				)

                run_fetch = False
                continue

			# When the next check time has passed, toggle the run fetch variable and update the check time
            if datetime.now() >= upcoming_check_time:
                check_time = datetime.now()
                upcoming_check_time = check_time + check_interval
                run_fetch = True
                logger.info(
					f"FRET fetched on: {check_time}. Next check will be on: {upcoming_check_time}"
				)
            else:
				# Wait for one minute before checking again.
                time.sleep(60)
    except KeyboardInterrupt:
		# When fetching is complete, gather historical data for evaluation.

		# Calculate correct end date as actual data is only reported up three days prior
        final_fetch_time_api_format = deepcopy(check_time)
        final_fetch_time_api_format -= timedelta(days=4)
		# Lastly, update date format so it's proper api date format.
        final_fetch_time_api_format = final_fetch_time_api_format.strftime("%Y-%m-%d")

        logger.info(f'Now getting historical data up to: {final_fetch_time_api_format}')

        historical_arg_et = ETArg("actual_et", args={
			"endpoint": timeseries_endpoint,
			"variable": "ET",
			"date_range": ["2016-01-01", final_fetch_time_api_format]
		})
        historical_arg_eto = ETArg("actual_eto", args={
			"endpoint": timeseries_endpoint,
			"variable": "ETo",
			"date_range": ["2016-01-01", final_fetch_time_api_format]
		})
        historical_arg_etof = ETArg("actual_etof", args={
			"endpoint": timeseries_endpoint,
			"variable": "ETof",
			"date_range": ["2016-01-01", final_fetch_time_api_format]
		})

        logger.info("Fetching Monterey County historical data.")
        mo_historical_fetch = ETPreprocess(monterey_queue, monterey_fields, api_key=api_key) # type: ignore
        mo_historical_fetch.start(request_args=[historical_arg_et, historical_arg_eto, historical_arg_etof], frequency="daily", packets=True, logger=logger)
        mo_historical_fetch.export("data/monterey_historical.csv")

        logger.info("Fetching Kern County historical data.")
        ke_historical_fetch = ETPreprocess(kern_queue, kern_fields, api_key=api_key) # type: ignore
        ke_historical_fetch.start(request_args=[historical_arg_et, historical_arg_eto, historical_arg_etof], frequency="daily", packets=True, logger=logger)
        ke_historical_fetch.export("data/kern_historical.csv")

if __name__ == "__main__":
    main()
