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
import pandas as pd
import sys
import time

# LOGGING CONFIG
# File handler that allows files to show all log entries
file_log_handler = logging.FileHandler(
	filename=datetime.now().strftime("logs/main_%Y_%m_%d_%H_%M_%S.log")
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

api_key = dotenv_values(".env").get("ET_KEY")
kern_fields = pd.read_csv("./data/Kern.csv", low_memory=False).set_index("OPENET_ID").sample(5)
monterey_fields = pd.read_csv("./data/Monterey.csv", low_memory=False).set_index("OPENET_ID").sample(5)

def main():
    # Plan is to auto fetch FRET data at midnight every 6 days. Script is to run continuously, checking for this time interval.
    check_time = datetime.now()
    check_interval = timedelta(days=6)
    run_fetch = True

    monterey_queue = Queue(monterey_fields.index.to_list())
    kern_queue = Queue(kern_fields.index.to_list())
    
    et_arg = ETArg("fret_et", args={"endpoint": fret_endpoint, "variable": "ET"})
    eto_arg = ETArg("fret_et", args={"endpoint": fret_endpoint, "variable": "ETo"})
    etof_arg = ETArg("fret_et", args={"endpoint": fret_endpoint, "variable": "ETof"})

    while run_fetch:
        # This method performs a do-while loop. Initially running the FRET data fetch, and does so when the run_fetch toggle is True.
        if run_fetch is True:
            monterey_fret = ETPreprocess(deepcopy(monterey_queue), monterey_fields, api_key=api_key)
            monterey_fret.start(
                request_args=[et_arg, eto_arg, etof_arg],
                logger=logger,
                packets=True,
                frequency=None,
            )
            monterey_fret.export("monterey_fret.csv")
            
            kern_fret = ETPreprocess(
                deepcopy(kern_queue), kern_fields, api_key=api_key
            )
            kern_fret.start(
                request_args=[et_arg, eto_arg, etof_arg],
                logger=logger,
                packets=True,
                frequency=None,
            )
            kern_fret.export("kern_fret.csv")

		# If current time has not reached the next check time
        if datetime.now() < check_time + check_interval:
            run_fetch = False
        # When the next check time has passed, toggle the run fetch variable and update the check time
        else:
            check_time = datetime.now()
            run_fetch = True
        
        # Check once per minute
        time.sleep(60)

if __name__ == "__main__":
	main()
