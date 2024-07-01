# -*- coding: utf-8 -*-
"""
Created on Thu Jun  13 10:06:44 2024

@author: Robin Fishman
"""
from datetime import datetime
from ETPreprocess import ETPreprocess
from Queue import Queue

import logging
import pandas as pd
import sys

# LOGGING CONFIG
# File handler that allows files to show all log entries
file_log_handler = logging.FileHandler(filename=datetime.now().strftime(f'logs/sample_points_main_%Y_%m_%d_%H_%M_%S.log'))

# Stream handler that prints log entries at level WARNING or higher
stdout_log_handler = logging.StreamHandler(stream=sys.stdout)
stdout_log_handler.setLevel(logging.WARNING)

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[file_log_handler, stdout_log_handler])
logger = logging.getLogger(__name__)
# END LOGGING CONFIG

timeseries_endpoint = "https://developer.openet-api.org/raster/timeseries/point"
forecast_endpoint = "https://developer.openet-api.org/experimental/raster/timeseries/forecasting/seasonal"

# DataFrame: k(OPENET_ID), v(CROP_2020, .geo)
sample_points_reference = pd.read_csv('sample_points.csv', low_memory=False).set_index('OPENET_ID')
sample_points_queue = Queue(sample_points_reference.index.to_list())

def main():
	sample_data = ETPreprocess(sample_points_queue, sample_points_reference)
	failed_attempts = sample_data.start(timeseries_endpoint, forecast_endpoint, logger=logger)
 
	logger.info(f"Finished processing. {str(failed_attempts)} fields failed.")
	# logger.info("\n" + sample_data.data_table.to_string().replace('\n', '\n\t'))
	sample_data.data_table.to_csv("samples_data_table_2014.csv")

if __name__ == '__main__':
	main()