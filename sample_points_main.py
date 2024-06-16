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

logging.basicConfig(level=logging.INFO, filename=datetime.now().strftime(f'logs/sample_points_main_%Y_%m_%d_%H_%M_%S.log'),
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

timeseries_endpoint = "https://developer.openet-api.org/raster/timeseries/point"
forecast_endpoint = "https://developer.openet-api.org/experimental/raster/timeseries/forecasting/seasonal"

# DataFrame: k(OPENET_ID), v(CROP_2020, .geo)
sample_points_reference = pd.read_csv('sample_points.csv', low_memory=False).set_index('OPENET_ID')
sample_points_queue = Queue(sample_points_reference.index.to_list()[:1])

def main():
	sample_data = ETPreprocess(sample_points_queue, sample_points_reference)
	failed_attempts = sample_data.start(timeseries_endpoint, forecast_endpoint, logger=logger)
 
	logger.info(f"Finished processing. {str(failed_attempts)} fields failed.")
	logger.info("\n" + sample_data.data_table.to_string().replace('\n', '\n\t'))

if __name__ == '__main__':
	main()