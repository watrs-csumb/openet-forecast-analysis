# -*- coding: utf-8 -*-
"""
Created on Thu Jun  13 10:06:44 2024

@author: Robin Fishman
"""
from ETPreprocess import ETPreprocess
from Queue import Queue

import pandas as pd

timeseries_endpoint = "https://developer.openet-api.org/raster/timeseries/point"
forecast_endpoint = "https://developer.openet-api.org/experimental/raster/timeseries/forecasting/seasonal"

# DataFrame: k(OPENET_ID), v(CROP_2020, .geo)
sample_points_reference = pd.read_csv('sample_points.csv', low_memory=False).set_index('OPENET_ID')
sample_points_queue = Queue(sample_points_reference.index.to_list())

if __name__ == '__main__':
	sample_data = ETPreprocess(sample_points_queue, sample_points_reference)
	failed_attempts = sample_data.start(timeseries_endpoint, forecast_endpoint)
	
	print("[LOG] Finished processing. " + str(failed_attempts) + " fields failed.")