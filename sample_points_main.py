# -*- coding: utf-8 -*-
"""
Created on Thu Jun  13 10:06:44 2024

@author: Robin Fishman
"""
from ETRequest import ETRequest
from Queue import Queue

import ast
import json
import pandas as pd
import requests

sample_points_timeseries = {}
sample_points_forecast = {}

timeseries_endpoint = "https://developer.openet-api.org/raster/timeseries/point"
forecast_endpoint = "https://developer.openet-api.org/experimental/raster/timeseries/forecasting/seasonal"

"""
Concurrency approaches:
	1) Simplest: in a while loop for each attempt, use try-except statement until loop ends
	2) Organic: in a try-except statement:
		a) Make attempt
		b) If requests.exceptions.RequestException, start a timer
			- This requires new method with (func, attempts) params
			- Timeout is exponential (e.g. 2 seconds, 20 seconds, 200 seconds)
		c) Allow the option of manually reattempting
			- Prompt in terminal or GUI
		d) Reattempts fetch from a queue of field IDs
			- A field ID is only removed *after* a successful attempt
	3) Researched: using HTTPAdapter & urllib3 Retry libraries
		a) Start an HTTP Session
		b) Create a retry strategy and mount it to the session
""" 

# DataFrame: k(OPENET_ID), v(CROP_2020, .geo)
sample_points_reference = pd.read_csv('sample_points.csv', low_memory=False).set_index('OPENET_ID')
sample_points_queue = Queue(sample_points_reference.index.to_list())

def EvalSampleData():
	failed_fields = 0
	while sample_points_queue.is_empty() is False:
		timeseries_success = False
		forecast_success = False
		current_field_id = sample_points_queue.front()
		current_point_coordinates = json.loads(sample_points_reference['.geo'][current_field_id])['coordinates']
		
		print("[LOG] Now analyzing field ID " + current_field_id)
		# Fetch timeseries data
		timeseries_arg = {
				"date_range": [
					"2023-01-01", "2023-12-31"
				],
				"interval": "monthly",
				"geometry": current_point_coordinates,
				"model": "Ensemble",
				"units": "mm",
				"variable": "ET",
				"reference_et": "gridMET",
				"file_format": "JSON"
			}
		
		timeseries_res = ETRequest(timeseries_endpoint, timeseries_arg)
		timeseries_success = timeseries_res.success()
		# Fetch forecasted data
		forecast_arg = {
				"date_range": [
					"2016-01-01", "2023-12-31"
				],
				"interval": "monthly",
				"geometry": current_point_coordinates,
				"model": "Ensemble",
				"units": "mm",
				"variable": "ET",
				"reference_et": "gridMET",
				"file_format": "JSON"
			}
	
		forecast_res = ETRequest(forecast_endpoint, forecast_arg)
		forecast_success = forecast_res.success()
		# If both are successful, store it!
		if timeseries_success and forecast_success:
			print("[LOG] Successful")
		else:
			print("[WARN] Analyzing for " + current_field_id + " failed")
			failed_fields+=1
   
		sample_points_queue.dequeue()
		print("[LOG] " + str(sample_points_queue.size()) + " fields remaining")

	return failed_fields

print("[LOG] Finished processing. " + str(EvalSampleData()) + " fields failed.")