# -*- coding: utf-8 -*-
"""
Created on Thu Jun  13 10:06:44 2024

@author: Robin Fishman
"""
from dotenv import dotenv_values
from Queue import Queue

import ast
import json
import pandas as pd
import requests

sample_points_timeseries = {}
sample_points_forecast = {}

timeseries_endpoint = "https://developer.openet-api.org/raster/timeseries/point"
forecast_endpoint = "https://developer.openet-api.org/experimental/raster/timeseries/forecasting/seasonal"

header = {"Authorization": dotenv_values(".env").get("ET_KEY")}

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
sample_points_reference = pd.read_csv('sample_points.csv', low_memory=False)
sample_points_queue = Queue(sample_points_reference.index)