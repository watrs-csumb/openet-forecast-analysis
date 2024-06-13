# -*- coding: utf-8 -*-
"""
Created on Thu Jun  13 10:06:44 2024

@author: Robin Fishman
"""
import ast
from dotenv import dotenv_values
import json
import pandas as pd
import requests

sample_points = pd.read_csv('sample_points.csv', low_memory=False)

sample_points_timeseries = {}
sample_points_forecast = {}

timeseries_endpoint = "https://developer.openet-api.org/raster/timeseries/point"
forecast_endpoint = "https://developer.openet-api.org/experimental/raster/timeseries/forecasting/seasonal"

header = {"Authorization": dotenv_values(".env").get("ET_KEY")}
