# -*- coding: utf-8 -*-
"""
Created on Tue Jun  4 09:57:15 2024

@author: robin
"""
import ast
import pandas as pd

# First see if I can get one table to work
sample_points_forecast = pd.read_csv("sample_points_forecast.csv", low_memory=False)
sample_points_forecast.drop('Unnamed: 0', axis=1, inplace=True)

spf_dict = sample_points_forecast.to_dict(orient='list')

sample_forecast_df = {
    'field_id': [],
    'time': [],
    'et_expected_mm': []
}

for entry in spf_dict:
    sample_entry = spf_dict[entry]
    
    for data in sample_entry:
        # Convert data into a dict using safer eval() method
        data = ast.literal_eval(data)
        
        sample_forecast_df['field_id'].append(entry)
        sample_forecast_df['time'].append(data['time'])
        sample_forecast_df['et_expected_mm'].append(data['et'])

sample_forecast_df = pd.DataFrame(sample_forecast_df)

sample_points_timeseries = pd.read_csv("sample_points_timeseries.csv", low_memory=False)
sample_points_timeseries.drop('Unnamed: 0', axis=1, inplace=True)

spt_dict = sample_points_timeseries.to_dict(orient='list')

sample_timeseries_df = {
    'field_id': [],
    'time': [],
    'et_actual_mm': []
}

for entry in spt_dict:
    sample_entry = spt_dict[entry]
    
    for data in sample_entry:
        data = ast.literal_eval(data)
        
        sample_timeseries_df['field_id'].append(entry)
        sample_timeseries_df['time'].append(data['time'])
        sample_timeseries_df['et_actual_mm'].append(data['et'])

sample_timeseries_df = pd.DataFrame(sample_timeseries_df)

df_by_join = sample_timeseries_df.set_index(['field_id', 'time']).join(sample_forecast_df.set_index(['field_id', 'time']))
