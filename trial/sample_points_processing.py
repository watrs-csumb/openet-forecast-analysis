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

# Ideas
# - During generation, use the .content 'time' field as a multi-index. Use 'et' field for data value
#                  - .content field e.g.  [{time: '01-01-2023', 'et': 15.0}, ...]

sample_df = {
    'field': [],
    'time': [],
    'data': []
    }

for entry in spf_dict:
    sample_entry = spf_dict[entry]
    
    for data in sample_entry:
        # Convert data into a dict using safer eval() method
        data = ast.literal_eval(data)
        
        sample_df['field'].append(entry)
        sample_df['time'].append(data['time'])
        sample_df['data'].append(data['et'])

sample_df = pd.DataFrame(sample_df)

