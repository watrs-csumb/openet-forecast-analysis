# -*- coding: utf-8 -*-
"""
Created on Mon Sep 16 8:42:18 2024

@author: Robin Fishman
"""
from dotenv import dotenv_values

import gzip
import json
import pandas as pd
import requests

boundaries_endpoint = "https://developer.openet-api.org/geodatabase/metadata/boundaries"

api_key = dotenv_values(".env").get("ET_KEY")
kern_fields = pd.read_csv("./data/kern_worst_mae_fields.csv", low_memory=False, dtype={
	'field_id': object,
	'crop': int,
	'mae': float
})
monterey_fields = pd.read_csv("./data/monterey_worst_mae_fields.csv", low_memory=False, dtype={
	'field_id': object,
	'crop': int,
	'mae': float
})

def get_polygons(fields, export, field_ref) -> pd.DataFrame:
	res = requests.post(boundaries_endpoint,
			   headers={"Content-Type": "application/json", "Authorization": api_key},
			   json={"field_ids": fields})
	
	data = eval(gzip.decompress(res.content).decode())

	df = pd.DataFrame(columns=["field_id", "CROP_2023", ".geo"])
	for feat in data["features"]:
		str_format_id = 'CA_' + feat['properties']['field_id'][2:]
		crop = field_ref[field_ref['field_id'] == feat['properties']['field_id']]['crop'].values[0]
		feat['geometry']['coordinates'] = [x for f in feat['geometry']['coordinates'] for s in f for x in s]
		df = pd.concat([pd.DataFrame([[str_format_id, crop, feat['geometry']]], columns=df.columns), df], ignore_index=True)

	with open(export, 'w') as file:
		json.dump(data, file, ensure_ascii=False, indent=4)
	
	return df

def main():
	get_polygons(kern_fields['field_id'].to_list(), "./data/geo/kern_polygons.geojson", kern_fields).to_csv('data/kern_polygons.csv', index=False)
	get_polygons(monterey_fields['field_id'].to_list(), "./data/geo/monterey_polygons.geojson", monterey_fields).to_csv('data/monterey_polygons.csv', index=False)
	
if __name__ == "__main__":
	main()
