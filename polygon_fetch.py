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
kern_fields = pd.read_csv("./data/Kern.csv", low_memory=False).sample(50)
monterey_fields = pd.read_csv("./data/Monterey.csv", low_memory=False).sample(50)

def get_polygons(fields, export, field_ref) -> pd.DataFrame|None:
	res = requests.post(boundaries_endpoint,
			headers={"Content-Type": "application/json", "Authorization": api_key},
			json={"field_ids": fields})
	
	data = eval(gzip.decompress(res.content).decode())
	df = pd.DataFrame(columns=["field_id", "CROP_2023", ".geo"])
	for feat in data["features"]:
		str_format_id = 'CA_' + feat['properties']['field_id'][2:]
		crop = field_ref[field_ref['OPENET_ID'] == feat['properties']['field_id']]['CROP_2023'].values[0]
		feat['geometry']['coordinates'] = [x for f in feat['geometry']['coordinates'] for s in f for x in s]
		df = pd.concat([pd.DataFrame([[str_format_id, crop, feat['geometry']]], columns=df.columns), df], ignore_index=True)
	with open(export, 'w') as file:
		json.dump(data, file, ensure_ascii=False, indent=4)
	
	return df

def main():
	monterey_fields['OPENET_ID'] = '06' + monterey_fields['OPENET_ID'].str.slice(start=3)
	kern_fields['OPENET_ID'] = '06' + kern_fields['OPENET_ID'].str.slice(start=3)

	get_polygons(kern_fields['OPENET_ID'].to_list(), "./data/geo/kern_polygons_large.geojson", kern_fields).to_csv('data/kern_polygons_large.csv', index=False)
	get_polygons(monterey_fields['OPENET_ID'].to_list(), "./data/geo/monterey_polygons_large.geojson", monterey_fields).to_csv('data/monterey_polygons_large.csv', index=False)
	
	kern_fields.to_csv('./data/random_kern.csv', index=False)
	monterey_fields.to_csv('./data/random_monterey.csv', index=False)

if __name__ == "__main__":
	main()
