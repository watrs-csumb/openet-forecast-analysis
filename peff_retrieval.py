import pandas as pd
import geopandas as gpd

from pathlib import Path
from requests import post

import argparse
import dotenv
import json

from src.ETRequest_old import ETRequest

parser = argparse.ArgumentParser()
parser.add_argument("file")

ts_endpoint = "https://developer.openet-api.org/raster/timeseries/point"
peff_endpoint = "http://localhost:8080/experimental/peff/point"

api_key = dotenv.dotenv_values("./.env").get("ET_KEY")

def main():
    args = parser.parse_args()
    file_path = Path(args.file)
    gdf = gpd.read_file(file_path)
    variables = ["et", "eto", "pr"]
    
    aws_values = [100, 350, 640]
    combos = [(a, f) for f in [list(gdf.iterfeatures())] for a in aws_values]
    dict_point_ts = []
    
    for aws, feat in combos:
        point_coordinates = feat["geometry"]["coordinates"]
        print("Retrieving aws:", aws, "; for ", point_coordinates)
        # print(f"Retrieving {var} timeseries")
        req = ETRequest(
            request_endpoint=peff_endpoint,
            key=api_key,
            request_params={
                "geometry": list(point_coordinates),
                "date_range": [
                    "2016-01-01",
                    "2022-12-31"
                ],
                "file_format": "JSON",
                "interval": "daily",
                "reference_et": "gridMET",
                "units": "mm",
                "model": "ensemble",
                "aws": aws,
                "buckets": 2,
                "spinup": 7,
                "debug": False,
                "variable": "etaw"
            }
        )
        req.send()
        if "detail" in req.response.json():
            print("Nothing found in ", point_coordinates)
            continue

        dict_point_ts += [{"point": point_coordinates, "aws": aws} | timestep for timestep in req.response.json()]

    df = pd.DataFrame.from_records(dict_point_ts, index="point")
    
    df.drop_duplicates().to_csv("data/peff_points_aws_ts.csv")
    
    print("Now fetching timeseries PR and ETo.")
    var_tables = []
    for var in variables:
        var_data = []
        for feat in gdf.iterfeatures():
            point_coordinates = feat["geometry"]["coordinates"]
            req = ETRequest(
                ts_endpoint,
                {
                    "date_range": ["2020-01-01", "2022-12-31"],
                    "geometry": list(point_coordinates),
                    "file_format": "JSON",
                    "reference_et": "gridMET",
                    "model": "ensemble",
                    "units": "mm",
                    "interval": "daily",
                    "variable": var
                }, api_key
            )
            req.send()
            
            if "detail" in req.response.json():
                print("Nothing found in ", point_coordinates)
                continue
            
            var_data += [{"point": point_coordinates} | timestep for timestep in req.response.json()]
        
        df = pd.DataFrame.from_records(var_data, index="point")
        var_tables.append(df)
    
    main_table = var_tables[0]
    for table in var_tables[1:]:
        main_table = pd.merge(main_table, table, how="left", on=["point", "time"])
    
    main_table.to_csv("data/peff_points_var_ts.csv")

if __name__ == "__main__":
    main()
