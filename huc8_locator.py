import gzip
import requests
import sys
import time

import ee
import numpy as np
import pandas as pd

from dotenv import dotenv_values

from src import ETRequest

endpoints = {
    "fieldId": "https://openet-api.org/geodatabase/metadata/ids",
    "fieldProps": "https://openet-api.org/geodatabase/metadata/properties",
    "timeseries": "https://openet-api.org/geodatabase/timeseries",
}

ee.Authenticate()
ee.Initialize(project='watrs-rfishman')
KEY = dotenv_values('.env').get('ET_KEY')

dataset = ee.FeatureCollection('USGS/WBD/2017/HUC08')

class HUC8:
    def get_huc8_metadata(huc8_id) -> pd.DataFrame:
        # Filter huc8 IDs to return element matching ID provided.
        elements: ee.Collection = dataset.filter(ee.Filter.eq('huc8', huc8_id))
        # Localize.
        info = elements.getInfo()
        
        # Extract coordinates from dataset element.
        boundaries_raw = [x["geometry"]["coordinates"] for x in info["features"]]
        # Flatten coordinate list as float values.
        boundaries = np.array(boundaries_raw).flatten().astype(float).tolist()
        
        # Request Handler for getting Field IDs from geometry.
        id_req = ETRequest(request_endpoint=endpoints['fieldId'],
                        request_params= {
                            "geometry": boundaries
                        }, key=KEY)
        # Queue request.
        id_res = id_req.send()
        
        # Unzip data. List of Field IDs.
        field_Ids = eval(gzip.decompress(id_res.content).decode())
        
        # Request Handler for getting field metadata.
        metadata_req = ETRequest(request_endpoint=endpoints['fieldProps'],
                                request_params={
                                    "field_ids": field_Ids
                                }, key=KEY)
        metadata_res = metadata_req.send()
        
        # Unzips data. List of Dicts['field_id', 'hectares', 'crop_2016', ..., 'crop_2022', ...]
        metadata = eval(gzip.decompress(metadata_res.content).decode())
        
        # Convert to pandas dataframe.
        df = pd.DataFrame.from_records(metadata)
        
        return df

    def get_timeseries_data(field_ids) -> pd.DataFrame:
        # Request Handler for timeseries data.
        req = ETRequest(request_endpoint=endpoints['timeseries'],
                        request_params={
                            "date_range": ["2022-01-01", "2022-12-31"],
                            "interval": "monthly",
                            "field_ids": field_ids,
                            "models": [
                                "Ensemble",
                                "geeSEBAL",
                                "SSEBop",
                                "SIMS",
                                "DisALEXI",
                                "PTJPL",
                                "eeMetric"
                            ],
                            "variables": ["ETof"],
                            "file_format": "JSON"
                        }, key=KEY)
        res = req.send()
        
        data = eval(gzip.decompress(res.content).decode())
        
        df = pd.DataFrame(data)
        
        return df

def main():
    huc8Id = sys.argv[1]
    # Gets metadata from field IDs found in HUC8 boundary.
    metadata = HUC8.get_huc8_metadata(huc8Id)
    # metadata.to_csv(f"./data/huc8/{huc8Id}_metadata.csv")
    
    top_crops = metadata['crop_2022'].value_counts(ascending=False)[:3]
    
    collecting_fields = metadata[metadata['crop_2022'].isin(top_crops.index.to_list())]
    
    start = time.perf_counter()
    data = HUC8.get_timeseries_data(collecting_fields['field_id'].astype(str).tolist())
    stop = time.perf_counter()
    
    # data.to_csv(f"./data/huc8/{huc8Id}_values.csv")
    
    print(f'{collecting_fields['field_id'].agg('count')} fields took {round((stop-start), 2)}')
    
    return

if __name__ == '__main__':
    main()
