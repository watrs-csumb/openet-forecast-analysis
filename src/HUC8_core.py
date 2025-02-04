import gzip

import ee
import numpy as np
import pandas as pd

from src.ETRequest import ETRequest

endpoints = {
    "fieldId": "https://openet-api.org/geodatabase/metadata/ids",
    "fieldProps": "https://openet-api.org/geodatabase/metadata/properties",
    "timeseries": "https://openet-api.org/geodatabase/timeseries",
}

class HUC8:
    def __init__(self, et_api_key):
        self.dataset = ee.FeatureCollection("USGS/WBD/2017/HUC08")
        self.KEY = et_api_key
    
    def get_huc8_metadata(self, huc8_id) -> pd.DataFrame:
        # Filter huc8 IDs to return element matching ID provided.
        elements: ee.Collection = self.dataset.filter(ee.Filter.eq("huc8", huc8_id))
        # Localize.
        info = elements.getInfo()

        # Extract coordinates from dataset element.
        boundaries_raw = [x["geometry"]["coordinates"] for x in info["features"]]
        # Flatten coordinate list as float values.
        boundaries = np.array(boundaries_raw).flatten().astype(float).tolist()

        # Request Handler for getting Field IDs from geometry.
        id_req = ETRequest(
            request_endpoint=endpoints["fieldId"],
            request_params={"geometry": boundaries},
            key=self.KEY,
        )
        # Queue request.
        id_res = id_req.send()

        # Unzip data. List of Field IDs.
        field_Ids = eval(gzip.decompress(id_res.content).decode())

        # Request Handler for getting field metadata.
        metadata_req = ETRequest(
            request_endpoint=endpoints["fieldProps"],
            request_params={"field_ids": field_Ids},
            key=self.KEY,
        )
        metadata_res = metadata_req.send()

        # Unzips data. List of Dicts['field_id', 'hectares', 'crop_2016', ..., 'crop_2022', ...]
        metadata = eval(gzip.decompress(metadata_res.content).decode())

        # Convert to pandas dataframe.
        df = pd.DataFrame.from_records(metadata)

        return df

    def get_timeseries_data(self, field_ids) -> pd.DataFrame:
        # Request Handler for timeseries data.
        req = ETRequest(
            request_endpoint=endpoints["timeseries"],
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
                    "eeMetric",
                ],
                "variables": ["ETof"],
                "file_format": "JSON",
            },
            key=self.KEY,
        )
        res = req.send()

        data = eval(gzip.decompress(res.content).decode())

        df = pd.DataFrame(data)

        return df
