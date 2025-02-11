import argparse
import gzip
import logging
import pathlib
import sys
import time

from typing import Optional, List, Union

if sys.version_info < (3, 8):
    print("Python 3.8+ is supported. Please update to use this script.")
    sys.exit(1)

OPTIONAL = "?"

try:
    import requests
except ImportError:
    print("Please run `pip install requests` and try again.")
    sys.exit(1)

try:
    import ee
    from ee.featurecollection import FeatureCollection
    from ee.collection import Collection
    from ee.filter import Filter
    from ee.oauth import _valid_credentials_exist
except ImportError:
    print("Please run `pip install earthengine-api` and try again.")
    sys.exit(1)

try:
    import numpy as np
except ImportError:
    print("Please run `pip install numpy` and try again.")
    sys.exit(1)

try:
    import pandas as pd
except ImportError:
    print("Please run `pip install pandas` and try again.")
    sys.exit(1)

parser = argparse.ArgumentParser(add_help=True)
parser.add_argument("-huc8", "--huc8", nargs=OPTIONAL, type=str, help="HUC8 Code")
parser.add_argument("-y", "--year", nargs=OPTIONAL, default="2022", help="Year of Reference. Default, 2022")
parser.add_argument("-e", "--exclude", nargs=OPTIONAL, default=[], type=list, help="List of USDS CDL codes to exclude for EToF maxes")
parser.add_argument("-d", "--dest", nargs=OPTIONAL, default=None, help="Dir/File prefix for output. Defaults to huc8 code")
parser.add_argument("-t", "--top", nargs=OPTIONAL, default=3, type=int, help="Number of top crops to fetch for each watershed. To include all, enter 0. Default, 3")
parser.add_argument("-p", "--peak", nargs=2, default=(4,8), type=tuple, help="Start and end months of peak season. Default, (4,8)")
parser.add_argument("-k", "--key", help="OpenET API Key")

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

endpoints = {
    "fieldId": "https://openet-api.org/geodatabase/metadata/ids",
    "fieldProps": "https://openet-api.org/geodatabase/metadata/properties",
    "timeseries": "https://openet-api.org/geodatabase/timeseries",
}

ee.Authenticate()

if not _valid_credentials_exist():
    print("No valid Earth Engine credentials found. Please run `earthengine authenticate` and try again.")
    sys.exit(1)

ee.Initialize()

dataset = FeatureCollection("USGS/WBD/2017/HUC08")

def request_handler(**kwargs) -> Optional[requests.Response]:
    try:
        req = requests.post(timeout=60, **kwargs)
        
        if req.status_code != 200:
            print(f"Error {req.status_code}: {req.text}")
            sys.exit(1)
            return
        
        return req
    except KeyboardInterrupt:
        sys.exit(1)

def get_huc8_metadata(huc8_id: str, api_key: str) -> Optional[pd.DataFrame]:
    # Filter huc8 IDs to return element matching ID provided.
    elements: Collection = dataset.filter(Filter.eq("huc8", huc8_id))
    
    # Localize.
    info = elements.getInfo()
    
    if info is None:
        return
    
    # If features list is empty, filter did not find the huc8 ID.
    if len(info["features"]) == 0:
        print("HUC8 ID not found. Please check ID provided.")
        sys.exit(1)
        return
    
    # Extract coordinates from dataset element.
    boundaries_raw = [x["geometry"]["coordinates"] for x in info["features"]]
    # Flatten coordinate list as float values.
    boundaries = np.array(boundaries_raw).flatten().astype(float).tolist()
    
    print("Found HUC8 ID. Fetching fields within watershed boundary...")
    # Request Handler for getting Field IDs from geometry.
    id_res = request_handler(
        url=endpoints["fieldId"],
        json={"geometry": boundaries},
        headers={"Authorization": api_key},
    )
    
    if id_res is None:
        return
    
    # Check if request was successful.
    if id_res.status_code != 200:
        print(f"Error {id_res.status_code}: {id_res.text}")
        sys.exit(1)
        return
    
    # Unzip data. List of Field IDs.
    field_Ids = eval(gzip.decompress(id_res.content).decode())
    
    print(f"Found {len(field_Ids)} fields. Fetching metadata for all fields...")
    # Request Handler for getting field metadata.
    metadata_res = request_handler(
        url=endpoints["fieldProps"],
        json={"field_ids": field_Ids},
        headers={"Authorization": api_key},
    )
    
    if metadata_res is None:
        return
    
    # Check if request was successful.
    if metadata_res.status_code != 200:
        print(f"Error {metadata_res.status_code}: {metadata_res.text}")
        sys.exit(1)
        return
    
    # Unzips data. List of Dicts["field_id", "hectares", "crop_2016", ..., "crop_2022", ...]
    metadata = eval(gzip.decompress(metadata_res.content).decode())
    
    # Convert to pandas dataframe.
    df = pd.DataFrame.from_records(metadata)
    
    return df

def get_timeseries_data(field_ids: List[str], api_key: str, year: Union[str, int]) -> Optional[pd.DataFrame]:
    print(f"Fetching {year} EToF timeseries data for {len(field_ids)} fields...")
    # Request Handler for timeseries data.
    res = request_handler(
        url=endpoints["timeseries"],
        json={
            "date_range": [f"{year}-01-01", f"{year}-12-31"],
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
        headers={"Authorization": api_key},
    )
    
    if res is None:
        return
    
    # Check if request was successful.
    if res.status_code != 200:
        print(f"Error {res.status_code}: {res.text}")
        sys.exit(1)
        return
    
    data = eval(gzip.decompress(res.content).decode())

    df = pd.DataFrame(data)

    return df

def main():
    args = parser.parse_args()
    
    huc8Id = args.huc8
    year = args.year
    filename = args.dest or huc8Id
    api_key = args.key
    crop_excludes = args.exclude
    n_crops = args.top
    s_peak, e_peak = args.peak
    
    if n_crops < 0:
        print("Number of crops cannot be negative.")
        sys.exit(1)
    
    if s_peak not in range(1, 13) or e_peak not in range(1, 13):
        print("Months must be between 1 and 12.")
        sys.exit(1)
    
    if e_peak < s_peak:
        print("End of peak season cannot be before start of peak season.")
        sys.exit(1)
    
    # If filename is a directory, append HUC8 ID to it.
    if pathlib.Path(filename).is_dir():
        filename = f"{filename}/{huc8Id}"
    
    crop_col = f"crop_{year}"
    
    # Gets metadata from field IDs found in HUC8 boundary.
    metadata = get_huc8_metadata(huc8Id, api_key=api_key)
    
    if metadata is None:
        return
    
    # Check if metadata contains year provided.
    if crop_col not in metadata.columns:
        print(f"{year} is not present in field metadata.")
        return
    
    metadata_loc = f"{filename}_metadata_{year}.csv"
    metadata.to_csv(metadata_loc)
    print(f"Exported metadata to {metadata_loc}")
    
    
    if n_crops > 0:
        print(f"Trimming fields for top {n_crops} crops...")
        # Goes through list of crops to exclude.
        excluder = metadata[~metadata[crop_col].isin(crop_excludes)]
        # Grabs the top crops for the given year.
        top_crops = excluder[crop_col].value_counts(ascending=False)[:n_crops].index.to_list()
        
        metadata = excluder[excluder[crop_col].isin(top_crops)]
    
    start = time.perf_counter()
    data = get_timeseries_data(metadata["field_id"].astype(str).tolist(), api_key=api_key, year=year)
    stop = time.perf_counter()
    
    if data is None:
        print("Failed to get timeseries data.")
        return None
    
    data.to_csv(f"{filename}_values_{year}.csv")
    
    print(f"{metadata['field_id'].agg('count')} fields took {round((stop-start), 2)} seconds.")
    
    crops_max = data.groupby(["field_id", "collection"])["value_mm"].max().round(2)
    crops_max.reset_index().to_csv(f"{filename}_etof_maxes_{year}.csv", index=False)
    
    return

if __name__ == "__main__":
    main()
