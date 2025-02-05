import argparse
import gzip
import sys
import time

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
parser.add_argument("-e", "--exclude", nargs=OPTIONAL, type=list, help="List of USDS CDL codes to exclude for EToF maxes")
parser.add_argument("-d", "--dest", nargs=OPTIONAL, default=None, help="Dir/File prefix for output. Defaults to huc8 code")
parser.add_argument("-k", "--key", help="OpenET API Key")

endpoints = {
    "fieldId": "https://openet-api.org/geodatabase/metadata/ids",
    "fieldProps": "https://openet-api.org/geodatabase/metadata/properties",
    "timeseries": "https://openet-api.org/geodatabase/timeseries",
}

ee.Authenticate()
ee.Initialize()

dataset = ee.FeatureCollection("USGS/WBD/2017/HUC08")

def request_handler(**kwargs) -> requests.Response | None:
    try:
        req = requests.post(timeout=60, **kwargs)
        
        if req.status_code != 200:
            print(f"Error {req.status_code}: {req.text}")
            sys.exit(1)
            return
        
        return req
    except KeyboardInterrupt:
        sys.exit(1)

def get_huc8_metadata(huc8_id, api_key) -> pd.DataFrame:
    # Filter huc8 IDs to return element matching ID provided.
    elements: ee.Collection = dataset.filter(ee.Filter.eq("huc8", huc8_id))
    # Localize.
    info = elements.getInfo()
    
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
    
    # Unzip data. List of Field IDs.
    field_Ids = eval(gzip.decompress(id_res.content).decode())
    
    print(f"Found {len(field_Ids)} fields. Fetching metadata for all fields...")
    # Request Handler for getting field metadata.
    metadata_res = request_handler(
        url=endpoints["fieldProps"],
        json={"field_ids": field_Ids},
        headers={"Authorization": api_key},
    )
    
    # Unzips data. List of Dicts["field_id", "hectares", "crop_2016", ..., "crop_2022", ...]
    metadata = eval(gzip.decompress(metadata_res.content).decode())
    
    # Convert to pandas dataframe.
    df = pd.DataFrame.from_records(metadata)
    
    return df

def get_timeseries_data(field_ids, api_key, year) -> pd.DataFrame:
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
    
    crop_col = f"crop_{year}"
    
    # Gets metadata from field IDs found in HUC8 boundary.
    metadata = get_huc8_metadata(huc8Id, api_key=api_key)
    
    # Check if metadata contains year provided.
    if crop_col not in metadata.columns:
        print(f"{year} is not present in field metadata.")
        return
    
    metadata.to_csv(f"{filename}_metadata_{year}.csv")
    
    print("Trimming fields for top 3 crops...")
    # Goes through list of crops to exclude.
    excluder = metadata[~metadata[crop_col].isin(crop_excludes)]
    # Grabs the top 3 crops for the given year.
    top_crops = excluder[crop_col].value_counts(ascending=False)[:3]
    
    collecting_fields = excluder[
        excluder[crop_col].isin(top_crops.index.to_list())
    ]
    
    start = time.perf_counter()
    data = get_timeseries_data(collecting_fields["field_id"].astype(str).tolist(), api_key=api_key, year=year)
    stop = time.perf_counter()
    
    data.to_csv(f"{filename}_values_{year}.csv")
    
    print(f"{collecting_fields["field_id"].agg("count")} fields took {round((stop-start), 2)} seconds.")
    
    crops_max = data.groupby([crop_col, "collection"])["value_mm"].max()
    crops_max.reset_index().to_csv(f"{filename}_etof_maxes_{year}.csv")
    
    return

if __name__ == "__main__":
    main()
