import argparse
import gzip
import pathlib
import sys

if sys.version_info < (3, 11):
    print("Python 3.11+ is supported. Please update to use this script.")
    sys.exit(1)
    
try:
    from requests import post, Response
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
    import geopandas as gpd
    # Geopandas has the below as dependencies so they'll be available if geopandas is installed.
    import pandas as pd
    from shapely import get_coordinates
    from numpy import mean as np_mean
except ImportError:
    print("Please run `pip install geopandas` and try again.")
    sys.exit(1)

_TQDM_ENABLED = False

try:
    from tqdm import tqdm
    tqdm.pandas()
    _TQDM_ENABLED = True
except ImportError:
    pass

parser = argparse.ArgumentParser()
parser.add_argument("--state", "-s", required=True, help="State Abbreviation or FIPS Code")
parser.add_argument("--year", "-y", required=True, help="Year")
parser.add_argument("--output", "-o", required=False, help="Output directory")
parser.add_argument("--key", "-k", required=True, help="OpenET API Key")

collections = {
    "states": "TIGER/2018/States",
    "counties": "TIGER/2018/Counties",
}

endpoints = {
    "timeseries": "https://openet-api.org/geodatabase/timeseries",
    "fieldId": "https://openet-api.org/geodatabase/metadata/ids",
    "fieldProps": "https://openet-api.org/geodatabase/metadata/properties",
}

ee.Authenticate()

if not _valid_credentials_exist():
    print("No valid Earth Engine credentials found. Please run `earthengine authenticate` and try again.")
    sys.exit(1)

ee.Initialize()

def request_handler(**kwargs) -> Response | None:
    try:
        req = post(timeout=260, **kwargs)
        
        if req.status_code != 200:
            print(f"Error {req.status_code}: {req.text}")
            sys.exit(1)
            return
        
        return req
    except KeyboardInterrupt:
        sys.exit(1)

def fetch_state(state: str) -> Collection:
    return FeatureCollection(collections["states"]).filter(
        Filter.Or(
            Filter.eq("STUSPS", state),
            Filter.eq("STATEFP", state)
        ))

def fetch_counties(state: str) -> Collection:
    state_coll = fetch_state(state).first()
    return FeatureCollection(collections["counties"]).filter(
        Filter.eq("STATEFP", state_coll.get("STATEFP"))
    )

def get_timeseries(outer: list[float], year: int, api_key: str, **kwargs) -> pd.DataFrame:
    fields_req = post(
        url = endpoints["fieldId"],
        headers = {"Authorization": api_key},
        json = {"geometry": outer}
    )
    
    if not fields_req.ok:
        raise Exception(fields_req.json()["detail"])
    
    field_ids = eval(gzip.decompress(fields_req.content).decode())
    
    metadata_req = post(
        url = endpoints["fieldProps"],
        headers = {"Authorization": api_key},
        json = {"field_ids": field_ids}
    )
    
    if not metadata_req.ok:
        raise Exception(metadata_req.json()["detail"])
    
    metadata_res = eval(gzip.decompress(metadata_req.content).decode())
    
    properties = pd.DataFrame.from_records(metadata_res)
    crop_years = [col for col in list(properties.columns) if col.startswith("crop_")]
    # Filter crop_year for just the request year, if not available use the last available.
    properties = properties[["field_id"] + (crop_years if f"crop_{year}" in crop_years else [crop_years[-1]])]
    
    timeseries_req = post(
        url = endpoints["timeseries"],
        headers = {"Authorization": api_key},
        json = {
            "field_ids": field_ids,
            "date_range": [f"{year}-01-01", f"{year}-12-31"],
            "interval": "monthly",
            "models": [
                "Ensemble",
                "geeSEBAL",
                "SSEBop",
                "SIMS",
                "DisALEXI",
                "PTJPL",
                "eeMetric",
            ],
            "variables": ["ET"],
            "file_format": "JSON"
        }
    )
    
    if not timeseries_req.ok:
        raise Exception(timeseries_req.json()["detail"])
    
    data = eval(gzip.decompress(timeseries_req.content).decode())
    
    df = pd.DataFrame(data).merge(properties, how="left", on="field_id")
    
    for key, value in kwargs.items():
        df[key] = value
    
    return df
    
def main():
    args = parser.parse_args()
    key = args.key
    year = args.year
    state = args.state
    output = args.output
    
    pathlib.Path(output).mkdir(parents=False, exist_ok=True)
    
    counties = fetch_counties(state).getInfo()["features"]  # type: ignore
    print(f"Found {len(counties)} counties in {state}")
    
    gdf = gpd.GeoDataFrame.from_features(counties)
    
    def timeseries(x: pd.DataFrame) -> pd.DataFrame:
        return get_timeseries(list(get_coordinates(x["geometry"]).flatten()), year, key, county=x["NAME"])
    
    print("Fetching timeseries ET data for each county...")
    if _TQDM_ENABLED:
        counties_timeseries = gdf.groupby(["NAME", "geometry"]).progress_apply(timeseries) # type: ignore
    else:
        counties_timeseries = gdf.groupby(["NAME", "geometry"]).apply(timeseries)
    
    print(counties_timeseries)
    crop_col = [col for col in counties_timeseries.columns if col.startswith("crop_")][0]
    
    print("Aggregating averages per county by crop type...")
    if _TQDM_ENABLED:
        averages = counties_timeseries.groupby(["county", crop_col]).progress_apply(np_mean) # type: ignore
    else:
        averages = counties_timeseries.groupby(["county", crop_col]).mean()
    
    print("Exporting to CSV...")
    averages.to_csv(f"{output}/{state}_{year}_county_timeseries_avg_by_crop_type.csv")

if __name__ == "__main__":
    main()
