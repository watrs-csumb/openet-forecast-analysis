import argparse
import contextlib
from datetime import datetime, timedelta
import gzip
import json
import pathlib
import sys

cdl_allowlist = [1, 28, 36, 21, 4, 27, 5, 37]

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
    from tqdm.contrib import DummyTqdmFile

    @contextlib.contextmanager
    def std_out_err_redirect():
        out_err = sys.stdout, sys.stderr
        try:
            sys.stdout, sys.stderr = map(DummyTqdmFile, out_err)
            yield out_err[0]
        except Exception as e:
            raise e
        finally:
            sys.stdout, sys.stderr = out_err

    with std_out_err_redirect() as out:
        tqdm.pandas(file=out)

    _TQDM_ENABLED = True

except ImportError:
    pass

parser = argparse.ArgumentParser()
parser.add_argument(
    "--state", "-s", required=True, help="State Abbreviation or FIPS Code"
)
parser.add_argument("--year", "-y", type=int, required=True, help="Water year to fetch (October 1st to September 30th of following year)")
parser.add_argument("--output", "-o", required=False, help="Output directory")
parser.add_argument("--key", "-k", required=True, help="OpenET API Key")
parser.add_argument("--variable", "-v", required=True, type=str, default="et", help="Variable to fetch. Default: et")

collections = {
    "states": "TIGER/2018/States",
    "counties": "TIGER/2018/Counties",
}

endpoints = {
    "timeseries": "https://developer.openet-api.org/geodatabase/timeseries",
    "fieldId": "https://developer.openet-api.org/geodatabase/metadata/ids",
    "fieldProps": "https://developer.openet-api.org/geodatabase/metadata/properties",
}

ee.Authenticate()

if not _valid_credentials_exist():
    print(
        "No valid Earth Engine credentials found. Please run `earthengine authenticate` and try again."
    )
    sys.exit(1)

ee.Initialize()


def request_handler(**kwargs) -> Response | None:
    try:
        req = None
        for i in range(0, 3):
            req = post(timeout=260, **kwargs)

            if req.ok:
                return req

        if req is not None and req.status_code != 200:
            print(f"Error {req.status_code}: {req.text}")

    except KeyboardInterrupt:
        sys.exit(1)


def fetch_state(state: str) -> Collection:
    return FeatureCollection(collections["states"]).filter(
        Filter.Or(Filter.eq("STUSPS", state), Filter.eq("STATEFP", state))
    )


def fetch_counties(state: str) -> Collection:
    state_coll = fetch_state(state).first()
    return FeatureCollection(collections["counties"]).filter(
        Filter.eq("STATEFP", state_coll.get("STATEFP"))
    )


def get_timeseries(
    outer: list[float], variable: str, year: int, api_key: str, **kwargs
) -> pd.DataFrame:
    fields_req = request_handler(
        url=endpoints["fieldId"],
        headers={"Authorization": api_key},
        json={"geometry": outer},
    )

    if fields_req is None:
        print("Could not fetch fields in geometry")
        return pd.DataFrame()

    field_ids = eval(gzip.decompress(fields_req.content).decode())

    if len(field_ids) == 0:
        print("No fields found within county boundary. Skipping...")
        return pd.DataFrame()

    metadata_req = request_handler(
        url=endpoints["fieldProps"],
        headers={"Authorization": api_key},
        json={"field_ids": field_ids},
    )

    if metadata_req is None:
        print("Could not fetch field metadata")
        return pd.DataFrame()

    metadata_res = eval(gzip.decompress(metadata_req.content).decode())

    properties = pd.DataFrame.from_records(metadata_res)
    crop_years = [col for col in list(properties.columns) if col.startswith("crop_")]
    crop_col = (crop_years if f"crop_{year}" in crop_years else [crop_years[-1]])[0]
    # Filter crop_year for just the request year, if not available use the last available.
    properties = properties[["field_id"] + [crop_col]]
    properties = properties[properties[crop_col].isin(cdl_allowlist)]
    field_ids = properties["field_id"].tolist()

    if len(field_ids) == 0:
        print("No fields found containing crops in allowlist. Skipping...")
        return pd.DataFrame()

    end_date = min(datetime(year=year+1, month=9, day=30), datetime.now() - timedelta(weeks=1))
    timeseries_req = request_handler(
        url=endpoints["timeseries"],
        headers={"Authorization": api_key},
        json={
            "field_ids": field_ids,
            "date_range": [f"{year}-10-01", end_date.strftime("%Y-%m-%d")],
            "interval": "monthly",
            "models": ["Ensemble"],
            "variables": [variable],
            "file_format": "CSV",
        },
    )

    if timeseries_req is None:
        print("Could not fetch timeseries")
        return pd.DataFrame()

    data = json.loads(timeseries_req.content.decode())

    df = pd.json_normalize(data).merge(properties, how="left", on="field_id")

    for key, value in kwargs.items():
        df[key] = value

    return df


def main():
    args = parser.parse_args()
    key = args.key
    year = args.year
    state = args.state
    output = args.output
    variable = args.variable
    
    # Validate water year has started.
    if datetime.now() < datetime(year=year, month=10, day=1):
        raise ValueError("Specified water year has not started yet.")

    pathlib.Path(output).mkdir(parents=False, exist_ok=True)

    counties = fetch_counties(state).getInfo()["features"]  # type: ignore
    print(f"Found {len(counties)} counties in {state}")

    gdf = gpd.GeoDataFrame.from_features(counties)

    def timeseries(x: pd.DataFrame) -> pd.DataFrame:
        return get_timeseries(
            get_coordinates(x["geometry"]).flatten().astype(float).tolist(), 
            variable=variable,
            year=year, api_key=key
        )

    print("Fetching timeseries ET data for each county...")
    if _TQDM_ENABLED:
        counties_timeseries = (
            gdf.groupby(["NAME", "geometry"]).progress_apply(timeseries).reset_index() # type: ignore
        )
    else:
        counties_timeseries = (
            gdf.groupby(["NAME", "geometry"]).apply(timeseries).reset_index()
        )

    crop_col = [col for col in counties_timeseries.columns if col.startswith("crop_")][
        0
    ]

    print("Aggregating averages per county by crop type...")
    if _TQDM_ENABLED:
        averages = counties_timeseries.groupby(["NAME", crop_col, "collection"])[
            "value_mm"
        ].progress_apply(np_mean).round(3)  # type: ignore
    else:
        averages = counties_timeseries.groupby(["NAME", crop_col, "collection"])[
            "value_mm"
        ].mean().round(3)

    print("Exporting to CSV...")
    cdl_lookup = pd.read_csv(
        "https://media.githubusercontent.com/media/watrs-csumb/openet-forecast-analysis/refs/heads/main/data/cdl_codes.csv",
        index_col="Codes",
    )["Class_Names"]
    averages = averages.reset_index().merge(cdl_lookup, how="left", left_on=crop_col, right_index=True)
    # Rearrange columns.
    averages = averages[["NAME", "Class_Names", crop_col, "collection", "value_mm"]]
    averages.to_csv(f"{output}/{state}_{year}_county_timeseries_avg_{str(variable).lower()}_by_crop_type.csv", index=False)


if __name__ == "__main__":
    main()
