# possible values: local, dev, public
access_level = "local"
port = "8010"  # Required only if access_level is 'local'. Is ignored otherwise.

## Preferably do not edit below unless you know how to revert if something breaks ##
from dotenv import dotenv_values  # noqa: E402
from pathlib import Path # noqa: E402
from tqdm import tqdm  # noqa: E402
from src.ETRequest_old import ETRequest  # noqa: E402

import gzip  # noqa: E402
import logging  # noqa: E402

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import geopandas as gpd  # noqa: E402
import shapely as shp  # noqa: E402
import sys  # noqa: E402

# Stream handler that prints log entries at level WARNING or higher
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(stream=sys.stdout)],
)
logger = logging.getLogger(__name__)

api_key = dotenv_values("/Users/rfishman/Code/.env").get("OPENET_API_KEY")
req_header = {"Authorization": api_key}

endpoint_prefixes = {
    "local": "http://localhost:" + port,
    "dev": "https://developer.openet-api.org",
    "public": "https://openet-api.org",
}

endpoints = {
    "gdb": {
        "fields": f"{endpoint_prefixes[access_level]}/geodatabase/metadata/ids",
        "metadata": f"{endpoint_prefixes[access_level]}/geodatabase/metadata/properties",
        "boundaries": f"{endpoint_prefixes[access_level]}/geodatabase/metadata/boundaries",
        "timeseries": f"{endpoint_prefixes[access_level]}/geodatabase/timeseries",
    }
}


def retrieve_metadata(field_ids: list):
    req = ETRequest(
        request_endpoint=endpoints["gdb"]["metadata"],
        request_params={"field_ids": field_ids},
        key=api_key,
    )
    req.send()

    data = eval(gzip.decompress(req.response.content).decode())
    properties = pd.DataFrame.from_records(data)

    boundaries_req = ETRequest(
        request_endpoint=endpoints["gdb"]["boundaries"],
        request_params={"field_ids": field_ids},
        key=api_key
    )
    boundaries_req.send()

    boundaries = eval(gzip.decompress(boundaries_req.response.content).decode())
    gdf = gpd.GeoDataFrame.from_features(boundaries, crs="EPSG:4326")

    return gdf.merge(properties, on="field_id", how="inner")


def retrieve_ts(field_ids: list, variables: list[str], date_range: tuple[str, str]):
    req = ETRequest(
        request_endpoint=endpoints["gdb"]["timeseries"],
        key=api_key,
        request_params={
            "date_range": list(date_range),
            "interval": "monthly",
            "field_ids": field_ids,
            "models": ["ensemble"],
            "variables": variables,
            "file_format": "JSON",
        },
    )
    req.send()
    try:
        data = eval(gzip.decompress(req.response.content).decode())
    except Exception:
        data = eval(req.response.content.decode())

    timeseries = pd.DataFrame.from_records(data)

    return timeseries


def retrieve(
    geometry: np.ndarray,
    variables: list[str],
    date_range: tuple[str, str],
    packet_store: str,
):
    # First gets field boundaries within superregion.
    tqdm.write("Getting fields")
    fields_req = ETRequest(
        request_endpoint=endpoints["gdb"]["fields"],
        key=api_key,
        request_params={"geometry": geometry.tolist()},
    )
    fields_req.send()

    fields: list = eval(gzip.decompress(fields_req.response.content).decode())

    tqdm.write(f"Got {len(fields)} fields.")
    if len(fields) > 500:
        tqdm.write("Field count over 500. Chunking requests.")
        ind = 0
        chunked = 500
        while ind < len(fields):
            end_ind = min(ind + chunked, len(fields))
            subset = fields[ind:end_ind]
            tqdm.write("Getting metadata")
            metadata = retrieve_metadata(subset)

            metadata.to_file(
                packet_store + ".metadata." + str(ind) + ".geojson", driver="GeoJSON"
            )

            tqdm.write("Getting timeseries")
            timeseries = retrieve_ts(
                field_ids=subset, variables=variables, date_range=date_range
            )
            timeseries.to_csv(
                packet_store + ".timeseries." + str(ind) + ".csv", index=False
            )
            ind = end_ind

        return

    tqdm.write("Getting metadata")
    metadata = retrieve_metadata(fields)
    tqdm.write("Getting timeseries")
    timeseries = retrieve_ts(
        field_ids=fields, variables=variables, date_range=date_range
    )

    return {"metadata": metadata, "timeseries": timeseries}


def main():
    if not api_key:
        print("Please set OPENET_API_KEY in the .env file.")
        sys.exit(1)

    region = "sjr"
    features = gpd.read_file(f"data/cv_geojson/{region}_cv.json")

    coords = shp.get_coordinates(shp.convex_hull(features.geometry)).flatten()
    retrieve(
        coords,
        variables=["et", "eto", "ndvi", "pr"],
        date_range=("2019-01-01", "2024-12-31"),
        packet_store=f"data/cv_geojson/temp/{region}",
    )

    metadata_df = gpd.GeoDataFrame()
    timeseries_df = pd.DataFrame()

    temp_folder = Path("data/cv_geojson/temp")

    for md in temp_folder.glob(f"{region}*.geojson"):
        data = gpd.read_file(md)
        metadata_df = pd.concat([data, metadata_df])
    gpd.GeoDataFrame(metadata_df).to_file(f"data/{region}_metadata.geojson")

    for md in temp_folder.glob(f"{region}.timeseries*"):
        data = pd.read_csv(md)
        timeseries_df = pd.concat([data, timeseries_df])
    timeseries_df.to_csv(f"data/{region}_timeseries.csv")

if __name__ == "__main__":
    main()
