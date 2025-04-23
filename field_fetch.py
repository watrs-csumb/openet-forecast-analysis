import argparse
import gzip
import pathlib
from typing import Any

from dotenv import dotenv_values
from geopandas import GeoDataFrame
from pandas import DataFrame
from shapely import get_coordinates
from shapely.geometry.polygon import Polygon
from requests import post

FIELD_ID_ENDPOINT = "https://developer.openet-api.org/geodatabase/metadata/ids"
BOUNDARIES_ENDPOINT = "https://developer.openet-api.org/geodatabase/metadata/boundaries"
PROPERTIES_ENDPOINT = "https://developer.openet-api.org/geodatabase/metadata/properties"

parser = argparse.ArgumentParser()
parser.add_argument("--outer", "-i")
parser.add_argument("--asset", "-a")
parser.add_argument("--name-property", "-p")
parser.add_argument("--export-shp", "-s", action="store_true")

def get_intersecting_fields(outer: Polygon, key: str) -> GeoDataFrame:
    outer_coords = list(get_coordinates(outer).flatten())

    fields_req = post(
        url = FIELD_ID_ENDPOINT,
        headers = {"Authorization": key},
        json = {"geometry": outer_coords}
    )

    if not fields_req.ok:
        raise Exception(fields_req.json()["detail"])
    
    field_ids = eval(gzip.decompress(fields_req.content).decode())

    boundaries_req = post(
        url = BOUNDARIES_ENDPOINT,
        headers = {"Authorization": key},
        json = {"field_ids": field_ids}
    )

    if not boundaries_req.ok:
        raise Exception(boundaries_req.json()["detail"])

    boundaries = eval(gzip.decompress(boundaries_req.content).decode())

    gdf = GeoDataFrame.from_features(boundaries, columns=["field_id", "geometry"], crs="EPSG:4326")

    properties_req = post(
        url = PROPERTIES_ENDPOINT,
        headers = {"Authorization": key},
        json = {"field_ids": field_ids}
    )

    if not properties_req.ok:
        raise Exception(properties_req.json()["detail"])

    properties = eval(gzip.decompress(properties_req.content).decode())
    df = DataFrame.from_records(properties)
    
    # Get only the latest year for the crop columns.
    crop_years = [col for col in list(df.columns) if col.startswith("crop_")]
    df = df[["field_id"] + crop_years]

    # add properties to gdf
    gdf = gdf.merge(df, left_on="field_id", right_on="field_id", how="left")
    
    return gdf[["field_id"] + crop_years + ["geometry"]]
    
def ee_GIF(region_asset: str, key: str, shp: bool = False, name_prop: str = "NAME"):
    from ee import FeatureCollection, Initialize # type: ignore
    
    Initialize()
    
    asset: Any = FeatureCollection(region_asset).getInfo()
    
    for feature in asset["features"]:
        shapely_poly = Polygon(feature["geometry"]["coordinates"][0])
        
        gdf = get_intersecting_fields(shapely_poly, key)
        
        gdf.to_file(f"{feature["properties"][name_prop]}_properties.geojson", driver="GeoJSON")
        
        if shp:
            pathlib.Path.mkdir(feature["properties"][name_prop], exist_ok=True)
            
            gdf.to_file(f"{feature["properties"][name_prop]}/{feature["properties"][name_prop]}.shp", driver="ESRI Shapefile")

def main():
    args = parser.parse_args()
    key = dotenv_values(".env")["ET_KEY"]
    assert key
    
    if args.outer:
        file = pathlib.Path(args.outer)
        
        # Validate outer filename exists.
        if not file.exists():
            raise FileNotFoundError(f"File {args.outer} does not exist.")
        
        outer_border = GeoDataFrame.from_file(args.outer)
        gdf = get_intersecting_fields(outer_border.iloc[0].geometry, key)
        
        gdf.to_file(f"{file.stem}_fields.geojson", driver="GeoJSON")
    
    if args.asset:
        if not args.name_property:
            raise Exception("Name property must be specified.")
        
        ee_GIF(args.asset, key, shp=args.export_shp, name_prop=args.name_property)

if __name__ == "__main__":
    main()