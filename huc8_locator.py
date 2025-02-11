import argparse
import gzip
import logging
import pathlib
import sys
import time

from typing import Any, Optional, List, Union, Tuple

if sys.version_info < (3, 8):
    print("Python 3.8+ is supported. Please update to use this script.")
    sys.exit(1)

OPTIONAL = "?"
KCP_NULL = -9999

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
parser.add_argument("-huc8", "--huc8", nargs=1, type=str, required=True, help="HUC8 Code")
parser.add_argument("-y", "--year", nargs=OPTIONAL, default="2022", help="Year of Reference. Default, 2022")
parser.add_argument("-d", "--dest", nargs=OPTIONAL, default=None, help="Directory or file prefix for output. Defaults to huc8 code")
parser.add_argument("-t", "--top", nargs=OPTIONAL, default=0, type=int, help="Number of top crops to fetch for each watershed. To include all, enter 0. Default, 0")
parser.add_argument("-p", "--peak", nargs=2, default=(4,8), type=int, help="Start and end months of peak season. Default, (4,8)")
parser.add_argument("-k", "--key", required=True, help="OpenET API Key")

group = parser.add_mutually_exclusive_group()
group.add_argument("-e", "--exclude", nargs='*', default=[], help="List of USDS CDL codes to exclude for EToF maxes")
group.add_argument("-i", "--include", nargs='*', default=[], help="List of USDS CDL codes to search for EToF maxes")

# Graphical
parser.add_argument("--box", action="store_true", help="Save boxplots for EToF")

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
        req = requests.post(timeout=260, **kwargs)
        
        if req.status_code != 200:
            print(f"Error {req.status_code}: {req.text}")
            sys.exit(1)
            return
        
        return req
    except KeyboardInterrupt:
        sys.exit(1)

def get_huc8_metadata(huc8_id: str, api_key: str) -> Tuple[pd.DataFrame, Any]:
    # Filter huc8 IDs to return element matching ID provided.
    elements: Collection = dataset.filter(Filter.eq("huc8", huc8_id))
    
    # Localize.
    info = elements.getInfo()
    
    if info is None:
        print("Unexpected error from Earth Engine. Please try again.")
        sys.exit(1)
    
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
        print("Unexpected error during field fetching. Please try again.")
        print("Use the request query for reference: \n\turl: ", endpoints["fieldId"], "\n\tgeometry: ", boundaries)
        sys.exit(1)
    
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
        print("Unexpected error during metadata retrieval. Please try again.")
        print("Use the request query for reference: \n\turl: ", endpoints["fieldProps"], "\n\tfield_ids: ", field_Ids)
        sys.exit(1)
    
    # Check if request was successful.
    if metadata_res.status_code != 200:
        print(f"Error {metadata_res.status_code}: {metadata_res.text}")
        sys.exit(1)
        return
    
    # Unzips data. List of Dicts["field_id", "hectares", "crop_2016", ..., "crop_2022", ...]
    metadata = eval(gzip.decompress(metadata_res.content).decode())
    
    # Convert to pandas dataframe.
    df = pd.DataFrame.from_records(metadata)
    
    return df, info

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
    
    huc8Id = args.huc8[0]
    year = args.year
    filename = args.dest or huc8Id
    api_key = args.key
    n_crops = args.top
    s_peak, e_peak = args.peak
    
    crop_excludes = args.exclude
    crop_includes = args.include
    
    # Graphical flags.
    boxplot = args.box
    
    if boxplot:
        try:
            import seaborn as sns
            import matplotlib.pyplot as plt
        except ImportError:
            print("To generate boxplot, install seaborn using `pip install seaborn`.")
            sys.exit(1)
    
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
    metadata, ee_info = get_huc8_metadata(huc8Id, api_key=api_key)
    
    # Check if metadata contains year provided.
    if crop_col not in metadata.columns:
        print(f"{year} is not present in field metadata.")
        return
    
    metadata_loc = f"{filename}_metadata_{year}.csv"
    metadata.to_csv(metadata_loc)
    print(f"Exported metadata to {metadata_loc}")
    
    if len(crop_excludes) > 0:
        # Goes through list of crops to exclude. If empty, does nothing.
        metadata = metadata[~metadata[crop_col].isin(crop_excludes)]
    
    if len(crop_includes) > 0:
        # Modify metadata to only include crops in allowlist.
        metadata = metadata[metadata[crop_col].isin(crop_includes)]
    
    if n_crops > 0:
        print(f"Trimming fields for top {n_crops} crops...")
        # Grabs the top crops for the given year.
        top_crops = metadata[crop_col].value_counts(ascending=False)[:n_crops].index.to_list()
        
        metadata = metadata[metadata[crop_col].isin(top_crops)]

    # Discard other crop years.
    metadata = metadata[["field_id", crop_col, "hectares"]]
    
    start = time.perf_counter()
    data = get_timeseries_data(metadata["field_id"].astype(str).tolist(), api_key=api_key, year=year)
    stop = time.perf_counter()
    
    if data is None:
        print("Failed to get timeseries data.")
        return None
    
    # Filter data for peak season.
    data["time"] = pd.to_datetime(data["time"])
    data = data[(data["time"].dt.month >= s_peak) & (data["time"].dt.month <= e_peak)]
    
    # Join metadata columns.
    data = data.set_index("field_id").join(metadata.set_index("field_id"), on="field_id", how="left", validate="many_to_one").reset_index()
    
    data.to_csv(f"{filename}_values_{year}.csv", index=False)
    
    print(f"{metadata['field_id'].agg('count')} fields took {round((stop-start), 2)} seconds.")
    
    # Calculate max EToF for each crop.
    crops_max = data.groupby(['field_id', crop_col, "collection"])["value_mm"].agg('max')
    crops_max.round(2).reset_index().to_csv(f"{filename}_etof_maxes_{year}.csv", index=False)
    
    if boxplot:
        print("Generating boxplot...")
        # Import CDL lookup table.
        cdl_lookup = pd.read_csv(
            "https://media.githubusercontent.com/media/aetriusgx/openet/refs/heads/main/data/cdl_codes.csv",
            index_col="Codes"
        )["Class_Names"]
        kcp_lookup = pd.read_csv(
            "https://raw.githubusercontent.com/aetriusgx/openet/refs/heads/main/data/1993%20NEH%20Kcp.csv",
            index_col="cdl_code", na_values={'kcp': KCP_NULL} # type: ignore
        )[['kcp']]
        
        ee_props = ee_info["features"][0]["properties"]
        # Watershed Name.
        watershed = f'{ee_props["name"]}, {ee_props["states"]} ({huc8Id})'
        
        # Join CDL lookup table.
        data_plotter = crops_max.reset_index().join(cdl_lookup, on=crop_col)
        
        # Join KCP lookup table.
        data_plotter = data_plotter.join(kcp_lookup, on=crop_col)
        
        # Filter out crops with null kcp values.
        data_plotter = data_plotter[~data_plotter["kcp"].isnull()]
        
        # Calculate n_wrap so that facet grid remains square-like.
        n_wrap = int(np.ceil(np.sqrt(data_plotter["Class_Names"].nunique())))
        
        with sns.axes_style("darkgrid"): # type: ignore
            boxplot = sns.catplot(  # type: ignore
                data=data_plotter.reset_index(),
                kind="box",
                x="collection",
                y="value_mm",
                col="Class_Names",
                col_wrap=n_wrap,
                estimator="median",
                errorbar=("pi", 50),
                sharex=False,
                showfliers=False,
                width=0.2,
                formatter=lambda x: x.split("_")[0].capitalize(),
            )

            boxplot.set_titles(col_template="{col_name}")
            boxplot.despine(left=True)
            boxplot.tick_params(axis="x", rotation=90)
            boxplot.set_ylabels("EToF$_{max}$ | Kc$_{MAX}$")
            boxplot.set_xlabels("Model")
            
            for (row, col, hue), data_rch in boxplot.facet_data():
                try:
                    # Get kcp value.
                    kcp_val = data_rch["kcp"].unique()[0]
                    
                    ax = boxplot.facet_axis(row, col)
                    
                    ax.axhline(y=kcp_val, linestyle="dotted")
                except Exception:
                    print(f"Failed to plot KCP line. Crop code {data_rch[crop_col].unique()[0]} likely undocumented.")
            
            plt.tight_layout() # type: ignore
            
            plt.suptitle(f"{watershed} (n={data_plotter.reset_index()['field_id'].nunique()})", y=1.02) # type: ignore
            
            fig_name = f"Boxplot Grid for Interquartile Ranges of EToF_max by Crop and Model for {watershed} ({s_peak}-{e_peak}, {year}).png"
            
            boxplot.savefig(f"{filename} {fig_name}")
            print(f"Exported boxplot to {fig_name}")
        
    
    return

if __name__ == "__main__":
    main()
