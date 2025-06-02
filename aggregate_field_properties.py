import pandas as pd
from requests import post

import argparse
import gzip
import pathlib

parser = argparse.ArgumentParser()
parser.add_argument(
    "--fields",
    "-f",
    required=True,
    help="Path to file containing fields. First column will be retrieved from.",
)
parser.add_argument("--key", "-k", required=True, help="OpenET API Key.")
parser.add_argument("--output", "-o", nargs="?", help="Output directory.")

properties_endpoint = "https://developer.openet-api.org/geodatabase/metadata/properties"


def main():
    args = parser.parse_args()
    fn = args.fields
    key = args.key
    output = args.output or "./"

    if not pathlib.Path(fn).exists():
        raise FileNotFoundError(f"File {fn} could not be found.")

    if not pathlib.Path(output).is_dir():
        raise OSError(f"Directory {output} could not be found.")

    fields = pd.read_csv(fn, index_col=0)
    fips = pd.read_csv("https://raw.githubusercontent.com/watrs-csumb/openet-forecast-analysis/refs/heads/main/data/fips_lookup.csv", dtype=str)
    
    def append_fips_code(df: pd.Series):
        fips_code = fips[fips["abbr"] == df.name.split("_")[0]]["fips"].values[0]
        normalized_name = fips_code + df.name.split("_")[1]
        return normalized_name
    
    fields = fields.apply(append_fips_code, axis=1)
    
    req = post(
        properties_endpoint, headers={"Authorization": key}, json={"field_ids": fields.values.tolist()}
    )
    
    if not req.ok:
        raise Exception(f"Something went wrong.\n{req.text}")
    
    res = eval(gzip.decompress(req.content).decode())
    
    data = pd.DataFrame.from_records(res)
    
    def append_abbr(df: pd.Series):
        abbr = fips[fips["fips"] == df["field_id"][:2]]["abbr"].values[0]
        df["field_id"] = abbr + "_" + df["field_id"][2:]
        return df
        
    data = data.apply(append_abbr, axis=1)
    
    data.to_csv(f"{output}{fn.split(".csv")[0]}_properties.csv", index=False)


if __name__ == "__main__":
    main()
