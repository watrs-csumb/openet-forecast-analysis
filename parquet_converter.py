import argparse
import os
import pathlib

import pandas as pd

parser = argparse.ArgumentParser(description="Convert CSV files to Parquet format.")
parser.add_argument("-o", "--output", type=str, default="output", help="Output Parquet directory.")
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument("-f", "--file", type=str, help="Path to the CSV file to convert.")
group.add_argument("-d", "--directory", type=str, help="Directory containing CSV files to convert.")

def main():
    args = parser.parse_args()

    if args.file:
        args.file = pathlib.Path(args.file)
        # Convert a single CSV file to Parquet
        df = pd.read_csv(args.file)
        output_file = str(args.file.stem) + ".parquet"
        df.to_parquet(output_file)
        print(f"Converted {args.file} to {output_file}")

    elif args.directory:
        # Convert all CSV files in a directory to Parquet
        for filename in os.listdir(args.directory):
            if filename.endswith('.csv'):
                csv_file = os.path.join(args.directory, filename)
                df = pd.read_csv(csv_file)
                output_file = os.path.join(args.output, f"{os.path.splitext(filename)[0]}.parquet")
                df.to_parquet(output_file)
                print(f"Converted {csv_file} to {output_file}")
    else:
        print("Please provide either a file or a directory to convert.")

if __name__ == "__main__":
    main()
