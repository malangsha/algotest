#!/usr/bin/env python3
"""
read_dump_parquet.py

A simple utility to read a Parquet file and print its contents.
Usage:
    python read_dump_parquet.py <parquet_file> [--output-csv <csv_file>]
"""
import sys
import pandas as pd

def main():
    if len(sys.argv) < 2:
        print("Usage: python read_dump_parquet.py <parquet_file> [--output-csv <csv_file>]")
        sys.exit(1)

    file_path = sys.argv[1]
    try:
        df = pd.read_parquet(file_path)
    except Exception as e:
        print(f"Error reading Parquet file: {e}")
        sys.exit(1)

    # Print the DataFrame
    print(df.to_string(index=False))

    # Optional: dump to CSV if --output-csv is provided
    if "--output-csv" in sys.argv:
        idx = sys.argv.index("--output-csv")
        if idx + 1 < len(sys.argv):
            csv_path = sys.argv[idx + 1]
            df.to_csv(csv_path, index=False)
            print(f"Data dumped to CSV: {csv_path}")
        else:
            print("Error: --output-csv flag provided but no file path given.")

if __name__ == "__main__":
    main()

