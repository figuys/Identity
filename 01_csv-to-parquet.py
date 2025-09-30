import os
import sys
from glob import glob

import polars as pl

# Add the 'lib' directory to the system path to import PathManager
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'lib')))
from pipeline_utils import process_file


def convert_to_parquet(lf: pl.LazyFrame) -> pl.LazyFrame:
	"""A simple pass-through function as conversion is handled by the sink."""
	# No transformation needed, the sink handles the format change.
	return lf


def main():
	"""Main processing loop for step 01."""
	for src_file in glob(r'D:\GitHub\fiGuys\Identity\src\01\Part*.csv'):
		process_file(src_file, convert_to_parquet, source_reader=pl.scan_csv)


if __name__ == '__main__':
	main()
