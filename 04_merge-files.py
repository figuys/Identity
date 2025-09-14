import polars as pl
from glob import glob
import os

srcGlobs = 'D:\\OneLake\\src\\04\\Part1_0*.parquet'
srcFiles = glob(srcGlobs)
outFile = 'D:\\OneLake\\out\\04\\Part1.parquet'

try:
	lf = pl.concat([pl.scan_parquet(f) for f in srcFiles])
	lf.sink_parquet(outFile)

except Exception as e:
	print(f'Error combining Parquet files: {e}')
