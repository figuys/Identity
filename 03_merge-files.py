import polars as pl
from glob import glob
import os

srcGlobs = 'D:\\GitHub\\fiGuys\\Identity\\src\\03\\Part1_0*.parquet'
srcFiles = glob(srcGlobs)
outFile = 'D:\\GitHub\\fiGuys\\Identity\\out\\03\\Part1_0.parquet'

try:
	lf = pl.concat([pl.scan_parquet(f) for f in srcFiles])
	lf.sink_parquet(outFile)

except Exception as e:
	print(f'Error combining Parquet files: {e}')


srcGlobs = 'D:\\GitHub\\fiGuys\\Identity\\src\\03\\Part1_1*.parquet'
srcFiles = glob(srcGlobs)
outFile = 'D:\\GitHub\\fiGuys\\Identity\\out\\03\\Part1_1.parquet'

try:
	lf = pl.concat([pl.scan_parquet(f) for f in srcFiles])
	lf.sink_parquet(outFile)

except Exception as e:
	print(f'Error combining Parquet files: {e}')


srcGlobs = 'D:\\GitHub\\fiGuys\\Identity\\src\\03\\Part2_0*.parquet'
srcFiles = glob(srcGlobs)
outFile = 'D:\\GitHub\\fiGuys\\Identity\\out\\03\\Part2_0.parquet'

try:
	lf = pl.concat([pl.scan_parquet(f) for f in srcFiles])
	lf.sink_parquet(outFile)

except Exception as e:
	print(f'Error combining Parquet files: {e}')
