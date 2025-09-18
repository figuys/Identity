import polars as pl
from glob import glob

srcGlobs = 'D:\\GitHub\\fiGuys\\Identity\\src\\04\\Part1_*.parquet'
srcFiles = glob(srcGlobs)
outFile = 'D:\\GitHub\\fiGuys\\Identity\\out\\04\\Part1.parquet'

try:
	lf = pl.concat([pl.scan_parquet(f) for f in srcFiles])
	lf.sink_parquet(outFile)

except Exception as e:
	print(f'Error combining Parquet files: {e}')
