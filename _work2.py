import polars as pl
from glob import glob
import os

srcGlobs = 'D:\\OneLake\\src\\03\\Part1_00*.parquet'
srcFiles = glob(srcGlobs)
outFile = 'D:\\OneLake\\out\\03\\Part1_00.parquet'

try:
	lf = pl.concat([pl.scan_parquet(f) for f in srcFiles])

	lf.sink_parquet(outFile)

	for srcFile in srcFiles:
		if os.path.exists(srcFile):
			doneFile = srcFile.replace('source', 'done')

			if os.path.exists(doneFile):
				os.remove(doneFile)

			os.rename(srcFile, doneFile)

except Exception as e:
	print(f'Error combining Parquet files: {e}')


srcGlobs = 'D:\\OneLake\\src\\03\\Part1_01*.parquet'
srcFiles = glob(srcGlobs)
outFile = 'D:\\OneLake\\out\\03\\Part1_01.parquet'

try:
	lf = pl.concat([pl.scan_parquet(f) for f in srcFiles])

	lf.sink_parquet(outFile)

	for srcFile in srcFiles:
		if os.path.exists(srcFile):
			doneFile = srcFile.replace('source', 'done')

			if os.path.exists(doneFile):
				os.remove(doneFile)

			os.rename(srcFile, doneFile)

except Exception as e:
	print(f'Error combining Parquet files: {e}')


srcGlobs = 'D:\\OneLake\\src\\03\\Part2_00*.parquet'
srcFiles = glob(srcGlobs)
outFile = 'D:\\OneLake\\out\\03\\Part2_00.parquet'

try:
	lf = pl.concat([pl.scan_parquet(f) for f in srcFiles])

	lf.sink_parquet(outFile)

	for srcFile in srcFiles:
		if os.path.exists(srcFile):
			doneFile = srcFile.replace('source', 'done')

			if os.path.exists(doneFile):
				os.remove(doneFile)

			os.rename(srcFile, doneFile)

except Exception as e:
	print(f'Error combining Parquet files: {e}')
