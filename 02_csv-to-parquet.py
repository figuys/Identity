import os
import shutil
import polars as pl

from glob import glob

for srcFile in glob('D:\\GitHub\\fiGuys\\Identity\\src\\02\\Part*.csv'):
	outFile = srcFile.replace(r'src\02', r'out\02').replace('.csv', '.parquet')
	tmpFile = srcFile.replace(r'src\02', r'tmp\02').replace('.csv', '.parquet')
	badFile = srcFile.replace(r'src\02', r'err\02').replace('.csv', '.parquet')
	newFile = outFile.replace(r'out\02', r'src\03')

	os.makedirs(os.path.dirname(outFile), exist_ok=True)
	os.makedirs(os.path.dirname(tmpFile), exist_ok=True)
	os.makedirs(os.path.dirname(badFile), exist_ok=True)
	os.makedirs(os.path.dirname(newFile), exist_ok=True)

	print('')
	print(f'Reading file:  {srcFile}')
	lf = pl.scan_csv(srcFile, ignore_errors=True, infer_schema=False)

	print(f'Temping file:  {tmpFile}')
	lf.sink_parquet(tmpFile)

	print(f'Saveing file:  {outFile}')
	shutil.move(tmpFile, outFile)

	print(f'Staging file: {newFile}')
	shutil.copy(outFile, newFile)

	shutil.rmtree(os.path.dirname(tmpFile))
