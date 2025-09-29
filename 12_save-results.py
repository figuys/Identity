import os
import shutil
import polars as pl

from glob import glob

for srcFile in glob('D:\\GitHub\\fiGuys\\Identity\\src\\11\\Parts.parquet'):
	outFile = srcFile.replace(r'src\11', r'out\11')
	tmpFile = srcFile.replace(r'src\11', r'tmp\11')
	badFile = srcFile.replace(r'src\11', r'err\11')
	newFile = outFile.replace(r'out\11', r'src\12')

	os.makedirs(os.path.dirname(outFile), exist_ok=True)
	os.makedirs(os.path.dirname(tmpFile), exist_ok=True)
	os.makedirs(os.path.dirname(badFile), exist_ok=True)
	os.makedirs(os.path.dirname(newFile), exist_ok=True)

	print('')
	print(f'Reading file:  {srcFile}')
	lf = pl.scan_parquet(srcFile)

	print(f'Cleaning file: {srcFile}')

	# Clean Columns
	lf = lf.filter(pl.col('FName').eq('Paul') & pl.col('LName').eq('Murphy'))

	print(f'Writing file: {outFile}')
	lf.sink_parquet(outFile)

	shutil.copy(outFile, newFile)
	shutil.rmtree(os.path.dirname(tmpFile))
