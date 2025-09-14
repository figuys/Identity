import polars as pl
from glob import glob
import os
import shutil as sh

srcName = 'Identity'
srcStep = '01'
srcNext = '02'
srcGlobs = f'D:\\{srcName}\\src\\{srcStep}\\Part*.csv'
srcFiles = glob(srcGlobs)

for srcFile in srcFiles:
	outFile = srcFile.replace(f'src\\{srcStep}', f'out\\{srcStep}')
	newFile = outFile.replace(f'out\\{srcStep}', f'src\\{srcNext}')

	try:
		print(f'Converting {srcFile} to parquet format.')
		lf = pl.scan_csv(
			srcFile,
			ignore_errors=True,
			infer_schema=False,
		)
		lf.sink_parquet(outFile)

		if os.path.exists(outFile):
			if os.path.exists(newFile):
				os.remove(newFile)

			print(f'Staging {newFile}')
			sh.copy(outFile, newFile)

	except Exception as e:
		print(f'Error combining Parquet files: {e}')
