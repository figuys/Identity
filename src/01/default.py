import polars as pl
from glob import glob
import os
import shutil as sh

srcLetr = 'D'
srcRepo = 'GitHub'
srcComp = 'fiGuys'
srcName = 'Identity'
srcStep = f'{1:02}'
srcNext = f'{int(srcStep + 1):02}'
srcRoot = f'{srcLetr}:\\{srcRepo}\\{srcComp}\\{srcName}\\src'
srcGlobs = f'{srcRoot}\\{srcStep}\\Part*.csv'
srcFiles = glob(srcGlobs)


def flow_dataStreams(srcFile, outFile, newFile):
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


for srcFile in srcFiles:
	outFile = srcFile.replace(f'src\\{srcStep}', f'out\\{srcStep}')
	newFile = outFile.replace(f'out\\{srcStep}', f'src\\{srcNext}')

	flow_dataStreams(srcFile, outFile, newFile)
