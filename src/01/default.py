import shutil
import polars as pl
from glob import glob
import os

srcLetr = 'D'
srcRepo = 'GitHub'
srcComp = 'fiGuys'
srcPart = 'Identity'
srcStep = 1
srcNext = srcStep + 1
srcStepStr = f'{srcStep:02}'
srcNextStr = f'{srcNext:02}'
srcRoot = f'{srcLetr}:\\{srcRepo}\\{srcComp}\\{srcPart}\\src'
srcPath = f'{srcRoot}\\{srcStepStr}'
srcGlob = f'{srcPath}\\Part*.csv'


def flow_dataStreams(srcFile):
	outFile = adapt__srcFile_to_outFile(srcFile)
	newFile = adapt__outFile_to_newFile(outFile)

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
			shutil.copy(outFile, newFile)

	except Exception as e:
		print(f'Error combining Parquet files: {e}')


def adapt__srcFile_to_srcRoot(srcFile):
	srcRoot = os.path.split(srcFile)[0]

	if not os.path.exists(srcRoot):
		os.makedirs(srcRoot, exist_ok=True)

	return srcRoot


def adapt__srcFile_to_errFile(srcFile):
	return srcFile.replace(f'src\\{srcStepStr}', f'err\\{srcStepStr}')


def adapt__errFile_to_errRoot(errFile):
	return os.path.split(errFile)[0]


def adapt__srcFile_to_outFile(srcFile):
	return srcFile.replace(f'src\\{srcStepStr}', f'out\\{srcStepStr}').replace('.csv', '.parquet')


def adapt__outFile_to_outRoot(outFile):
	return os.path.split(outFile)[0]


def adapt__outFile_to_newFile(outFile):
	return outFile.replace(f'out\\{srcStepStr}', f'src\\{srcNextStr}')


for srcFile in glob(srcGlob):
	flow_dataStreams(srcFile)
