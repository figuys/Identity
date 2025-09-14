import os
import shutil
import polars as pl

from polars import LazyFrame
from polars import String, UInt32, Date, Boolean


from glob import glob

dList = ['01', '02', '03', '04', '05', '06', '07', '08', '09'] + [str(i) for i in range(10, 32)]
mList = ['01', '02', '03', '04', '05', '06', '07', '08', '09'] + [str(i) for i in range(10, 13)]


def convert_ToDate(date: String) -> String:
	y = date[:4]
	m = date[4:6]
	d = date[6:8]

	if m not in mList:
		m = '01'

	if d not in dList:
		d = '01'

	return f'{y}-{m}-{d}'


def castTo_UInt32(lf: LazyFrame, name: String, required: Boolean) -> LazyFrame:
	print(f'   # Change column type to UInt32 for column: {name}, required {required}')
	if required:
		lf = lf.filter(pl.col(name).is_not_null())

	lf = lf.with_columns(pl.col(name).cast(UInt32, Strict=required))

	return lf


def castTo_String(lf: LazyFrame, name: String, required: Boolean) -> LazyFrame:
	print(f'   # Change column type to String for column: {name}, required {required}')
	if required:
		lf = lf.filter(pl.col(name).is_not_null())

	lf = lf.with_columns(pl.col(name).cast(UInt32, Strict=required))

	return lf


def castTo_Date(lf: LazyFrame, name: String, required: Boolean) -> LazyFrame:
	print(f'   # Change column type to Date for column: {name}, required {required}')
	if required:
		lf = lf.filter(pl.col(name).is_not_null())

	lf = lf.with_columns(pl.col(name).map_elements(convert_ToDate, return_dtype=String).alias(name))
	lf = lf.with_columns(pl.col(name).cast(Date, strict=required).alias(name))

	return lf


for srcFile in glob('D:\\OneLake\\src\\02\\Part*.parquet'):
	outFile = srcFile.replace('src\\02', 'out\\02')
	newFile = outFile.replace('out\\02', 'src\\03')

	if os.path.exists(outFile) and os.path.exists(srcFile):
		os.remove(outFile)

	if os.path.exists(srcFile):
		print('')
		print(f'Reading file:  {srcFile}')
		lf = pl.scan_parquet(srcFile)

		print(f'Cleaning file: {srcFile}')

		# Correct RowId
		lf = castTo_UInt32(lf, 'RowId', True)
		# Correct SSN
		lf = castTo_String(lf, 'SSNumber', True)
		# Correct Dates
		lf = castTo_Date(lf, 'BDate', False)
		lf = castTo_Date(lf, 'StartDay', False)
		lf = castTo_Date(lf, 'Alt1Dob', False)
		lf = castTo_Date(lf, 'Alt2Dob', False)
		lf = castTo_Date(lf, 'Alt3Dob', False)

		print(f'Writing file: {outFile}')
		lf.sink_parquet(outFile)

	if os.path.exists(outFile):
		shutil.copy(outFile, newFile)
