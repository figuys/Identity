import os
import shutil
import polars as pl

from polars import Expr
from glob import glob


def only_digits(expr: Expr) -> Expr:
	return pl.when(expr.is_not_null()).then(expr.str.extract(r'\d+', 0)).otherwise(None)


def only_length(expr: Expr, *, length: int) -> Expr:
	return pl.when(expr.str.len_chars() == length).then(expr).otherwise(None)


for srcFile in glob(r'D:\GitHub\fiGuys\Identity\src\10\Parts.parquet'):
	outFile = srcFile.replace(r'src\10', r'out\10')
	tmpFile = srcFile.replace(r'src\10', r'tmp\10')
	badFile = srcFile.replace(r'src\10', r'err\10')
	newFile = outFile.replace(r'out\10', r'src\11')

	os.makedirs(os.path.dirname(outFile), exist_ok=True)
	os.makedirs(os.path.dirname(tmpFile), exist_ok=True)
	os.makedirs(os.path.dirname(badFile), exist_ok=True)
	os.makedirs(os.path.dirname(newFile), exist_ok=True)

	print('')
	print(f'Reading file:  {srcFile}')
	lf = pl.scan_parquet(srcFile)

	print(f'Cleaning file: {srcFile}')

	# Clean Columns
	lft = lf.with_columns(
		[
			pl.col('FName').str.strip_chars().str.to_titlecase(),
			pl.col('LName').str.strip_chars().str.to_titlecase(),
			pl.col('MName').str.strip_chars().str.to_titlecase(),
			pl.col('SName').str.strip_chars().str.to_uppercase(),
		]
	)
	lft.sink_parquet(tmpFile)

	print(f'Writing file: {outFile}')
	lf = pl.scan_parquet(tmpFile)
	lfo = lf.filter([pl.col('FName').is_not_null() & pl.col('LName').is_not_null()])
	lfo.sink_parquet(outFile)

	print(f'Writing file: {badFile}')
	lf = pl.scan_parquet(tmpFile)
	lfb = lf.filter([pl.col('FName').is_null() | pl.col('LName').is_null()])
	lfb.sink_parquet(badFile)

	shutil.copy(outFile, newFile)
	shutil.rmtree(os.path.dirname(tmpFile))
