import os
import shutil
import polars as pl

from polars import Expr
from glob import glob


def strip_chars(expr: Expr) -> Expr:
	return expr.str.strip_chars()


def only_digits(expr: Expr) -> Expr:
	return expr.str.replace_all(r'\D', '')


def only_length(expr: Expr, *, length: int) -> Expr:
	return pl.when(expr.str.len_chars() == length).then(expr).otherwise(pl.lit(None))


for srcFile in glob(r'D:\GitHub\fiGuys\Identity\src\07\Parts.parquet'):
	outFile = srcFile.replace(r'src\07', r'out\07')
	tmpFile = srcFile.replace(r'src\07', r'tmp\07')
	badFile = srcFile.replace(r'src\07', r'err\07')
	newFile = outFile.replace(r'out\07', r'src\08')

	os.makedirs(os.path.dirname(outFile), exist_ok=True)
	os.makedirs(os.path.dirname(tmpFile), exist_ok=True)
	os.makedirs(os.path.dirname(badFile), exist_ok=True)
	os.makedirs(os.path.dirname(newFile), exist_ok=True)

	print('')
	print(f'Reading file:  {srcFile}')
	lf = pl.scan_parquet(srcFile)

	print(f'Cleaning file: {srcFile}')

	# Clean Columns
	lf = lf.with_columns(
		[
			pl.col('Telephone').pipe(strip_chars).pipe(only_digits).pipe(only_length, length=10),
		]
	)

	print(f'Writing file: {outFile}')
	lf.sink_parquet(outFile)

	shutil.copy(outFile, newFile)
	shutil.rmtree(os.path.dirname(tmpFile))
