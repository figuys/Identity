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


for srcFile in glob(r'D:\GitHub\fiGuys\Identity\src\08\Parts.parquet'):
	outFile = srcFile.replace(r'src\08', r'out\08')
	tmpFile = srcFile.replace(r'src\08', r'tmp\08')
	badFile = srcFile.replace(r'src\08', r'err\08')
	newFile = outFile.replace(r'out\08', r'src\09')

	os.makedirs(os.path.dirname(outFile), exist_ok=True)
	os.makedirs(os.path.dirname(tmpFile), exist_ok=True)
	os.makedirs(os.path.dirname(badFile), exist_ok=True)
	os.makedirs(os.path.dirname(newFile), exist_ok=True)

	print('')
	print(f'Reading file:  {srcFile}')
	lf = pl.scan_parquet(srcFile)

	print(f'Temping file: {tmpFile}')
	lft = lf.with_columns(
		[
			pl.col('Zipcode')
			.pipe(strip_chars)
			.pipe(only_digits)
			.str.slice(0, 5)
			.pipe(only_length, length=5),
		]
	)
	lft.sink_parquet(tmpFile)

	print(f'Writing file: {outFile}')
	lf = pl.scan_parquet(tmpFile)
	lfo = lf.filter(pl.col('Zipcode').is_not_null())
	lfo.sink_parquet(outFile)

	print(f'Writing file: {badFile}')
	lf = pl.scan_parquet(tmpFile)
	lfb = lf.filter(pl.col('Zipcode').is_null())
	lfb.sink_parquet(badFile)

	shutil.copy(outFile, newFile)
	shutil.rmtree(os.path.dirname(tmpFile))
