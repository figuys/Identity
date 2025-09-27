import os
import shutil
import polars as pl

from polars import Expr
from glob import glob


def only_digits(expr: Expr) -> Expr:
	return pl.when(expr.is_not_null()).then(expr.str.extract(r'\d+', 0)).otherwise(None)


def only_length(expr: Expr, *, length: int) -> Expr:
	return pl.when(expr.str.len_chars() == length).then(expr).otherwise(None)


for srcFile in glob(r'D:\GitHub\fiGuys\Identity\src\06\Parts.parquet'):
	outFile = srcFile.replace(r'src\06', r'out\06a')
	badFile = srcFile.replace(r'src\06', r'err\06a')
	newFile = outFile.replace(r'out\06a', r'src\07a')

	os.makedirs(os.path.dirname(outFile), exist_ok=True)
	os.makedirs(os.path.dirname(badFile), exist_ok=True)
	os.makedirs(os.path.dirname(newFile), exist_ok=True)

	df = (
		pl.scan_parquet(srcFile)
		.with_columns(
			[
				pl.col('SSNumber').pipe(only_digits).pipe(only_length, length=9).alias('CleanSSN'),
			]
		)
		.with_columns(
			[
				pl.col('CleanSSN').is_not_null().alias('is_valid'),
			]
		)
		.collect(streaming=True)
	)

	good_df = df.filter(pl.col('is_valid')).drop('is_valid').rename({'CleanSSN': 'SSNumber'})
	bad_df = df.filter(~pl.col('is_valid')).drop('is_valid')

	if os.path.exists(outFile):
		os.remove(outFile)
	if os.path.exists(badFile):
		os.remove(badFile)

	good_df.write_parquet(outFile)
	bad_df.write_parquet(badFile)
	shutil.copy(outFile, newFile)

# for srcFile in glob('D:\\OneLake\\src\\06\\Parts.parquet'):
# 	outFile = srcFile.replace('src\\06', 'out\\06')
# 	newFile = outFile.replace('out\\06', 'src\\07')

# 	if os.path.exists(outFile):
# 		os.remove(outFile)

# 	if os.path.exists(srcFile):
# 		print('')
# 		print(f'Reading file:  {srcFile}')
# 		lf = pl.scan_parquet(srcFile)

# 		print(f'Cleaning file: {srcFile}')

# 		# Clean Columns
# 		lf = lf.with_columns(
# 			[
# 				pl.col('SSNumber').pipe(only_digits).pipe(only_length, length=9),
# 			]
# 		).filter(
# 			[
# 				pl.col('SSNumber').is_not_null(),
# 			]
# 		)

# 		print(f'Writing file: {outFile}')
# 		lf.sink_parquet(outFile)

# 	if os.path.exists(outFile):
# 		shutil.copy(outFile, newFile)
