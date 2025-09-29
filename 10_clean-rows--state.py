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


for srcFile in glob(r'D:\GitHub\fiGuys\Identity\src\09\Parts.parquet'):
	outFile = srcFile.replace(r'src\09', r'out\09')
	tmpFile = srcFile.replace(r'src\09', r'tmp\09')
	badFile = srcFile.replace(r'src\09', r'err\09')
	newFile = outFile.replace(r'out\09', r'src\10')

	os.makedirs(os.path.dirname(outFile), exist_ok=True)
	os.makedirs(os.path.dirname(tmpFile), exist_ok=True)
	os.makedirs(os.path.dirname(badFile), exist_ok=True)
	os.makedirs(os.path.dirname(newFile), exist_ok=True)

	print('')
	print(f'Reading file:  {srcFile}')
	lf = pl.scan_parquet(srcFile)

	print(f'Temping file: {tmpFile}')
	lft = lf.with_columns(
		[pl.col('State').pipe(strip_chars).pipe(only_length, length=2).str.to_uppercase()]
	)
	lft.sink_parquet(tmpFile)

	print(f'Writing file: {outFile}')
	lf = pl.scan_parquet(tmpFile)
	lfo = lf.filter(
		pl.col('State').eq('AK')
		| pl.col('State').eq('AL')
		| pl.col('State').eq('AR')
		| pl.col('State').eq('AZ')
		| pl.col('State').eq('CA')
		| pl.col('State').eq('CO')
		| pl.col('State').eq('CT')
		| pl.col('State').eq('DE')
		| pl.col('State').eq('FL')
		| pl.col('State').eq('GA')
		| pl.col('State').eq('HI')
		| pl.col('State').eq('IA')
		| pl.col('State').eq('ID')
		| pl.col('State').eq('IL')
		| pl.col('State').eq('IN')
		| pl.col('State').eq('KS')
		| pl.col('State').eq('KY')
		| pl.col('State').eq('LA')
		| pl.col('State').eq('MA')
		| pl.col('State').eq('MD')
		| pl.col('State').eq('ME')
		| pl.col('State').eq('MI')
		| pl.col('State').eq('MN')
		| pl.col('State').eq('MO')
		| pl.col('State').eq('MS')
		| pl.col('State').eq('MT')
		| pl.col('State').eq('NC')
		| pl.col('State').eq('ND')
		| pl.col('State').eq('NE')
		| pl.col('State').eq('NH')
		| pl.col('State').eq('NJ')
		| pl.col('State').eq('NM')
		| pl.col('State').eq('NV')
		| pl.col('State').eq('NY')
		| pl.col('State').eq('OH')
		| pl.col('State').eq('OK')
		| pl.col('State').eq('OR')
		| pl.col('State').eq('PA')
		| pl.col('State').eq('RI')
		| pl.col('State').eq('SC')
		| pl.col('State').eq('SD')
		| pl.col('State').eq('TN')
		| pl.col('State').eq('TX')
		| pl.col('State').eq('UT')
		| pl.col('State').eq('VA')
		| pl.col('State').eq('VT')
		| pl.col('State').eq('WA')
		| pl.col('State').eq('WI')
		| pl.col('State').eq('WV')
		| pl.col('State').eq('WY')
		| pl.col('State').eq('DC')
		| pl.col('State').eq('AA')
		| pl.col('State').eq('AE')
		| pl.col('State').eq('AP')
	)
	lfo.sink_parquet(outFile)

	print(f'Writing file: {badFile}')
	lf = pl.scan_parquet(tmpFile)
	lfb = lf.filter(
		[
			pl.col('State').ne('AK'),
			pl.col('State').ne('AL'),
			pl.col('State').ne('AR'),
			pl.col('State').ne('AZ'),
			pl.col('State').ne('CA'),
			pl.col('State').ne('CO'),
			pl.col('State').ne('CT'),
			pl.col('State').ne('DE'),
			pl.col('State').ne('FL'),
			pl.col('State').ne('GA'),
			pl.col('State').ne('HI'),
			pl.col('State').ne('IA'),
			pl.col('State').ne('ID'),
			pl.col('State').ne('IL'),
			pl.col('State').ne('IN'),
			pl.col('State').ne('KS'),
			pl.col('State').ne('KY'),
			pl.col('State').ne('LA'),
			pl.col('State').ne('MA'),
			pl.col('State').ne('MD'),
			pl.col('State').ne('ME'),
			pl.col('State').ne('MI'),
			pl.col('State').ne('MN'),
			pl.col('State').ne('MO'),
			pl.col('State').ne('MS'),
			pl.col('State').ne('MT'),
			pl.col('State').ne('NC'),
			pl.col('State').ne('ND'),
			pl.col('State').ne('NE'),
			pl.col('State').ne('NH'),
			pl.col('State').ne('NJ'),
			pl.col('State').ne('NM'),
			pl.col('State').ne('NV'),
			pl.col('State').ne('NY'),
			pl.col('State').ne('OH'),
			pl.col('State').ne('OK'),
			pl.col('State').ne('OR'),
			pl.col('State').ne('PA'),
			pl.col('State').ne('RI'),
			pl.col('State').ne('SC'),
			pl.col('State').ne('SD'),
			pl.col('State').ne('TN'),
			pl.col('State').ne('TX'),
			pl.col('State').ne('UT'),
			pl.col('State').ne('VA'),
			pl.col('State').ne('VT'),
			pl.col('State').ne('WA'),
			pl.col('State').ne('WI'),
			pl.col('State').ne('WV'),
			pl.col('State').ne('WY'),
			pl.col('State').ne('DC'),
			pl.col('State').ne('AA'),
			pl.col('State').ne('AE'),
			pl.col('State').ne('AP'),
		]
	)
	lfb.sink_parquet(badFile)

	shutil.copy(outFile, newFile)
	shutil.rmtree(os.path.dirname(tmpFile))
