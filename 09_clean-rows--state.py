import os
import shutil
import polars as pl

from polars import Expr
from glob import glob

sAbbr = [
	# 50 U.S. states
	'AK',
	'AL',
	'AR',
	'AZ',
	'CA',
	'CO',
	'CT',
	'DE',
	'FL',
	'GA',
	'HI',
	'IA',
	'ID',
	'IL',
	'IN',
	'KS',
	'KY',
	'LA',
	'MA',
	'MD',
	'ME',
	'MI',
	'MN',
	'MO',
	'MS',
	'MT',
	'NC',
	'ND',
	'NE',
	'NH',
	'NJ',
	'NM',
	'NV',
	'NY',
	'OH',
	'OK',
	'OR',
	'PA',
	'RI',
	'SC',
	'SD',
	'TN',
	'TX',
	'UT',
	'VA',
	'VT',
	'WA',
	'WI',
	'WV',
	'WY',
	# Federal District
	'DC',
	# Military "states"
	'AA',  # Armed Forces Americas (except Canada)
	'AE',  # Armed Forces Europe, Middle East, Africa, Canada
	'AP',  # Armed Forces Pacific
]


# Remove non-digit characters
def only_digits(expr: Expr) -> Expr:
	return pl.when(expr.is_not_null()).then(expr.str.extract(r'\d+', 0)).otherwise(None)


def only_length(expr: Expr, *, length: int) -> Expr:
	return pl.when(expr.str.len_chars() == length).then(expr).otherwise(None)


for srcFile in glob('D:\\GitHub\\fiGuys\\Identity\\src\\09\\Parts.parquet'):
	outFile = srcFile.replace('src\\09', 'out\\09')
	newFile = outFile.replace('out\\09', 'src\\10')

	if os.path.exists(outFile):
		os.remove(outFile)

	if os.path.exists(srcFile):
		print('')
		print(f'Reading file:  {srcFile}')
		lf = pl.scan_parquet(srcFile)

		print(f'Cleaning file: {srcFile}')

		# Clean Columns
		lf = lf.with_columns(
			[
				pl.col('State').str.strip_chars().str.to_uppercase(),
			]
		).filter(
			[
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
				# pl.col('Zipcode').is_not_null(),
			]
		)

		print(f'Writing file: {outFile}')
		# print(
		# 	lf.explain(
		# 		format='plain',
		# 		optimized=True,
		# 	)
		# )
		lf.sink_parquet(outFile)

	if os.path.exists(outFile):
		shutil.copy(outFile, newFile)
