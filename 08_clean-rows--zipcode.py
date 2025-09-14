import os
import shutil
import polars as pl

from polars import Expr
from glob import glob

sAbbr = sorted(
	[
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
)


# Remove non-digit characters
def only_digits(expr: Expr) -> Expr:
	return pl.when(expr.is_not_null()).then(expr.str.extract(r'\d+', 0)).otherwise(None)


def only_length(expr: Expr, *, length: int) -> Expr:
	return pl.when(expr.str.len_chars() == length).then(expr).otherwise(None)


for srcFile in glob('D:\\OneLake\\src\\08\\Parts.parquet'):
	outFile = srcFile.replace('src\\08', 'out\\08')
	newFile = outFile.replace('out\\08', 'src\\09')

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
				# pl.col('FName').str.strip_chars().str.to_titlecase(),
				# pl.col('LName').str.strip_chars().str.to_titlecase(),
				# pl.col('MName').str.strip_chars().str.to_titlecase(),
				# pl.col('SName').str.strip_chars().str.to_uppercase(),
				# pl.col('Address').str.strip_chars().str.to_uppercase(),
				# pl.col('City').str.strip_chars().str.to_titlecase(),
				# pl.col('County').str.strip_chars().str.to_titlecase(),
				# pl.col('State').str.strip_chars().str.to_uppercase(),
				pl.col('Zipcode').pipe(only_digits).str.slice(0, 5).pipe(only_length, length=5),
				# pl.col('Alt1Name').str.strip_chars().str.to_titlecase(),
				# pl.col('Alt2Name').str.strip_chars().str.to_titlecase(),
				# pl.col('Alt3Name').str.strip_chars().str.to_titlecase(),
			]
		).filter(
			[
				# pl.col('Telephone').is_null() | pl.col('Telephone').str.len_chars().eq(10),
				# pl.col('LName').is_not_null(),
				# pl.col('State').is_in(sAbbr),
				pl.col('Zipcode').is_not_null(),
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
