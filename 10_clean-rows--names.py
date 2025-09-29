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
sAdam = sorted(
	[
		'33012',
		'33015',
		'33186',
		'33157',
		'33033',
		'33027',
		'33178',
		'33142',
		'33177',
		'33032',
		'33161',
		'33165',
		'33196',
		'33125',
		'33176',
		'33018',
		'33175',
		'33193',
		'33126',
		'33016',
		'33179',
		'33147',
		'33162',
		'33155',
		'33169',
		'33010',
		'33160',
		'33172',
		'33014',
		'33134',
		'33055',
		'33030',
		'33056',
		'33139',
		'33183',
		'33135',
		'33141',
		'33180',
		'33121',
		'33174',
		'33013',
		'33130',
		'33150',
		'33133',
		'33143',
		'33156',
		'33173',
		'33127',
		'33145',
		'33054',
		'33144',
		'33138',
		'33185',
		'33166',
		'33167',
		'33189',
		'33168',
		'33140',
		'33184',
		'33131',
		'33137',
		'33181',
		'33034',
		'33187',
		'33136',
		'33154',
		'33146',
		'33149',
		'33035',
		'33129',
		'33182',
		'33170',
		'33190',
		'33132',
		'33037',
		'33128',
		'33110',
		'33031',
		'33194',
		'33158',
		'34141',
		'33122',
		'33148',
		'33199',
		'33039',
		'33109',
		'33191',
		'33107',
		'33159',
		'33256',
		'33002',
		'33011',
		'33164',
		'33163',
		'33188',
		'33192',
		'33197',
		'33233',
		'33231',
		'33238',
		'33234',
		'33242',
		'33239',
		'33245',
		'33247',
		'33257',
		'33261',
		'33269',
		'33266',
		'33283',
		'33280',
		'33299',
		'33296',
		'33090',
		'33092',
		'33101',
		'33102',
		'33112',
		'33111',
		'33116',
		'33114',
		'33119',
		'33124',
		'33152',
		'33151',
		'33153',
		'33206',
		'33198',
		'33195',
		'33017',
		'33222',
		'33106',
		'33243',
		'33255',
		'33265',
	]
)


# Remove non-digit characters
def only_digits(expr: Expr) -> Expr:
	return pl.when(expr.is_not_null()).then(expr.str.extract(r'\d+', 0)).otherwise(None)


def only_length(expr: Expr, *, length: int) -> Expr:
	return pl.when(expr.str.len_chars() == length).then(expr).otherwise(None)


for srcFile in glob('D:\\GitHub\\fiGuys\\Identity\\src\\10\\Parts.parquet'):
	outFile = srcFile.replace('src\\10', 'out\\10')
	newFile = outFile.replace('out\\10', 'src\\11')

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
				pl.col('Zipcode').is_in(sAdam).alias('Zipcode'),
				# pl.col('Alt1Name').str.strip_chars().str.to_titlecase(),
				# pl.col('Alt2Name').str.strip_chars().str.to_titlecase(),
				# pl.col('Alt3Name').str.strip_chars().str.to_titlecase(),
			]
		).filter(
			[
				# pl.col('Telephone').is_null() | pl.col('Telephone').str.len_chars().eq(10),
				# pl.col('FName').is_not_null() & pl.col('LName').is_not_null(),
				# pl.col('State').is_in(sAbbr),
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
