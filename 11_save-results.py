import os
import shutil
import polars as pl

from polars import Expr
from glob import glob

custZipcodes = [
	'33002',
	'33010',
	'33011',
	'33012',
	'33013',
	'33014',
	'33015',
	'33016',
	'33017',
	'33018',
	'33030',
	'33031',
	'33032',
	'33033',
	'33034',
	'33035',
	'33036',
	'33037',
	'33039',
	'33054',
	'33055',
	'33056',
	'33090',
	'33092',
	'33101',
	'33109',
	'33114',
	'33116',
	'33119',
	'33122',
	'33124',
	'33125',
	'33126',
	'33127',
	'33128',
	'33129',
	'33130',
	'33131',
	'33132',
	'33133',
	'33134',
	'33135',
	'33136',
	'33137',
	'33138',
	'33139',
	'33140',
	'33141',
	'33142',
	'33143',
	'33144',
	'33145',
	'33146',
	'33147',
	'33149',
	'33150',
	'33151',
	'33152',
	'33153',
	'33154',
	'33155',
	'33156',
	'33157',
	'33158',
	'33160',
	'33161',
	'33162',
	'33163',
	'33164',
	'33165',
	'33166',
	'33167',
	'33168',
	'33169',
	'33170',
	'33172',
	'33173',
	'33174',
	'33175',
	'33176',
	'33177',
	'33178',
	'33179',
	'33180',
	'33181',
	'33182',
	'33183',
	'33184',
	'33185',
	'33186',
	'33187',
	'33188',
	'33189',
	'33190',
	'33193',
	'33194',
	'33196',
	'33197',
	'33199',
]


# Remove non-digit characters
def only_digits(expr: Expr) -> Expr:
	return pl.when(expr.is_not_null()).then(expr.str.extract(r'\d+', 0)).otherwise(None)


def only_length(expr: Expr, *, length: int) -> Expr:
	return pl.when(expr.str.len_chars() == length).then(expr).otherwise(None)


for srcFile in glob('D:\\OneLake\\src\\11\\Parts.parquet'):
	outFile = srcFile.replace('src\\11', 'out\\11')
	newFile = outFile.replace('out\\11', 'src\\12')

	if os.path.exists(outFile):
		os.remove(outFile)

	if os.path.exists(srcFile):
		print('')
		print(f'Reading file:  {srcFile}')
		lf = pl.scan_parquet(srcFile)

		print(f'Cleaning file: {srcFile}')

		# Clean Columns
		lf = (
			lf.with_columns(
				[
					# pl.col('FName').str.strip_chars().str.to_titlecase(),
					# pl.col('LName').str.strip_chars().str.to_titlecase(),
					# pl.col('MName').str.strip_chars().str.to_titlecase(),
					# pl.col('SName').str.strip_chars().str.to_uppercase(),
					# pl.col('Address').str.strip_chars().str.to_uppercase(),
					# pl.col('City').str.strip_chars().str.to_titlecase(),
					# pl.col('County').str.strip_chars().str.to_titlecase(),
					# pl.col('State').str.strip_chars().str.to_uppercase(),
					# pl.col('Alt1Name').str.strip_chars().str.to_titlecase(),
					# pl.col('Alt2Name').str.strip_chars().str.to_titlecase(),
					# pl.col('Alt3Name').str.strip_chars().str.to_titlecase(),
				]
			)
			.filter(
				[
					pl.col('Zipcode').is_in(custZipcodes).alias('Zipcode'),
					pl.col('Telephone').is_not_null().alias('Telephone'),
					# pl.col('FName').is_not_null() & pl.col('LName').is_not_null(),
					# pl.col('State').is_in(sAbbr),
					# pl.col('Zipcode').is_not_null(),
				]
			)
			.unique(subset=['Telephone', 'Zipcode', 'SSNumber'], keep='any')
			.drop(
				'SSNumber',
				'Alt1Name',
				'Alt2Name',
				'Alt3Name',
				'Alt1Dob',
				'Alt2Dob',
				'Alt3Dob',
				'StartDay',
			)
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
