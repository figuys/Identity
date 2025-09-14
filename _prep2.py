import shutil
import polars as pl

from polars import LazyFrame
from polars import String, UInt32, Date

from datetime import date

from glob import glob

dList = ['01', '02', '03', '04', '05', '06', '07', '08', '09'] + [str(i) for i in range(10, 32)]
mList = ['01', '02', '03', '04', '05', '06', '07', '08', '09'] + [str(i) for i in range(10, 13)]
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
cAbbr = sorted(
	[
		'AR',
		'CO',
		'FL',
		'GA',
		'IA',
		'IL',
		'IN',
		'KS',
		'KY',
		'LA',
		'MD',
		'MI',
		'MO',
		'MS',
		'MT',
		'NC',
		'NV',
		'OH',
		'OK',
		'SC',
		'SD',
		'TN',
		'TX',
		'UT',
		'VA',
		'WI',
		'WV',
		'WY',
	]
)


def clean_date(date: String) -> String:
	y = date[:4]
	m = date[4:6]
	d = date[6:8]

	if m not in mList:
		m = '01'

	if d not in dList:
		d = '01'

	return f'{y}-{m}-{d}'


def clean_data(lf: LazyFrame) -> LazyFrame:
	# Drop columns: Alt3Dob, Alt2Dob and 8 other columns
	# for colName in [
	#     "Alt3Dob",
	#     "Alt2Dob",
	#     "Alt1Dob",
	#     "StartDay",
	#     "Alt3Name",
	#     "Alt2Name",
	#     "Alt1Name",
	#     "County",
	#     "SName",
	#     "MName",
	# ]:
	#     # print(f'# Dropping column: {colName}')
	#     lf = lf.drop(colName)

	# Change column type to UInt32 for specified columns
	for colName in ['RowId']:
		print(f'# Change column type to UInt32 for column: {colName}')
		lf = lf.filter(pl.col(colName).is_not_null())
		lf = lf.with_columns(pl.col(colName).cast(UInt32))

	# Change column type to String for specified columns
	for colName in [
		'FName',
		'LName',
		'BDate',
		'Address',
		'City',
		'State',
		'Zipcode',
		'Telephone',
		'SSNumber',
		'Alt3Dob',
		'Alt2Dob',
		'Alt1Dob',
		'StartDay',
		'Alt3Name',
		'Alt2Name',
		'Alt1Name',
		'County',
		'SName',
		'MName',
	]:
		print(f'# Change column type to String for column: {colName}')
		lf = lf.with_columns(pl.col(colName).cast(String))

	# Drop rows with missing data in specified columns
	# for colName in [
	#     "FName",
	#     "LName",
	#     "Address",
	#     "City",
	#     "State",
	#     "Zipcode",
	#     "SSNumber",
	# ]:
	#     print(f'# Drop rows with missing data in column: {colName}')
	#     lf = lf.filter(pl.col(colName).is_not_null())
	#     lf = lf.filter(pl.col(colName).str.len_chars() > 0)

	# Calculating column BDate_d, BDate_m, BDate_Y from column: BDate
	for col in [
		'BDate',
		'Alt3Dob',
		'Alt2Dob',
		'Alt1Dob',
		'StartDay',
	]:
		lf = lf.with_columns(pl.col(col).map_elements(clean_date, return_dtype=String).alias(col))

		# Change column type to Date for column: BDate
		lf = lf.with_columns(pl.col(col).cast(Date, strict=False))

	print('Drop rows where SSNumber is not exactly 9 characters')
	lf = lf.filter(pl.col('SSNumber').str.len_chars() == 9)

	print('Filter rows based on column: Telephone')
	lf = lf.filter(pl.col('Telephone').is_null() | (pl.col('Telephone').str.len_chars() == 10))

	# Ensure 5 digit Zipcode
	print('# Ensure 5 digit Zipcode')
	lf = lf.with_columns(pl.col('Zipcode').str.slice(0, 5).alias('Zipcode')).filter(
		pl.col('Zipcode').str.len_chars() == 5
	)

	print('# Filter rows based on column: State')
	lf = lf.with_columns(pl.col('State').str.strip_chars().alias('State'))
	lf = lf.with_columns(pl.col('State').str.slice(0, 2).alias('State'))
	lf = lf.with_columns(pl.col('State').str.to_uppercase().alias('State'))
	lf = lf.filter(pl.col('State').str.len_chars() == 2)
	lf = lf.filter(pl.col('State').is_in(sAbbr))

	return lf


def chris_data(lf: LazyFrame) -> LazyFrame:
	lDate = date(year=1966, month=1, day=1)

	lf = lf.filter(pl.col('State').is_in(cAbbr))
	lf = lf.filter(pl.col('Telephone').is_not_null())
	lf = lf.filter(pl.col('Zipcode').is_in(zList))
	lf = lf.filter((pl.col('BDate') > lDate) | pl.col('BDate').is_null())

	return lf


for iKey in [
	'00',
]:
	csvGlobS = 'D:\\OneLake\\source\\Part*.txt'
	csvFiles = glob(csvGlobS)
	outFile = 'D:\\OneLake\\source\\List.parquet'

	print('')
	print(f'Reading file: {csvFiles}')
	lf = pl.scan_csv(csvFiles, ignore_errors=True)
	lf = clean_data(lf)
	# lf = chris_data(lf)

	print(f'Writing file: {outFile}')
	lf.sink_parquet(outFile)

	for csvFile in csvFiles:
		shutil.move(csvFile, csvFile.replace('source', 'done'))

# for srcFile in glob("D:\\OneLake\\source\\Part*.parquet"):
#     outFile = srcFile.replace("Part", "Main")

#     if os.path.exists(outFile) and os.path.exists(srcFile):
#         os.remove(outFile)

#     if os.path.exists(srcFile):
#         print("")
#         print(f"Reading file: {srcFile}")
#         lf = pl.scan_parquet(srcFile)
#         lf = clean_data(lf)
#         # lf = chris_data(lf)

#         print(f"Writing file: {outFile}")
#         lf.sink_parquet(outFile)

#     if os.path.exists(outFile):
#         shutil.move(srcFile, srcFile.replace("source", "done"))
