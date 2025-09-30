import os
import sys
from enum import Enum
from glob import glob

import polars as pl
from polars import Expr, LazyFrame, UInt8, UInt16, UInt32

# Add the 'lib' directory to the system path to import PathManager
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'lib')))
from pipeline_utils import process_file


class StrCasing(Enum):
	Ignore = 1
	Lower = 2
	Upper = 3
	Title = 4


# Helper functions for normalization
def strip_chars(expr: Expr) -> Expr:
	"""Expression to strip leading/trailing whitespace from a string column."""
	return expr.str.strip_chars()


def collapse_whitespace(expr: Expr) -> Expr:
	"""Expression to replace multiple whitespace characters with a single space."""
	return expr.str.replace_all(r'\s+', ' ')


def apply_type_casting(lf: LazyFrame) -> LazyFrame:
	"""Applies all type casting and string normalization for step 02."""

	####################
	## Bug Fix - Hack ##
	####################
	lf = lf.with_columns(pl.col('SSNumber\r').alias('SSNumber')).drop('SSNumber\r')

	# Correct RowId
	print('   # Change column type to UInt32 for column: RowId, required True')
	lf = lf.filter(pl.col('RowId').is_not_null()).with_columns(pl.col('RowId').cast(UInt32, strict=True))

	# Correct String columns
	print('   # Change column types to String and apply transformations')
	lf = lf.filter(pl.col('SSNumber').is_not_null()).with_columns(  # type: ignore
		# For columns needing title casing, apply cleaning then casing.
		pl.col(['FName', 'LName', 'MName', 'Alt1Name', 'Alt2Name', 'Alt3Name', 'City', 'County'])
		.pipe(strip_chars)
		.pipe(collapse_whitespace)
		.str.to_titlecase(),
		# For columns needing upper casing, apply cleaning then casing.
		pl.col(['SName', 'Address', 'State']).pipe(strip_chars).pipe(collapse_whitespace).str.to_uppercase(),
		# For other string columns (like SSNumber), just apply cleaning.
		pl.col('SSNumber').pipe(strip_chars).pipe(collapse_whitespace),
		# Note: This assumes all string columns fall into one of these groups.
		# If other string columns exist that need cleaning, they must be specified.
	)

	# Correct Dates
	print('   # Change column types to Date')
	date_cols = ['BDate', 'Alt1Dob', 'Alt2Dob', 'Alt3Dob', 'StartDay']  # Original date columns
	new_date_cols = []  # To hold all new expressions

	for col_name in date_cols:
		# 1. Parse the full date, turning invalid/incomplete dates into null
		new_date_cols.append(pl.col(col_name).str.pad_end(8, '0').str.to_date('%Y%m%d', strict=False, exact=True).alias(col_name))
		# 2. Extract Year, Month, and Day parts into new columns
		new_date_cols.append(
			pl.when(pl.col(col_name).str.len_chars() >= 4).then(pl.col(col_name).str.slice(0, 4).cast(UInt16, strict=False)).alias(f'{col_name}_Y')
		)
		new_date_cols.append(
			pl.when(pl.col(col_name).str.len_chars() >= 6).then(pl.col(col_name).str.slice(4, 2).cast(UInt8, strict=False)).alias(f'{col_name}_M')  # type: ignore
		)
		new_date_cols.append(
			pl.when(pl.col(col_name).str.len_chars() == 8).then(pl.col(col_name).str.slice(6, 2).cast(UInt8, strict=False)).alias(f'{col_name}_D')
		)

	lf = lf.with_columns(new_date_cols)
	return lf


def main():
	"""Main processing loop for step 02."""
	for src_file in glob('D:\\GitHub\\fiGuys\\Identity\\02\\src\\Part*.parquet'):
		process_file(src_file, apply_type_casting)


if __name__ == '__main__':
	main()
