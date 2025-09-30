import os
import shutil
from typing import Callable

import polars as pl

from path_manager import PathManager


def process_file(
	src_file: str,
	transform_func: Callable[[pl.LazyFrame], pl.LazyFrame],
	source_reader: Callable = pl.scan_parquet,
) -> None:
	"""
	Applies a transformation to a source file and saves the result,
	handling all path management and file operations for a pipeline step.

	Args:
	    src_file (str): The path to the source file.
	    transform_func (Callable): A function that takes a LazyFrame,
	                              applies transformations, and returns a LazyFrame.
	    source_reader (Callable): The Polars function to use for reading the
	                              source file (e.g., pl.scan_parquet, pl.scan_csv).
	"""
	paths = PathManager(src_file)
	paths.ensure_dirs()

	print('')
	print(f'Reading file:  {src_file}')
	# For scan_csv, we need to pass specific arguments
	if source_reader.__name__ == 'scan_csv':
		lf: pl.LazyFrame = source_reader(src_file, ignore_errors=True, infer_schema=False)
	else:
		lf: pl.LazyFrame = source_reader(src_file)

	print(f'Processing:    {os.path.basename(src_file)}')
	transformed_lf = transform_func(lf)

	print(f'Writing file:  {paths.out}')
	transformed_lf.sink_parquet(paths.tmp)
	shutil.move(paths.tmp, paths.out)

	print(f'Staging file:  {paths.new}')
	if os.path.exists(paths.out):
		shutil.copy(paths.out, paths.new)
