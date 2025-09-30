import os
import shutil
import sys
from glob import glob

import polars as pl

# Add the 'lib' directory to the system path to import PathManager
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'lib')))
from path_manager import PathManager

for srcFile in glob(r'D:\GitHub\fiGuys\Identity\01\src\Part*.csv'):
	paths = PathManager(srcFile)
	paths.ensure_dirs()

	print('')
	print(f'Reading file:  {srcFile}')
	lf = pl.scan_csv(srcFile, ignore_errors=True, infer_schema=False)

	print(f'Temping file:  {paths.tmp}')
	lf.sink_parquet(paths.tmp)

	print(f'Saving file:   {paths.out}')
	shutil.move(paths.tmp, paths.out)

	print(f'Staging file:  {paths.new}')
	shutil.copy(paths.out, paths.new)
