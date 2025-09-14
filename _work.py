import polars as pl
from glob import glob
import os

srcGlobs = "D:\\OneLake\\source\\Part*.csv"
srcFiles = glob(srcGlobs)

for srcFile in srcFiles:
    outFile = srcFile.replace(".csv", ".parquet")

    try:
        lf = pl.scan_csv(srcFile, ignore_errors=True, infer_schema=False)
        lf.sink_parquet(outFile)

        if os.path.exists(srcFile):
            doneFile = srcFile.replace("source", "done")

            if os.path.exists(doneFile):
                os.remove(doneFile)

            os.rename(srcFile, doneFile)

    except Exception as e:
        print(f"Error combining Parquet files: {e}")
