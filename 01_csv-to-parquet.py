import polars as pl
from glob import glob
import os
import shutil as sh

srcGlobs = "D:\\OneLake\\src\\01\\Part*.csv"
srcFiles = glob(srcGlobs)

for srcFile in srcFiles:
    outFile = srcFile.replace(".csv", ".parquet").replace("src", "out")

    try:
        print(f"Converting {srcFile} to parquet format.")
        lf = pl.scan_csv(
            srcFile,
            ignore_errors=True,
            infer_schema=False,
        )
        lf.sink_parquet(outFile)

        if os.path.exists(outFile):
            newFile = (
                srcFile.replace("\\out", "\\src")
                .replace("\\01", "\\02")
                .replace(".csv", ".parquet")
            )

            if os.path.exists(newFile):
                os.remove(newFile)
            print(f"Staging {newFile}")
            sh.copy(outFile, newFile)

    except Exception as e:
        print(f"Error combining Parquet files: {e}")
