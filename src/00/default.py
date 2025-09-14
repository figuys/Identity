# File sources
srcFiles = ["D:\\OneLake\\src\\00\\Part1.txt", "D:\\OneLake\\src\\00\\Part2.txt"]

# Chunk variables
chunkSize = 100_000_000

# Schemas
schemaIn = [
    "RowId",
    "FName",
    "LName",
    "MName",
    "SName",
    "BDate",
    "Address",
    "City",
    "County",
    "State",
    "Zipcode",
    "Telephone",
    "Alt1Name",
    "Alt2Name",
    "Alt3Name",
    "StartDay",
    "Alt1Dob",
    "Alt2Dob",
    "Alt3Dob",
    "SSNumber",
]

lineHeader = ",".join(schemaIn)
lineHeader = f"{lineHeader}\n"


def print_status():
    if readTick % 10_000_000 == 0:
        print(
            f"Records read: {readTick:_}\tRecords keep: {keepTick:_}\tErrors found: {skipTick:_}"
        )


for sourcePath in srcFiles:
    # Tick variables
    keepTick = 0
    readTick = 0
    skipTick = 0
    partTick = 0
    chunkTick = 1
    # File variables
    keepFile = None
    skipFile = None
    with open(sourcePath, "rt", errors="ignore", newline=None) as readFile:
        for sourceLine in readFile:
            if keepFile is None:
                keepFile = open(
                    sourcePath.replace(".txt", f"_{partTick:02}.csv").replace(
                        "src", "out"
                    ),
                    "wt",
                    errors="ignore",
                    encoding="utf-8",
                    newline=None,
                )

            if skipFile is None:
                skipFile = open(
                    sourcePath.replace(".txt", f"_{partTick:02}.csv").replace(
                        "src", "err"
                    ),
                    "wt",
                    errors="ignore",
                    encoding="utf=8",
                    newline=None,
                )

            readTick += 1

            if chunkTick == 1:
                keepFile.write(lineHeader)
                skipFile.write(lineHeader)
                keepTick += 1
                chunkTick += 1
                print_status()
            elif sourceLine.count(",") == 19:
                keepFile.write(sourceLine)
                keepTick += 1
                chunkTick += 1
                print_status()
            else:
                skipFile.write(sourceLine)
                skipTick += 1
                chunkTick += 1
                print_status()

            if chunkTick == chunkSize:
                keepFile.flush()
                keepFile.close()
                keepFile = None

                skipFile.close()
                skipFile = None

                partTick += 1
                chunkTick = 1
        print_status()
