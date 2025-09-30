import csv
import os
import re
import shutil
import unicodedata
from glob import glob

# Declare a global list of allowed characters (will be filled lazily)
gAllowed: list[str]
# Declare a global compiled regex pattern (will be compiled lazily)
gPattern: re.Pattern[str]

# Define the CSV header columns expected in output files
headers = [
	'RowId',
	'FName',
	'LName',
	'MName',
	'SName',
	'BDate',
	'Address',
	'City',
	'County',
	'State',
	'Zipcode',
	'Telephone',
	'Alt1Name',
	'Alt2Name',
	'Alt3Name',
	'StartDay',
	'Alt1Dob',
	'Alt2Dob',
	'Alt3Dob',
	'SSNumber',
]


# Periodically print processing status based on counters provided
def print_status(counts: dict[str, int]) -> None:
	# Read total rows read
	rTick: int = counts['rTick']
	# Read total rows kept/written
	kTick: int = counts['kTick']
	# Read total rows where fields were removed (subtracted)
	sTick: int = counts['sTick']
	# Read total rows where fields were added
	aTick: int = counts['aTick']

	# Print every 1,000,000 rows; otherwise do nothing
	return (
		None if rTick % 1_000_000 else print(f'{"Read:":<6} {rTick:<13_} {"Kept:":<6} {kTick:<13_} {"Sub:":<5} {sTick:<8_} {"Add:":<5} {aTick:<8_}')
	)


# Build and cache the list of allowed Unicode characters
def getAllowed() -> list[str]:
	# Use a global cache
	global gAllowed

	# Initialize global if not defined
	if 'gAllowed' not in globals():
		gAllowed = []

	# Populate cache once with characters whose Unicode categories are allowed
	if len(gAllowed) == 0:
		for codepoint in range(0x110000):
			# Get Unicode general category for the character
			cat = unicodedata.category(chr(codepoint))
			# Allow Letters (L), Numbers (N), Punctuation (P), Symbols (S), and space separator (Zs)
			if cat[0] in 'LNP' or cat[0] == 'S' or cat == 'Zs':
				gAllowed.append(chr(codepoint))

	# Return cached list
	return gAllowed


# Build and cache a regex that matches any character NOT allowed
def getPattern() -> re.Pattern[str]:
	# Use a global cache
	global gPattern

	# Compile the pattern once; escape all allowed chars and negate the class
	if 'gPattern' not in globals():
		gPattern = re.compile(f'[^{re.escape("".join(getAllowed()))}]')

	# Return cached pattern
	return gPattern


# Remove all disallowed characters from a line
def clean_line(line: str) -> str:
	return getPattern().sub('', line)


# Ensure a row has the expected number of fields by inserting/removing near pINDEX
def fix_fields(row: list[str], counts: dict[str, int]) -> list[str]:
	# Expected field count
	eCOUNT: int = counts['eCOUNT']
	# Preferred index around which to add/remove fields
	pINDEX: int = counts['pINDEX']

	# Preserve the last field separately (e.g., SSNumber)
	last = row[-1]
	# Work on all fields except last
	core = row[:-1]

	# Too few → insert blanks before last field
	if len(row) < eCOUNT:
		# Increment add counter
		counts['aTick'] += 1
		# Insert empty strings at pINDEX until total (core + last) reaches expected
		while len(core) + 1 < eCOUNT:  # +1 for last
			core.insert(pINDEX, '')

	# Too many → trim backwards from index pINDEX
	elif len(row) > eCOUNT:
		# Increment subtract counter
		counts['sTick'] += 1
		# Start deleting from pINDEX downwards
		cut_index = pINDEX
		while len(core) + 1 > eCOUNT and cut_index >= 0:
			# Delete element at cut_index if within bounds
			if cut_index < len(core):
				del core[cut_index]
			# Move leftwards
			cut_index -= 1

	# Reattach the last field and return
	fixed = core + [last]
	return fixed


# Iterate all source text parts in the input directory
for srcFile in glob('D:\\GitHub\\fiGuys\\Identity\\src\\00\\Part*.txt'):
	# Construct temp, output, error, and next-stage paths
	tmpFile = srcFile.replace(r'src\00', r'tmp\00')
	outFile = srcFile.replace(r'src\00', r'out\00')
	errFile = srcFile.replace(r'src\00', r'err\00')
	newFile = outFile.replace(r'out\00', r'src\01')

	# Ensure directories exist for all output locations
	os.makedirs(os.path.dirname(outFile), exist_ok=True)
	os.makedirs(os.path.dirname(tmpFile), exist_ok=True)
	os.makedirs(os.path.dirname(errFile), exist_ok=True)
	os.makedirs(os.path.dirname(newFile), exist_ok=True)

	# Initialize counters and configuration
	counts = {
		'eCOUNT': 20,  # Expected number of fields per row
		'pINDEX': 18,  # Pivot index near which to add/remove fields
		'cSIZE': 25_000_000,  # Chunk size (rows per output file)
		'rTick': 0,  # Rows read
		'kTick': 0,  # Rows kept/written
		'aTick': 0,  # Rows where fields were added
		'sTick': 0,  # Rows where fields were removed
		'pTick': 0,  # Output part index
		'cTick': 1,  # Current row counter within the chunk (1 triggers header)
	}

	# Open the source file for reading text as UTF-8, ignoring errors
	with open(srcFile, 'rt', errors='ignore', newline=None, encoding='utf-8') as sFile:
		# CSV reader for the source file
		sRead = csv.reader(sFile)
		# Handle to the current temp output file (None until created)
		kFile = None
		# CSV writer bound to kFile (None until created)
		kSave = None

		# Process each row in the source
		for row in sRead:
			# Increment rows read
			counts['rTick'] += 1

			# Skip empty rows
			if not row:
				continue

			# Lazily open a new temp CSV file for the current part if not open
			if kFile is None:
				kFile = open(
					tmpFile.replace('.txt', f'_{counts["pTick"]:02d}.csv'),
					'wt',
					errors='ignore',
					encoding='utf-8',
					newline=None,
				)

			# Lazily create a CSV writer for the open file
			if kSave is None:
				kSave = csv.writer(kFile)

			# If this is the first row in the chunk, write header
			if counts['cTick'] == 1:
				counts['cTick'] += 1
				counts['kTick'] += 1
				kSave.writerow(headers)
				print_status(counts)
			else:
				# If row already has expected field count, write as-is
				if len(row) == counts['eCOUNT']:
					counts['cTick'] += 1
					counts['kTick'] += 1
					kSave.writerow(row)
					print_status(counts)
				else:
					# Otherwise fix field count then write
					counts['cTick'] += 1
					counts['kTick'] += 1
					kSave.writerow(fix_fields(row, counts))
					print_status(counts)

			# If chunk reached its size, rotate files and reset counters
			if counts['cTick'] == counts['cSIZE']:
				# Release writer to ensure buffers flushed
				kSave = None
				# Flush and close the temp file
				kFile.flush()
				kFile.close()
				kFile = None

				# Move temp CSV to final out location for this part
				shutil.move(
					tmpFile.replace('.txt', f'_{counts["pTick"]:02d}.csv'),
					outFile.replace('.txt', f'_{counts["pTick"]:02d}.csv'),
				)
				# Copy the out file to the next pipeline stage directory
				shutil.copy(
					outFile.replace('.txt', f'_{counts["pTick"]:02d}.csv'),
					newFile.replace('.txt', f'_{counts["pTick"]:02d}.csv'),
				)

				# Advance to next part and reset chunk counter to trigger header
				counts['pTick'] += 1
				counts['cTick'] = 1

				# Print status after rotation
				print_status(counts)

		# After finishing source, if an output file is open, finalize it
		if kFile is not None:
			# Release writer handle
			kSave = None
			# Flush and close the temp file
			kFile.flush()
			kFile.close()
			kFile = None

			# Move last temp CSV to out location
			shutil.move(
				tmpFile.replace('.txt', f'_{counts["pTick"]:02d}.csv'),
				outFile.replace('.txt', f'_{counts["pTick"]:02d}.csv'),
			)
			# Copy to next stage directory
			shutil.copy(
				outFile.replace('.txt', f'_{counts["pTick"]:02d}.csv'),
				newFile.replace('.txt', f'_{counts["pTick"]:02d}.csv'),
			)

			# Increment part index and reset chunk counter
			counts['pTick'] += 1
			counts['cTick'] = 1

			# Final status print
			print_status(counts)
