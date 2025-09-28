import csv
import os
import shutil
from glob import glob

# Chunk variables
cSize = 10_000_000

# Schemas
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

def print_status(rTick: int, kTick: int, sTick: int) -> None:
	if rTick % 1_000_000 == 0:
		print(f'{"Read:":<6} {rTick:<13_} {"Kept:":<6} {kTick:<13_} {"Skip:":<6} {sTick:<13_}')


def clean_line(line: str) -> str:
	# lineN = unicodedata.normalize('NFC', line)
	# lineC = regex.sub(r'[^\p{L}\p{N}\p{Z}\p{P}\p{S}]', '', lineN)
	# lineR = regex.sub(r'\s+', ' ', lineC).strip()
	result = line.strip()

	return result


for srcFile in glob('D:\\GitHub\\fiGuys\\Identity\\src\\01\\Part*.txt'):
	tmpFile = srcFile.replace(r'src\01', r'tmp\01')
	outFile = srcFile.replace(r'src\01', r'out\01')
	errFile = srcFile.replace(r'src\01', r'err\01')
	newFile = outFile.replace(r'out\01', r'src\02')

	os.makedirs(os.path.dirname(tmpFile), exist_ok=True)
	os.makedirs(os.path.dirname(outFile), exist_ok=True)
	os.makedirs(os.path.dirname(errFile), exist_ok=True)
	os.makedirs(os.path.dirname(newFile), exist_ok=True)

	kTick: int = 0
	rTick: int = 0
	sTick: int = 0
	pTick: int = 0
	cTick: int = 1

	kFile = None
	kSave = None
	sFile = None
	sSave = None

	with open(srcFile, 'rt', errors='ignore', encoding='utf-8') as readFile:
		for srcRow in csv.reader(readFile):
			errFile = errFile.replace('.txt', f'_{pTick:03d}.csv')
			outFile = outFile.replace('.txt', f'_{pTick:03d}.csv')
			newFile = newFile.replace('.txt', f'_{pTick:03d}.csv')
			tmpFile = tmpFile.replace('.txt', f'_{pTick:03d}.csv')

			if kFile is None:
				kFile = open(tmpFile, 'wt', errors='ignore', encoding='utf-8')
			if kSave is None:
				kSave = csv.writer(kFile)
			if sFile is None:
				sFile = open(errFile, 'wt', errors='ignore', encoding='utf-8')
			if sSave is None:
				sSave = csv.writer(sFile)

			if not srcRow:
				continue

			rTick += 1
			if cTick == 1:
				kSave.writerow(headers)
				sSave.writerow(headers)
				kTick += 1
				cTick += 1
				print_status(rTick, kTick, sTick)
			elif len(srcRow) == 20:
				kSave.writerow(srcRow)
				kTick += 1
				cTick += 1
				print_status(rTick, kTick, sTick)
			else:
				sSave.writerow(srcRow)
				sTick += 1
				cTick += 1
				print_status(rTick, kTick, sTick)

			if cTick == cSize:
				kSave = None
				sSave = None

				kFile.flush()
				kFile.close()
				kFile = None

				sFile.flush()
				sFile.close()
				sFile = None

				shutil.move(tmpFile, outFile)
				shutil.copy(outFile, newFile)

				pTick += 1
				cTick = 1

				print_status(rTick, kTick, sTick)

	print_status(rTick, kTick, sTick)
