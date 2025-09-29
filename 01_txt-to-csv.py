import csv
import os
import re
import shutil
import unicodedata
from glob import glob

gAllowed: list[str]
gPattern: re.Pattern[str]

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


def getAllowed() -> list[str]:
	global gAllowed

	if 'gAllowed' not in globals():
		gAllowed = []

	if len(gAllowed) == 0:
		for codepoint in range(0x110000):
			cat = unicodedata.category(chr(codepoint))
			if cat[0] in 'LNP' or cat[0] == 'S' or cat == 'Zs':
				gAllowed.append(chr(codepoint))

	return gAllowed


def getPattern() -> re.Pattern[str]:
	global gPattern

	if 'gPattern' not in globals():
		gPattern = re.compile(f'[^{re.escape("".join(getAllowed()))}]')

	return gPattern


def clean_line(line: str) -> str:
	return getPattern().sub('', line)


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

	with open(srcFile, 'rt', errors='ignore', encoding='utf-8', newline='') as readFile:
		srcReader = csv.reader(readFile)
		for srcLine in srcReader:
			errFile = errFile.replace('.txt', f'_{pTick:03d}.csv')
			outFile = outFile.replace('.txt', f'_{pTick:03d}.csv')
			newFile = newFile.replace('.txt', f'_{pTick:03d}.csv')
			tmpFile = tmpFile.replace('.txt', f'_{pTick:03d}.csv')

			if kFile is None:
				kFile = open(tmpFile, 'wt', errors='ignore', encoding='utf-8', newline='')
			if kSave is None:
				kSave = csv.writer(kFile)
			if sFile is None:
				sFile = open(errFile, 'wt', errors='ignore', encoding='utf-8', newline='')
			if sSave is None:
				sSave = csv.writer(sFile)

			if not srcLine:
				continue

			rTick += 1
			if cTick == 1:
				kSave.writerow(headers)
				sSave.writerow(headers)
				kTick += 1
				cTick += 1
				print_status(rTick, kTick, sTick)
			elif len(srcLine) == 20:
				kSave.writerow(srcLine)
				kTick += 1
				cTick += 1
				print_status(rTick, kTick, sTick)
			else:
				sSave.writerow(srcLine)
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
