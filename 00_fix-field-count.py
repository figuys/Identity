import csv
import os
import re
import shutil
import unicodedata
from glob import glob

gAllowed: list[str]
gPattern: re.Pattern[str]

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


def print_status(counts: dict[str, int]) -> None:
	rTick: int = counts['rTick']
	kTick: int = counts['kTick']
	sTick: int = counts['sTick']
	aTick: int = counts['aTick']

	return (
		None
		if (rTick % 1_000_000)
		else print(
			f'{"Read:":<6} {rTick:<13_} {"Kept:":<6} {kTick:<13_} {"Sub:":<5} {sTick:<8_} {"Add:":<5} {aTick:<8_}'
		)
	)


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


def fix_fields(row: list[str], counts: dict[str, int]) -> list[str]:
	eCOUNT: int = counts['eCOUNT']
	pINDEX: int = counts['pINDEX']

	last = row[-1]
	core = row[:-1]

	# Too few → insert blanks before last field
	if len(row) < eCOUNT:
		counts['aTick'] += 1
		while len(core) + 1 < eCOUNT:  # +1 for last
			core.insert(pINDEX, '')

	# Too many → trim backwards from index pINDEX
	elif len(row) > eCOUNT:
		counts['sTick'] += 1
		cut_index = pINDEX
		while len(core) + 1 > eCOUNT and cut_index >= 0:
			if cut_index < len(core):
				del core[cut_index]
			cut_index -= 1

	fixed = core + [last]
	return fixed


for srcFile in glob('D:\\GitHub\\fiGuys\\Identity\\src\\00\\Part*.txt'):
	tmpFile = srcFile.replace(r'src\00', r'tmp\00')
	outFile = srcFile.replace(r'src\00', r'out\00')
	errFile = srcFile.replace(r'src\00', r'err\00')
	newFile = outFile.replace(r'out\00', r'src\01')

	os.makedirs(os.path.dirname(outFile), exist_ok=True)
	os.makedirs(os.path.dirname(tmpFile), exist_ok=True)
	os.makedirs(os.path.dirname(errFile), exist_ok=True)
	os.makedirs(os.path.dirname(newFile), exist_ok=True)

	counts = {
		'eCOUNT': 20,
		'pINDEX': 18,
		'cSIZE': 25_000_000,
		'rTick': 0,
		'kTick': 0,
		'aTick': 0,
		'sTick': 0,
		'pTick': 0,
		'cTick': 1,
	}

	with open(srcFile, 'rt', errors='ignore', newline=None, encoding='utf-8') as sFile:
		sRead = csv.reader(sFile)
		kFile = None
		kSave = None

		for row in sRead:
			counts['rTick'] += 1

			if not row:
				continue

			if kFile is None:
				kFile = open(
					tmpFile.replace('.txt', f'_{counts["pTick"]:03d}.csv'),
					'wt',
					errors='ignore',
					encoding='utf-8',
					newline=None,
				)

			if kSave is None:
				kSave = csv.writer(kFile)

			if counts['cTick'] == 1:
				counts['cTick'] += 1
				counts['kTick'] += 1
				kSave.writerow(headers)
				print_status(counts)
			else:
				if len(row) == counts['eCOUNT']:
					counts['cTick'] += 1
					counts['kTick'] += 1
					kSave.writerow(row)
					print_status(counts)
				else:
					counts['cTick'] += 1
					counts['kTick'] += 1
					kSave.writerow(fix_fields(row, counts))
					print_status(counts)

			if counts['cTick'] == counts['cSIZE']:
				kSave = None
				kFile.flush()
				kFile.close()
				kFile = None

				shutil.move(
					tmpFile.replace('.txt', f'_{counts["pTick"]:03d}.csv'),
					outFile.replace('.txt', f'_{counts["pTick"]:03d}.csv'),
				)
				shutil.copy(
					outFile.replace('.txt', f'_{counts["pTick"]:03d}.csv'),
					newFile.replace('.txt', f'_{counts["pTick"]:03d}.csv'),
				)

				counts['pTick'] += 1
				counts['cTick'] = 1

				print_status(counts)

		if kFile is not None:
			kSave = None
			kFile.flush()
			kFile.close()
			kFile = None

			shutil.move(
				tmpFile.replace('.txt', f'_{counts["pTick"]:03d}.csv'),
				outFile.replace('.txt', f'_{counts["pTick"]:03d}.csv'),
			)
			shutil.copy(
				outFile.replace('.txt', f'_{counts["pTick"]:03d}.csv'),
				newFile.replace('.txt', f'_{counts["pTick"]:03d}.csv'),
			)

			counts['pTick'] += 1
			counts['cTick'] = 1

			print_status(counts)
