import csv
import os
import shutil

from glob import glob


def print_status():
	if rTick % 1_000_000 == 0:
		print(f'Read: {rTick:_010D} Keep: {kTick:_010D} Sub: {sTick:_010D} Add: {aTick:_010D}')


for srcFile in glob('D:\\GitHub\\fiGuys\\Identity\\src\\00\\Part*.txt'):
	tmpFile = srcFile.replace(r'src\00', r'tmp\00')
	outFile = srcFile.replace(r'src\00', r'out\00')
	errFile = srcFile.replace(r'src\00', r'err\00')
	newFile = outFile.replace(r'out\00', r'src\01')

	os.makedirs(os.path.dirname(outFile), exist_ok=True)
	os.makedirs(os.path.dirname(tmpFile), exist_ok=True)
	os.makedirs(os.path.dirname(errFile), exist_ok=True)
	os.makedirs(os.path.dirname(newFile), exist_ok=True)

	eCOUNT: int = 20
	pINDEX: int = 18

	rTick: int = 0
	kTick: int = 0
	sTick: int = 0
	aTick: int = 0

	with (
		open(srcFile, 'rt', errors='ignore', newline=None, encoding='utf-8') as iFile,
		open(tmpFile, 'wt', errors='ignore', newline=None, encoding='utf-8') as oFile,
	):
		reader = csv.reader(iFile)
		writer = csv.writer(oFile)

		for row in reader:
			if not row:
				continue
			else:
				rTick += 1

			if len(row) == eCOUNT:
				writer.writerow(row)
				kTick += 1
				print_status()
				continue

			last = row[-1]
			core = row[:-1]

			# Too few → insert blanks before last field
			if len(row) < eCOUNT:
				aTick += 1
				while len(core) + 1 < eCOUNT:  # +1 for last
					core.insert(pINDEX, '')

			# Too many → trim backwards from index 18
			elif len(row) > eCOUNT:
				sTick += 1
				cut_index = pINDEX
				while len(core) + 1 > eCOUNT and cut_index >= 0:
					if cut_index < len(core):
						del core[cut_index]
					cut_index -= 1

			fixed = core + [last]  # put last back
			writer.writerow(fixed)
			print_status()

	shutil.move(tmpFile, outFile)
	shutil.copy(outFile, newFile)
