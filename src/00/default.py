import os
import shutil

srcLetr = 'D'
srcRepo = 'GitHub'
srcComp = 'fiGuys'
srcName = 'Identity'
srcStep = 0
srcStepStr = f'{srcStep:02}'
srcNextStr = f'{(srcStep + 1):02}'
srcRoot = f'{srcLetr}:\\{srcRepo}\\{srcComp}\\{srcName}\\src'
srcFiles = [f'{srcRoot}\\{srcStepStr}\\Part1.txt', f'{srcRoot}\\{srcStepStr}\\Part2.txt']

# Chunk variables
chunkSize = 100_000_000

# Schemas
schemaIn = [
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

lineHeader = ','.join(schemaIn)
lineHeader = f'{lineHeader}\n'


def print_status():
	if readTick % 10_000_000 == 0:
		print(f'Records read: {readTick:_}\tRecords keep: {keepTick:_}\tErrors found: {skipTick:_}')


def adapt__srcFile_to_errFile(srcFile):
	errFile = srcFile.replace(f'src\\{srcStepStr}', f'err\\{srcStepStr}')
	errRoot = os.path.split(errFile)[0]

	if not os.path.exists(errRoot):
		os.makedirs(errRoot, exist_ok=True)

	return [errFile, errRoot]


def adapt__srcFile_to_outFile(srcFile):
	outFile = srcFile.replace(f'src\\{srcStepStr}', f'out\\{srcStepStr}')
	outRoot = os.path.split(outFile)[0]

	if not os.path.exists(outRoot):
		os.makedirs(outRoot, exist_ok=True)

	return [outFile, outRoot]


def adapt__outFile_to_newFile(outFile):
	newFile = outFile.replace(f'out\\{srcStepStr}', f'src\\{srcNextStr}')
	newRoot = os.path.split(newFile)[0]

	if not os.path.exists(newRoot):
		os.makedirs(newRoot, exist_ok=True)

	return [newFile, newRoot]


for srcFile in srcFiles:
	outFile, outRoot = adapt__srcFile_to_outFile(srcFile)
	errFile, errRoot = adapt__srcFile_to_errFile(srcFile)
	newFile, newRoot = adapt__outFile_to_newFile(outFile)

	# Tick variables
	keepTick = 0
	readTick = 0
	skipTick = 0
	partTick = 0
	chunkTick = 1

	# File variables
	keepFile = None
	skipFile = None

	with open(srcFile, 'rt', errors='ignore', newline=None) as readFile:
		for sourceLine in readFile:
			if keepFile is None:
				keepFile = open(
					srcFile.replace('.txt', f'_{partTick:02}.csv').replace('src', 'out'),
					'wt',
					errors='ignore',
					encoding='utf-8',
					newline=None,
				)

			if skipFile is None:
				skipFile = open(
					srcFile.replace('.txt', f'_{partTick:02}.csv').replace('src', 'err'),
					'wt',
					errors='ignore',
					encoding='utf=8',
					newline=None,
				)

			readTick += 1

			if chunkTick == 1:
				keepFile.write(lineHeader)
				skipFile.write(lineHeader)
				keepTick += 1
				chunkTick += 1
				print_status()
			elif sourceLine.count(',') == 19:
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

				src = srcFile.replace('.txt', f'_{partTick:02}.csv').replace('src\\00', 'out\\00')
				dst = srcFile.replace('.txt', f'_{partTick:02}.csv').replace('src\\00', 'src\\01')

				shutil.copy(src, dst)

				partTick += 1
				chunkTick = 1

				print_status()

		print_status()
