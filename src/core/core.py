import sys
sys.path.append( '../..' )
from src.core.init import Initializer
import socket
import string
import json
import io
from datetime import datetime
import time
import difflib
import re
import os

def commit(init, comment = ''):
	# get new files
	new_files, new_lm, old_files, old_lm = init.getListtoCompare()

	if comment != '':
		comment = ', ' + comment

	init.storage.resetBuffer()
	# checks who is the user
	idx = 0
	if init.user == None:
		username = socket.gethostbyname(socket.gethostname())
	else:
		username = init.user.username

	commits = []
	toadd = []
	toremove = []

	for idx, f in enumerate(old_files):
		# relative file location
		f_relative = f.replace(init.prefix_curr,"")

		# checks if the file was modified or remove
		if f_relative in new_files:
			if new_lm[new_files.index(f_relative)] != old_lm[idx]:
				# checks the latest diff and sets the new path
				n = init.getLatestDiffNumber(f_relative) + 1
				diff = init.prefix_diffs + f_relative + '/' + str(n).zfill(10)

				# computes the diff and stores it
				storeDiff(init.storage,diff,f_relative,f)

				commit = {
					'key'	: f_relative,
					'diff'	: diff,
					'version' : n,
					'type'	: 'modified',
					'date'	: new_lm[idx],
					'source': username,
					'comment' : 'modified '+ f_relative + comment,
				}

				print('-- modified '+ f_relative)

				toadd.append(f_relative)

				commitpath = init.prefix_commit + f_relative + '/' + str(n).zfill(10)
				commits.append(commitpath)
				init.storage.addFileFromBinaryGlobal(commitpath,io.BytesIO(json.dumps(commit).encode('ascii')))
				updateFileHistory(init,f_relative,commit)
		else:
			# checks the latest diff and sets the new path
			n = init.getLatestDiffNumber(f_relative) + 1
			diff = init.prefix_diffs + f_relative + '/' + str(n).zfill(10)
			init.storage.addFileFromBinaryGlobal(diff,io.BytesIO("".encode('ascii')))

			commit = {
				'key'	: f_relative,
				'diff'	: diff,
				'version' : n,
				'type'	: 'remove',
				'date'	: old_lm[idx],
				'source': username,
				'comment' : 'removed '+ f_relative+comment,
			}

			print('-- removed '+ f_relative)

			toremove.append(f_relative)

			commitpath = init.prefix_commit + f_relative + '/' + str(n).zfill(10)
			commits.append(commitpath)
			init.storage.addFileFromBinaryGlobal(commitpath,io.BytesIO(json.dumps(commit).encode('ascii')))
			updateFileHistory(init,f_relative,commit)

	init.storage.resetBuffer()

	old_rel_files = [x.replace(init.prefix_curr,"") for x in old_files]

	for idx, f in enumerate(new_files):
		# checks if the file was modified or remove
		if not (f in old_rel_files):
			start_time = time.time()

			# checks the latest diff and sets the new path
			n = init.getLatestDiffNumber(f) + 1
			diff = init.prefix_diffs + f + '/' + str(n).zfill(10)

			storeDiff(init.storage,diff,f,f)

			commit = {
				'key'	: f,
				'diff'	: diff,
				'version' : n,
				'type'	: 'add',
				'date'	: new_lm[idx],
				'source': username,
				'comment' : 'added '+ f+comment,
			}

			print('-- added '+ f)			

			toadd.append(f)

			commitpath = init.prefix_commit + f + '/' + str(n).zfill(10)
			commits.append(commitpath)
			init.storage.addFileFromBinaryGlobal(commitpath,io.BytesIO(json.dumps(commit).encode('ascii')))
			updateFileHistory(init,f,commit)
	init.storage.resetBuffer()	

	# updates the current version in .stack
	if len(commits):
		init.copyCurrentCommit(toadd,toremove)
		updateHistory(init,commits)
	init.storage.resetBuffer()
	return True

def computeDiff(bin1,bin2):
	"""
	Get unified string diff between two strings. Trims top two lines.
	Returns empty string if strings are identical.
	"""
	_no_eol = "\ No newline at end of file"
	diffs = difflib.unified_diff(bin1.splitlines(True),bin2.splitlines(True),n=0)
	try: 
		_,_ = next(diffs),next(diffs)
	except StopIteration: 
		pass
	return ''.join([d if d[-1] == '\n' else d+'\n'+_no_eol+'\n' for d in diffs])

def applyDiff(bin1,patch):
	s = s.splitlines(True)
	p = patch.splitlines(True)
	t = ''
	i = sl = 0
	_hdr_pat = re.compile("^@@ -(\d+),?(\d+)? \+(\d+),?(\d+)? @@$")
	(midx,sign) = (1,'+') if not revert else (3,'-')
	while i < len(p) and p[i].startswith(("---","+++")): 
		i += 1 # skip header lines
	while i < len(p):
		m = _hdr_pat.match(p[i])
		if not m: 
			raise Exception("Bad patch -- regex mismatch [line "+str(i)+"]")
		l = int(m.group(midx))-1 + (m.group(midx+1) == '0')
		if sl > l or l > len(s):
		  raise Exception("Bad patch -- bad line num [line "+str(i)+"]")
		t += ''.join(s[sl:l])
		sl = l
		i += 1
		while i < len(p) and p[i][0] != '@':
			if i+1 < len(p) and p[i+1][0] == '\\':
				line = p[i][:-1]
				i += 2
			else: 
				line = p[i]
				i += 1
			if len(line) > 0:
				if line[0] == sign or line[0] == ' ':
					t += line[1:]
				sl += (line[0] != sign)
	t += ''.join(s[sl:])
	return t

def storeDiff(storage, diff_path, key_old, key_new):
	# loads files
	# bin_old = storage.loadFileGlobal(key_old)
	# bin_new = storage.loadFileGlobal(key_new)

	# diff surrogate, save latest snapshot
	storage.copyFileGlobal(key_new,diff_path)
	return True

def updateHistory(init,commits):
	# loads file global
	metapath = init.prefix_meta+'history.json'
	history = json.load(init.storage.loadFileGlobal(metapath))
	init.storage.resetBuffer()

	# adds to the latest commits
	time = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
	history[str(len(history)+1)] = {'commits': commits, 'date': time}

	init.storage.addFileFromBinaryGlobal(metapath,io.BytesIO(json.dumps(history).encode('ascii')))
	init.storage.resetBuffer()
	return True

def updateFileHistory(init,key,commit):
	# loads file global
	metapath = init.prefix_history+key+'/history.json'
	if init.storage.checkIfEmpty(init.prefix_history): 
		history = {}
		history[1] = commit
	else:
		history = json.load(init.storage.loadFileGlobal(metapath))
		history[str(len(history)+1)] = commit
		init.storage.resetBuffer()
		
	init.storage.addFileFromBinaryGlobal(metapath,io.BytesIO(json.dumps(history).encode('ascii')))
	init.storage.resetBuffer()
	return True

def add(init, files=[], location='?'):
	if location == '?':
		location = ''
	
	# adds each file
	for file in files:
		print('adding '+file+'...')
		init.storage.addFile(file, '', location)
	init.storage.resetBuffer()
	return True

def add_from_binary(init, filename='', binary='', location='?'):
	if location == '?':
		location = ''
	
	# adds each file
	print('adding '+filename+'...')
	init.storage.addFileFromBinary(filename, binary)
	init.storage.resetBuffer()
	return True

def remove(init, files=[], location='?'):
	if location == '?':
		location = ''

	# adds each file
	for file in files:
		print('removing file '+location+file+'...')
		init.storage.removeFile(location+file)
	return True

def removeGlobal(init, files=[], location='?'):
	if location == '?':
		location = ''

	# adds each file
	for file in files:
		print('removing file '+location+file+'...')
		init.storage.removeFileGlobal(location+file)
	return True

def remove_full(init, key):
	# diff location
	n_versions = init.getLatestDiffNumber(key)
	init.storage.removeFile(key)
	for version in range(1,n_versions+1):
		diff = init.prefix_diffs + key + '/' + str(version).zfill(10)

		# generates an empty diff
		init.storage.addFileFromBinaryGlobal(diff,io.BytesIO("".encode('ascii')))
		init.storage.resetBuffer()

	return True

def remove_diff(init, key, version):
	# diff location
	diff = init.prefix_diffs + key + '/' + str(version).zfill(10)

	# generates an empty diff
	init.storage.addFileFromBinaryGlobal(diff,io.BytesIO("".encode('ascii')))
	init.storage.resetBuffer()
	return True

def revertFile(init, key, version):
	# finds the commit version
	diff = init.prefix_diffs + key + '/' + str(version).zfill(10)
	init.storage.copyFileGlobal(diff,key)
	print('reverted file ' + key + ' to version ' + str(version))
	return True

def revertCommit(init, target_version):
	if int(target_version) == 0:
		return False

	# finds the commit version
	metapath = init.prefix_meta+'history.json'
	history = json.load(init.storage.loadFileGlobal(metapath))
	
	print('reverting to version' + str(target_version))

	for i in range(len(history),int(target_version),-1):
		print('checking version' + str(i))
		for commit_ in history[str(i)]['commits']:
			cmit = json.load(init.storage.loadFileGlobal(commit_))
			init.storage.resetBuffer()
			if cmit['type'] == 'add':
				removeGlobal(init, [cmit['key']])
			elif cmit['type'] == 'remove':
				revertFile(init,cmit['key'],cmit['version']-1)
			else:
				revertFile(init,cmit['key'],cmit['version'])
			init.storage.resetBuffer()
	return True

def get_key_history(init, key):
	# finds the commit version
	metapath = init.prefix_history+key+'/history.json'
	history = json.load(init.storage.loadFileGlobal(metapath))
	return history

def pull(init, files=[],version='current'):

	if version == 'current':
		# saves each file
		for key in files:
			newFile = open(os.path.basename(key), "wb")
			binary = init.storage.loadFileGlobal(key)
			newFile.write(binary.read())
	else:
		gtfo = False
		# finds the commit of interest
		metapath = init.prefix_meta+'history.json'
		history = json.load(init.storage.loadFileGlobal(metapath))
		for key in files:
			if key[-1] == '/':
				print('Do not pull directories')
				return False
			for i in range(len(history),int(version)-1,-1):
				for commit in history[str(i)]['commits']:
					# reads each file version
					cmit = json.load(init.storage.loadFileGlobal(commit))
					if str(cmit['version']) == version and cmit['key'] == key:
						if cmit['type'] != 'remove':
							newFile = open(os.path.basename(key), "wb")
							key = init.prefix_diffs + key + '/' + str(cmit['version']).zfill(10)
							newFile.write(init.storage.loadFileGlobal(key).read())
							init.storage.resetBuffer()
						gtfo = True
					if gtfo:
						break
			if gtfo:
				gtfo = False
				break

	init.storage.resetBuffer()

	return True

def printHistory(init):
	# loads file global
	metapath = init.prefix_meta+'history.json'
	history = json.load(init.storage.loadFileGlobal(metapath))
	init.storage.resetBuffer()

	# prints history
	for i in range(len(history),0,-1):
		print('Commit: '+str(i)+' Date: '+ history[str(i)]['date'])
		idx = 0
		for commit in history[str(i)]['commits']:
			# reads each file version
			cmit = json.load(init.storage.loadFileGlobal(commit))
			print('-- '+cmit['comment']+' by '+cmit['source'])
			idx += 1
			if idx > 4:
				print('-- ('+str(len(history[str(i)]['commits'])-idx)+' more changes not showed)')
				break

	init.storage.resetBuffer()

def printDiff(init, v2, v1, file=''):
	# loads file global
	metapath = init.prefix_meta+'history.json'
	history = json.load(init.storage.loadFileGlobal(metapath))
	init.storage.resetBuffer()

	v2 = int(v2)
	v1 = int(v1) 

	if file == '':
		# prints history
		print('Comparing commit '+str(v2)+' and commit '+str(v1))
		print('Commit: '+str(v2)+' Date: '+ history[str(v2)]['date'])
		print('Commit: '+str(v1)+' Date: '+ history[str(max(v1,1))]['date'])

		for i in range(v2,v1,-1):
			for commit in history[str(i)]['commits']:
				# reads each file version
				cmit = json.load(init.storage.loadFileGlobal(commit))
				print('-- '+cmit['comment']+' by '+cmit['source'])
	else:
		print('Comparing file '+file+' between commit '+str(v2)+' and commit '+str(v1))
		idx = 0;
		# prints history
		for i in range(v2,v1,-1):
			for commit in history[str(i)]['commits']:
				# reads each file version
				cmit = json.load(init.storage.loadFileGlobal(commit))
				# print(cmit['comment']+' by '+cmit['source'])
				if cmit['key'] == init.storage.dataset+file:
					print('Version: '+str(cmit['version'])+' Date: '+ history[str(i)]['date'])
					print('-- '+cmit['comment']+' by '+cmit['source'])
				if str(cmit['version']) == '1':
					init.storage.resetBuffer()
					return True
					break
		init.storage.resetBuffer()

def printStatus(init):
	# loads file global
	metapath = init.prefix_meta+'current.json'
	current = json.load(init.storage.loadFileGlobal(metapath))
	init.storage.resetBuffer()
	if len(current['keys']) > 0:
		print('List of files in last commit:')
		for i in range(len(current['keys'])):
			print('\t-- '+current['keys'][i] + '\tlast modified: '+str(current['lm'][i]))

		init.storage.resetBuffer()
	else:
		print('everything is ok!')
		print('please add or commit a file')