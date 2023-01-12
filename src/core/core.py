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
from pathlib import Path
from src.comm.docker_ver import *
path_home = os.getenv('LCP_DKR')+'/' if docker_ver() else str(Path.home())

def commit(init, comment = ''):
	# get new files
	# print('reading the data list')
	# TODO: Oprimize
	t0 = time.time()
	new_files, new_lm, old_files, old_lm = init.get_list_to_compare()
	# print(f'time get data list {time.time() - t0}s for {len(old_files)} datapoints')

	if comment != '':
		comment = ', ' + comment

	init.storage.reset_buffer()
	# checks who is the user
	idx = 0
	if init.user == None:
		try:
			username = socket.gethostbyname(socket.gethostname())
		except:
			username = 'unknown'
	else:
		username = init.user.username

	commits = []
	toadd = []
	toremove = []

	added = []
	modified = []
	removed = []
	# print('starting a commit')
	t0 = time.time()

	# new_files_s = set(new_files)

	for idx, f in enumerate(old_files):
		# relative file location
		f_relative = f

		# checks if the file was modified or remove
		try:
			idx_ = new_files.index(f_relative)
		except:
			idx_ = -1

		if idx_ > -1:
			if new_lm[idx_] != old_lm[idx]:
				# checks the latest diff and sets the new path
				n = init.get_latest_diff_number(f_relative) + 1
				diff = init.prefix_diffs + f_relative + '/' + str(n).zfill(10)

				# computes the diff and stores it
				if init.storage.type == 'local':
					store_diff(init.storage,diff,f_relative,f)
				else:
					store_diff(init.storage,diff,f_relative,f)

				commit = {
					'key'	: f_relative,
					'diff'	: diff,
					'version' : n,
					'type'	: 'modified',
					'date'	: new_lm[idx_],
					'source': username,
					'comment' : 'modified '+ f_relative + comment,
				}

				print('-- modified '+ f_relative)

				toadd.append(f_relative)
				modified.append(f_relative)
	
				commitpath = init.prefix_commit + f_relative + '/' + str(n).zfill(10)
				commits.append(commitpath)
				init.storage.add_file_from_binary_global(commitpath,io.BytesIO(json.dumps(commit).encode('ascii')))
				update_file_history(init,f_relative,commit)
		else:
			# checks the latest diff and sets the new path
			n = init.get_latest_diff_number(f_relative) + 1
			diff = init.prefix_diffs + f_relative + '/' + str(n).zfill(10)
			init.storage.add_file_from_binary_global(diff,io.BytesIO("".encode('ascii')))

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
			removed.append(f_relative)

			prefix_commit = init.prefix_commit

			commitpath = prefix_commit + f_relative + '/' + str(n).zfill(10)
			commits.append(commitpath)
			init.storage.add_file_from_binary_global(commitpath,io.BytesIO(json.dumps(commit).encode('ascii')))
			update_file_history(init,f_relative,commit)

	# print(f'time to do first comparisons {time.time() - t0}s for {len(old_files)} datapoints')
	init.storage.reset_buffer()

	old_rel_files_s = set(old_files)

	for idx, f in enumerate(new_files):
		# checks if the file was modified or remove
		if not (f in old_rel_files_s):
			if not '.DS_Store' in f:
				# checks the latest diff and sets the new path
				n = init.get_latest_diff_number(f) + 1
				diff = init.prefix_diffs + f + '/' + str(n).zfill(10)

				if init.storage.type == 'local':
					store_diff(init.storage,diff,f,f)
				else:
					store_diff(init.storage,diff,f,f)

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
				added.append(f)

				prefix_commit = init.prefix_commit
				commitpath = prefix_commit + f + '/' + str(n).zfill(10)
				commits.append(commitpath)
				init.storage.add_file_from_binary_global(commitpath,io.BytesIO(json.dumps(commit).encode('ascii')))
				update_file_history(init,f,commit)
	init.storage.reset_buffer()	

	# print(f'time to do all comparisons {time.time() - t0}s for {len(old_files)} datapoints')

	# updates the current version in .stack
	if len(commits):
		init.copy_current()
		update_history(init,commits)
	init.storage.reset_buffer()

	return (len(commits) > 0), added, modified, removed

def add_version(init, label=''):
		keys = {}
		dataset = init.storage.load_dataset()
		metapath = init.prefix_versions + 'versions.json'
		history = json.load(init.storage.load_file_global(metapath))
		ver_path = init.prefix_versions + f'/{len(history.keys())}.json'
		
		for file in dataset:
			ver = init.get_latest_diff_number(file['key'])
			keys[file['key']] = init.prefix_diffs + file['key'] + '/' + str(ver).zfill(10)

		init.storage.add_file_from_binary_global(ver_path,io.BytesIO(json.dumps(keys).encode('ascii')))
		time = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
		
		if label == '':
			label = f'Checkpoint {len(history.keys())}'

		history[str(len(history.keys()))] = {'path': ver_path, 'schema_path': '', 'date': time, 'label': label}
		init.storage.add_file_from_binary_global(metapath,io.BytesIO(json.dumps(history).encode('ascii')))

def compute_diff(bin1,bin2):
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

def apply_diff(s,patch):
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

def store_diff(storage, diff_path, key_old, key_new):
	# loads files
	# bin_old = storage.load_file_global(key_old)
	# bin_new = storage.load_file_global(key_new)

	# diff surrogate, save latest snapshot
	storage.copy_file_global(key_new,diff_path)
	return True

def update_history(init,commits):
	# loads file global
	metapath = init.prefix_meta+'history.json'
	history = json.load(init.storage.load_file_global(metapath))
	init.storage.reset_buffer()

	# adds to the latest commits
	time = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
	history[str(len(history)+1)] = {'commits': commits, 'date': time}

	init.storage.add_file_from_binary_global(metapath,io.BytesIO(json.dumps(history).encode('ascii')))
	init.storage.reset_buffer()
	return True

def update_file_history(init,key,commit):
	# loads file global
	metapath = init.prefix_history+key+'/history.json'
	if init.storage.check_if_empty(init.prefix_history+key+'/'): 
		history = {}
		history[1] = commit
	else:
		history = json.load(init.storage.load_file_global(metapath))
		history[str(len(history)+1)] = commit
		init.storage.reset_buffer()
		
	init.storage.add_file_from_binary_global(metapath,io.BytesIO(json.dumps(history).encode('ascii')))
	init.storage.reset_buffer()
	return True

def add(init, files=[], location='?'):
	if location == '?':
		location = ''
	
	# adds each file
	for file in files:
		print('adding '+file+'...')
		init.storage.add_file(file, '', location)
	init.storage.reset_buffer()
	return True

def add_from_binary(init, filename='', binary='', location='?'):
	if location == '?':
		location = ''
	
	# adds each file
	print('adding '+filename+'...')
	init.storage.add_file_from_binary(filename, binary)
	init.storage.reset_buffer()
	return True

def rename_file(init, key, new_key):
	# renames the files
	init.storage.copy_file(key,new_key)
	init.storage.remove_file(key)

	# renames the diffs
	n_vers = init.get_latest_diff_number(key)
	for v in range(1,n_vers+1):
		prv_diff = init.prefix_diffs + key + '/' + str(v).zfill(10)
		nxt_diff = init.prefix_diffs + new_key + '/' + str(v).zfill(10)
		init.storage.copy_file_global(prv_diff,nxt_diff)
		init.storage.remove_file_global(prv_diff)

	prv_diff = init.prefix_history + key + '/history.json'
	nxt_diff = init.prefix_history + new_key + '/history.json'
	init.storage.copy_file_global(prv_diff,nxt_diff)

	print('rename file ' + key + ' to ' + new_key)
	return True

def copy_file_to_new_dataset(init, key, new_dataset):
	# renames the files
	init.storage.copy_file_global(init.storage.dataset+key,new_dataset+key)

	# renames the diffs
	n_vers = init.get_latest_diff_number(key)
	for v in range(1,n_vers+1):
		prv_diff = init.prefix_diffs + key + '/' + str(v).zfill(10)
		nxt_diff = init.prefix_diffs.replace(init.storage.dataset,new_dataset) + key + '/' + str(v).zfill(10)
		init.storage.copy_file_global(prv_diff,nxt_diff)
		
	prv_diff = init.prefix_history + key + '/history.json'
	nxt_diff = init.prefix_history.replace(init.storage.dataset,new_dataset) + key + '/history.json'
	init.storage.copy_file_global(prv_diff,nxt_diff)

	print('rename file ' + key + ' to ' + new_dataset)
	return True

def move_file_to_dataset(init, key, new_dataset):
	# renames the files
	init.storage.copy_file_global(init.storage.dataset+key,new_dataset+key)
	init.storage.remove_file_global(init.storage.dataset+key)

	# renames the diffs
	n_vers = init.get_latest_diff_number(key)
	for v in range(1,n_vers+1):
		prv_diff = init.prefix_diffs + key + '/' + str(v).zfill(10)
		nxt_diff = init.prefix_diffs.replace(init.storage.dataset,new_dataset) + key + '/' + str(v).zfill(10)
		init.storage.copy_file_global(prv_diff,nxt_diff)
		init.storage.remove_file_global(prv_diff)

	prv_diff = init.prefix_history + key + '/history.json'
	nxt_diff = init.prefix_history.replace(init.storage.dataset,new_dataset) + key + '/history.json'
	init.storage.copy_file_global(prv_diff,nxt_diff)
	init.storage.remove_file_global(prv_diff)

	print('moving file ' + key + ' to ' + new_dataset)
	return True

def remove(init, files=[], location='?'):
	if location == '?':
		location = ''

	# adds each file
	for file in files:
		print('deleting file '+location+file+'...')
		init.storage.remove_file(location+file)
	return True

def remove_global(init, files=[], location='?'):
	if location == '?':
		location = ''

	# adds each file
	for file in files:
		print('deleting file '+location+file+'...')
		init.storage.remove_file_global(location+file)
	return True

def remove_full(init, key):
	# diff location
	n_versions = init.get_latest_diff_number(key)
	init.storage.remove_file(key)
	for version in range(1,n_versions+1):
		diff = init.prefix_diffs + key + '/' + str(version).zfill(10)

		# generates an empty diff
		init.storage.add_file_from_binary_global(diff,io.BytesIO("".encode('ascii')))
		init.storage.reset_buffer()

	return True

def remove_diff(init, key, version):
	# diff location
	diff = init.prefix_diffs + key + '/' + str(version).zfill(10)

	# generates an empty diff
	init.storage.add_file_from_binary_global(diff,io.BytesIO("".encode('ascii')))
	init.storage.reset_buffer()
	return True

def revert_file(init, key, version):
	# finds the commit version
	diff = init.prefix_diffs + key + '/' + str(version).zfill(10)
	init.storage.copy_file_global(diff,key)

	print('reverted file ' + key + ' to version ' + str(version))
	return True

def revert_commit(init, target_version):
	if int(target_version) == 0:
		return False

	# finds the commit version
	metapath = init.prefix_meta+'history.json'
	history = json.load(init.storage.load_file_global(metapath))

	print('reverting dataset to version ' + str(target_version))

	for i in range(len(history),int(target_version),-1):
		for commit_ in history[str(i)]['commits']:
			cmit = json.load(init.storage.load_file_global(commit_))
			init.storage.reset_buffer()
			if cmit['type'] == 'add':
				remove_global(init, [cmit['key']])
			elif cmit['type'] == 'remove':
				revert_file(init,cmit['key'],cmit['version']-1)
			else:
				revert_file(init,cmit['key'],cmit['version']-1)
			init.storage.reset_buffer()
	return True

def get_key_history(init, key):
	# finds the commit version
	metapath = init.prefix_history+key+'/history.json'
	history = json.load(init.storage.load_file_global(metapath))
	return history

def pull(init, files=[],version='current'):

	if version == 'current':
		# saves each file
		for key in files:
			newFile = open(os.path.basename(key), "wb")
			binary = init.storage.load_file_global(key)
			newFile.write(binary.read())
	else:
		gtfo = False
		# finds the commit of interest
		metapath = init.prefix_meta+'history.json'
		history = json.load(init.storage.load_file_global(metapath))
		for key in files:
			if key[-1] == '/':
				print('Do not pull directories')
				return False
			for i in range(len(history),int(version)-1,-1):
				for commit in history[str(i)]['commits']:
					# reads each file version
					if init.storage.type == 'local':
						cmit = json.load(init.storage.load_file_global(commit))
					else:
						cmit = json.load(init.storage.load_file_global(commit))
					if str(cmit['version']) == version and cmit['key'] == key:
						if cmit['type'] != 'remove':
							newFile = open(os.path.basename(key), "wb")
							key = init.prefix_diffs + key + '/' + str(cmit['version']).zfill(10)
							newFile.write(init.storage.load_file_global(key).read())
							init.storage.reset_buffer()
						gtfo = True
					if gtfo:
						break
			if gtfo:
				gtfo = False
				break

	init.storage.reset_buffer()

	return True

def print_history(init):
	# loads file global
	metapath = init.prefix_meta+'history.json'
	history = json.load(init.storage.load_file_global(metapath))
	init.storage.reset_buffer()

	print('\n--------------------------')
	print('- History of the dataset -')
	print('--------------------------\n')

	# prints history
	for i in range(len(history),0,-1):
		print('Version: '+str(i)+' Date: '+ history[str(i)]['date'])
		idx = 0
		for commit in history[str(i)]['commits']:
			# reads each file version
			if init.storage.type == 'local':
				cmit = json.load(init.storage.load_file_global(commit))
			else:
				cmit = json.load(init.storage.load_file_global(commit))
			print('-- '+cmit['comment']+' by '+cmit['source'])
			idx += 1
			if idx > 4:
				print('-- ('+str(len(history[str(i)]['commits'])-idx)+' more changes not showed)')
				break

	print('')

	init.storage.reset_buffer()

def print_diff(init, v2, v1, file=''):
	# loads file global
	metapath = init.prefix_meta+'history.json'
	history = json.load(init.storage.load_file_global(metapath))
	init.storage.reset_buffer()

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
				if init.storage.type == 'local':
					cmit = json.load(init.storage.load_file_global(commit))
				else:
					cmit = json.load(init.storage.load_file_global(commit))
				print('-- '+cmit['comment']+' by '+cmit['source'])
	else:
		print('Comparing file '+file+' between commit '+str(v2)+' and commit '+str(v1))
		idx = 0
		# prints history
		for i in range(v2,v1,-1):
			for commit in history[str(i)]['commits']:
				# reads each file version
				if init.storage.type == 'local':
					cmit = json.load(init.storage.load_file_global(commit))
				else:
					cmit = json.load(init.storage.load_file_global(commit))
				# print(cmit['comment']+' by '+cmit['source'])
				if cmit['key'] == init.storage.dataset+file:
					print('Version: '+str(cmit['version'])+' Date: '+ history[str(i)]['date'])
					print('-- '+cmit['comment']+' by '+cmit['source'])
				if str(cmit['version']) == '1':
					init.storage.reset_buffer()
					return True
					break
		init.storage.reset_buffer()