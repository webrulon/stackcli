import socket
import string
import json
import io
from datetime import datetime
import time
from pathlib import Path

from src.comm.docker_ver import *
path_home = '/localpath/' if docker_ver() else str(Path.home())

class Initializer(object):
	"""docstring for Initializer"""
	def __init__(self, storage ,user=None):
		super(Initializer, self).__init__()
		self.storage = storage
		self.dataset = storage.loadDataset()
		self.user = user
		
		# prefixes
		if self.storage.type == 'local':
			if not docker_ver():
				surrogate_dataset = self.storage.dataset.replace('', '/localpath/')
			else:
				surrogate_dataset = self.storage.dataset

			self.prefix_meta = path_home+'/.stack' +  surrogate_dataset
			self.prefix_curr = path_home+'/.stack' +  surrogate_dataset + 'current/'
			self.prefix_commit = path_home+'/.stack' + surrogate_dataset + 'commits/'
			self.prefix_history = path_home+'/.stack' + surrogate_dataset + 'history/'
			self.prefix_diffs = path_home+'/.stack' + surrogate_dataset + 'diffs/'
		else:
			self.prefix_meta = '.stack/'+self.storage.dataset
			self.prefix_curr = '.stack/'+self.storage.dataset+'current/'
			self.prefix_commit = '.stack/'+self.storage.dataset+'commits/'
			self.prefix_history = '.stack/'+self.storage.dataset+'history/'
			self.prefix_diffs = '.stack/'+self.storage.dataset+'diffs/'

		self.start_check()

	def defineSchema(self):
		# defines the dataset schema for exporting and visualizing
		return True

	def removeSetup(self):
		if self.storage.type == 'local':
			self.storage.removeFileGlobal(path_home+'/.stack/')
		else:
			self.storage.removeFileGlobal('.stack/')
		return True

	def start_check(self):
		# checks if the dataset exists
		if not self.verify_setup():
			self.setupDataset()
		return True

	def verify_setup(self):
		# verifies if a setup .stack/ directory exists
		if self.storage.checkIfEmpty(self.prefix_meta):
			return False

		if self.storage.checkIfEmpty(self.prefix_curr):
			return False

		if self.storage.checkIfEmpty(self.prefix_diffs):
			return False

		if self.storage.checkIfEmpty(self.prefix_commit):
			return False

		return True

	def setupDataset(self):
		# performs all key operations
		self.copyCurrent()
		self.setupDiffs()
		self.setupCommits()
		self.setupHistory()
		
		return True

	def copyCurrent(self):
		# backup of the current version to keep track of changes
		if self.storage.checkIfEmpty(self.prefix_curr):
			for file in self.dataset:
				self.storage.copyFileGlobal(file['key'],self.prefix_curr+file['key'])
		else:
			self.storage.removeFile(self.prefix_curr)
			self.dataset = self.storage.loadDataset()
			for file in self.dataset:
				self.storage.copyFileGlobal(file['key'],self.prefix_curr+file['key'])	
		self.storage.resetBuffer()

		# saves the metadata of the backup
		print('loading dataset list')
		metapath = self.prefix_meta + 'current.json'
		keys, lm = self.storage.loadDatasetList()
		current = {'keys': keys, 'lm': lm}
		self.storage.addFileFromBinaryGlobal(metapath,io.BytesIO(json.dumps(current).encode('ascii')))
		self.storage.resetBuffer()
		return True

	def copyCurrentCommit(self, toadd, todelete):
		# backup of the current version to keep track of changes
		if self.storage.checkIfEmpty(self.prefix_curr):
			for file in toadd:
				self.storage.copyFileGlobal(file,self.prefix_curr+file)
			for file in todelete:
				self.storage.removeFileGlobal(self.prefix_curr+file)
		else:
			for file in toadd:
				self.storage.copyFileGlobal(file,self.prefix_curr+file)
			for file in todelete:
				self.storage.removeFileGlobal(self.prefix_curr+file)
		self.storage.resetBuffer()

		# saves the metadata of the backup
		metapath = self.prefix_meta + 'current.json'
		print('loading dataset list')
		keys, lm = self.storage.loadDatasetList()
		current = {'keys': keys, 'lm': lm}
		self.storage.addFileFromBinaryGlobal(metapath,io.BytesIO(json.dumps(current).encode('ascii')))
		self.storage.resetBuffer()
		return True

	def setupDiffs(self):
		# adds all files, creates a diff for each
		# located in .stack/meta/diffs/abcde/DIFF_ID

		# stores the diffs of the first commit
		for file in self.dataset:
			# TODO: let's name the diffs with just numbering for now
			diff = self.prefix_diffs + file['key'] + '/' + str(1).zfill(10)
			self.storage.copyFileGlobal(file['key'],diff)		
		return True

	def setupHistory(self):
		# meta-data
		# located in .stack/meta/history.json
		""" 
			json = {
				0 : ['c1.json','c2.json'],
				1 : ['c3.json'],
			}
		"""
		commits = []
		for file in self.dataset:
			commitpath = self.prefix_commit + file['key'] + '/' + str(1).zfill(10)
			commits.append(commitpath)

		time = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
		history = {1: {'commits': commits, 'date': time}}
		metapath = self.prefix_meta+'history.json'
		self.storage.addFileFromBinaryGlobal(metapath,io.BytesIO(json.dumps(history).encode('ascii')))

		return True

	def setupCommits(self):
		# meta-data for each commit
		# located in .stack/meta/commits/abcde/COMMIT_ID
		""" 
			json = {
				'file' : 'img1_jpg',
				'id:   : 'abcdefg'
				'diff' : 'dff1',
				'type' : 'add',
				'date' : [...],
				'source' : '0.0.0.0',
				'comment' : '',
			}
		"""

		# checks who is the user
		idx = 0
		if self.user == None:
			username = socket.gethostbyname(socket.gethostname())
		else:
			username = self.user.username

		# stores the medadata of the first commit
		for file in self.dataset:
			# gets the address of the diff
			# TODO: let's name the diffs with just numbering for now
			diff = self.prefix_diffs + file['key'] + '/' + str(1).zfill(10)

			commit = {
				'key'	: file['key'],
				'diff'	: diff,
				'type'	: 'add',
				'version' : 1,
				'date'	: file['last_modified'],
				'source': username,
				'comment' : 'added '+ file['key'],
			}

			history = {}
			history[1] = commit
			commitpath = self.prefix_commit + file['key'] + '/' + str(1).zfill(10)
			self.storage.addFileFromBinaryGlobal(commitpath,io.BytesIO(json.dumps(commit).encode('ascii')))
			
			self.storage.resetBuffer()
			
			histpath = self.prefix_history + file['key'] + '/history.json'
			self.storage.addFileFromBinaryGlobal(histpath,io.BytesIO(json.dumps(history).encode('ascii')))
			self.storage.resetBuffer()

		self.storage.resetBuffer()
		
		return True

	def getLatestDiffNumber(self, key):
		# checks all the diffs
		diff_path, _ = self.storage.loadListInPath(self.prefix_diffs + key + '/')
		
		# gets the list in number
		diff_path = [int(x.replace(self.prefix_diffs + key + '/','')) for x in diff_path]
		
		if len(diff_path):
			return max(diff_path)
		else:
			return 0

	def getListtoCompare(self):
		# get new files
		new_files, new_lm = self.storage.loadDatasetList()
		metapath = self.prefix_meta + 'current.json'
		current = json.load(self.storage.loadFileGlobal(metapath))

		return new_files, new_lm, current['keys'], current['lm']

def main():
	import sys
	sys.path.append( '../..' )

	from src.storage.classes.s3 import S3Bucket
	
	cloud = S3Bucket('stacktest123')
	cloud.connectBucket()
	cloud.createDataset('dataset1/')
	print('dataset created')

	init = Initializer(cloud)
	print('init created')

	init.copyCurrent()
	print('current version copied')

	init.setupDiffs()
	print('first diffs added')

	init.setupCommits()
	print('first commits added')

	init.setupHistory()
	print('first change added')

	init.storage.copyFile('image.png','image2.png')
	
	nf,_,of,_ = init.getListtoCompare()
	print('list of new files')
	print(nf)

	print('list of old files')
	print(of)

if __name__ == '__main__':
	main()
