import socket
import string
import json
import io
from datetime import datetime
import time
from pathlib import Path
import os
from src.comm.docker_ver import *
path_home = os.getenv('LCP_DKR')+'/' if docker_ver() else str(Path.home())

class Initializer(object):
	"""docstring for Initializer"""
	def __init__(self, storage ,user=None):
		super(Initializer, self).__init__()
		self.storage = storage
		self.dataset = storage.load_dataset()
		self.user = user
		self.schema = 'Files'
		
		# prefixes
		if self.storage.type == 'local':
			self.prefix_meta = path_home + '/.stack/' +  self.storage.dataset
			self.prefix_commit = path_home + '/.stack/' + self.storage.dataset + 'commits/'
			self.prefix_history = path_home + '/.stack/' + self.storage.dataset + 'history/'
			self.prefix_diffs = path_home + '/.stack/' + self.storage.dataset + 'diffs/'
		else:
			self.prefix_meta = '.stack/'+self.storage.dataset
			self.prefix_commit = '.stack/'+self.storage.dataset+'commits/'
			self.prefix_history = '.stack/'+self.storage.dataset+'history/'
			self.prefix_diffs = '.stack/'+self.storage.dataset+'diffs/'

		self.start_check()

	def remove_setup(self):
		if self.storage.type == 'local':
			self.storage.remove_file_global(path_home+'/.stack/')
		else:
			self.storage.remove_file_global('.stack/')
		return True

	def start_check(self):
		# checks if the dataset exists
		if not self.verify_setup():
			self.setup_dataset()
		return True

	def verify_setup(self):
		# verifies if a setup .stack/ directory exists
		if self.storage.check_if_empty(self.prefix_meta):
			return False

		if self.storage.check_if_empty(self.prefix_diffs):
			return False

		if self.storage.check_if_empty(self.prefix_commit):
			return False

		return True

	def setup_dataset(self):
		# performs all key operations
		self.copy_current()
		self.setup_diffs()
		self.setup_commits()
		self.setup_history()
		
		return True

	def copy_current_commit(self, toadd, todelete):
		# saves the metadata of the backup
		metapath = self.prefix_meta + 'current.json'
		
		keys, lm = self.storage.load_dataset_list()
		current = {'keys': keys, 'lm': lm}
		self.storage.add_file_from_binary_global(metapath,io.BytesIO(json.dumps(current).encode('ascii')))
		self.storage.reset_buffer()
		return True

	def copy_current(self):
		# backup of the current version to keep track of changes
		metapath = self.prefix_meta + 'current.json'
		keys, lm = self.storage.load_dataset_list()
		current = {'keys': keys, 'lm': lm}
		self.storage.add_file_from_binary_global(metapath,io.BytesIO(json.dumps(current).encode('ascii')))
		self.storage.reset_buffer()
		return True


	def setup_diffs(self):
		# adds all files, creates a diff for each
		# located in .stack/meta/diffs/abcde/DIFF_ID

		# stores the diffs of the first commit
		for file in self.dataset:
			# TODO: let's name the diffs with just numbering for now
			diff = self.prefix_diffs + file['key'] + '/' + str(1).zfill(10)
			self.storage.copy_file_global(file['key'],diff)		
		return True

	def setup_history(self):
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
			
			prefix_commit = self.prefix_commit

			commitpath = prefix_commit + file['key'] + '/' + str(1).zfill(10)
			commits.append(commitpath)

		time = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
		history = {1: {'commits': commits, 'date': time}}
		metapath = self.prefix_meta+'history.json'
		self.storage.add_file_from_binary_global(metapath,io.BytesIO(json.dumps(history).encode('ascii')))

		return True

	def setup_commits(self):
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
			self.storage.add_file_from_binary_global(commitpath,io.BytesIO(json.dumps(commit).encode('ascii')))
			
			self.storage.reset_buffer()
			
			histpath = self.prefix_history + file['key'] + '/history.json'
			self.storage.add_file_from_binary_global(histpath,io.BytesIO(json.dumps(history).encode('ascii')))
			self.storage.reset_buffer()

		self.storage.reset_buffer()
		
		return True

	def get_latest_diff_number(self, key):
		# checks all the diffs
		diff_path, _ = self.storage.load_list_in_path(self.prefix_diffs + key + '/')
		
		# gets the list in number
		diff_path = [int(x.replace(self.prefix_diffs + key + '/','')) for x in diff_path]
		
		if len(diff_path):
			return max(diff_path)
		else:
			return 0
	
	def get_list_to_compare(self):
		# get new files
		new_files, new_lm = self.storage.load_dataset_list()
		metapath = self.prefix_meta + 'current.json'
		current = json.load(self.storage.load_file_global(metapath))

		return new_files, new_lm, current['keys'], current['lm']

def main():
	import sys
	sys.path.append( '../..' )

	from src.storage.classes.s3 import S3Bucket
	
	cloud = S3Bucket('stacktest123')
	cloud.connect_bucket()
	cloud.create_dataset('dataset1/')
	print('dataset created')

	init = Initializer(cloud)
	print('init created')

	init.setup_diffs()
	print('first diffs added')

	init.setup_commits()
	print('first commits added')

	init.setup_history()
	print('first change added')

	init.storage.copy_file('image.png','image2.png')
	
	nf,_,of,_ = init.get_list_to_compare()
	print('list of new files')
	print(nf)

	print('list of old files')
	print(of)

if __name__ == '__main__':
	main()
