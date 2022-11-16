from src.core.init import Initializer
from src.core.core import *
from src.user.user import User
from src.user.license.license import License
from src.storage.classes.s3 import S3Bucket
from src.storage.classes.gcs import GCSBucket
from src.storage.classes.local import Local
from pathlib import Path
import pickle

import argparse

class CLI(object):
	"""docstring for CLI"""
	def __init__(self, reset=False):
		super(CLI, self).__init__()
		if reset:
			self.Initializer = None
		elif Path(str(Path.home())+'/config.stack').exists():
			file2 = open(str(Path.home())+'/config.stack', 'rb')
			config = pickle.load(file2)
			file2.close()
			if config['type'] == 'local':
				cloud = Local()
				cloud.create_dataset(config['dataset'])
				self.Initializer = Initializer(cloud)
			elif config['type'] == 's3':
				cloud = S3Bucket(config['bucket'])
				cloud.connect_bucket()
				cloud.create_dataset(config['dataset'])
				self.Initializer = Initializer(cloud)
			elif config['type'] == 'gcs':
				cloud = GCSBucket(config['bucket'])
				cloud.connect_bucket()
				cloud.create_dataset(config['dataset'])
				self.Initializer = Initializer(cloud)
			else:
				self.Initializer = None
		else:
			self.Initializer = None
		
	def init(self, storage = None):
		# checks if another dataset exists
		do = True
		exists = Path(str(Path.home())+'/config.stack').exists()
		
		if exists:
			print('init file found in ' + str(Path.home())+'/config.stack')
			print('Do you want to initialize another dataset?')
			yn = input("[Y/n]: ")
			if yn == 'y' or yn == 'Y':
				do = True
			else:
				do = False
		if do:
			# builds a config file
			config = {}
			storage = input('Specify your storage location (path or URI): ')
			print('initializing dataset in ' + storage.lower())
			if storage == None:
				pass
			if 's3' in storage.lower():
				bucket_data = storage.split("/")[1:]
				config['bucket'] = bucket_data[1]
				config['dataset'] = bucket_data[2]+'/'
				config['type'] = 's3'
			elif 'gs' in storage.lower():
				bucket_data = storage.split("/")[1:]
				config['bucket'] = bucket_data[1]
				config['dataset'] = bucket_data[2]+'/'
				config['type'] = 'gcs'
			else:
				config['dataset'] = storage
				config['type'] = 'local'
			config['storage'] = storage
			# stores the config file
			file = open(str(Path.home())+'/config.stack', 'wb')
			pickle.dump(config,file)
			file.close()

			if not self.Initializer.verify_setup():
				self.Initializer.setup_dataset()

			# creates dataset
			return True

	def add(self, path, subpath=''):
		if len(subpath)>1:
			if subpath[-1] != '/':
				subpath = subpath + '/'
		add(self.Initializer,[path],subpath)
		return True

	def pull_all(self,version = 'current'):
		print('downloading files from last commit')
		metapath = self.Initializer.prefix_meta + 'current.json'
		current = json.load(self.Initializer.storage.load_file_global(metapath))
		pull(self.Initializer, current['keys'], version)
		return True

	def pull(self, file, version = 'current'):
		if file == '.' or file == 'all':
			return self.pull_all(version)
		else:
			if not self.Initializer.storage.dataset in file:
				file = self.Initializer.storage.dataset + file
			pull(self.Initializer, [file], version)
		return True

	def status(self):
		print_status(self.Initializer)
		return True

	def start_check(self):
		return self.Initializer.start_check()

	def remove(self, key, subpath=''):
		if len(subpath)>1:
			if subpath[-1] != '/':
				subpath = subpath + '/'
		remove(self.Initializer,[key],subpath)
		return True

	def commit(self, comment=''):
		print(comment)

		commit(self.Initializer, comment)
		print('commit done!')
		return True

	def reset(self):
		print('Are you sure you want to reset? It will delete all diffs and previous versions [y/n]')
		yn = input("[Y/n]: ")
		if yn == 'y' or yn == 'Y':
			self.Initializer.remove_setup()
			self.Initializer.setup_dataset()
			print('reset complete')
		return True

	def revert(self, version=0):
		assert(version != '')
		revert_commit(self.Initializer, int(version))
		return True

	def revert_file(self, key, version):
		if self.Initializer.storage.dataset in key:
			revert_file(self.Initializer, key, int(version))
		else:
			revert_file(self.Initializer, self.Initializer.storage.dataset+key, int(version))
		self.Initializer.storage.reset_buffer()

		return True

	def history(self):
		print_history(self.Initializer)

	def diff(self, v1, v0, file=''):
		print_diff(self.Initializer, v1, v0, file)
		return True

	def logout(self):
		print('loging you out of ' + self.Initializer.storage.type + '://' + self.Initializer.storage.BUCKET_NAME + '/' + self.Initializer.storage.dataset)
		import os
		os.remove(str(Path.home())+'/config.stack')
		return True

if __name__ == '__main__':
	# parses arguments
	parser = argparse.ArgumentParser(description='Stack command-line interface.')
	parser.add_argument("command", nargs='?', help="Command to call(init, add, remove, commit, revert, history, diff, reset)", type=str, default='status')
	parser.add_argument("options", nargs='*', help="options for the command-line (path, versions)", type=str, default=[''])
	args = parser.parse_args()

	# starts a command line tool
	try:
		cli = CLI()
	except:
		import os
		os.remove(str(Path.home())+'/config.stack')
		cli = CLI()
		cli.init()
 
	if args.command == 'init':
		print(args.options)
		if args.options[0] == '.':
			cli.init()
		else:
			cli.init(storage = args.options[0])

	elif args.command == 'status':
		cli.status()

	elif args.command == 'reset':
		cli.reset()

	elif args.command == 'add':
		if len(args.options) > 1:
			cli.add(args.options[0],args.options[1])
		else:
			cli.add(args.options[0])

	elif args.command == 'diff':
		if len(args.options) == 2:
			cli.diff(args.options[0],args.options[1])
		if len(args.options) == 3:
			cli.diff(args.options[0],args.options[1], args.options[2])

	elif args.command == 'pull':
		if len(args.options) > 1:
			cli.pull(args.options[0],args.options[1])
		else:
			cli.pull(args.options[0])

	elif args.command == 'remove':
		if len(args.options) > 1:
			cli.remove(args.options[0],args.options[1])
		else:
			cli.remove(args.options[0])

	elif args.command == 'commit' or args.command == 'push':
		if args.options[0] != '':
			cli.commit(args.options[0])
		else:
			cli.commit()

	elif args.command == 'revert':
		if len(args.options) > 1:
			cli.revert_file(args.options[0],args.options[1])
		else:
			cli.revert(args.options[0])

	elif args.command == 'history':
		cli.history()
	elif args.command == 'logout':
		cli.logout()