from src.core.init import Initializer
from src.core.core import *
from src.user.user import User
from src.user.license.license import License
from src.storage.classes.gcs import GCSBucket
from src.storage.classes.local import Local
from pathlib import Path
import pickle

import argparse

class CLI(object):
	"""docstring for CLI"""
	def __init__(self):
		super(CLI, self).__init__()
		if Path(str(Path.home())+'/config.stack').exists():
			file2 = open(str(Path.home())+'/config.stack', 'rb')
			config = pickle.load(file2)
			file2.close()
			if config['type'] == 'local':
				cloud = Local()
				cloud.createDataset(config['dataset'])
				self.Initializer = Initializer(cloud)
			elif config['type'] == 's3':
				cloud = S3Bucket(config['bucket'])
				cloud.connectBucket()
				cloud.createDataset(config['dataset'])
				self.Initializer = Initializer(cloud)
			elif config['type'] == 'gcs':
				cloud = GCSBucket(config['bucket'])
				cloud.connectBucket()
				cloud.createDataset(config['dataset'])
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
			print('initializing dataset in ' + storage.lower())
			if storage == None:
				storage = input('Specify your storage location (path or URI): ')
			if 's3' in storage.lower():
				bucket_data = storage.split("/")[1:]
				config['bucket'] = bucket_data[1]
				config['dataset'] = bucket_data[2]+'/'
				config['type'] = 's3'
			elif 'gcs' in storage.lower():
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

			# creates dataset
			return True

	def add(self, path, subpath=''):
		if len(subpath)>1:
			if subpath[-1] != '/':
				subpath = subpath + '/'
		add(self.Initializer,[path],subpath)
		return True

	def pull(self, file, version = 'current'):
		if not self.Initializer.storage.dataset in file:
			file = self.Initializer.storage.dataset + file
		pull(self.Initializer, [file], version)
		return True

	def status(self):
		printStatus(self.Initializer)
		return True

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
			self.Initializer.removeSetup()
			self.Initializer.setupDataset()
			print('reset complete')
		return True

	def revert(self, version):
		revert(self.Initializer, int(version))
		return True

	def revertFile(self, key, version):
		if self.Initializer.storage.dataset in key:
			revertFile(self.Initializer, key, int(version))
		else:
			revertFile(self.Initializer, self.Initializer.storage.dataset+key, int(version))
		return True

	def history(self):
		printHistory(self.Initializer)

	def diff(self, v1, v0, file=''):
		printDiff(self.Initializer, v1, v0, file)
		return True

if __name__ == '__main__':
	# parses arguments
	parser = argparse.ArgumentParser(description='Stack command-line interface.')
	parser.add_argument("command", nargs='?', help="Command to call(init, add, remove, commit, revert, history, diff, reset)", type=str, default='init')
	parser.add_argument("options", nargs='*', help="options for the command-line (path, versions)", type=str, default=[''])
	args = parser.parse_args()

	# starts a command line tool
	cli = CLI()

	if args.command == 'init':
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
			cli.revertFile(args.options[0],args.options[1])
		else:
			cli.revert(args.options[0])

	elif args.command == 'history':
		cli.history()