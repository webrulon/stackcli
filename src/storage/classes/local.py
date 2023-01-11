import sys
sys.path.append( '../../../' )
import os
import shutil
from pathlib import Path
import time
from src.comm.docker_ver import *
path_home = os.getenv('LCP_DKR') if docker_ver() else str(Path.home())

class Local(object):
	"""docstring for Storage"""
	def __init__(self):
		self.type = "local"
		self.dataset = "./"
		self.prefix_ignore = ''
		self.credentials = {}

	def create_dataset(self,location,verbose=False):
		# transforms to absolute path
		if path_home in location:
			location.replace(path_home,'')

		if location[0] == '~':
			if len(location) > 1:
				location = path_home+location[1:]
			else:
				location = path_home
		if location[0] == '/':
			location = path_home + location
		else:
			location = path_home + '/' + location

		if location[-1] != '/':
			location = location + '/'
			self.dataset = location

		if not os.path.exists(location):
			print('dataset directory does not exist')
			print('failed to connect')
			raise Exception('dataset directory does not exist')

		return False

	def check_if_empty(self,path):
		if os.path.exists(path):
			dir = os.listdir(path)
			return (len(dir) == 0)
		else:
			return True

	def add_file(self,filepath,target_name='',subpath=''):
		if filepath[-1] == '/':
			for root,dirs,files in os.walk(filepath):
				for file in files:
					if not os.path.exists(os.path.dirname(self.dataset+subpath)):
						os.makedirs(os.path.dirname(self.dataset+subpath))
					head_tail = os.path.split(os.path.join(root,file))
					shutil.copyfile(os.path.join(root,file), self.dataset+subpath+head_tail[1])	
				self.reset_buffer()
		else:
			if not os.path.exists(os.path.dirname(self.dataset+subpath)):
				os.makedirs(os.path.dirname(self.dataset+subpath))
			head_tail = os.path.split(filepath)
			shutil.copyfile(filepath, self.dataset+subpath+head_tail[1])
		return True

	def add_folder(self,folderpath):
		if not os.path.exists(self.dataset + folderpath):
			os.makedirs(self.dataset + folderpath)
		return True

	def add_file_from_binary(self,filename,f):
		return self.add_file_from_binary_global(self.dataset+filename,f)

	def add_file_from_binary_global(self,filename,f):
		if not os.path.exists(os.path.dirname(filename)):
			os.makedirs(os.path.dirname(filename))
		binary_file = open(filename, "wb")
		binary_file.write(f.read())
		binary_file.close()
		return True

	def remove_file(self,filename):
		return self.remove_file_global(self.dataset+filename)

	def remove_file_global(self,filename):
		if os.path.exists(filename):
			if filename[-1] == '/':
				shutil.rmtree(filename)
			else:
				os.remove(filename)
		return True

	def load_file(self,filename):
		return self.load_file_global(self.dataset+filename)

	def load_file_bytes(self,filename,bi,bf):
		return self.load_file_global(self.dataset+filename,bi,bf)

	def load_file_global(self,filename):
		return open(filename,'rb')

	def load_file_global_bytes(self,filename,bi,bf):
		f = open(filename, "rb")
		f.seek(bi)
		data = f.read(bf-bi)
		return data

	def load_file_metadata(self,filename):
		path = self.dataset+filename
		metadata = {
			'key' : self.dataset+filename,
			'date_loaded' : time.strftime("%m/%d/%Y, %H:%M:%S", time.gmtime(os.path.getmtime(path))),
			'date_added' : time.strftime("%m/%d/%Y, %H:%M:%S", time.gmtime(os.path.getctime(path))),
			'last_modified' : time.strftime("%m/%d/%Y, %H:%M:%S", time.gmtime(os.path.getmtime(path))),
		}
		return metadata

	def load_file_metadata_global(self,filename):
		path = filename
		metadata = {
			'key' : self.dataset+filename,
			'date_loaded' : time.strftime("%m/%d/%Y, %H:%M:%S", time.gmtime(os.path.getmtime(path))),
			'date_added' : time.strftime("%m/%d/%Y, %H:%M:%S", time.gmtime(os.path.getctime(path))),
			'last_modified' : time.strftime("%m/%d/%Y, %H:%M:%S", time.gmtime(os.path.getmtime(path))),
		}
		return metadata

	def load_dataset(self):
		file_m = []
		for root, dirs, files in os.walk(self.dataset, topdown=False):
			for name in files:
				path = os.path.join(root, name)
				if path[-1] != '/':
					metadata = {
						'key' : path,
						'last_modified' : time.strftime("%m/%d/%Y, %H:%M:%S", time.gmtime(os.path.getmtime(path)))
					}
					file_m.append(metadata)
		return file_m

	
	def load_dataset_list(self):
		return self.load_list_in_path(self.dataset)

	def load_list_in_path(self,path):
		keys = []
		last_m = []

		for root, dirs, files in os.walk(path, topdown=False):
			for name in files:
				keys.append(os.path.join(root, name))
				last_m.append(time.strftime("%m/%d/%Y, %H:%M:%S", time.gmtime(os.path.getmtime(os.path.join(root, name)))))
		return keys, last_m

	def load_files_in_dataset(self):
		files = []
		for root, dirs, files in os.walk(self.dataset, topdown=False):
			for name in files:
				path = os.path.join(root, name)
		return True

	def list_files_in_path(self,dir_path):
		for root, dirs, files in os.walk(self.dataset + dir_path, topdown=False):
			for name in files:
				path = os.path.join(root, name)
		return True

	def copy_file(self,filepath,full_target_name):
		return self.copy_file_global(self.dataset + filepath, self.dataset + full_target_name)

	def copy_file_global(self,filepath,full_target_name):
		head_tail = os.path.split(filepath)
		os.makedirs(os.path.dirname(full_target_name), exist_ok=True)
		if filepath[-1] != '/':
			shutil.copyfile(filepath, full_target_name)
		return True

	def get_size_of_file(self, filepath):
		return self.get_size_of_file_global(self.dataset + filepath)

	def get_size_of_file_global(self, filepath):
		return os.path.getsize(filepath)

	def reset_buffer(self):
		pass

def main():
	storage = Local()
	
	print('create dataset')
	storage.create_dataset('/Users/bernardo/test_data/')
	
	print('add folder')
	storage.add_folder('.stack')
	print('add subfolder')
	storage.add_folder('subtree')
	print('list in dataset')
	storage.load_files_in_dataset()
	print('list in path')
	storage.list_files_in_path('')
	
	print('adds a file')
	storage.add_file('../../tests/image.png')
	print(storage.load_file_metadata('image.png'))

	storage.copy_file('image.png',storage.dataset+'/copies/image4.png')
	storage.list_files_in_path('')

if __name__ == '__main__':
	main()