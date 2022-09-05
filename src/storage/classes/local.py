import os
import shutil
from pathlib import Path
import time

class Local(object):
	"""docstring for Storage"""
	def __init__(self):
		self.type = "local"
		self.dataset = "./"
		self.credentials = {}

	def createDataset(self,location):
		# transforms to absolute path
		if location[0] == '~':
			if len(location) > 1:
				location = str(Path.home())+location[1:]
			else:
				location = str(Path.home())
		location = str(os.path.abspath(location))
		#print('Initializing dataset at '+location)
		if location[-1] != '/':
			location = location + '/'
		if not os.path.exists(location):
			os.makedirs(location)
		self.dataset = location
		return True

	def checkIfEmpty(self,path):
		if os.path.exists(path):
			dir = os.listdir(path)
			return (len(dir) == 0)
		else:
			return True

	def addFile(self,filepath,target_name='',subpath=''):
		if filepath[-1] == '/':
			for root,dirs,files in os.walk(filepath):
				for file in files:
					if not os.path.exists(os.path.dirname(self.dataset+subpath)):
						os.makedirs(os.path.dirname(self.dataset+subpath))
					head_tail = os.path.split(os.path.join(root,file))
					shutil.copyfile(os.path.join(root,file), self.dataset+subpath+head_tail[1])	
				self.resetBuffer()
		else:
			if not os.path.exists(os.path.dirname(self.dataset+subpath)):
				os.makedirs(os.path.dirname(self.dataset+subpath))
			head_tail = os.path.split(filepath)
			shutil.copyfile(filepath, self.dataset+subpath+head_tail[1])
		return True

	def addFolder(self,folderpath):
		if not os.path.exists(self.dataset + folderpath):
			os.makedirs(self.dataset + folderpath)
		return True

	def addFileFromBinary(self,filename,f):
		return self.addFileFromBinaryGlobal(self.dataset+filename,f)

	def addFileFromBinaryGlobal(self,filename,f):
		if not os.path.exists(os.path.dirname(filename)):
			os.makedirs(os.path.dirname(filename))
		binary_file = open(filename, "wb")
		binary_file.write(f.read())
		binary_file.close()
		return True

	def removeFile(self,filename):
		return self.removeFileGlobal(self.dataset+filename)

	def removeFileGlobal(self,filename):
		if os.path.exists(filename):
			if filename[-1] == '/':
				shutil.rmtree(filename)
			else:
				os.remove(filename)
		return True

	def loadFile(self,filename):
		return self.loadFileGlobal(self.dataset+filename)

	def loadFileBytes(self,filename,bi,bf):
		return self.loadFileGlobal(self.dataset+filename,bi,bf)

	def loadFileGlobal(self,filename):
		return open(filename,'rb')

	def loadFileGlobalBytes(self,filename,bi,bf):
		f = open(filename, "rb")
		f.seek(bi)
		data = f.read(bf-bi)
		return data

	def loadFileMetadata(self,filename):
		path = self.dataset+filename
		metadata = {
			'key' : self.dataset+filename,
			'date_loaded' : time.strftime("%m/%d/%Y, %H:%M:%S", time.gmtime(os.path.getmtime(path))),
			'date_added' : time.strftime("%m/%d/%Y, %H:%M:%S", time.gmtime(os.path.getctime(path))),
			'last_modified' : time.strftime("%m/%d/%Y, %H:%M:%S", time.gmtime(os.path.getmtime(path))),
		}
		return metadata

	def loadDataset(self):
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

	
	def loadDatasetList(self):
		return self.loadListInPath(self.dataset)

	def loadListInPath(self,path):
		keys = []
		last_m = []

		for root, dirs, files in os.walk(path, topdown=False):
			for name in files:
				keys.append(os.path.join(root, name))
				last_m.append(time.strftime("%m/%d/%Y, %H:%M:%S", time.gmtime(os.path.getmtime(os.path.join(root, name)))))
		return keys, last_m

	def listFilesinDataset(self):
		files = []
		for root, dirs, files in os.walk(self.dataset, topdown=False):
			for name in files:
				path = os.path.join(root, name)
				print(path)
		return True

	def listEverythinginDataset(self):
		for root, dirs, files in os.walk(self.dataset, topdown=False):
			for name in files:
				path = os.path.join(root, name)
				print(path.name)
		return True

	def listFilesinPath(self,dir_path):
		for root, dirs, files in os.walk(self.dataset + dir_path, topdown=False):
			for name in files:
				path = os.path.join(root, name)
				print(path)
		return True

	def listEverythinginPath(self,dir_path):
		for root, dirs, files in os.walk(self.dataset + dir_path, topdown=False):
			for name in files:
				path = os.path.join(root, name)
				print(path)
		return True

	def copyFile(self,filepath,full_target_name):
		return self.copyFileGlobal(self.dataset + filepath, self.dataset + full_target_name)

	def copyFileGlobal(self,filepath,full_target_name):
		head_tail = os.path.split(filepath)
		os.makedirs(os.path.dirname(full_target_name), exist_ok=True)
		if filepath[-1] != '/':
			shutil.copyfile(filepath, full_target_name)
		return True

	def resetBuffer(self):
		pass

def main():
	storage = Local()
	
	print('create dataset')
	storage.createDataset('/Users/bernardo/test_data/')
	
	print('add folder')
	storage.addFolder('.stack')
	print('add subfolder')
	storage.addFolder('subtree')
	print('list in dataset')
	storage.listFilesinDataset()
	print('list in path')
	storage.listFilesinPath('')
	
	print('adds a file')
	storage.addFile('../../tests/image.png')
	print(storage.loadFileMetadata('image.png'))

	storage.copyFile('image.png',storage.dataset+'/copies/image4.png')
	storage.listFilesinPath('')

if __name__ == '__main__':
	main()