# from src.storage import Storage
import os
import logging
from google.cloud import storage
import maskpass
import io
from pathlib import Path


class GCSBucket(object):
	"""docstring for Storage"""
	def __init__(self, BUCKET_NAME):
		# self = Cloud().__init__()
		self.type = "gcs"
		self.dataset = ""
		self.BUCKET_NAME = BUCKET_NAME
		self.credentials = {}
		self.client = None
		self.bucket = None

	def connectBucket(self):
		# creates a client 
		key_path = str(Path.home())+'/.gs_key'
		os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key_path
		try:		
			self.client = storage.Client()
		except:
			# in case the keys are not in the computer
			print('1) Create a service account here: https://console.cloud.google.com/projectselector/iam-admin/serviceaccounts/create')
			print('2) Give it access to Cloud Storage')
			print('3) Go to your new service account/keys')
			print('4) Create a new key in .json format and save it as ~/.gs_key')
			return False

		# checks if the bucket exists
		print('Connecting to your bucket...')
		buckets = self.client.list_buckets()
		found = False
		for bucket in buckets:
			if bucket.name == self.BUCKET_NAME:
				found = True
				break

		# connects to bucket or finds another one
		if found:
			self.bucket = self.client.get_bucket(self.BUCKET_NAME)
		else:			
			print('Could not find your desired bucket...')
			print('Do you want to creat a bucket with the name '+self.BUCKET_NAME+"?")
			yn = input("[Y/n]: ")
			if yn == "y" or yn == "Y":
				self.bucket = self.client.create_bucket(self.BUCKET_NAME)
			else:
				print('pick another bucket from this list:')
				for bucket in buckets:
					print("-" + bucket.name)
				self.BUCKET_NAME = input("Enter another bucket name: ")
				return self.connectBucket()
		return True

	def connect_bucket_api(self,binary):
		# reads the gs_key
		print('creating key file')
		key_path = str(Path.home())+'/.gs_key'
		binary_file = open(key_path, "wb")
		binary_file.write(binary.read())
		binary_file.close()

		# sets env variable for google cloud
		print('exporting key')	
		os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key_path

		print('connecting to client')
		try:
			self.client = storage.Client()
		except:
			return False

		# checks if the bucket exists
		print('Connecting to your bucket...')
		buckets = self.client.list_buckets()
		for bucket in buckets:
			if bucket.name == self.BUCKET_NAME:
				self.bucket = self.client.get_bucket(self.BUCKET_NAME)
				return True
		return False

	def createDataset(self,location):
		# assigns location
		if location[-1] != '/':
			location = location + '/'
		self.dataset = location
		return True

	def addFile(self,filepath,target_name='',subpath=''):
		# adding new file
		if target_name == '':
			target_name = os.path.basename(filepath)
		if filepath[-1] == '/':
			for root,dirs,files in os.walk(filepath):
				for file in files:
					blob = self.bucket.blob(self.dataset+subpath+os.path.basename(filepath))
					blob.upload_from_filename(os.path.join(root,file))	
			self.resetBuffer()
		else:
			blob = self.bucket.blob(self.dataset+subpath+os.path.basename(filepath))
			blob.upload_from_filename(filepath)
		return True

	def addFileFromBinary(self,filename,f):
		return self.addFileFromBinaryGlobal(self.dataset+filename,f)

	def addFileFromBinaryGlobal(self,filename,f):
		blob = self.bucket.blob(filename)
		blob.upload_from_file(file_obj=f, rewind=True)
		return True

	def removeFile(self,filename):
		return self.removeFileGlobal(self.dataset+filename)

	def removeFileGlobal(self,filename):
		# deletes files in path
		if filename[-1] == '/':
			for blob in self.client.list_blobs(self.BUCKET_NAME, prefix=filename, delimiter=None):
				blob.delete()
			self.resetBuffer()
		else:
			blob = self.bucket.blob(filename)
			blob.delete()
		return True

	def loadFile(self,filename):
		return self.loadFileGlobal(self.dataset+filename)

	def loadFileBytes(self,filename,bi,bf):
		return self.loadFileGlobalBytes(self.dataset+filename,bi,bf)

	def loadFileGlobal(self,filename):
		blob = self.bucket.blob(filename)
		file_obj = blob.download_as_string()
		return io.BytesIO(file_obj)

	def loadFileGlobalBytes(self,filename,bi,bf):
		blob = self.bucket.blob(filename)
		file_obj = blob.download_as_string(start=bi,end=bf)
		return io.BytesIO(file_obj)	

	def loadFileMetadata(self,filename,debug=False):
		blob = self.bucket.get_blob(self.dataset + filename)
		if blob.name[-1] != '/':
			metadata = {
				'key' : self.dataset + filename,
				'date_loaded' : blob.time_created.strftime("%m/%d/%Y, %H:%M:%S"),
				'date_added' : blob.time_created.strftime("%m/%d/%Y, %H:%M:%S"),
				'last_modified' : blob.updated.strftime("%m/%d/%Y, %H:%M:%S"),
				'HostId' : blob.id,
			}
			return metadata

	def listFilesinPath(self,path):
		blobs = self.client.list_blobs(self.BUCKET_NAME, prefix=path, delimiter=None)
		for blob in blobs:
			print(f'-- {blob.name}')
		return True

	def loadDataset(self):
		files = []
		blobs = self.client.list_blobs(self.BUCKET_NAME, prefix=self.dataset, delimiter=None)
		# labels each file
		for blob in blobs:
			if blob.name[-1] != '/':
				metadata = {
					'key' : blob.name,
					'last_modified' : blob.updated.strftime("%m/%d/%Y, %H:%M:%S"),
				}
				files.append(metadata)
		return files

	def loadDatasetList(self):
		# loads all the metadata associated with the dataset
		return self.loadListInPath(self.dataset)

	def loadListInPath(self,path):
		# loads all the metadata associated within a path
		keys = []
		last_m = []

		blobs = self.client.list_blobs(self.BUCKET_NAME, prefix=path, delimiter=None)

		# loads each file
		for blob in blobs:
			if blob.name[-1] != '/':
				keys.append(blob.name)
				last_m.append(blob.updated.strftime("%m/%d/%Y, %H:%M:%S"))
			
		return keys, last_m

	def checkIfEmpty(self,path):
		# checks the number of files in a directory
		blobs = self.client.list_blobs(self.BUCKET_NAME, prefix=path, delimiter=None)
		idx = 0
		for b in blobs:
			idx = idx + 1
		return (idx == 0)

	def listFilesinDataset(self):
		# lists all the files in the main path of the dataset
		return self.listFilesinPath(self.dataset)

	def copyFile(self,filepath,full_target_name):
		# adding new file
		return self.copyFileGlobal(self.dataset+filepath,self.dataset+full_target_name)

	def copyFileGlobal(self,filepath,full_target_name):
		source_blob = self.bucket.blob(filepath)
		blob_copy = self.bucket.copy_blob(source_blob, self.bucket, full_target_name)
		return True

	def resetBuffer(self):
		return True

def main():
	# connects a bucket
	bucket = 'gs://stack123'
	cloud = GCSBucket('stack123')
	cloud.connectBucket()

	# connects to a dataset in the bucket
	cloud.createDataset('dataset1/')

	# list the files in the bucket
	cloud.listFilesinPath('')
	cloud.listFilesinDataset()

	# addes on file
	cloud.addFile('../../tests/image.png','image.png')
	cloud.addFile('../../tests/image.png','base/image.png',subpath='base/')
	cloud.resetBuffer()

	# opens one file
	body = cloud.loadFile('image.png')
	print(body)

	meta = cloud.loadFileMetadata('image.png')
	print(meta)

	# adds two file from binaries
	cloud.addFileFromBinary('extra/image2.png',open("../../tests/image.png", "rb"))
	cloud.addFileFromBinary('extra/image3.png',cloud.loadFile('base/image.png'))
	cloud.resetBuffer()

	# removes a file
	cloud.removeFile('extra/image2.png')

	# loads the dataset in memory
	dataset = cloud.loadDataset()
	print(cloud.loadDataset())

	cloud.listFilesinDataset()
	
	cloud.copyFileGlobal(cloud.dataset+'extra/image3.png',cloud.dataset+'extra/image4.png')
	cloud.listFilesinPath('')
	print('success')

if __name__ == '__main__':
	main()