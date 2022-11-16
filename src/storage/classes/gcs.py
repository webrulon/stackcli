# from src.storage import Storage
import os
import logging
from google.cloud import storage
import maskpass
import io
from pathlib import Path
import sys
sys.path.append( '../../../' )
from src.comm.docker_ver import *
path_home = os.getenv('LCP_DKR')+'/' if docker_ver() else str(Path.home())

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

	def connect_bucket(self, verbose=False):
		# creates a client 
		key_path = path_home+'/.gs/'+self.BUCKET_NAME+'/gs_key'
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
		if verbose:
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
			if verbose:
				print('could not find your desired bucket')
				print('connection failed')
				raise Exception('Could not find your desired bucket')
		return True

	def connect_bucket_api(self,binary):
		# reads the gs_key
		print('creating key file')
		key_path = path_home+'/.gs/'+self.BUCKET_NAME+'/gs_key'

		if not os.path.isfile(key_path):
			try:
				os.mkdir(path_home+'/.gs/')
			except:
				pass
			try: 
				os.mkdir(path_home+'/.gs/'+self.BUCKET_NAME+'/')
			except:
				pass

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

	def reconnect_bucket_api(self):
		try:
			key_path = path_home+'/.gs/'+self.BUCKET_NAME+'/gs_key'	
			os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key_path

			print('connecting to client')
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

	def create_dataset(self,location):
		# assigns location
		if location[-1] != '/':
			location = location + '/'
		self.dataset = location
		self.raw_location = location
		return True

	def add_file(self,filepath,target_name='',subpath=''):
		# adding new file
		if target_name == '':
			target_name = os.path.basename(filepath)
		if filepath[-1] == '/':
			for root,dirs,files in os.walk(filepath):
				for file in files:
					blob = self.bucket.blob(self.dataset+subpath+os.path.basename(filepath))
					blob.upload_from_filename(os.path.join(root,file))	
			self.reset_buffer()
		else:
			blob = self.bucket.blob(self.dataset+subpath+os.path.basename(filepath))
			blob.upload_from_filename(filepath)
		return True

	def add_file_from_binary(self,filename,f):
		return self.add_file_from_binary_global(self.dataset+filename,f)

	def add_file_from_binary_global(self,filename,f):
		blob = self.bucket.blob(filename)
		blob.upload_from_file(file_obj=f, rewind=True)
		return True

	def remove_file(self,filename):
		return self.remove_file_global(self.dataset+filename)

	def remove_file_global(self,filename):
		# deletes files in path
		if filename[-1] == '/':
			for blob in self.client.list_blobs(self.BUCKET_NAME, prefix=filename, delimiter=None):
				blob.delete()
			self.reset_buffer()
		else:
			blob = self.bucket.blob(filename)
			blob.delete()
		return True

	def load_file(self,filename):
		return self.load_file_global(self.dataset+filename)

	def load_file_bytes(self,filename,bi,bf):
		return self.load_file_global_bytes(self.dataset+filename,bi,bf)

	def load_file_global(self,filename):
		blob = self.bucket.blob(filename)
		file_obj = blob.download_as_string()
		return io.BytesIO(file_obj)

	def load_file_global_bytes(self,filename,bi,bf):
		blob = self.bucket.blob(filename)
		file_obj = blob.download_as_string(start=bi,end=bf)
		return io.BytesIO(file_obj)	

	def load_file_metadata(self,filename,debug=False):
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

	def load_file_metadata_global(self,filename,debug=False):
		blob = self.bucket.get_blob(filename)
		if blob.name[-1] != '/':
			metadata = {
				'key' : self.dataset + filename,
				'date_loaded' : blob.time_created.strftime("%m/%d/%Y, %H:%M:%S"),
				'date_added' : blob.time_created.strftime("%m/%d/%Y, %H:%M:%S"),
				'last_modified' : blob.updated.strftime("%m/%d/%Y, %H:%M:%S"),
				'HostId' : blob.id,
			}
			return metadata

	def list_files_in_path(self,path):
		blobs = self.client.list_blobs(self.BUCKET_NAME, prefix=path, delimiter=None)
		for blob in blobs:
			print(f'-- {blob.name}')
		return True

	def load_dataset(self):
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

	def load_dataset_list(self):
		# loads all the metadata associated with the dataset
		return self.load_list_in_path(self.dataset)

	def load_list_in_path(self,path):
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

	def check_if_empty(self,path):
		# checks the number of files in a directory
		blobs = self.client.list_blobs(self.BUCKET_NAME, prefix=path, delimiter=None)
		idx = 0
		for b in blobs:
			idx = idx + 1
		return (idx == 0)

	def load_files_in_dataset(self):
		# lists all the files in the main path of the dataset
		return self.list_files_in_path(self.dataset)

	def copy_file(self,filepath,full_target_name):
		# adding new file
		return self.copy_file_global(self.dataset+filepath,self.dataset+full_target_name)

	def copy_file_global(self,filepath,full_target_name):
		source_blob = self.bucket.blob(filepath)
		blob_copy = self.bucket.copy_blob(source_blob, self.bucket, full_target_name)
		return True

	def get_size_of_file(self, filepath):
		return self.get_size_of_file_global(self.dataset + filepath)

	def get_size_of_file_global(self, filepath):
		return self.bucket.get_blob(filepath).size

	def reset_buffer(self):
		return True

def main():
	# connects a bucket
	bucket = 'gs://stack123'
	cloud = GCSBucket('stack123')
	cloud.connect_bucket()

	# connects to a dataset in the bucket
	cloud.create_dataset('dataset1/')

	# list the files in the bucket
	cloud.list_files_in_path('')
	cloud.load_files_in_dataset()

	# addes on file
	cloud.add_file('../../tests/image.png','image.png')
	cloud.add_file('../../tests/image.png','base/image.png',subpath='base/')
	cloud.reset_buffer()

	# opens one file
	body = cloud.load_file('image.png')
	print(body)

	meta = cloud.load_file_metadata('image.png')
	print(meta)

	# adds two file from binaries
	cloud.add_file_from_binary('extra/image2.png',open("../../tests/image.png", "rb"))
	cloud.add_file_from_binary('extra/image3.png',cloud.load_file('base/image.png'))
	cloud.reset_buffer()

	# removes a file
	cloud.remove_file('extra/image2.png')

	# loads the dataset in memory
	dataset = cloud.load_dataset()
	print(cloud.load_dataset())

	cloud.load_files_in_dataset()
	
	cloud.copy_file_global(cloud.dataset+'extra/image3.png',cloud.dataset+'extra/image4.png')
	cloud.list_files_in_path('')
	print('success')

if __name__ == '__main__':
	main()