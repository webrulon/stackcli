# from src.storage import Storage
import os
import configparser
import maskpass
import io
from pathlib import Path
import sys
sys.path.append( '../../../' )
from src.comm.docker_ver import *
path_home = os.getenv('LCP_DKR')+'/' if docker_ver() else str(Path.home())
from supabase import create_client, Client

class SupabaseBucket(object):
	"""docstring for Storage"""
	def __init__(self, BUCKET_NAME):
		# self = Cloud().__init__()
		self.type = "s3"
		self.dataset = ""
		self.BUCKET_NAME = BUCKET_NAME
		self.user = None
		self.client = None

	def connect_bucket_api(self, user, client):
		self.client = client
		self.user =  user

		# checks if the target bucket exist
		buckets = self.client.storage().list_buckets()

		for bucket in buckets:
			if bucket == self.BUCKET_NAME:
				self.bucket = self.client.storage().get_bucket(bucket)
				return True
		return False

	def reconnect_bucket_api(self):
		# checks if the target bucket exist
		buckets = self.client.storage().list_buckets()

		for bucket in buckets:
			if bucket == self.BUCKET_NAME:
				self.bucket = self.client.storage().get_bucket(bucket)
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
					try:
						self.client.storage().from_(self.BUCKET_NAME).upload(self.dataset+subpath+os.path.join(root,file), file)
					except:
						self.remove_file_global(self.dataset+subpath+os.path.join(root,file))
						self.client.storage().from_(self.BUCKET_NAME).upload(self.dataset+subpath+os.path.join(root,file), file)

		else:
			try:
				self.client.storage().from_(self.BUCKET_NAME).upload(self.dataset+subpath+target_name, filepath)
			except:
				self.remove_file_global(self.dataset+subpath+target_name)
				self.client.storage().from_(self.BUCKET_NAME).upload(self.dataset+subpath+target_name, filepath)
		return True

	def add_file_from_binary(self,filename,f):
		return self.add_file_from_binary_global(self.dataset+filename,f)

	def add_file_from_binary_global(self,filename,f):
		try:
			self.client.storage().from_(self.BUCKET_NAME).upload(filename, f)
		except:
			self.remove_file_global(filename)
			self.client.storage().from_(self.BUCKET_NAME).upload(filename, f)
		return True

	def remove_file(self,filename):
		# deletes files in path
		return self.remove_file_global(self.dataset+filename)

	def remove_file_global(self,filename):
		# deletes files in path
		filename = filename.replace('//','/')
		if filename[-1] == '/':
			self.client.storage().from_(self.BUCKET_NAME).remove(self.client.storage().from_(self.BUCKET_NAME).list(filename))
		elif filename[-1] == '*':
			self.client.storage().from_(self.BUCKET_NAME).remove(self.client.storage().from_(self.BUCKET_NAME).list(filename[:-1]))
		else:
			self.client.storage().from_(self.BUCKET_NAME).remove([filename])
		return True

	def load_file(self,filename):
		return self.load_file_global(self.dataset+filename)

	def load_file_bytes(self,filename, b_i, b_f):
		return self.load_file_global_bytes(self.dataset+filename,b_i,b_f)

	def load_file_global(self,filename):
		return self.client.storage().from_(self.BUCKET_NAME).download(filename.replace('//','/'))

	def load_file_global_bytes(self,filename,b_i,b_f):
		return self.client.storage().from_(self.BUCKET_NAME).download(filename.replace('//','/'))[b_i:b_f]

	def load_file_metadata(self,filename,debug=False):
		# gets the metadata
		obj = self.client.storage().from_(self.BUCKET_NAME).list()
		metadata = {
			'key' : self.dataset+filename
		}

		return metadata

	def load_file_metadata_global(self,filename,debug=False):
		# gets the metadata
		obj = self.client.storage().from_(self.BUCKET_NAME).list(os.path.dirname(filename.replace('//','/')))

		metadata = {
			'key' : self.filename
		}

		return metadata

	def list_files_in_path(self,path):
		for obj in self.client.storage().from_(self.BUCKET_NAME).list(path):
			if obj['id'] is None:
				self.list_files_in_path(path+obj['name'])
			else:
				if path[-1] == '/':
					print(f"-- {path+obj['name']}")
				else:
					print(f"-- {path+'/'+obj['name']}")
		return True

	def list_files_in_dataset(self):
		return self.list_files_in_path(self.dataset)

	def load_dataset_subdict(self, path):
		keys = []
		for file in self.client.storage().from_(self.BUCKET_NAME).list(path):
			if file['id'] is None:
				keys += self.load_dataset_subdict(path+file['name'])
			else:
				if path[-1] == '/':
					keys.append({
						'key': path+file['name'],
						'last_modified': file['updated_at']
					})
				else:
					keys.append({
						'key': path+'/'+file['name'],
						'last_modified': file['updated_at']
					})

		return keys
	
	def load_dataset(self):
		# loads all the metadata associated with the dataset
		return self.load_dataset_subdict(self.dataset)

	def load_dataset_list(self):
		# loads all the metadata associated with the dataset
		return self.load_list_in_path(self.dataset)

	def load_list_in_path(self,path):
		# loads all the metadata associated within a path
		keys = []
		last_m = []

		for file in self.client.storage().from_(self.BUCKET_NAME).list(path):
			if file['id'] is None:
				k_sub, lm_sub = self.list_files_in_path(path+file['name'])
				keys += k_sub
				last_m += lm_sub
			else:
				if path[-1] == '/':
					keys.append(path+file['name'])
				else:
					keys.append(path+'/'+file['name'])
				last_m.append(file['updated_at'])

		return keys, last_m

	def check_if_empty(self,path):
		# checks the number of files in a directory
		summary = self.client.storage().from_(self.BUCKET_NAME).list(path)
		return (len(summary) == 0)

	def load_files_in_dataset(self):
		# lists all the files in the main path of the dataset
		return self.list_files_in_path(self.dataset)

	def copy_file(self,filepath,full_target_name):
		# adding new file
		return self.copy_file_global(self.dataset+filepath,self.dataset+full_target_name)

	def copy_file_global(self,filepath,full_target_name):
		# adding new file
		try:
			self.client.storage().from_(self.BUCKET_NAME).move(filepath, full_target_name)
		except:
			self.remove_file_global(full_target_name)
			self.client.storage().from_(self.BUCKET_NAME).move(filepath, full_target_name)
		return True

	def get_size_of_file(self, filepath):
		return self.get_size_of_file_global(self.dataset + filepath)

	def get_size_of_file_global(self, filepath):
		return len(self.client.storage().from_(self.BUCKET_NAME).download(filepath)['body'])

	def reset_buffer(self):
		pass

def main():
	# connects a bucket
	cloud = SupabaseBucket('stack123')
	

	url: str = ''
	key: str = ''
	
	client: Client = create_client(url, key)
	cloud.connect_bucket_api('', client=client)

	# connects to a dataset in the bucket
	cloud.create_dataset('dataset1/')

	# list the files in the bucket
	cloud.list_files_in_path('dataset1/')
	cloud.load_files_in_dataset()

	# addes on file
	cloud.add_file('image.jpeg','/test_dir/image.jpeg')
	
	# opens one file
	body = cloud.load_file('/test_dir/image.jpeg')
	meta = cloud.load_file_metadata('/test_dir/image.jpeg')

	# adds two file from binaries
	cloud.add_file_from_binary('extra/image2.jpeg',open("image.jpeg", "rb"))
	cloud.add_file_from_binary('extra/image3.jpeg',cloud.load_file('/test_dir/image.jpeg'))

	# removes a file
	cloud.remove_file('extra/image2.jpeg')

	# loads the dataset in memory
	dataset = cloud.load_dataset()
	cloud.copy_file('extra/image3.jpeg','extra/image4.jpeg')
	cloud.load_files_in_dataset()

if __name__ == '__main__':
	main()
