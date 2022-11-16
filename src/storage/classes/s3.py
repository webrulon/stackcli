# from src.storage import Storage
import os
import boto3
import boto3.s3.transfer as s3transfer
import botocore
import configparser
import maskpass
import io
from pathlib import Path
import sys
sys.path.append( '../../../' )
from src.comm.docker_ver import *
path_home = os.getenv('LCP_DKR')+'/' if docker_ver() else str(Path.home())

class S3Bucket(object):
	"""docstring for Storage"""
	def __init__(self, BUCKET_NAME):
		# self = Cloud().__init__()
		self.type = "s3"
		self.dataset = ""
		self.BUCKET_NAME = BUCKET_NAME
		self.credentials = {
			"aws_access_key_id": "",
			"aws_secret_access_key": "",
			"region": "us-east-1",
		}
		self.resource = None
		self.s3t = None
		self.bucket = None

	def connect_bucket(self, verbose=False):
		print('connecting to bucket')
		# checks if the credentials are in the computer
		if not os.path.isfile(path_home+'/.aws/'+self.BUCKET_NAME+'/credentials'):
			print("AWS is not setup in this machine. Please follow the following setups:")
			print('\n')
			print("1) Go to https://console.aws.amazon.com/iamv2/")
			print("2) Choose 'Users' and click on 'Add user'.")
			print("3) Give the user a name (for example, StackBot).")
			print("4) Check 'Enable programmatic access'.")
			print("5) go to 'Set permissions', click 'Attach Existing Policies'.")
			print("6) Check 'AmazonS3FullAccess'")
			print('\n')
			print('Please copy your keys...')
			print('\n')

			self.credentials['aws_access_key_id'] = maskpass.askpass(prompt="Access key ID: ", mask="#")
			self.credentials['aws_secret_access_key'] = maskpass.askpass(prompt="Secret access key: ", mask="#")
			print('\n')

			config = configparser.ConfigParser()
			config['default'] = {}
			config['default']['aws_access_key_id'] = self.credentials['aws_access_key_id']
			config['default']['aws_secret_access_key'] = self.credentials['aws_secret_access_key']
			os.mkdir(path_home+'/.aws/'+self.BUCKET_NAME+'/')
			with open(path_home+'/.aws/'+self.BUCKET_NAME+'/credentials', 'w') as configfile:
				config.write(configfile)
		else:
			config = configparser.ConfigParser()
			config.read(path_home+'/.aws/'+self.BUCKET_NAME+'/credentials')

			self.credentials['aws_access_key_id'] = config['default']['aws_access_key_id']
			self.credentials['aws_secret_access_key'] = config['default']['aws_secret_access_key']

		# checks if the config is there
		if not os.path.isfile(path_home+'/.aws/'+self.BUCKET_NAME+'/config'):
			print('Please select your region (https://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region)')
			print('[Press enter for us-east-2]')

			self.credentials['region'] = input("region: ")

			if self.credentials['region'] == "":
				self.credentials['region'] = 'us-east-2'

			config = configparser.ConfigParser()
			config['default'] = {}
			config['default']['region'] = self.credentials['region']

			with open(path_home+'/.aws/'+self.BUCKET_NAME+'/config', 'w') as configfile:
				config.write(configfile)
		else:
			config = configparser.ConfigParser()
			config.read(path_home+'/.aws/'+self.BUCKET_NAME+'/config')
			self.credentials['region'] = config['default']['region']

		if verbose:
			print('Connecting to your bucket...')
		self.resource = boto3.resource('s3',aws_access_key_id=self.credentials['aws_access_key_id'],aws_secret_access_key=self.credentials['aws_secret_access_key'])
		self.reset_buffer()

		# checks if the target bucket exist
		found = False
		buckets = self.resource.buckets.all()

		try:
			for bucket in buckets:
				if bucket.name == self.BUCKET_NAME:
					found = True
					break
		except:
			print('invalid credentials')
			print('removing config files')
			os.remove(path_home+'/.aws/'+self.BUCKET_NAME+'/credentials')
			os.remove(path_home+'/.aws/'+self.BUCKET_NAME+'/config')
			os.rmdir(path_home+'/.aws/'+self.BUCKET_NAME+'/')

			print('try again...')

			return self.connect_bucket()

		# creates a new bucket or picks another bucket

		if found == False:
			if verbose:
				print('could not find your desired bucket')
				print('connection failed')
				raise Exception('Could not find your desired bucket')
		else:
			self.bucket = self.resource.Bucket(name=self.BUCKET_NAME)

		return found

	def connect_bucket_api(self,keys_dict):
		# checks if the credentials are in the computer

		if keys_dict['key1'] == 'NoKey' or keys_dict['key2'] == 'NoKey':
			config = configparser.ConfigParser()
			config.read(path_home+'/.aws/'+self.BUCKET_NAME+'/credentials')
			keys_dict['key1'] = config['default']['aws_access_key_id']
			keys_dict['key2'] = config['default']['aws_secret_access_key']

		if keys_dict['key3'] == 'NoRegion':
			config = configparser.ConfigParser()
			config.read(path_home+'/.aws/'+self.BUCKET_NAME+'/credentials')
			keys_dict['key3'] = config['default']['region']

		self.credentials['aws_access_key_id'] = keys_dict['key1']
		self.credentials['aws_secret_access_key'] = keys_dict['key2']
		self.credentials['region'] = keys_dict['key3']

		config = configparser.ConfigParser()
		config['default'] = {}
		config['default']['aws_access_key_id'] = self.credentials['aws_access_key_id']
		config['default']['aws_secret_access_key'] = self.credentials['aws_secret_access_key']
		config['default']['region'] = self.credentials['region']

		print('saving credentials')
		if not os.path.isfile(path_home+'/.aws/'+self.BUCKET_NAME+'/credentials'):
			try:
				os.mkdir(path_home+'/.aws/')
			except:
				pass
			try: 
				os.mkdir(path_home+'/.aws/'+self.BUCKET_NAME+'/')
			except:
				pass

		with open(path_home+'/.aws/'+self.BUCKET_NAME+'/credentials', 'w') as configfile:
			config.write(configfile)

		config = configparser.ConfigParser()
		config['default'] = {}
		config['default']['region'] = self.credentials['region']

		print('saving config')
		with open(path_home+'/.aws/'+self.BUCKET_NAME+'/config', 'w') as configfile:
			config.write(configfile)

		print('Connecting to your bucket...')
		self.resource = boto3.resource('s3',aws_access_key_id=self.credentials['aws_access_key_id'],aws_secret_access_key=self.credentials['aws_secret_access_key'])
		self.reset_buffer()

		# checks if the target bucket exist
		buckets = self.resource.buckets.all()

		for bucket in buckets:
			if bucket.name == self.BUCKET_NAME:
				found = True
				self.bucket = self.resource.Bucket(name=self.BUCKET_NAME)
				return True

		return False

	def reconnect_bucket_api(self):
		# checks if the credentials are in the computer
		config = configparser.ConfigParser()
		config.read(path_home+'/.aws/'+self.BUCKET_NAME+'/credentials')

		self.credentials['aws_access_key_id'] = config['default']['aws_access_key_id']
		self.credentials['aws_secret_access_key'] = config['default']['aws_secret_access_key']
		self.resource = boto3.resource('s3',aws_access_key_id=self.credentials['aws_access_key_id'],aws_secret_access_key=self.credentials['aws_secret_access_key'])
		self.reset_buffer()
		
		# checks if the target bucket exist
		buckets = self.resource.buckets.all()

		for bucket in buckets:
			if bucket.name == self.BUCKET_NAME:
				found = True
				self.bucket = self.resource.Bucket(name=self.BUCKET_NAME)
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
					self.s3t.upload(os.path.join(root,file),self.BUCKET_NAME,self.dataset+subpath+file)
			self.reset_buffer()
		else:
			self.s3t.upload(filepath,self.BUCKET_NAME,self.dataset+subpath+os.path.basename(filepath))
		return True

	def add_file_from_binary(self,filename,f):
		return self.add_file_from_binary_global(self.dataset+filename,f)

	def add_file_from_binary_global(self,filename,f):
		self.s3t.upload(f,self.BUCKET_NAME,filename)
		return True

	def remove_file(self,filename):
		# deletes files in path
		return self.remove_file_global(self.dataset+filename)

	def remove_file_global(self,filename):
		# deletes files in path
		if filename[-1] == '/':
			for obj in self.bucket.objects.filter(Prefix=filename):
				self.s3t.delete(self.BUCKET_NAME, obj.key)
			self.reset_buffer()
		elif filename[-1] == '*':
			for obj in self.bucket.objects.filter(Prefix=filename[:-1]):
				self.s3t.delete(self.BUCKET_NAME, obj.key)
			self.reset_buffer()
		else:
			self.s3t.delete(self.BUCKET_NAME, filename)

		return True

	def load_file(self,filename):
		return self.load_file_global(self.dataset+filename)

	def load_file_bytes(self,filename, b_i, b_f):
		return self.load_file_global_bytes(self.dataset+filename,b_i,b_f)

	def load_file_global(self,filename):
		return self.resource.meta.client.get_object(Bucket=self.BUCKET_NAME,Key=filename)['Body']

	def load_file_global_bytes(self,filename,b_i,b_f):
		bytes_rage = 'bytes=' + str(bi) + '-' + str(b_f)
		return self.resource.meta.client.get_object(Bucket=self.BUCKET_NAME,Key=filename,Range=bytes_rage)['Body']

	def load_file_metadata(self,filename,debug=False):
		# gets the metadata
		obj = self.resource.Object(self.BUCKET_NAME,self.dataset+filename).get()

		metadata = {
			'key' : self.dataset+filename,
			'date_loaded' : obj['ResponseMetadata']['HTTPHeaders']['date'],
			'date_added' : obj['ResponseMetadata']['HTTPHeaders']['date'],
			'last_modified' : obj['ResponseMetadata']['HTTPHeaders']['last-modified'],
			'HostId' : obj['ResponseMetadata']['HostId'],
		}

		return metadata

	def load_file_metadata_global(self,filename,debug=False):
		# gets the metadata
		obj = self.resource.Object(self.BUCKET_NAME,filename).get()

		metadata = {
			'key' : self.dataset+filename,
			'date_loaded' : obj['ResponseMetadata']['HTTPHeaders']['date'],
			'date_added' : obj['ResponseMetadata']['HTTPHeaders']['date'],
			'last_modified' : obj['ResponseMetadata']['HTTPHeaders']['last-modified'],
			'HostId' : obj['ResponseMetadata']['HostId'],
		}

		return metadata

	def list_files_in_path(self,path):
		for obj in self.bucket.objects.filter(Prefix=path):
			print(f'-- {obj.key}')
		return True

	def load_dataset(self):
		# loads all the metadata associated with the dataset
		files = []
		params = {"Bucket" : self.BUCKET_NAME, "Prefix" : self.dataset}
		objects = self.resource.meta.client.list_objects_v2(**params)

		# labels each file
		if 'Contents' in objects.keys():
			for obj in objects['Contents']:
				if obj["Key"][-1] != '/':
					metadata = {
						'key' : obj['Key'],
						'last_modified' : obj["LastModified"].strftime("%m/%d/%Y, %H:%M:%S"),
					}
					files.append(metadata)
			return files
		else:
			return []

	def load_dataset_list(self):
		# loads all the metadata associated with the dataset
		return self.load_list_in_path(self.dataset)

	def load_list_in_path(self,path):
		# loads all the metadata associated within a path
		keys = []
		last_m = []

		params = {"Bucket" : self.BUCKET_NAME, "Prefix" : path}
		objects = self.resource.meta.client.list_objects_v2(**params)

		# loads each file
		if 'Contents' in objects.keys():
			for obj in objects['Contents']:
				if obj['Key'][-1] != '/':
					keys.append(obj['Key'])
					last_m.append(obj["LastModified"].strftime("%m/%d/%Y, %H:%M:%S"))

			return keys, last_m
		else:
			return [],[]

	def check_if_empty(self,path):
		# checks the number of files in a directory
		summary = self.bucket.objects.filter(Prefix=path)
		count = 0
		for obj in summary:
			if obj.key[-1] != '/':
				count += 1
		return (count == 0)

	def load_files_in_dataset(self):
		# lists all the files in the main path of the dataset
		return self.list_files_in_path(self.dataset)

	def copy_file(self,filepath,full_target_name):
		# adding new file
		return self.copy_file_global(self.dataset+filepath,self.dataset+full_target_name)

	def copy_file_global(self,filepath,full_target_name):
		# adding new file
		copy_source = {
			'Bucket' : self.BUCKET_NAME,
			'Key' : filepath
		}
		self.s3t.copy(copy_source=copy_source,bucket=self.BUCKET_NAME, key=full_target_name)
		return True

	def get_size_of_file(self, filepath):
		return self.get_size_of_file_global(self.dataset + filepath)

	def get_size_of_file_global(self, filepath):
		return self.bucket.Object(filepath).content_length

	def reset_buffer(self):
		if self.s3t != None:
			self.s3t.shutdown()
		botocore_config = botocore.config.Config(max_pool_connections=80)
		self.resource.meta.client.config = botocore_config
		transfer_config = s3transfer.TransferConfig(use_threads=True,max_concurrency=80,multipart_threshold=25*1024,multipart_chunksize=25*1024)
		self.s3t = s3transfer.create_transfer_manager(self.resource.meta.client, transfer_config)

def main():
	# connects a bucket
	cloud = S3Bucket('stacktest123')
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
	cloud.reset_buffer()

	# loads the dataset in memory
	dataset = cloud.load_dataset()
	print(cloud.load_dataset())

	cloud.load_files_in_dataset()

	cloud.copy_file('extra/image3.png','extra/image4.png')
	cloud.list_files_in_path('')
	print('success')

if __name__ == '__main__':
	main()
