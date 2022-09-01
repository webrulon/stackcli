# from src.storage import Storage
import os
import boto3
import boto3.s3.transfer as s3transfer
import botocore
import configparser
import maskpass
import io
from pathlib import Path


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
			"region": "us-east-2",
		}
		self.resource = None
		self.s3t = None
		self.bucket = None

	def connectBucket(self):
		# checks if the credentials are in the computer
		home_dir = str(Path.home())
		if not os.path.isfile(home_dir+'/.aws/credentials'):

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
			os.mkdir(home_dir+'/.aws/')
			with open(home_dir+'/.aws/credentials', 'w') as configfile:
				config.write(configfile)
		else:
			config = configparser.ConfigParser()
			config.read(home_dir+'/.aws/credentials')

			self.credentials['aws_access_key_id'] = config['default']['aws_access_key_id']
			self.credentials['aws_secret_access_key'] = config['default']['aws_secret_access_key']

		# checks if the config is there
		if not os.path.isfile(home_dir+'/.aws/config'):
			print('Please select your region (https://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region)')
			print('[Press enter for us-east-2')

			self.credentials['region'] = input("region: ")

			if self.credentials['region'] == "":
				self.credentials['region'] = 'us-east-2'

			config = configparser.ConfigParser()
			config['default'] = {}
			config['default']['region'] = self.credentials['region']

			with open(home_dir+'/.aws/config', 'w') as configfile:
				config.write(configfile)
		else:
			config = configparser.ConfigParser()
			config.read(home_dir+'/.aws/config')
			self.credentials['region'] = config['default']['region']

		print('Connecting to your bucket...')
		self.resource = boto3.resource('s3',aws_access_key_id=self.credentials['aws_access_key_id'],aws_secret_access_key=self.credentials['aws_secret_access_key'])
		self.resetBuffer()

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
			os.remove(home_dir+'/.aws/credentials')
			os.remove(home_dir+'/.aws/config')
			os.rmdir(home_dir+'/.aws/')

			print('try again...')

			return self.connectBucket()

		# creates a new bucket or picks another bucket

		if found == False:
			print('Could not find your desired bucket...')
			print('Do you want to creat a bucket with the name '+self.BUCKET_NAME+"?")
			yn = input("[Y/n]: ")
			if yn == "y" or yn == "Y":
				bucket = self.resource.create_bucket(Bucket=self.BUCKET_NAME, CreateBucketConfiguration={'LocationConstraint': self.credentials['region']})
			else:
				print('pick another bucket from this list:')
				for bucket in buckets:
					print("-" + bucket.name)
				self.BUCKET_NAME = input("Enter another bucket name: ")
				return self.connectBucket()
		self.bucket = self.resource.Bucket(name=self.BUCKET_NAME)

		return True

	def connect_bucket_api(self,keys_dict):
		# checks if the credentials are in the computer
		self.credentials['aws_access_key_id'] = keys_dict['key1']
		self.credentials['aws_secret_access_key'] = keys_dict['key2']
		self.credentials['region'] = keys_dict['key3']

		config = configparser.ConfigParser()
		config['default'] = {}
		config['default']['aws_access_key_id'] = self.credentials['aws_access_key_id']
		config['default']['aws_secret_access_key'] = self.credentials['aws_secret_access_key']
		config['default']['region'] = self.credentials['region']

		print('saving credentials')
		with open(str(Path.home())+'/.aws/credentials', 'w') as configfile:
			config.write(configfile)

		config = configparser.ConfigParser()
		config['default'] = {}
		config['default']['region'] = self.credentials['region']

		print('saving config')
		with open(str(Path.home())+'/.aws/config', 'w') as configfile:
			config.write(configfile)

		print('Connecting to your bucket...')
		self.resource = boto3.resource('s3',aws_access_key_id=self.credentials['aws_access_key_id'],aws_secret_access_key=self.credentials['aws_secret_access_key'])
		self.resetBuffer()

		# checks if the target bucket exist
		buckets = self.resource.buckets.all()

		for bucket in buckets:
			if bucket.name == self.BUCKET_NAME:
				found = True
				self.bucket = self.resource.Bucket(name=self.BUCKET_NAME)
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
					self.s3t.upload(os.path.join(root,file),self.BUCKET_NAME,self.dataset+subpath+file)
			self.resetBuffer()
		else:
			self.s3t.upload(filepath,self.BUCKET_NAME,self.dataset+subpath+os.path.basename(filepath))
		return True

	def addFileFromBinary(self,filename,f):
		return self.addFileFromBinaryGlobal(self.dataset+filename,f)

	def addFileFromBinaryGlobal(self,filename,f):
		self.s3t.upload(f,self.BUCKET_NAME,filename)
		return True

	def removeFile(self,filename):
		# deletes files in path
		return self.removeFileGlobal(self.dataset+filename)

	def removeFileGlobal(self,filename):
		# deletes files in path
		if filename[-1] == '/':
			for obj in self.bucket.objects.filter(Prefix=filename):
				self.s3t.delete(self.BUCKET_NAME, obj.key)
			self.resetBuffer()
		elif filename[-1] == '*':
			for obj in self.bucket.objects.filter(Prefix=filename[:-1]):
				self.s3t.delete(self.BUCKET_NAME, obj.key)
			self.resetBuffer()
		else:
			self.s3t.delete(self.BUCKET_NAME, filename)

		return True

	def loadFile(self,filename):
		return self.loadFileGlobal(self.dataset+filename)

	def loadFileBytes(self,filename, b_i, b_f):
		return self.loadFileGlobalBytes(self.dataset+filename,b_i,b_f)

	def loadFileGlobal(self,filename):
		return self.resource.meta.client.get_object(Bucket=self.BUCKET_NAME,Key=filename)['Body']

	def loadFileGlobalBytes(self,filename,b_i,b_f):
		bytes_rage = 'bytes=' + str(bi) + '-' + str(b_f)
		return self.resource.meta.client.get_object(Bucket=self.BUCKET_NAME,Key=filename,Range=bytes_rage)['Body']

	def loadFileMetadata(self,filename,debug=False):
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

	def listFilesinPath(self,path):
		for obj in self.bucket.objects.filter(Prefix=path):
			print(f'-- {obj.key}')
		return True

	def loadDataset(self):
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

	def loadDatasetList(self):
		# loads all the metadata associated with the dataset
		return self.loadListInPath(self.dataset)

	def loadListInPath(self,path):
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

	def checkIfEmpty(self,path):
		# checks the number of files in a directory
		summary = self.bucket.objects.filter(Prefix=path)
		count = 0
		for obj in summary:
			if obj.key[-1] != '/':
				count += 1
		return (count == 0)

	def listFilesinDataset(self):
		# lists all the files in the main path of the dataset
		return self.listFilesinPath(self.dataset)

	def copyFile(self,filepath,full_target_name):
		# adding new file
		return self.copyFileGlobal(self.dataset+filepath,self.dataset+full_target_name)

	def copyFileGlobal(self,filepath,full_target_name):
		# adding new file
		copy_source = {
			'Bucket' : self.BUCKET_NAME,
			'Key' : filepath
		}
		self.s3t.copy(copy_source=copy_source,bucket=self.BUCKET_NAME, key=full_target_name)
		return True

	def resetBuffer(self):
		if self.s3t != None:
			self.s3t.shutdown()
		botocore_config = botocore.config.Config(max_pool_connections=80)
		self.resource.meta.client.config = botocore_config
		transfer_config = s3transfer.TransferConfig(use_threads=True,max_concurrency=80,multipart_threshold=25*1024,multipart_chunksize=25*1024)
		self.s3t = s3transfer.create_transfer_manager(self.resource.meta.client, transfer_config)

def main():
	# connects a bucket
	cloud = S3Bucket('stacktest123')
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
	cloud.resetBuffer()

	# loads the dataset in memory
	dataset = cloud.loadDataset()
	print(cloud.loadDataset())

	cloud.listFilesinDataset()

	cloud.copyFile('extra/image3.png','extra/image4.png')
	cloud.listFilesinPath('')
	print('success')

if __name__ == '__main__':
	main()
