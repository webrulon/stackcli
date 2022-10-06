import sys
sys.path.append( '../../..' )
from datetime import *
import cv2
import numpy as np
import os
import io
import json
from pathlib import Path
from src.comm.docker_ver import *
path_home = os.getenv('LCP_DKR')+'/' if docker_ver() else str(Path.home())

class yolo_schema(object):
	"""docstring for YOLO"""
	def __init__(self, init):
		self.init = init
		self.schema_path = self.init.prefix_meta + 'schema.json'
		self.meta_path = self.init.prefix_meta + 'yolo_data.json'
		self.status = {}
		self.filtered = False

	def create_schema_file(self):
		# generates the schema file for a yolo dataset
		schema = {}
		current = json.load(self.init.storage.loadFileGlobal(self.init.prefix_meta+'current.json'))

		k = 0
		idx = 0

		# finds the images
		for key in current['keys']:
			if self.is_image(key):
				dp = {}
				labels = self.get_labels(key)
				
				print(labels)

				dp['key'] = key
				dp['lm'] = current['lm'][idx]
				dp['classes'] = [self.label_name(labels[label]['0']) for label in labels]
				dp['labels'] = labels
				dp['n_classes'] = len(dp['classes'])
				dp['resolution'] = self.get_resolution(key)
				dp['size'] = self.init.storage.get_size_of_file_global(key)/1024

				schema[str(k)] = dp
				k += 1
			idx += 1
		
		schema['len'] = k

		# stores dp
		self.init.storage.addFileFromBinaryGlobal(self.schema_path,io.BytesIO(json.dumps(schema).encode('ascii')))
		self.init.storage.resetBuffer()

		return True

	def compute_meta_data(self):
		schema = json.load(self.init.storage.loadFileGlobal(self.schema_path))
		classes = []
		resolutions = []
		size = []
		lm = []

		# for class distribution. Not used yet
		n_class = []

		for val in schema:
			if val != 'len':
				for cl in schema[val]['classes']:
					if not cl in classes:
						classes.append(cl)
				if not schema[val]['resolution'] in resolutions:
					resolutions.append(schema[val]['resolution'])
				if not schema[val]['lm'] in lm:
					lm.append(schema[val]['lm'])
				if not schema[val]['size'] in size:
					size.append(schema[val]['size'])
		metadata = {'classes': classes, 'resolutions': resolutions, 'size': size, 'lm': lm}

		self.init.storage.addFileFromBinaryGlobal(self.meta_path,io.BytesIO(json.dumps(metadata).encode('ascii')))
		self.init.storage.resetBuffer()

		return True

	def update_schema_file(self,added=[],modified=[],removed=[]):
		# loads the existing schema file
		schema = json.load(self.init.storage.loadFileGlobal(self.schema_path))

		# finds the images
		idx = int(schema['len'])
		for key in added:
			if self.is_image(key):
				dp = {}
				labels = self.get_labels(key)
				
				dp['key'] = key
				dp['lm'] = self.init.storage.loadFileMetadataGlobal(key)['last-modified']
				dp['classes'] = [self.label_name(label[0]) for label in labels]
				dp['labels'] = labels
				dp['n_classes'] = len(dp['classes'])
				dp['resolution'] = self.get_resolution(key)
				dp['size'] = self.init.storage.get_size_of_file_global(key)

				schema[str(idx)] = dp

				idx += 1

		for key in modified:
			if self.is_image(key):
				dp = {}
				labels = self.get_labels(key)
				
				dp['key'] = key
				dp['lm'] = self.init.storage.loadFileMetadataGlobal(key)['last-modified']
				dp['classes'] = [self.label_name(label[0]) for label in labels]
				dp['labels'] = labels
				dp['n_classes'] = len(dp['classes'])
				dp['resolution'] = self.get_resolution(key)
				dp['size'] = self.init.storage.get_size_of_file_global(key)

				schema[[k for k, v in schema.items() if v['key'] == key][0]] = dp

		for key in removed:
			if self.is_image(key):
				ref = [k for k, v in schema.items() if v['key'] == key][0]
				del schema[ref]
				for k in range(ref, idx):
					schema[k] = schema[str(int(k) + 1)]
				idx -= 1

		schema['len'] = idx

		# stores dp
		self.init.storage.addFileFromBinaryGlobal(self.schema_path,io.BytesIO(json.dumps(schema).encode('ascii')))
		self.init.storage.resetBuffer()

		return self.compute_meta_data()

	def is_image(self, key):
		# checks the extension
		extension = os.path.splitext(os.path.basename(key))[1]
		if extension in ['.jpg','.jpeg','.png','.bmp','.gif','.tiff','.svg']:
			return True
		return False

	def get_resolution(self, key):
		# reads image
		img_str = self.init.storage.loadFileGlobal(key)
		
		# formats to cv2
		nparr = np.fromstring(img_str.read(), np.uint8)
		im = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

		# returns shape
		return str(im.shape[1]) + 'x' + str(im.shape[0])

	def label_name(self, class_number):
		return str(class_number)

	def get_labels(self, filename, version='current'):
		# reads the labels
		if version == 'current':
			basename = os.path.splitext(os.path.basename(filename))[0]

			ls, _ = self.init.storage.loadDatasetList()

			matches = [match for match in ls if basename+'.txt' in match]
			
			try: 
				labels_str = self.init.storage.loadFileGlobal(matches[0])
			except:
				return {}
		else:
			assert(int(version) > 0)

			basename = os.path.splitext(os.path.basename(filename))[0]
			ls, _ = self.init.storage.loadDatasetList()

			matches = [match for match in ls if basename+'.txt' in match]
			
			path = self.init.prefix_diffs + matches[0] + '/' + str(int(version)).zfill(10)
			labels_str = self.init.storage.loadFileGlobal(path)

		labels = {}
		i = 0
		for line in labels_str.readlines():
			labels[str(i)] = {}
			j = 0
			for x in line.split():
				labels[str(i)][str(j)] = float(x)
				j += 1
			i += 1

		return labels

	def branch(self, branch_name, type_ ='copy', versions=[]):
		# try: 
		# copies each file to the new dataset
		dataset = self.init.storage.dataset
		if self.init.storage.type == 'local':
			branch_name = path_home + '/' + branch_name + '/'

		if len(versions) == 0:
			for f in self.status['keys']:
				versions.append('current')
		idx = 0
		if type_ == 'copy':
			for f in self.status['keys']:
				if versions[idx] == 'current':
					basename = os.path.splitext(os.path.basename(f))[0]
					ls, _ = self.init.storage.loadDatasetList()
					matches = [match for match in ls if basename+'.txt' in match]
				else:
					assert(int(versions[idx]) > 0)
					basename = os.path.splitext(os.path.basename(f))[0]
					ls, _ = self.init.storage.loadDatasetList()
					matches = [match for match in ls if basename+'.txt' in match]
					path = self.init.prefix_diffs + matches[0] + '/' + str(int(version)).zfill(10)

				if versions[idx] == 'current':
					self.init.storage.copyFileGlobal(f,branch_name+f.replace(dataset,''))
					self.init.storage.copyFileGlobal(matches[0],branch_name+matches[0].replace(dataset,''))
				else:
					self.init.storage.copyFileGlobal(self.init.prefix_diffs+f+'/'+str(int(versions[idx])).zfill(10),branch_name+f)
					basename = os.path.splitext(os.path.basename(f))[0]
					ls, _ = self.init.storage.loadDatasetList()
					matches2 = [match for match in ls if basename+'.txt' in match]
					self.init.storage.copyFileGlobal(matches[0],branch_name+matches2[0].replace(dataset,''))
				idx += 1
		else:
			for f in self.status['keys']:
				if versions[idx] == 'current':
					self.init.storage.removeFileGlobal(dataset+f)
				else:
					self.init.storage.copyFileGlobal(self.init.prefix_diffs+f+'/'+str(int(versions[idx])).zfill(10),branch_name+f)
				idx += 1
		return True
		# except:
		# 	return False


	def read_all_files(self):
		# queries the json
		schema = json.load(self.init.storage.loadFileGlobal(self.schema_path))
		status = {'keys': [], 'lm': []}
		for dp in schema:
			if dp != 'len':
				status['keys'].append(schema[dp]['key'])
				status['lm'].append(schema[dp]['lm'])
		self.status = status
		return status

	def apply_filters(self, filters={}):
		schema = json.load(self.init.storage.loadFileGlobal(self.schema_path))
		status = {'keys': [], 'lm': []}

		operation = filters['operation']

		for dp in schema:
			if dp != 'len':
				if operation == 'OR':
					add = False
					for f in filters:
						for filt in filters[f]:
							if filt == 'class':
								if filters[f]['class'] in schema[dp]['classes']:
									add = True
							if filt == 'resolution':
								if schema[dp]['resolution'] in filters[f]['resolution']:
									add = True
				if operation == 'AND':
					add = False
					sum_class = 0
					sum_res = 0
					for f in filters:
						for filt in filters[f]:
							if filt == 'class':
								if filters[f]['class'] in schema[dp]['classes']:
									add = True
									sum_class = 1
							if filt == 'resolution':
								if schema[dp]['resolution'] in filters[f]['resolution']:
									add = True
									sum_res = 1
						if sum_class + sum_res == 2:
							add = True
						else:
							add = False
				if add:
					status['keys'].append(schema[dp]['key'])
					status['lm'].append(schema[dp]['lm'])
		
		self.filtered = True
		self.status = status
		return status

	def get_status(self):
		return status
