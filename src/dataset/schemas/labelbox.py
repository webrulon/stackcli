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

class labelbox_schema(object):
	"""docstring for YOLO"""
	def __init__(self, init):
		self.init = init
		self.schema_path = self.init.prefix_meta + 'schema.json'
		self.meta_path = self.init.prefix_meta + 'labelbox_data.json'
		self.labelbox_file = []
		self.status = {}
		self.filtered = False
		self.bounding_box_thumbs = True

	def create_schema_file(self):
		# generates the schema file for a yolo dataset
		schema = {}
		current = json.load(self.init.storage.loadFileGlobal(self.init.prefix_meta+'current.json'))
		json_name = self.get_labels_filename()
		self.labelbox_file = json.load(self.init.storage.loadFileGlobal(json_name))

		k = 0
		idx = 0

		# finds the images
		for key in current['keys']:
			if self.is_image(key):
				dp = {}
				labels = self.get_labels(key)
				dp['key'] = key
				dp['lm'] = current['lm'][idx]
				dp['classes'] = [labels[label]['0'] for label in labels]
				dp['labels'] = labels
				dp['tags'] = []
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
		tags = []

		n_class = {}
		n_res = {}
		n_lm = {}
		n_size = {}
		n_tags = {}

		for val in schema:
			if val != 'len':
				for cl in schema[val]['classes']:
					if not cl in classes:
						classes.append(cl)
						n_class[cl] = 1
					else:
						n_class[cl] += 1
				
				if not schema[val]['resolution'] in resolutions:
					resolutions.append(schema[val]['resolution'])
					n_res[schema[val]['resolution']] = 1
				else:
					n_res[schema[val]['resolution']] += 1
				
				if not schema[val]['lm'] in lm:
					lm.append(schema[val]['lm'])
					n_lm[schema[val]['lm']] = 1
				else:
					n_lm[schema[val]['lm']] += 1
				
				if not schema[val]['size'] in size:
					size.append(schema[val]['size'])
					n_size[schema[val]['size']] = 1
				else:
					n_size[schema[val]['size']] += 1

				if 'tags' in schema[val].keys():
					for tag in schema[val]['tags']:
						if not tag in tags:
							tags.append(tag)
							n_tags[tag] = 1
						else:
							n_tags[tag] += 1

		metadata = {'classes': classes, 'resolutions': resolutions, 'size': size, 'lm': lm, 'tags': tags, 'n_class': n_class, 'n_res': n_res, 'n_lm': n_lm, 'n_tags': n_tags}

		self.init.storage.addFileFromBinaryGlobal(self.meta_path,io.BytesIO(json.dumps(metadata).encode('ascii')))
		self.init.storage.resetBuffer()

		return True

	def get_tags(self, key):
		schema = json.load(self.init.storage.loadFileGlobal(self.schema_path))

		for val in schema.keys():
			if schema[val] is dict:
				if schema[val]['key'] == key:
					if 'tags' in schema[val].keys():
						return schema[val]['tags']
		return []

	def add_tag(self, key, tag):
		schema = json.load(self.init.storage.loadFileGlobal(self.schema_path))

		idx = 0
		for val in schema.keys():
			if schema[val] is dict:
				if schema[val]['key'] == key:
					if 'tags' in schema[val].keys():
						if not tag in val['tags']:
							schema[val]['tags'].append(tag)
					else:
						schema[val]['tags'] = []
						schema[val]['tags'].append(tag)

		# stores schema file
		self.init.storage.addFileFromBinaryGlobal(self.schema_path,io.BytesIO(json.dumps(schema).encode('ascii')))
		self.init.storage.resetBuffer()

		return self.compute_meta_data()

	def remove_tag(self, key, tag):
		schema = json.load(self.init.storage.loadFileGlobal(self.schema_path))

		for val in schema.keys():
			if schema[val] is dict:
				if schema[str(val)]['key'] == key:
					if 'tags' in schema[str(val)].keys():
						if not tag in schema[str(val)]['tags']:
							schema[str(val)]['tags'].pop(tag)

		# stores schema file
		self.init.storage.addFileFromBinaryGlobal(self.schema_path,io.BytesIO(json.dumps(schema).encode('ascii')))
		self.init.storage.resetBuffer()

		return self.compute_meta_data()

	def get_thumbnail(self, filename):
		if self.bounding_box_thumbs:
			# loads image string
			img_str = self.init.storage.loadFile(filename)
			# formats to cv2
			nparr = np.fromstring(img_str.read(), np.uint8)
			img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
			
			shapes = np.zeros_like(img, np.uint8)
			borders = np.zeros_like(img, np.uint8)

			dh, dw, _ = img.shape

			basename = os.path.splitext(os.path.basename(filename))[0]

			ls, _ = self.init.storage.loadDatasetList()

			fl = self.get_labels(filename)

			for dt in fl.keys():

				res = fl[dt]

				# Split string to float
				cl = res['0']
				x = float(res['1'])
				y = float(res['2'])
				w = float(res['3'])
				h = float(res['4'])

				color_str = str(cl)[::-1] + "c" + str(cl)
				
				hsh = 0

				for char in color_str:
					hsh = ord(char) + (hsh << 5) - hsh

				colour = "#"
				colour += ('00' + hex((hsh >> 0) & 0xFF))[-2:].replace('x','0')
				colour += ('00' + hex((hsh >> 8) & 0xFF))[-2:].replace('x','0')
				colour += ('00' + hex((hsh >> 16) & 0xFF))[-2:].replace('x','0')
				
				colour = colour[1:]

				color = tuple(int(colour[i:i+2],16) for i in (4,2,0))
				
				l = int((x - w / 2) * dw)
				r = int((x + w / 2) * dw)
				t = int((y - h / 2) * dh)
				b = int((y + h / 2) * dh)
				
				if l < 0:
					l = 0
				if r > dw - 1:
					r = dw - 1
				if t < 0:
					t = 0
				if b > dh - 1:
					b = dh - 1

				cv2.rectangle(shapes, (l, t), (r, b), color, -1)
				cv2.rectangle(img, (l, t), (r, b), color, 1)

			mask = shapes.astype(bool)
			img[mask] = cv2.addWeighted(img, 0.5, shapes, 0.5, 0)[mask]

			string_res = io.BytesIO(cv2.imencode('.jpg', img)[1].tostring())
			
			return string_res
		else: 
			return self.init.storage.loadFile(filename)

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
				dp['lm'] = self.init.storage.loadFileMetadataGlobal(key)['last_modified']
				dp['classes'] = [self.label_name(label[0]) for label in labels]
				dp['labels'] = labels
				dp['tags'] = []
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
				dp['lm'] = self.init.storage.loadFileMetadataGlobal(key)['last_modified']
				dp['classes'] = [self.label_name(label[0]) for label in labels]
				dp['labels'] = labels
				dp['tags'] = self.get_tags(key)
				dp['n_classes'] = len(dp['classes'])
				dp['resolution'] = self.get_resolution(key)
				dp['size'] = self.init.storage.get_size_of_file_global(key)

				ref = 0

				for k, v in schema.items():
					if k != 'len':
						if v['key'] == key:
							ref = k

				schema[ref] = dp

		for key in removed:
			if self.is_image(key):
				ref = 0

				for k, v in schema.items():
					if k != 'len':
						if v['key'] == key:
							ref = k
							
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

	def get_resolution_values(self, key):
		# reads image
		img_str = self.init.storage.loadFile(key)
		
		# formats to cv2
		nparr = np.fromstring(img_str.read(), np.uint8)
		im = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

		# returns shape
		return {'w': str(im.shape[1]), 'h': str(im.shape[0])}

	def label_name(self, class_number):
		return str(class_number)

	def get_labels(self, filename, version='current'):
		# reads the labels
		labelbox_file = json.load(self.init.storage.loadFileGlobal(self.get_labels_filename(version)))

		labels = {}
		i = 0

		for dp in labelbox_file:
			if dp['External ID'] in filename:
				loaded_labels = dp['Label']['objects']

		res = self.get_resolution_values(filename)

		for line in loaded_labels:
			labels[str(i)] = {}
			j = 0
			labels[str(i)][str(0)] = line['value']
			labels[str(i)][str(1)] = (int(line['bbox']['left']) + int(line['bbox']['width'])/2)/int(res['w'])
			labels[str(i)][str(2)] = (int(line['bbox']['top']) + int(line['bbox']['height'])/2)/int(res['h'])
			labels[str(i)][str(3)] = int(line['bbox']['width'])/int(res['w'])
			labels[str(i)][str(4)] = int(line['bbox']['height'])/int(res['h'])
			
			i = i + 1

		return labels

	def get_labels_filename(self, version = 'current'):
		# reads the labels
		current = json.load(self.init.storage.loadFileGlobal(self.init.prefix_meta+'current.json'))
		json_name = ''
		for key in current['keys']:
			if os.path.splitext(os.path.basename(key))[1] == '.json':
				json_name = key

		if version == 'current':
			return json_name
		else:
			return self.init.prefix_diffs + json_name + '/' + str(int(version)).zfill(10)	

	def set_labels(self, filename, labels_array):
		# reads the labels
		labelbox_file = json.load(self.init.storage.loadFileGlobal(self.get_labels_filename()))
		labelbox_new = labelbox_file
		
		res = self.get_resolution_values(filename)

		idx = 0
						
		for elem in labelbox_file:
			if elem['External ID'] in filename:
				j_arr = []
				for i in range(len(labels_array)-1):
					cl = labels_array[str(i)]['0']
					x = float(labels_array[str(i)]['1'])
					y = float(labels_array[str(i)]['2'])
					w = float(labels_array[str(i)]['3'])
					h = float(labels_array[str(i)]['4'])


					print(cl)
					print(w)
					print(h)
					print(x)
					print(y)

					print(res)

					bbox = {'top': (y-h/2)*float(res['h']), 'left': (x-w/2)*float(res['w']), 'height': h*int(res['h']), 'width': w*int(res['w'])}

					j = 0

					print(labelbox_file[idx])

					for obj in labelbox_file[idx]['Label']['objects']:
						if obj['value'] == cl:
							if not j in j_arr:
								labelbox_file[idx]['Label']['objects'][j]['bbox'] = bbox
								j_arr.append(j)
								break
						j += 1
				print(labelbox_file)
				self.init.storage.addFileFromBinaryGlobal(self.get_labels_filename(),io.BytesIO(json.dumps(labelbox_file).encode('ascii')))
				return True
			idx += 1

	def branch(self, branch_name, type_ ='copy', versions=[]):
		dataset = self.init.storage.dataset
		if self.init.storage.type == 'local':
			branch_name = path_home + '/' + branch_name + '/'

		if len(versions) == 0:
			for f in self.status['keys']:
				versions.append('current')
		idx = 0
		if type_ == 'copy':
			labelbox_new = []
			labelbox_file = json.load(self.init.storage.loadFileGlobal(self.get_labels_filename()))
					
			for f in self.status['keys']:
				if versions[idx] == 'current':
					self.init.storage.copyFileGlobal(f,branch_name+f.replace(dataset,''))
					
				else:
					self.init.storage.copyFileGlobal(self.init.prefix_diffs+f+'/'+str(int(versions[idx])).zfill(10),branch_name+f)
									
				for label in labelbox_file:
					if label['External ID'] in f:
						labelbox_new.append(label)
				idx += 1

			self.init.storage.addFileFromBinaryGlobal(branch_name+self.get_labels_filename().replace(dataset,''),io.BytesIO(json.dumps(labelbox_new).encode('ascii')))
		else:
			labelbox_new = []
			labelbox_file = json.load(self.init.storage.loadFileGlobal(self.get_labels_filename()))
				
			for f in self.status['keys']:
				if versions[idx] == 'current':
					self.init.storage.removeFileGlobal(dataset+f)
				else:
					self.init.storage.copyFileGlobal(self.init.prefix_diffs+f+'/'+str(int(versions[idx])).zfill(10),branch_name+f)
				k = 0
				for label in labelbox_file:
					if label['External ID'] in f:
						labelbox_new.append(label)
						del labelbox_file[k]
						k += 1
				idx += 1

			self.init.storage.addFileFromBinaryGlobal(self.get_labels_filename(),io.BytesIO(json.dumps(labelbox_file).encode('ascii')))
			self.init.storage.addFileFromBinaryGlobal(branch_name+self.get_labels_filename().replace(dataset,''),io.BytesIO(json.dumps(labelbox_new).encode('ascii')))
		return True

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
							if filt == 'name':
								if add:
									if filters[f]['name'] in schema[dp]['key']:
										add = True
									else:
										add = False
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
							if filt == 'name':
								if add:
									if filters[f]['name'] in schema[dp]['key']:
										add = True
									else:
										add = False
										sum_res = 0
										sum_class = 0
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
