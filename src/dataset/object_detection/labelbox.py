import sys
sys.path.append( '../../..' )
from datetime import *
import cv2
import numpy as np
from tzlocal import get_localzone
import os
import io
import json
from pathlib import Path
from src.comm.docker_ver import *
path_home = os.getenv('LCP_DKR')+'/' if docker_ver() else str(Path.home())

class labelbox_schema(object):
	def __init__(self, init):
		self.init = init
		self.schema_path = self.init.prefix_meta + 'schema.json'
		self.meta_path = self.init.prefix_meta + 'labelbox_data.json'
		self.labelbox_file = []
		self.status = {}
		self.filtered = False
		self.sliced = False
		self.schema = None
		self.bounding_box_thumbs = True
		self.in_version = False
		self.version_keys = None
		self.selected_version = None
		self.version_schema = ''

	def create_schema_file(self):
		# generates the schema file for a labelbox bounding box dataset
		schema = {}
		current = self.init.load_current()
		json_name = self.get_labels_filename()
		self.labelbox_file = json.load(self.init.storage.load_file_global(json_name))

		k = 0
		idx = 0

		# finds the images
		for key in current['keys']:
			if self.is_image(key):
				dp = {}
				labels = self.get_labels_global(key)
				dp['key'] = key
				dp['lm'] = current['lm'][idx]
				dp['classes'] = [labels[label]['0'] for label in labels]
				dp['labels'] = labels
				dp['tags'] = []
				dp['slices'] = []
				dp['n_classes'] = len(dp['classes'])
				dp['resolution'] = self.get_resolution(key)
				dp['size'] = self.init.storage.get_size_of_file_global(key)/1024

				schema[str(k)] = dp
				k += 1
			idx += 1
		
		schema['len'] = k

		# stores dp
		self.init.storage.add_file_from_binary_global(self.schema_path,io.BytesIO(json.dumps(schema).encode('ascii')))
		self.init.storage.reset_buffer()

		return True
	
	def recompute_schema_file(self):
		schema = {}
		current = self.init.load_current()
		json_name = self.get_labels_filename()
		self.labelbox_file = json.load(self.init.storage.load_file_global(json_name))

		k = 0
		idx = 0

		# finds the images
		for key in current['keys']:
			if self.is_image(key):
				dp = {}
				labels = self.get_labels_global(key)

				dp['key'] = key
				dp['lm'] = current['lm'][idx]
				dp['classes'] = [labels[label]['0'] for label in labels]
				dp['labels'] = labels
				dp['tags'] = self.get_tags(key)
				dp['labels'] = self.get_labels(key)
				dp['n_classes'] = len(dp['classes'])
				dp['resolution'] = self.get_resolution(key)
				dp['size'] = self.init.storage.get_size_of_file_global(key)/1024

				schema[str(k)] = dp
				k += 1
			idx += 1
		
		schema['len'] = k

		# stores dp
		self.init.storage.add_file_from_binary_global(self.schema_path,io.BytesIO(json.dumps(schema).encode('ascii')))
		self.init.storage.reset_buffer()

		return True
		
	def compute_meta_data(self):
		schema = self.get_schema()
		
		classes = []
		resolutions = []
		size = []
		lm = []
		tags = []
		classes_per_image = []
		slices = []

		n_class = {}
		n_res = {}
		n_lm = {}
		n_size = {}
		n_tags = {}
		n_slices = {}

		for val in schema:
			if type(self.schema[val]) is dict:
				if not len(schema[val]['classes']) in classes_per_image:
					classes_per_image.append(len(schema[val]['classes']))
				
				if 'slice' in schema[val].keys():
					for sl in schema[val]['slices']:
						if not sl in slices:
							slices.append(sl)
							n_slices[sl] = 1
						else:
							n_slices[sl] += 1

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

		metadata = {'classes': classes, 'resolutions': resolutions, 'size': size, 'lm': lm, 'slices': slices, 'n_slices': n_slices, 'tags': tags, 'n_class': n_class, 'n_res': n_res, 'n_lm': n_lm, 'n_tags': n_tags, 'classes_per_image': classes_per_image}
		self.metadata = metadata

		self.init.storage.add_file_from_binary_global(self.meta_path,io.BytesIO(json.dumps(metadata).encode('ascii')))
		self.init.storage.reset_buffer()

		return True

	def get_tags(self, key):
		schema = self.get_schema()

		for val in schema.keys():
			if type(schema[val]) is dict:
				if key in schema[val]['key']:
					if 'tags' in schema[val].keys():
						return schema[val]['tags']
		return []

	def add_tag(self, key, tag):
		schema = self.get_schema()
		idx = 0
		for val in schema.keys():
			if type(schema[val]) is dict:
				if key == schema[val]['key']:
					if 'tags' in schema[val].keys():
						if not tag in schema[val]['tags']:
							schema[val]['tags'].append(tag)
					else:
						schema[val]['tags'] = []
						schema[val]['tags'].append(tag)

		# stores schema file
		self.init.storage.add_file_from_binary_global(self.schema_path,io.BytesIO(json.dumps(schema).encode('ascii')))
		self.init.storage.reset_buffer()

		return self.compute_meta_data()

	def add_many_tag(self, keys, tag):
		schema = self.get_schema()
		idx = 0
		for val in schema.keys():
			if type(schema[val]) is dict:
				if schema[val]['key'] in keys:
					if 'tags' in schema[val].keys():
						if not tag in schema[val]['tags']:
							schema[val]['tags'].append(tag)
					else:
						schema[val]['tags'] = []
						schema[val]['tags'].append(tag)
					keys.remove(schema[val]['key'])

		# stores schema file
		self.init.storage.add_file_from_binary_global(self.schema_path,io.BytesIO(json.dumps(schema).encode('ascii')))
		self.init.storage.reset_buffer()

		return self.compute_meta_data()

	def remove_tag(self, key, tag):
		schema = self.get_schema()

		for val in schema.keys():
			if type(schema[val]) is dict:
				if key in schema[val]['key']:
					if 'tags' in schema[val].keys():
						if tag in schema[val]['tags']:
							schema[val]['tags'].remove(tag)

		# stores schema file
		self.init.storage.add_file_from_binary_global(self.schema_path,io.BytesIO(json.dumps(schema).encode('ascii')))
		self.init.storage.reset_buffer()

		return self.compute_meta_data()

	def remove_all_tags(self, key):
		schema = self.get_schema()

		for val in schema.keys():
			if type(schema[val]) is dict:
				if key in schema[val]['key']:
					if 'tags' in schema[val].keys():
						schema[val]['tags'] = []

		# stores schema file
		self.init.storage.add_file_from_binary_global(self.schema_path,io.BytesIO(json.dumps(schema).encode('ascii')))
		self.init.storage.reset_buffer()

		return self.compute_meta_data()

	def many_remove_all_tags(self, keys):
		schema = self.get_schema()

		for val in schema.keys():
			if type(schema[val]) is dict:
				if schema[val]['key'] in keys:
					if 'tags' in schema[val].keys():
						schema[val]['tags'] = []
					keys.remove(schema[val]['key'])

		# stores schema file
		self.init.storage.add_file_from_binary_global(self.schema_path,io.BytesIO(json.dumps(schema).encode('ascii')))
		self.init.storage.reset_buffer()

		return self.compute_meta_data()

	def get_thumbnail(self, filename):
		if self.bounding_box_thumbs:
			# loads image string
			img_str = self.init.storage.load_file_global(filename)
			# formats to cv2
			nparr = np.fromstring(img_str.read(), np.uint8)
			img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
			
			shapes = np.zeros_like(img, np.uint8)
			borders = np.zeros_like(img, np.uint8)

			dh, dw, _ = img.shape
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
			return self.init.storage.load_file_global(filename)

	def update_schema_file(self,added=[],modified=[],removed=[]):
		# loads the existing schema file
		schema = self.get_schema()

		# finds the images
		idx = int(schema['len'])
		for key in added:
			if self.is_image(key):
				dp = {}
				labels = self.get_labels_global(key)
				
				dp['key'] = key
				dp['lm'] = self.init.storage.load_file_metadata_global(key)['last_modified']
				dp['classes'] = [self.label_name(label[0]) for label in labels]
				dp['labels'] = labels
				dp['slices'] = []
				dp['n_classes'] = len(dp['classes'])
				dp['resolution'] = self.get_resolution(key)
				dp['tags'] = []
				dp['size'] = self.init.storage.get_size_of_file_global(key)

				schema[str(idx)] = dp

				idx += 1

		if len(modified) > 0:
			self.recompute_schema_file()
			schema = self.get_schema()

		for key in removed:
			if self.is_image(key):
				ref = []
				for k, v in schema.items():
					if type(v) is dict:
						if v['key'] == key:
							ref.append(k)
				for k in ref:
					del schema[k]
					idx -= 1
				copy_schema = {}
				idx_copy = 0
				for k, v in schema.items():
					if type(v) is dict:
						copy_schema[str(idx_copy)] = v
						idx_copy += 1

				schema = copy_schema

		schema['len'] = idx

		# stores dp
		self.schema = schema
		self.init.storage.add_file_from_binary_global(self.schema_path,io.BytesIO(json.dumps(schema).encode('ascii')))
		self.init.storage.reset_buffer()

		return self.compute_meta_data()

	def is_image(self, key):
		# checks the extension
		extension = os.path.splitext(os.path.basename(key))[1]
		if extension in ['.jpg','.jpeg','.png','.bmp','.gif','.tiff','.svg']:
			return True
		return False

	def has_image(self, key, path = False):
		# checks the extension
		extension = os.path.splitext(os.path.basename(key))[1]
		if extension in ['.jpg','.jpeg','.png','.bmp','.gif','.tiff','.svg']:
			return True
		return False

	def get_resolution(self, key):
		# reads image
		img_str = self.init.storage.load_file_global(key)
		
		# formats to cv2
		nparr = np.fromstring(img_str.read(), np.uint8)
		im = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

		# returns shape
		return str(im.shape[1]) + 'x' + str(im.shape[0])

	def get_resolution_values(self, key):
		# reads image
		img_str = self.init.storage.load_file_global(key)
		
		# formats to cv2
		nparr = np.fromstring(img_str.read(), np.uint8)
		im = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

		# returns shape
		return {'w': str(im.shape[1]), 'h': str(im.shape[0])}

	def get_resolution_values_global(self, key):
		# reads image
		img_str = self.init.storage.load_file_global(key)
		
		# formats to cv2
		nparr = np.fromstring(img_str.read(), np.uint8)
		im = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

		# returns shape
		return {'w': str(im.shape[1]), 'h': str(im.shape[0])}

	def label_name(self, class_number):
		return str(class_number)

	def get_labels(self, filename, version='current'):
		# reads the labels
		labelbox_file = json.load(self.init.storage.load_file_global(self.get_labels_filename(version)))

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

	def get_labels_global(self, filename, version='current'):
		# reads the labels
		labelbox_file = json.load(self.init.storage.load_file_global(self.get_labels_filename(version)))

		labels = {}
		i = 0

		for dp in labelbox_file:
			if dp['External ID'] in filename:
				loaded_labels = dp['Label']['objects']

		res = self.get_resolution_values_global(filename)

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
		current = self.init.load_current()
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
		labelbox_file = json.load(self.init.storage.load_file_global(self.get_labels_filename()))
		
		res = self.get_resolution_values(filename)

		idx = 0
						
		for elem in labelbox_file:
			if elem['External ID'] in filename:
				for i in range(len(labels_array)-1):
					cl = labels_array[str(i)]['0']
					x = float(labels_array[str(i)]['1'])
					y = float(labels_array[str(i)]['2'])
					w = float(labels_array[str(i)]['3'])
					h = float(labels_array[str(i)]['4'])

					bbox = {'top': (y-h/2)*float(res['h']), 'left': (x-w/2)*float(res['w']), 'height': h*int(res['h']), 'width': w*int(res['w'])}
					
					if (i >= len(labelbox_file[idx]['Label']['objects'])):
						copy = labelbox_file[idx]['Label']['objects'][i-1].copy()
						labelbox_file[idx]['Label']['objects'].append(copy)

					labelbox_file[idx]['Label']['objects'][i]['value'] = cl
					labelbox_file[idx]['Label']['objects'][i]['bbox'] = bbox
				if(len(labelbox_file[idx]['Label']['objects']) > len(labels_array)-1):
					for i in range(len(labels_array)-1,len(labelbox_file[idx]['Label']['objects'])):
						labelbox_file[idx]['Label']['objects'].pop()
				self.init.storage.add_file_from_binary_global(self.get_labels_filename(),io.BytesIO(json.dumps(labelbox_file).encode('ascii')))
				return True
			idx += 1

	def branch(self, branch_name, type_ ='copy'):
		dataset = self.init.storage.dataset
		if self.init.storage.type == 'local':
			branch_name = path_home + '/' + branch_name + '/'

		idx = 0
		if type_ == 'copy':
			labelbox_new = []
			labelbox_file = json.load(self.init.storage.load_file_global(self.get_labels_filename()))
					
			for f in self.status['keys']:
				self.init.storage.copy_file_global(f,branch_name+f.replace(dataset,''))
				for label in labelbox_file:
					if label['External ID'] in f:
						labelbox_new.append(label)
				idx += 1

			self.init.storage.add_file_from_binary_global(branch_name+self.get_labels_filename().replace(dataset,''),io.BytesIO(json.dumps(labelbox_new).encode('ascii')))
		else:
			labelbox_new = []
			labelbox_file = json.load(self.init.storage.load_file_global(self.get_labels_filename()))
				
			for f in self.status['keys']:
				self.init.storage.remove_file_global(dataset+f)
				k = 0
				for label in labelbox_file:
					if label['External ID'] in f:
						labelbox_new.append(label)
						del labelbox_file[k]
						k += 1
				idx += 1

			self.init.storage.add_file_from_binary_global(self.get_labels_filename(),io.BytesIO(json.dumps(labelbox_file).encode('ascii')))
			self.init.storage.add_file_from_binary_global(branch_name+self.get_labels_filename().replace(dataset,''),io.BytesIO(json.dumps(labelbox_new).encode('ascii')))
		return True

	def read_all_files(self):
		# queries the json
		schema = self.get_schema()
		status = {'keys': [], 'lm': []}
		for dp in schema:
			if type(self.schema[dp]) is dict:
				status['keys'].append(schema[dp]['key'])
				status['lm'].append(schema[dp]['lm'])
		self.status = status
		return status

	def get_schema(self):
		if self.schema == None:
			try:
				self.schema = json.load(self.init.storage.load_file_global(self.schema_path))
			except:
				self.create_schema_file()
		return self.schema

	def get_metadata(self):
		if self.filtered:
			schema = self.get_schema()
		
			classes = []
			resolutions = []
			size = []
			lm = []
			tags = []
			classes_per_image = []
			slices = []

			n_class = {}
			n_res = {}
			n_lm = {}
			n_size = {}
			n_tags = {}
			n_slices = {}

			for val in self.status['dp']:
				if not len(schema[val]['classes']) in classes_per_image:
					classes_per_image.append(len(schema[val]['classes']))
				
				if 'slices' in schema[val].keys():
					for sl in schema[val]['slices']:
						if not sl in slices:
							slices.append(sl)
							n_slices[sl] = 1
						else:
							n_slices[sl] += 1

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
			return {'classes': classes, 'resolutions': resolutions, 'size': size, 'lm': lm, 'slices': slices, 'n_slices': n_slices, 'tags': tags, 'n_class': n_class, 'n_res': n_res, 'n_lm': n_lm, 'n_tags': n_tags, 'classes_per_image': classes_per_image}
		else:
			return json.load(self.init.storage.load_file_global(self.meta_path))

	def reset_filters(self):
		self.filtered = False
		return True
	
	def apply_filters(self, filters={}):
		schema = self.get_schema()
		status = {'keys': [], 'lm': [], 'dp': []}

		if len(filters) == 0:
			self.filtered = False
			return self.status

		for dp in schema:
			if type(self.schema[dp]) is dict:
				if self.sliced:
					if 'slices' in schema[dp].keys():
						pre_add = any([sl in schema[dp]['slices'] for sl in self.selected_slices])
					else:
						pre_add = False
				else:
					pre_add = True

				if pre_add:
					add_num =  []
					add_class =  []
					add_res =  []
					add_name =  []
					add_tag =  []
					add_date =  []
					add_box =  []

					for f in filters:
						for filt in filters[f]:
							if filt == 'num_classes':
								
								min_cl = int(filters[f]['num_classes'][0])
								max_cl = int(filters[f]['num_classes'][1])
								
								if (len(schema[dp]['classes']) <= max_cl) and (len(schema[dp]['classes']) >= min_cl):
									add_num.append(True)
								else:
									add_num.append(False)
							
							if filt == 'class':
								if filters[f]['class'] in schema[dp]['classes']:
									add_class.append(True)
								else:
									add_class.append(False)
									
							if filt == 'resolution':
								if filters[f]['resolution'] == schema[dp]['resolution']:
									add_res.append(True)
								else:
									add_res.append(False)

							if filt == 'name':
								if filters[f]['name'] in schema[dp]['key']:
									add_name.append(True)
								else:
									add_name.append(False)

							if filt == 'tag':
								if 'tags' in schema[dp]:
									if filters[f]['tag'] in schema[dp]['tags']:
										add_tag.append(True)
									else:
										add_tag.append(False)
								else:
									add_tag.append(False)

							if filt == 'date':
								d_min = datetime.strptime(filters[f]['date'][0], '%Y/%m-%d').date()
								d_max = datetime.strptime(filters[f]['date'][1], '%Y/%m-%d').date()
								date = datetime.strptime(schema[dp]['lm'], '%m/%d/%Y, %H:%M:%S').astimezone(get_localzone()).date()

								if date <= d_max and date >= d_min:
									add_date.append(True)
								else:
									add_date.append(False)

							if filt == 'box_area':

								a_min = min(float(filters[f]['box_area'][0])/100, float(filters[f]['box_area'][1])/100)
								a_max = max(float(filters[f]['box_area'][0])/100, float(filters[f]['box_area'][1])/100)

								if 'labels' in schema[dp]:
									if len(schema[dp]['labels']) == 0:
										add_box.append(False)

									for i in range(len(schema[dp]['labels'])):
										area = float(schema[dp]['labels'][str(i)]['3']) * float(schema[dp]['labels'][str(i)]['4'])
										if (area >= a_min) and (area <= a_max):
											add_box.append(True)
										else:
											add_box.append(False)
								else:
									add_box.append(False)
					
					if len(add_class) == 0:
						add_class = [True]
					if len(add_res) == 0:
						add_res = [True]
					if len(add_name) == 0:
						add_name = [True]
					if len(add_tag) == 0:
						add_tag = [True]
					if len(add_box) == 0:
						add_box = [True]
					if len(add_num) == 0:
						add_num = [True]
					if len(add_date) == 0:
						add_date = [True]
						
					add = all([any(add_class),any(add_res),any(add_name),any(add_tag),any(add_box),any(add_num),any(add_date)])
					if add:
						status['keys'].append(schema[dp]['key'])
						status['lm'].append(schema[dp]['lm'])
						status['dp'].append(dp)

		self.filtered = True
		self.status = status
		return status

	def add_slice(self, slice_name=''):
		status = set(self.status['keys'])
		for val in self.schema.keys():
			if type(self.schema[val]) is dict:
				if self.schema[val]['key'] in status:
					if 'slices' in self.schema[val].keys():
						if not slice_name in self.schema[val]['slices']:
							self.schema[val]['slices'].append(slice_name)
					else:
						self.schema[val]['slices'] = [slice_name]

		# stores schema file
		self.init.storage.add_file_from_binary_global(self.schema_path,io.BytesIO(json.dumps(self.schema).encode('ascii')))
		self.init.storage.reset_buffer()

		return self.compute_meta_data()

	def remove_slice(self, slice_name=''):
		for val in self.schema.keys():
			if type(self.schema[val]) is dict:
				if 'slices' in self.schema[val].keys():
					if slice_name in self.schema[val]['slices']:
						self.schema[val]['slices'].remove(slice_name)

		# stores schema file
		self.init.storage.add_file_from_binary_global(self.schema_path,io.BytesIO(json.dumps(self.schema).encode('ascii')))
		self.init.storage.reset_buffer()

		return self.compute_meta_data()
	
	def select_slice(self, slices=[]):
		if len(slices) == 0:
			self.sliced = False
			self.selected_slices = []
			return self.status

		self.sliced = True
		self.selected_slices = slices
		return self.apply_filters({'slic': []})
	
	def get_slices(self):
		schema = self.get_schema()
		n_slices = {}
		slices = []
		for val in schema:
			if type(self.schema[val]) is dict:
				if 'slices' in schema[val].keys():
						for sl in schema[val]['slices']:
							if not sl in slices:
								slices.append(sl)
								n_slices[sl] = 1
							else:
								n_slices[sl] += 1

		return n_slices

	def get_slices_key(self, key):
		keys = self.schema.keys()
		for val in keys:
			if type(self.schema[val]) is dict:
				if key in self.schema[val]['key']:
					if 'slices' in self.schema[val].keys():
						return self.schema[val]['slices']
		return []

	def get_status(self):
		return self.status