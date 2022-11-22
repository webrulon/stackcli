import sys
sys.path.append( '../../..' )
from datetime import *
from tzlocal import get_localzone
import cv2
import numpy as np
import time
import os
import io
import json
from pathlib import Path
from src.comm.docker_ver import *
path_home = os.getenv('LCP_DKR')+'/' if docker_ver() else str(Path.home())


from multiprocessing import Pool

def run_multiprocessing(func, i, n_processors):
    with Pool(processes=n_processors) as pool:
        return pool.map(func, i)

class yolo_schema(object):
	"""docstring for YOLO"""
	def __init__(self, init):
		self.init = init
		self.schema = None
		self.metadata = None
		self.schema_path = self.init.prefix_meta + 'schema.json'
		self.meta_path = self.init.prefix_meta + 'yolo_data.json'
		self.status = {}
		self.filtered = False
		self.bounding_box_thumbs = True
		ls, _ = self.init.storage.load_dataset_list()
		self.ls = ls

	def get_schema_from_key(self, key):
		dp = {}
		labels = self.get_labels(key)
		dp['key'] = key
		dp['lm'] = ''
		dp['classes'] = [labels[label]['0'] for label in labels]
		dp['labels'] = labels
		dp['n_classes'] = len(dp['classes'])
		dp['tags'] = []
		dp['resolution'] = self.get_resolution(key)
		dp['size'] = self.init.storage.get_size_of_file_global(key)/1024
		return dp

	def create_schema_file_in_parallel(self):
		schema = {}
		current = json.load(self.init.storage.load_file_global(self.init.prefix_meta+'current.json'))

		t0 = time.time()
		with Pool(processes=6) as pool:
			arr_res =  pool.map(self.get_schema_from_key, current['keys'])
		
		schema['len'] = len(arr_res)
		print(f'time to compute parallelized {time.time()-t0}s')

		return True

	def create_schema_file(self):
		# generates the schema file for a yolo dataset
		schema = {}
		current = json.load(self.init.storage.load_file_global(self.init.prefix_meta+'current.json'))

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
				dp['n_classes'] = len(dp['classes'])
				dp['tags'] = []
				dp['resolution'] = self.get_resolution(key)
				dp['size'] = self.init.storage.get_size_of_file_global(key)/1024
				schema[str(k)] = dp
				k += 1
			idx += 1

		schema['len'] = k

		# stores dp
		self.schema = schema
		self.init.storage.add_file_from_binary_global(self.schema_path,io.BytesIO(json.dumps(schema).encode('ascii')))
		self.init.storage.reset_buffer()

		return True

	def get_schema(self):
		if self.schema == None:
			try:
				self.schema = json.load(self.init.storage.load_file_global(self.schema_path))
			except:
				self.create_schema_file()
		return self.schema

	def compute_meta_data(self):
		schema = self.get_schema()
		
		classes = []
		resolutions = []
		size = []
		lm = []
		tags = []
		classes_per_image = []

		n_class = {}
		n_res = {}
		n_lm = {}
		n_size = {}
		n_tags = {}

		for val in schema:
			if val != 'len':
				if not len(schema[val]['classes']) in classes_per_image:
					classes_per_image.append(len(schema[val]['classes']))

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

		metadata = {'classes': classes, 'resolutions': resolutions, 'size': size, 'lm': lm, 'tags': tags, 'n_class': n_class, 'n_res': n_res, 'n_lm': n_lm, 'n_tags': n_tags, 'classes_per_image': classes_per_image}
		self.metadata = metadata

		self.init.storage.add_file_from_binary_global(self.meta_path,io.BytesIO(json.dumps(metadata).encode('ascii')))
		self.init.storage.reset_buffer()

		return True

	def get_thumbnail(self, filename):
		if self.bounding_box_thumbs:
			# loads image string
			img_str = self.init.storage.load_file(filename)
			# formats to cv2
			nparr = np.fromstring(img_str.read(), np.uint8)
			img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
			
			shapes = np.zeros_like(img, np.uint8)

			dh, dw, _ = img.shape

			basename = os.path.splitext(os.path.basename(filename))[0]

			ls = self.ls

			matches = [match for match in ls if basename+'.txt' in match]
			 
			fl = self.init.storage.load_file_global(matches[0])

			for dt in fl.readlines():

				# Split string to float
				res = dt.split()
				cl = int(res[0])
				x = float(res[1])
				y = float(res[2])
				w = float(res[3])
				h = float(res[4])

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
			return self.init.storage.load_file(filename)

	def update_schema_file(self,added=[],modified=[],removed=[]):
		# loads the existing schema file
		schema = self.get_schema()

		# finds the images

		print(added)
		print(modified)
		print(removed)

		idx = int(schema['len'])
		for key in added:
			if self.is_image(key):
				dp = {}
				labels = self.get_labels(key)
				
				dp['key'] = key
				dp['lm'] = self.init.storage.load_file_metadata_global(key)['last_modified']
				dp['classes'] = [labels[label]['0'] for label in labels]
				dp['labels'] = labels
				dp['n_classes'] = len(dp['classes'])
				dp['resolution'] = self.get_resolution(key)
				dp['tags'] = []
				dp['size'] = self.init.storage.get_size_of_file_global(key)

				schema[str(idx)] = dp
				print(dp)

				idx += 1

		for key in modified:
			if self.is_image(key) or self.has_image(key):
				if self.has_image(key):
					key_img = self.has_image(key, path=True)
				else:
					key_img = key
				dp = {}
				labels = self.get_labels(key_img)
				
				dp['key'] = key_img
				dp['lm'] = self.init.storage.load_file_metadata_global(key)['last_modified']
				dp['classes'] = [labels[label]['0'] for label in labels]
				dp['labels'] = labels
				dp['n_classes'] = len(dp['classes'])
				dp['tags'] = self.get_tags(key_img)
				dp['resolution'] = self.get_resolution(key_img)
				dp['size'] = self.init.storage.get_size_of_file_global(key_img)

				ref = 0

				for k, v in schema.items():
					if k != 'len':
						if v['key'] == key_img:
							ref = k

				schema[ref] = dp

		for key in removed:
			if self.is_image(key):
				ref = []
				for k, v in schema.items():
					if k != 'len':
						if v['key'] == key:
							ref.append(k)
				for k in ref:
					del schema[k]
					for k_ in range(int(k), idx-1):
						schema[k_] = schema[str(int(k_) + 1)]
					idx -= 1
		schema['len'] = idx

		# stores dp
		self.init.storage.add_file_from_binary_global(self.schema_path,io.BytesIO(json.dumps(schema).encode('ascii')))
		self.init.storage.reset_buffer()
		self.schema = schema

		return self.compute_meta_data()

	def get_tags(self, key):
		keys = self.schema.keys()
		for val in keys:
			if type(self.schema[val]) is dict:
				if key in self.schema[val]['key']:
					if 'tags' in self.schema[val].keys():
						return self.schema[val]['tags']
		return []

	def add_tag(self, key, tag):
		for val in self.schema.keys():
			if type(self.schema[val]) is dict:
				if key == self.schema[val]['key']:
					if 'tags' in self.schema[val].keys():
						if not tag in self.schema[val]['tags']:
							self.schema[val]['tags'].append(tag)
					else:
						self.schema[val]['tags'] = []
						self.schema[val]['tags'].append(tag)

		# stores schema file
		self.init.storage.add_file_from_binary_global(self.schema_path,io.BytesIO(json.dumps(self.schema).encode('ascii')))
		self.init.storage.reset_buffer()

		return self.compute_meta_data()

	def add_many_tag(self, keys, tag):
		for val in self.schema.keys():
			if type(self.schema[val]) is dict:
				if self.schema[val]['key'] in keys:
					if 'tags' in self.schema[val].keys():
						if not tag in self.schema[val]['tags']:
							self.schema[val]['tags'].append(tag)
					else:
						self.schema[val]['tags'] = []
						self.schema[val]['tags'].append(tag)
					keys.remove(self.schema[val]['key'])

		# stores schema file
		self.init.storage.add_file_from_binary_global(self.schema_path,io.BytesIO(json.dumps(self.schema).encode('ascii')))
		self.init.storage.reset_buffer()

		return self.compute_meta_data()

	def remove_tag(self, key, tag):
		keys = self.schema.keys()
		for val in keys:
			if type(self.schema[val]) is dict:
				if key in self.schema[val]['key']:
					if 'tags' in self.schema[val].keys():
						if tag in self.schema[val]['tags']:
							self.schema[val]['tags'].remove(tag)

		# stores schema file
		self.init.storage.add_file_from_binary_global(self.schema_path,io.BytesIO(json.dumps(self.schema).encode('ascii')))
		self.init.storage.reset_buffer()

		return self.compute_meta_data()

	def remove_all_tags(self, key):
		keys = self.schema.keys()
		for val in keys:
			if type(self.schema[val]) is dict:
				if key in self.schema[val]['key']:
					if 'tags' in self.schema[val].keys():
						self.schema[val]['tags'] = []

		# stores schema file
		self.init.storage.add_file_from_binary_global(self.schema_path,io.BytesIO(json.dumps(self.schema).encode('ascii')))
		self.init.storage.reset_buffer()

		return self.compute_meta_data()

	def many_remove_all_tags(self, keys):
		for val in self.schema.keys():
			if type(self.schema[val]) is dict:
				if self.schema[val]['key'] in keys:
					if 'tags' in self.schema[val].keys():
						self.schema[val]['tags'] = []
					keys.remove(self.schema[val]['key'])

		# stores schema file
		self.init.storage.add_file_from_binary_global(self.schema_path,io.BytesIO(json.dumps(self.schema).encode('ascii')))
		self.init.storage.reset_buffer()

		return self.compute_meta_data()

	def is_image(self, key):
		# checks the extension
		extension = os.path.splitext(os.path.basename(key))[1]
		if extension in ['.jpg','.jpeg','.png','.bmp','.gif','.tiff','.svg']:
			return True
		return False

	def has_image(self, key, path = False):
		basename = os.path.splitext(os.path.basename(key))[0]
		ls = self.ls
		matches = [match for match in ls if basename in match]

		for m in matches:
			if self.is_image(m):
				if path:
					return m
				else:
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

	def label_name(self, class_number):
		return str(class_number)

	def get_labels(self, filename, version='current'):
		# reads the labels
		if version == 'current':
			basename = os.path.splitext(os.path.basename(filename))[0]

			if self.ls != None:
				ls = set(self.ls)
			else:
				ls, _ = self.init.storage.load_dataset_list()
				self.ls = ls
				ls = set(ls)

			matches = [match for match in ls if basename+'.txt' in match]
			try: 
				labels_str = self.init.storage.load_file_global(matches[0])
			except:
				return {}
		else:
			assert(int(version) > 0)

			basename = os.path.splitext(os.path.basename(filename))[0]
			ls = set(self.ls)

			matches = [match for match in ls if basename+'.txt' in match]
			
			path = self.init.prefix_diffs + matches[0] + '/' + str(int(version)).zfill(10)
			labels_str = self.init.storage.load_file_global(path)

		labels = {}
		i = 0
		for line in labels_str.readlines():
			labels[str(i)] = {}
			j = 0
			for x in line.split():
				try: 
					if j == 0:
						labels[str(i)][str(j)] = x.decode("utf-8")
					else:
						labels[str(i)][str(j)] = float(x)
				except:
					labels[str(i)][str(j)] = 0
				j += 1
			i = i + 1
		return labels

	def get_labels_filename(self, filename, version = 'current'):
		if version == 'current':
			basename = os.path.splitext(os.path.basename(filename))[0]

			ls = self.ls

			matches = [match for match in ls if basename+'.txt' in match]
			
			return matches[0]
		else:
			assert(int(version) > 0)

			basename = os.path.splitext(os.path.basename(filename))[0]
			ls = self.ls

			matches = [match for match in ls if basename+'.txt' in match]
			
			return self.init.prefix_diffs + matches[0] + '/' + str(int(version)).zfill(10)

	def set_labels(self, filename, labels_array):
		# reads the labels
		basename = os.path.splitext(os.path.basename(filename))[0]
		ls = self.ls
		matches = [match for match in ls if basename+'.txt' in match]

		labels_string = ''

		for i in range(len(labels_array)-1):
			cl = labels_array[str(i)]['0']
			w = labels_array[str(i)]['1']
			h = labels_array[str(i)]['2']
			x = labels_array[str(i)]['3']
			y = labels_array[str(i)]['4']

			labels_string = labels_string + f'{cl} {w} {h} {x} {y}\n'

		if (len(matches) > 0):
			self.init.storage.add_file_from_binary_global(matches[0],io.BytesIO(labels_string.encode("utf-8")))
		else:
			string = basename+'.txt'
			self.init.storage.add_file_from_binary(string,io.BytesIO(labels_string.encode("utf-8")))

		return True

	def branch(self, branch_name, type_ ='copy', versions=[]):
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
					ls = self.ls
					matches = [match for match in ls if basename+'.txt' in match]
				else:
					assert(int(versions[idx]) > 0)
					basename = os.path.splitext(os.path.basename(f))[0]
					ls = self.ls
					matches = [match for match in ls if basename+'.txt' in match]
					path = self.init.prefix_diffs + matches[0] + '/' + str(int(versions[idx])).zfill(10)

				if versions[idx] == 'current':
					self.init.storage.copy_file_global(f,branch_name+f.replace(dataset,''))
					self.init.storage.copy_file_global(matches[0],branch_name+matches[0].replace(dataset,''))
				else:
					self.init.storage.copy_file_global(self.init.prefix_diffs+f+'/'+str(int(versions[idx])).zfill(10),branch_name+f)
					basename = os.path.splitext(os.path.basename(f))[0]
					ls = self.ls
					matches2 = [match for match in ls if basename+'.txt' in match]
					self.init.storage.copy_file_global(matches[0],branch_name+matches2[0].replace(dataset,''))
				idx += 1
		else:
			for f in self.status['keys']:
				if versions[idx] == 'current':
					self.init.storage.remove_file_global(dataset+f)
				else:
					self.init.storage.copy_file_global(self.init.prefix_diffs+f+'/'+str(int(versions[idx])).zfill(10),branch_name+f)
				idx += 1
		return True

	def read_all_files(self):
		# queries the json
		schema = self.get_schema()
		status = {'keys': [], 'lm': []}
		for dp in schema:
			if dp != 'len':
				status['keys'].append(schema[dp]['key'])
				status['lm'].append(schema[dp]['lm'])
		self.status = status
		return status

	def get_metadata(self):
		if self.filtered:
			schema = self.get_schema()
		
			classes = []
			resolutions = []
			size = []
			lm = []
			tags = []
			classes_per_image = []

			n_class = {}
			n_res = {}
			n_lm = {}
			n_size = {}
			n_tags = {}

			for val in self.status['dp']:
				if not len(schema[val]['classes']) in classes_per_image:
					classes_per_image.append(len(schema[val]['classes']))
					
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

			print(lm)
			lm.sort()
			print(lm)
			return {'classes': classes, 'resolutions': resolutions, 'size': size, 'lm': lm, 'tags': tags, 'n_class': n_class, 'n_res': n_res, 'n_lm': n_lm, 'n_tags': n_tags, 'classes_per_image': classes_per_image}
		else:
			return json.load(self.init.storage.load_file_global(self.meta_path))

	def apply_filters(self, filters={}):
		schema = self.get_schema()
		status = {'keys': [], 'lm': [], 'dp': []}
		
		if len(filters) == 0:
			self.filtered = False
			return self.status

		for dp in schema:

			if dp != 'len':
				
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
								print(f'yes')
								add_name.append(True)
							else:
								print(f'no')
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

	def get_status(self):
		return self.status
