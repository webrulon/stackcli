import sys
sys.path.append( '../../..' )
from datetime import *
from tzlocal import get_localzone
import cv2
import numpy as np
import time
import zipfile
import os
import csv
import copy
import io
import json
import pandas as pd
from functools import reduce
import numpy as np

from pathlib import Path
from src.comm.docker_ver import *
path_home = os.getenv('LCP_DKR')+'/' if docker_ver() else str(Path.home())
import hashlib

class multi_seq2seq_csv_schema(object):
	def __init__(self, init):
		self.init = init
		self.schema = None
		self.metadata = None
		self.schema_path = self.init.prefix_meta + 'schema.json'
		self.meta_path = self.init.prefix_meta + 'multi_seq2seq_csv_data.json'
		self.status = {}
		self.filtered = False
		self.sliced = False
		self.selected_slices = False
		self.bounding_box_thumbs = True
		
		self.in_version = False
		self.version_keys = None
		self.selected_version = None
		self.version_schema = ''

		ls, _ = self.init.storage.load_dataset_list()
		self.ls = ls

	def create_schema_file(self):
		# generates the schema file for a yolo dataset
		schema = {}
		current = self.init.load_current()
		k = 0
		idx = 0

		# finds the images
		for key in current['keys']:
			if '.csv' in key:
				idx_key = 0
				dataset = self.init.storage.load_file_global(key)
				local_array = pd.read_csv(dataset, encoding='unicode_escape', encoding_errors='backslashreplace', lineterminator='\n').to_dict('records')
				# local_array = list(csv.reader(dataset, delimiter=','))
				
				print(local_array)
				
				for elem in local_array:
					el = list(list(elem.values()))
					dp = {}
					dp['filename'] = key
					dp['keys'] = el[:-1]
					dp['res'] = el[-1] 
					dp['lm'] = current['lm'][idx]
					dp['metadata'] = {}
					dp['tags'] = []
					dp['slices'] = []
					dp['idx'] = idx_key
					hash = hashlib.md5(reduce(lambda a, b: str(a)+str(b), dp['keys']).encode('utf-8')).hexdigest()
					dp['versions'] = [{'key': hash, 'version': self.init.get_latest_diff_number(key), 'type': 'added', 'source': 'N/A', 'comment': '', 'file': 'raw', 'diff': self.init.prefix_diffs + key + '/' + str(self.init.get_latest_diff_number(key)).zfill(10), 'date': current['lm'][idx]}]
					
					schema[hash] = dp
					idx_key +=1
					k += 1
			idx += 1

		schema['len'] = k

		# stores dp
		self.schema = schema
		self.init.storage.add_file_from_binary_global(self.schema_path,io.BytesIO(json.dumps(schema).encode('ascii')))
		self.init.storage.reset_buffer()
		self.copy_schema_to_latest_version_checkpoint()

		return True

	def get_labels(self, key, version='current'):
		if version == 'current':
			schema = self.get_schema()
			return schema[key]
		else:
			schema = self.get_schema()
			diff = schema[key]['versions'][int(version)-1]['diff']
			dataset = self.init.storage.load_file_global(diff)
			local_array = pd.read_csv(dataset, encoding='unicode_escape', encoding_errors='backslashreplace', lineterminator='\n').to_dict('records')
			for elem in local_array:
				el = list(elem.values())
				if el[:-1] == schema[key]['keys']:
					dp = {} 
					dp['filename'] = schema[key]['filename']
					dp['keys'] = el[:-1]
					dp['res'] = el[-1] 

					dp['tags'] = []
					dp['slices'] = []
					return dp
			return {}

	def copy_schema_to_latest_version_checkpoint(self):
		try:
			versions = self.init.load_versions()
		except:
			self.init.setup_versions()
			versions = self.init.load_versions()

		schema_copy_path = self.init.prefix_versions + '/schemas/' + f'{len(versions.keys())}.json' 
		schema_copy = copy.deepcopy(self.schema)

		for dp in schema_copy:
			if type(schema_copy[dp]) is dict:
				ver = self.init.get_latest_diff_number(schema_copy[dp]['filename'])
				schema_copy[dp]['filename'] = self.init.prefix_diffs + schema_copy[dp]['filename'] + '/' + str(ver).zfill(10)

		self.init.storage.add_file_from_binary_global(schema_copy_path,io.BytesIO(json.dumps(schema_copy).encode('ascii')))
		keys = list(versions.keys())
		versions[keys[-1]]['schema_path'] = schema_copy_path

		metapath = self.init.prefix_versions + 'versions.json'
		self.init.storage.add_file_from_binary_global(metapath,io.BytesIO(json.dumps(versions).encode('ascii')))
		return True

	def select_version(self, version, schema_file, keys):
		self.in_version = True
		self.selected_version = version
		self.version_schema = schema_file
		self.ls = keys.values()

	def reset_to_current_version(self):
		self.in_version = False
		self.version_keys = None
		self.selected_version = None
		self.schema = None
		self.filtered = None
		self.sliced = None

		ls, _ = self.init.storage.load_dataset_list()
		self.ls = ls
		self.schema = json.load(self.init.storage.load_file_global(self.schema_path))

		return True

	def get_schema(self):
		if self.schema == None:
			try:
				if self.in_version:
					self.schema = json.load(self.init.storage.load_file_global(self.version_schema))
				else:
					self.schema = json.load(self.init.storage.load_file_global(self.schema_path))
			except:
				self.create_schema_file()
		return self.schema

	def compute_meta_data(self):
		schema = self.get_schema()
		
		in_len = []
		out_len = []
		lm = []
		tags = []
		slices = []

		n_in_len = {}
		n_out_len = {}
		n_lm = {}
		n_tags = {}
		n_slices = {}

		for val in schema:
			if type(self.schema[val]) is dict:

				if 'slice' in schema[val].keys():
					for sl in schema[val]['slices']:
						if not sl in slices:
							slices.append(sl)
							n_slices[sl] = 1
						else:
							n_slices[sl] += 1
				
				k = schema[val]['keys']
				r = schema[val]['res']

				if not len(reduce(lambda a, b: str(a)+str(b), k)) in in_len:
					in_len.append(len(reduce(lambda a, b: str(a)+str(b), k)))
					n_in_len[len(reduce(lambda a, b: str(a)+str(b), k))] = 1
				else:
					n_in_len[len(reduce(lambda a, b: str(a)+str(b), k))] += 1

				if not len(r) in out_len:
					out_len.append(len(r))
					n_out_len[len(r)] = 1
				else:
					n_out_len[len(r)] += 1

				if not schema[val]['lm'] in lm:
					lm.append(schema[val]['lm'])
					n_lm[schema[val]['lm']] = 1
				else:
					n_lm[schema[val]['lm']] += 1

				if 'tags' in schema[val].keys():
					for tag in schema[val]['tags']:
						if not tag in tags:
							tags.append(tag)
							n_tags[tag] = 1
						else:
							n_tags[tag] += 1

		metadata = {'in_len': in_len,'out_len': out_len,'n_in_len': n_in_len,'n_out_len': n_out_len, 
		'lm': lm, 'slices': slices, 'n_slices': n_slices, 'n_tags': n_tags, 'tags': tags, 'n_lm': n_lm}
			
		self.metadata = metadata

		if not self.in_version:
			self.init.storage.add_file_from_binary_global(self.meta_path,io.BytesIO(json.dumps(metadata).encode('ascii')))
			self.init.storage.reset_buffer()

		return True

	def update_schema_file(self,added=[],modified=[],removed=[]):
		# loads the existing schema file
		schema = self.get_schema()
		# finds the images

		print('updating schema file')
		
		idx = int(schema['len'])

		for key in removed:
			for el in schema:
				if schema[el]['filename'] == key:
					del schema[el]
					idx -= 1

		for key in added:
			if '.csv' in key:
				idx_key = 0
				dataset = self.init.storage.load_file_global(key)
				local_array = pd.read_csv(dataset, encoding='unicode_escape', encoding_errors='backslashreplace', lineterminator='\n').to_dict('records')
				for elem in local_array:
					el = list(elem.values())
					dp = {}
					dp['filename'] = key
					dp['key'] = el[:-1]
					dp['res'] = el[-1] 
					dp['lm'] = self.init.storage.load_file_metadata_global(key)['last_modified']
					dp['metadata'] = {}
					dp['tags'] = []
					dp['slices'] = []
					dp['idx'] = idx_key
					hash = hashlib.md5(reduce(lambda a, b: str(a)+str(b), dp['keys']).encode('utf-8')).hexdigest()
					dp['versions'] = [{'key': hash, 'version': self.init.get_latest_diff_number(key), 'type': 'added', 'source': 'N/A', 'comment': '', 'file': 'raw', 'diff': self.init.prefix_diffs + key + '/' + str(self.init.get_latest_diff_number(key)).zfill(10), 'date': self.init.storage.load_file_metadata_global(key)['last_modified']}]
					schema[hash] = dp
					idx_key +=1
					k += 1
			idx += 1

		keys = set(schema.keys())

		for key in modified:
			if '.csv' in key:
				idx_key = 0
				dataset = self.init.storage.load_file_global(key)
				local_array = pd.read_csv(dataset, encoding='unicode_escape', encoding_errors='backslashreplace', lineterminator='\n').to_dict('records')
				for elem in local_array:
					el = list(elem.values())
					hash = hashlib.md5(reduce(lambda a, b: str(a)+str(b), el[:-1]).encode('utf-8')).hexdigest()
					if hash in list(keys):
						if not el[-1] == schema[hash]['res']:
							dp = {}
							dp['filename'] = key
							dp['keys'] = el[:-1]
							dp['res'] = el[-1]
							dp['lm'] = self.init.storage.load_file_metadata_global(key)['last_modified']
							dp['tags'] = schema[hash]['tags']
							dp['slices'] = schema[hash]['slices']
							dp['metadata'] = schema[hash]['metadata']
							dp['idx'] = idx_key
							list_ver = schema[hash]['versions']
							list_ver.append({'key': hash, 'version': self.init.get_latest_diff_number(key),'type': 'modified', 'source': 'N/A', 'comment': '', 'file': 'raw', 'diff': self.init.prefix_diffs + key + '/' + str(self.init.get_latest_diff_number(key)).zfill(10), 'date': self.init.storage.load_file_metadata_global(key)['last_modified']})
							dp['versions'] = list_ver
							dp['idx'] = idx_key
							schema[hash] = dp
					else:
						dp = {}
						dp['filename'] = key
						dp['keys'] = el[:-1]
						dp['res'] = el[-1]
						dp['lm'] = self.init.storage.load_file_metadata_global(key)['last_modified']
						dp['tags'] = []
						dp['slices'] = []
						dp['idx'] = idx_key
						dp['metadata'] = {}
						dp['versions'] = [{'key': hash, 'version': self.init.get_latest_diff_number(key), 'type': 'added', 'source': 'N/A', 'comment': '', 'file': 'raw', 'diff': self.init.prefix_diffs + key + '/' + str(self.init.get_latest_diff_number(key)).zfill(10), 'date': self.init.storage.load_file_metadata_global(key)['last_modified']}]
						schema[hash] = dp
						idx += 1
					idx_key +=1

				local_keys = []
				for elem in local_array:
					el = list(elem.values())
					local_keys.append(hashlib.md5(reduce(lambda a, b: str(a)+str(b), el[:-1]).encode('utf-8')).hexdigest())
				local_keys = set(local_keys)
				keys_new = set(schema.keys())
				for dp in list(keys_new):
					if type(schema[dp]) is dict:
						if not dp in local_keys:
							if schema[dp]['filename'] == key:
								del schema[dp]
								idx -= 1

		schema['len'] = idx

		# stores dp
		self.init.storage.add_file_from_binary_global(self.schema_path,io.BytesIO(json.dumps(schema).encode('ascii')))
		self.init.storage.reset_buffer()
		self.schema = schema

		return self.compute_meta_data()
	
	def get_tags(self, key):
		keys = self.get_schema().keys()
		if key in keys:
			if type(self.schema[key]) is dict:
				return self.schema[key]['tags']
		return []

	def add_metadata_tags(self, key, tag):
		keys = self.get_schema().keys()
		if key in keys:
			if type(self.schema[key]) is dict:
				self.schema[key]['metadata'][tag['key']] = tag['val']
		# stores schema file
		if self.in_version:
			self.init.storage.add_file_from_binary_global(self.schema_path,io.BytesIO(json.dumps(self.version_schema).encode('ascii')))
		else:
			self.init.storage.add_file_from_binary_global(self.schema_path,io.BytesIO(json.dumps(self.schema).encode('ascii')))
		self.init.storage.reset_buffer()

		return self.compute_meta_data()

	def remove_metadata_tags(self, key, tag):
		keys = self.get_schema().keys()
		if key in keys:
			if type(self.schema[key]) is dict:
				if tag['key'] in self.schema[key]['metadata'].keys():
					if tag['val'] in self.schema[key]['metadata'][tag['key']]:
						self.schema[key]['metadata'][tag['key']].remove(tag['val'])
		# stores schema file
		if self.in_version:
			self.init.storage.add_file_from_binary_global(self.schema_path,io.BytesIO(json.dumps(self.version_schema).encode('ascii')))
		else:
			self.init.storage.add_file_from_binary_global(self.schema_path,io.BytesIO(json.dumps(self.schema).encode('ascii')))
		self.init.storage.reset_buffer()

		return self.compute_meta_data()

	def add_tag(self, key, tag):
		keys = self.get_schema().keys()
		for val in keys:
			if type(self.schema[val]) is dict:
				if key == val:
					if 'tags' in self.schema[val].keys():
						if not tag in self.schema[val]['tags']:
							self.schema[val]['tags'].append(tag)
					else:
						self.schema[val]['tags'] = []
						self.schema[val]['tags'].append(tag)

		# stores schema file
		if self.in_version:
			self.init.storage.add_file_from_binary_global(self.schema_path,io.BytesIO(json.dumps(self.version_schema).encode('ascii')))
		else:
			self.init.storage.add_file_from_binary_global(self.schema_path,io.BytesIO(json.dumps(self.schema).encode('ascii')))
		self.init.storage.reset_buffer()

		return self.compute_meta_data()

	def add_many_tag(self, keys, tag):
		for val in list(self.get_schema().keys()):
			if type(self.schema[val]) is dict:
				if val in keys:
					if 'tags' in self.schema[val].keys():
						if not tag in self.schema[val]['tags']:
							self.schema[val]['tags'].append(tag)
					else:
						self.schema[val]['tags'] = []
						self.schema[val]['tags'].append(tag)
					keys.remove(val)

		# stores schema file
		if self.in_version:
			self.init.storage.add_file_from_binary_global(self.schema_path,io.BytesIO(json.dumps(self.version_schema).encode('ascii')))
		else:
			self.init.storage.add_file_from_binary_global(self.schema_path,io.BytesIO(json.dumps(self.schema).encode('ascii')))
		self.init.storage.reset_buffer()

		return self.compute_meta_data()

	def remove_tag(self, key, tag):
		keys = self.get_schema().keys()
		for val in keys:
			if type(self.schema[val]) is dict:
				if key == val:
					if 'tags' in self.schema[val].keys():
						if tag in self.schema[val]['tags']:
							self.schema[val]['tags'].remove(tag)

		# stores schema file
		if self.in_version:
			self.init.storage.add_file_from_binary_global(self.schema_path,io.BytesIO(json.dumps(self.version_schema).encode('ascii')))
		else:
			self.init.storage.add_file_from_binary_global(self.schema_path,io.BytesIO(json.dumps(self.schema).encode('ascii')))
		self.init.storage.reset_buffer()

		return self.compute_meta_data()

	def remove_all_tags(self, key):
		keys = self.get_schema().keys()
		for val in keys:
			if type(self.schema[val]) is dict:
				if key == val:
					if 'tags' in self.schema[val].keys():
						self.schema[val]['tags'] = []

		# stores schema file
		if self.in_version:
			self.init.storage.add_file_from_binary_global(self.schema_path,io.BytesIO(json.dumps(self.version_schema).encode('ascii')))
		else:
			self.init.storage.add_file_from_binary_global(self.schema_path,io.BytesIO(json.dumps(self.schema).encode('ascii')))
		self.init.storage.reset_buffer()

		return self.compute_meta_data()

	def many_remove_all_tags(self, keys):
		for val in list(self.get_schema().keys()):
			if type(self.schema[val]) is dict:
				if val in keys:
					if 'tags' in self.schema[val].keys():
						self.schema[val]['tags'] = []
					keys.remove(val)

		# stores schema file
		if self.in_version:
			self.init.storage.add_file_from_binary_global(self.schema_path,io.BytesIO(json.dumps(self.version_schema).encode('ascii')))
		else:
			self.init.storage.add_file_from_binary_global(self.schema_path,io.BytesIO(json.dumps(self.schema).encode('ascii')))
		self.init.storage.reset_buffer()

		return self.compute_meta_data()

	def set_labels(self, key, labels_array):
		schema = self.get_schema()
		dp = schema[key]
		dataset = self.init.storage.load_file_global(dp['filename'])
		local_array = pd.read_csv(dataset, encoding='unicode_escape', encoding_errors='backslashreplace', lineterminator='\n').to_dict('records')
		found = False
		for idx in range(len(local_array)):
			hash = hashlib.md5(reduce(lambda a, b: str(a)+str(b), list(local_array[idx].values())[:-1]).encode('utf-8')).hexdigest()
			if hash == key:
				for k in range(len(local_array[idx].keys())-1):
					if labels_array['keys'][k] == '':
						local_array[idx][list(local_array[idx].keys())[k]] = '?'
					else:
						local_array[idx][list(local_array[idx].keys())[k]] = labels_array['keys'][k]
				if labels_array['res'] == '':
					local_array[idx][list(local_array[idx].keys())[-1]] = '?'
				else:
					local_array[idx][list(local_array[idx].keys())[-1]] = labels_array['res']
				found = True
		if found == False:
			local_array.append(
				labels_array['keys'].append(labels_array['res'])
			)
		
		df = pd.DataFrame(local_array)
		from io import StringIO
		csv_buffer = StringIO()
		df.to_csv(csv_buffer, index=False)
		self.init.storage.add_file_from_binary_global(dp['filename'],io.BytesIO(bytes(csv_buffer.getvalue(), 'utf-8')))
		return True
	
	def remove_key(self, key):
		schema = self.get_schema()
		dp = schema[key]
		dataset = self.init.storage.load_file_global(dp['filename'])
		local_array = pd.read_csv(dataset, encoding='unicode_escape', encoding_errors='backslashreplace', lineterminator='\n').to_dict('records')
		
		for idx in range(len(local_array)):
			hash = hashlib.md5(reduce(lambda a, b: str(a)+str(b), list(local_array[idx].values())[:-1]).encode('utf-8')).hexdigest()
			if hash == key:
				del local_array[idx]
		
		df = pd.DataFrame(local_array)
		from io import StringIO
		csv_buffer = StringIO()
		df.to_csv(csv_buffer, index=False)
		self.init.storage.add_file_from_binary_global(dp['filename'],io.BytesIO(bytes(csv_buffer.getvalue(), 'utf-8')))
		return True
	
	def branch(self, branch_name, type_ ='copy'):
		# TODO
		schema = self.get_schema()
		dataset = self.init.storage.dataset
		if self.init.storage.type == 'local':
			branch_name = path_home + '/' + branch_name + '/'
		files = {}

		if self.filtered:
			status = self.status
		else:
			status = self.read_all_files()

		for dp in status['keys']:
			if schema[dp]['filename'] in files.keys():
				files[schema[dp]['filename']].append(dp)
			else:
				files[schema[dp]['filename']] = [dp]

		for key in files.keys():
			print(files[key])
			local_array = [{'text': schema[dp]['text'], 'entities': schema[dp]['entities']} for dp in files[key]]
			self.init.storage.add_file_from_binary_global(branch_name + key.replace(dataset,''),io.BytesIO(json.dumps(local_array).encode('ascii')))
		
		return True

	def merge(self, goal):
		# TODO
		dataset = self.init.storage.dataset
		if self.init.storage.type == 'local':
			goal = goal.replace(path_home,'')
			goal = path_home + '/' + goal + '/'

		if self.filtered:
			status = self.status
		else:
			status = self.read_all_files()

		schema = self.get_schema()
		files = {}
		for dp in status['keys']:
			if schema[dp]['filename'] in files.keys():
				files[schema[dp]['filename']].append(dp)
			else:
				files[schema[dp]['filename']] = [dp]

		files_goal, _ = self.init.storage.load_list_in_path(goal)
		files_goal = [f.replace(goal,'') for f in files_goal]
		for file in files.keys():
			if file.replace(dataset,'') in files_goal:
				local_array = self.init.storage.load_file_global(file)
				missing = [dp for dp in files[file]]
				missing_copy = list(missing)
				for idx in range(len(local_array)):
					for j in range(len(missing_copy)):
						if local_array[idx]['text'] == schema[missing[j]]['text']:
							local_array[idx]['entities'] = schema[missing[j]]['entities']
							missing_copy.pop(j)
						if len(missing_copy) == 0:
							break
				for dp in missing_copy:
					local_array.append({'text': schema[dp]['text'], 'entities': schema[dp]['entities']})
				self.init.storage.add_file_from_binary_global(goal+file.replace(dataset,''),io.BytesIO(json.dumps(local_array).encode('ascii')))
				# print(goal+file.replace(dataset,''))
			else:
				self.init.storage.copy_file_global(file,goal+file.replace(dataset,''))
				print(goal+file)
		return True

	def read_all_files(self):
		# queries the json
		schema = self.get_schema()
		
		status = {'keys': [], 'lm': [], 'filename': [], 'dp': []}
		for dp in schema:
			if type(self.schema[dp]) is dict:
				status['keys'].append(dp)
				status['filename'].append(schema[dp]['filename'])
				status['lm'].append(schema[dp]['lm'])
				status['dp'].append(schema[dp])
		self.status = status
		return status

	def download_files(self):
		if self.status == None:
			self.status = self.read_all_files()

		schema = self.get_schema()
		mem_zip = io.BytesIO()

		local_array = []
		for key in self.status['keys']:
			if type(self.schema[key]) is dict:
				local_array.append(schema[key]['keys'].append(schema[key]['res']))
		print(local_array)
		df = pd.DataFrame(local_array)
		from io import StringIO
		csv_buffer = StringIO()
		df.to_csv(csv_buffer, index=False)
		print(df)
		with zipfile.ZipFile(mem_zip, "a", zipfile.ZIP_DEFLATED, False) as zf:
			zf.writestr('dataset.zip', bytes(csv_buffer.getvalue(), 'ascii'))

		return mem_zip.getvalue()

	def export_openai(self):
		if self.status == None:
			self.status = self.read_all_files()

		schema = self.get_schema()
		local_array = []

		for key in self.status['keys']:
			string = ''
			for k in len(schema[key]['keys']):
				string += f"{schema[key]['keys'][k]}\n"
			local_array.append({'prompt': f"Context:\n{string}Answer:\n", 'completion': schema[key]['res']})
		print(local_array)
		mem_zip = io.BytesIO()
		with zipfile.ZipFile(mem_zip, "a", zipfile.ZIP_DEFLATED, False) as zf:
			zf.writestr('dataset.json', json.dumps(local_array).encode('ascii'))
		return mem_zip.getvalue()

	def get_metadata(self):
		if (self.filtered or self.in_version) and (self.metadata is None):
			schema = self.get_schema()

			in_len = []
			out_len = []
			lm = []
			tags = []
			slices = []

			n_in_len = {}
			n_out_len = {}
			n_lm = {}
			n_tags = {}
			n_slices = {}

			for val in self.status['keys']:
				if type(self.schema[val]) is dict:

					if 'slice' in schema[val].keys():
						for sl in schema[val]['slices']:
							if not sl in slices:
								slices.append(sl)
								n_slices[sl] = 1
							else:
								n_slices[sl] += 1
					
					k = schema[val]['keys']
					r = schema[val]['res']

					if not len(k) in in_len:
						in_len.append(len(k))
						n_in_len[len(k)] = 1
					else:
						n_in_len[len(k)] += 1

					if not len(r) in out_len:
						out_len.append(len(r))
						n_out_len[len(r)] = 1
					else:
						n_out_len[len(r)] += 1

					if not schema[val]['lm'] in lm:
						lm.append(schema[val]['lm'])
						n_lm[schema[val]['lm']] = 1
					else:
						n_lm[schema[val]['lm']] += 1

					if 'tags' in schema[val].keys():
						for tag in schema[val]['tags']:
							if not tag in tags:
								tags.append(tag)
								n_tags[tag] = 1
							else:
								n_tags[tag] += 1

			return {'in_len': in_len,'out_len': out_len,'n_in_len': n_in_len,'n_out_len': n_out_len, 
			'lm': lm, 'slices': slices, 'n_slices': n_slices, 'tags': tags, 'n_tags': n_tags, 'n_lm': n_lm}
		else:
			if self.metadata is None:
				self.metadata = json.load(self.init.storage.load_file_global(self.meta_path))
		return self.metadata

	def reset_filters(self):
		self.filtered = False
		self.metadata = None
		return True
	
	def apply_filters(self, filters={}):
		schema = self.get_schema()
		status = {'keys': [], 'lm': [], 'filename': [], 'dp': []}

		if len(filters) == 0:
			self.filtered = False
			return self.status
		self.metadata = None

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
					
					add_name =  []
					add_out =  []

					add_l_in =  []
					add_l_out =  []
					
					add_tag =  []
					add_date =  []

					for f in filters:
						for filt in filters[f]:
							if filt == 'name':
								if any([filters[f]['name'] in schema[dp]['keys'][k] for k in range(len(schema[dp]['keys']))]):
									add_name.append(True)
								else:
									add_name.append(False)

							if filt == 'out':
								if filters[f]['out'] in schema[dp]['res']:
									add_out.append(True)
								else:
									add_out.append(False)

							if filt == 'in_len':
								min_cl = int(filters[f]['in_len'][0])
								max_cl = int(filters[f]['in_len'][1])
								len_x = len(reduce(lambda a, b: str(a)+str(b), schema[dp]['keys']))
								if (len_x <= max_cl) and (len_x >= min_cl):
									add_l_in.append(True)
								else:
									add_l_in.append(False)

							if filt == 'out_len':
								min_cl = int(filters[f]['out_len'][0])
								max_cl = int(filters[f]['out_len'][1])
								if (len(schema[dp]['res']) <= max_cl) and (len(schema[dp]['res']) >= min_cl):
									add_l_in.append(True)
								else:
									add_l_in.append(False)
							
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

					
					if len(add_name) == 0:
						add_name = [True]

					if len(add_out) == 0:
						add_out = [True]

					if len(add_l_in) == 0:
						add_l_in = [True]
					
					if len(add_l_out) == 0:
						add_l_out = [True]

					if len(add_tag) == 0:
						add_tag = [True]
					if len(add_date) == 0:
						add_date = [True]
						
					add = all([any(add_name), any(add_out),any(add_l_in),any(add_l_out),any(add_tag),any(add_date)])
					if add:
						status['keys'].append(dp)
						status['filename'].append(schema[dp]['filename'])
						status['lm'].append(schema[dp]['lm'])
						status['dp'].append(schema[dp])

		self.filtered = True
		self.status = status
		return status

	def add_datapoint(self, key):
		schema = self.get_schema()
		dp = schema[list(schema.keys())[0]]
		dataset = self.init.storage.load_file_global(dp['filename'])
		local_array = pd.read_csv(dataset, encoding='unicode_escape', encoding_errors='backslashreplace', lineterminator='\n').to_dict('records')
		print(local_array)
		copy_dict = local_array[0].copy()
		key_list = list(copy_dict.keys())
		arr = {}
		for k in range(len(key_list)):
			if k == 0:
				if key == '':
					arr[key_list[0]] = 'Fill in'
				else:
					arr[key_list[0]] = key
			else:
				arr[key_list[k]] = '?'
		local_array.append(arr)
		
		df = pd.DataFrame(local_array)
		from io import StringIO
		csv_buffer = StringIO()
		df.to_csv(csv_buffer, index=False)
		self.init.storage.add_file_from_binary_global(dp['filename'],io.BytesIO(bytes(csv_buffer.getvalue(), 'utf-8')))
		return True
	
	def add_slice(self, slice_name=''):
		status = set(self.status['keys'])
		for val in self.schema.keys():
			if type(self.schema[val]) is dict:
				if val in status:
					if 'slices' in self.schema[val].keys():
						if not slice_name in self.schema[val]['slices']:
							self.schema[val]['slices'].append(slice_name)
					else:
						self.schema[val]['slices'] = [slice_name]

		# stores schema file
		if self.in_version:
			self.init.storage.add_file_from_binary_global(self.schema_path,io.BytesIO(json.dumps(self.version_schema).encode('ascii')))
		else:
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
		if self.in_version:
			self.init.storage.add_file_from_binary_global(self.schema_path,io.BytesIO(json.dumps(self.version_schema).encode('ascii')))
		else:
			self.init.storage.add_file_from_binary_global(self.schema_path,io.BytesIO(json.dumps(self.schema).encode('ascii')))
		self.init.storage.reset_buffer()

		return self.compute_meta_data()

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
				if key == val:
					if 'slices' in self.schema[val].keys():
						return self.schema[val]['slices']
		return []
		
	def select_slice(self, slices=[]):
		if len(slices) == 0:
			self.sliced = False
			self.selected_slices = []
			return self.status
		self.sliced = True
		self.selected_slices = slices
		return self.apply_filters({'slic': []})
	
	def get_status(self):
		return self.status

	def get_dp(self, key):
		schema = self.get_schema()
		return schema[key]	