import sys
sys.path.append( '../../..' )
from datetime import *
from tzlocal import get_localzone
import cv2
import numpy as np
import time
import zipfile
import os
import copy
import io
import json
from pathlib import Path
from src.comm.docker_ver import *
path_home = os.getenv('LCP_DKR')+'/' if docker_ver() else str(Path.home())
import hashlib

class spacy_ner_schema(object):
	def __init__(self, init):
		self.init = init
		self.schema = None
		self.metadata = None
		self.schema_path = self.init.prefix_meta + 'schema.json'
		self.meta_path = self.init.prefix_meta + 'spacy_ner_data.json'
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
			if '.json' in key:
				idx_key = 0
				local_array = json.load(self.init.storage.load_file_global(key))
				for el in local_array:
					dp = {}
					labels = el['entities']
					dp['filename'] = key
					dp['key'] = el['text']
					dp['text'] = el['text']
					dp['lm'] = current['lm'][idx]
					dp['entities'] = labels
					dp['entity_types'] = [label['type'] for label in labels]
					dp['n_entities'] = len(dp['entities'])
					dp['tags'] = []
					dp['slices'] = []
					dp['metadata'] = {}
					dp['length'] = len(dp['text'])
					dp['idx'] = idx_key
					dp['versions'] = [{'key': key, 'version': self.init.get_latest_diff_number(key), 'text': el['text'],'type': 'added', 'source': 'N/A', 'comment': '', 'file': 'raw', 'diff': self.init.prefix_diffs + key + '/' + str(self.init.get_latest_diff_number(key)).zfill(10), 'date': current['lm'][idx]}]
					hash = hashlib.md5(el['text'].encode('utf-8')).hexdigest()
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

	def get_labels(self, filename, version='current'):
		if version == 'current':
			schema = self.get_schema()
			return schema[filename]['entities']
		else:
			schema = self.get_schema()
			diff = schema[filename]['versions'][int(version)-1]['diff']
			local_array = json.load(self.init.storage.load_file_global(diff))
			for el in local_array:
				if el['text'] == schema[filename]['text']: 
					return el['entities']
			return []

	def get_text(self, filename, version='current'):
		if version == 'current':
			schema = self.get_schema()
			return schema[filename]['text']
		else:
			schema = self.get_schema()
			return schema[filename]['versions'][int(version)-1]['text']

	def set_text(self, filename, text=''):
		schema = self.get_schema()
		local_array = json.load(self.init.storage.load_file_global(schema[filename]['filename']))
		for idx in range(len(local_array)):
			if local_array[idx]['text'] == schema[filename]['text']:
				local_array[idx]['text'] = text
		self.init.storage.add_file_from_binary_global(schema[filename]['filename'],io.BytesIO(json.dumps(local_array).encode('ascii')))
		
		el = schema[filename]
		dp = {}
		labels = el['entities']
		dp['filename'] = schema[filename]['filename']
		dp['text'] = text
		dp['key'] = el['key']
		dp['metadata'] = el['metadata']
		dp['lm'] = self.init.storage.load_file_metadata_global(el['filename'])['last_modified']
		list_ver = el['versions']
		list_ver.append({'key': el['filename'], 'text': text, 'version': self.init.get_latest_diff_number(el['filename']),'type': 'modified', 'source': 'N/A', 'comment': '', 'file': 'raw', 'diff': self.init.prefix_diffs + el['filename'] + '/' + str(self.init.get_latest_diff_number(el['filename'])).zfill(10), 'date': self.init.storage.load_file_metadata_global(el['filename'])['last_modified']})
		dp['versions'] = list_ver
		dp['entities'] = labels
		dp['entity_types'] = [label['type'] for label in labels]
		dp['n_entities'] = len(dp['entities'])
		dp['tags'] = []
		dp['slices'] = []
		dp['length'] = len(dp['text'])
		dp['idx'] = el['idx']

		hash = hashlib.md5(el['text'].encode('utf-8')).hexdigest()
		schema[hash] = dp
		del schema[filename]

		self.init.storage.add_file_from_binary_global(self.schema_path,io.BytesIO(json.dumps(schema).encode('ascii')))
		self.init.storage.reset_buffer()

		return True
		

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
		
		entities = []
		lengths = []
		lm = []
		tags = []
		entities_per_sentence = []
		slices = []

		n_entity = {}
		n_len = {}
		n_lm = {}
		n_tags = {}
		n_slices = {}

		for val in schema:
			if type(self.schema[val]) is dict:
				if not len(schema[val]['entities']) in entities_per_sentence:
					entities_per_sentence.append(len(schema[val]['entities']))
				
				if 'slice' in schema[val].keys():
					for sl in schema[val]['slices']:
						if not sl in slices:
							slices.append(sl)
							n_slices[sl] = 1
						else:
							n_slices[sl] += 1

				for cl in schema[val]['entities']:
					if not cl['type'] in entities:
						entities.append(cl['type'])
						n_entity[cl['type']] = 1
					else:
						n_entity[cl['type']] += 1
				
				if not schema[val]['length'] in lengths:
					lengths.append(schema[val]['length'])
					n_len[schema[val]['length']] = 1
				else:
					n_len[schema[val]['length']] += 1
				
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

		metadata = {'entities': entities, 'lengths': lengths, 'lm': lm, 'slices': slices, 'n_slices': n_slices, 'tags': tags, 'n_entity': n_entity, 'n_len': n_len, 'n_lm': n_lm, 'n_tags': n_tags, 'entities_per_sentence': entities_per_sentence}
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
			if '.json' in key:
				idx_key = 0
				local_array = json.load(self.init.storage.load_file_global(key))
				for el in local_array:
					dp = {}
					labels = el['entities']
					dp['filename'] = key
					dp['key'] = el['text']
					dp['text'] = el['text']
					dp['lm'] = self.init.storage.load_file_metadata_global(key)['last_modified']
					dp['entities'] = labels
					dp['entity_types'] = [label['type'] for label in labels]
					dp['n_entities'] = len(dp['entities'])
					dp['tags'] = []
					dp['metadata'] = {}
					dp['slices'] = []
					dp['length'] = len(dp['text'])
					dp['idx'] = idx_key
					dp['versions'] = [{'key': key, 'text': el['text'], 'version': self.init.get_latest_diff_number(key), 'type': 'added', 'source': 'N/A', 'comment': '', 'file': 'raw', 'diff': self.init.prefix_diffs + key + '/' + str(self.init.get_latest_diff_number(key)).zfill(10), 'date': self.init.storage.load_file_metadata_global(key)['last_modified']}]
					hash = hashlib.md5(el['text'].encode('utf-8')).hexdigest()
					schema[hash] = dp
					idx_key +=1	
			idx += 1

		keys = set(schema.keys())

		for key in modified:
			if '.json' in key:
				idx_key = 0
				local_array = json.load(self.init.storage.load_file_global(key))
				for el in local_array:
					hash = hashlib.md5(el['text'].encode('utf-8')).hexdigest()
					if hash in list(keys):
						dp = {}
						labels = el['entities']
						dp['filename'] = key
						dp['key'] = schema[hash]['key']
						dp['text'] = el['text']
						if labels == schema[hash]['entities']:
							dp['lm'] = schema[hash]['lm']	
							dp['versions'] = schema[hash]['versions']
						else:
							dp['lm'] = self.init.storage.load_file_metadata_global(key)['last_modified']
							list_ver = schema[hash]['versions']
							# print(f"list ver {schema[el['text']]['versions']} and append {list_ver.append({'key': key, 'version': list_ver[-1]['version'],'type': 'modified', 'source': 'N/A', 'comment': '', 'file': 'raw', 'diff': self.init.prefix_diffs + key + '/' + str(self.init.get_latest_diff_number(key)).zfill(10), 'date': self.init.storage.load_file_metadata_global(key)['last_modified']})}")
							list_ver.append({'key': key, 'text': el['text'], 'version': self.init.get_latest_diff_number(key),'type': 'modified', 'source': 'N/A', 'comment': '', 'file': 'raw', 'diff': self.init.prefix_diffs + key + '/' + str(self.init.get_latest_diff_number(key)).zfill(10), 'date': self.init.storage.load_file_metadata_global(key)['last_modified']})
							dp['versions'] = list_ver
						dp['entities'] = labels
						dp['metadata'] = schema[hash]['metadata']
						dp['entity_types'] = [label['type'] for label in labels]
						dp['n_entities'] = len(dp['entities'])
						dp['tags'] = []
						dp['slices'] = []
						dp['length'] = len(dp['text'])
						dp['idx'] = idx_key
						schema[hash] = dp
					else:
						dp = {}
						labels = el['entities']
						dp['filename'] = key
						dp['key'] = el['text']
						dp['text'] = el['text']
						dp['lm'] = self.init.storage.load_file_metadata_global(key)['last_modified']
						dp['entities'] = labels
						dp['entity_types'] = [label['type'] for label in labels]
						dp['n_entities'] = len(dp['entities'])
						dp['tags'] = []
						dp['slices'] = []
						dp['length'] = len(dp['text'])
						dp['idx'] = idx_key
						dp['versions'] = [{'key': key, 'text': el['text'], 'version': self.init.get_latest_diff_number(key),'type': 'added', 'source': 'N/A', 'comment': '', 'file': 'raw', 'diff': self.init.prefix_diffs + key + '/' + str(self.init.get_latest_diff_number(key)).zfill(10), 'date': self.init.storage.load_file_metadata_global(key)['last_modified']}]
						schema[hash] = dp
						idx += 1
					idx_key +=1

				local_keys = set([el['text'] for el in local_array])
				keys_new = set(schema.keys())
				for dp in list(keys_new):
					if type(schema[dp]) is dict:
						if not schema[dp]['text'] in local_keys:
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
		keys = list(self.get_schema().keys())
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

	def label_name(self, class_number):
		return str(class_number)

	def set_labels(self, key, labels_array):
		schema = self.get_schema()
		dp = schema[key]
		local_array = json.load(self.init.storage.load_file_global(dp['filename']))
		for idx in range(len(local_array)):
			if type(local_array[idx]) is dict:
				if local_array[idx]['text'] == dp['text']:
					local_array[idx]['entities'] = labels_array
		self.init.storage.add_file_from_binary_global(dp['filename'],io.BytesIO(json.dumps(local_array).encode('ascii')))
		return True
	
	def branch(self, branch_name, type_ ='copy'):
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
				local_array = json.load(self.init.storage.load_file_global(file))
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
				status['dp'].append(schema[dp]['entities'])
		self.status = status
		return status

	def download_files(self):
		if self.status == None:
			self.status = self.read_all_files()

		schema = self.get_schema()
		local_array = []
		mem_zip = io.BytesIO()

		for key in self.status['keys']:
			local_array.append({'text': schema[key]['text'], 'entities': schema[key]['entities']})

		with zipfile.ZipFile(mem_zip, "a", zipfile.ZIP_DEFLATED, False) as zf:
			zf.writestr('dataset.json', json.dumps(local_array).encode('ascii'))
		return mem_zip.getvalue()

	def export_openai(self):
		if self.status == None:
			self.status = self.read_all_files()

		schema = self.get_schema()
		local_array = []
		mem_zip = io.BytesIO()

		for key in self.status['keys']:
			entities_str = f""
			for e in schema[key]['entities']:
				e_str = schema[key]['text'][e['start']-1:e['end']]
				entities_str += e_str
				entities_str += "\n"

			entities_str = entities_str[:-2]
			entities_str += ' variant END'
			local_array.append({'prompt': f"{schema[key]['text']}...\n\n###\n\n", 'completion': entities_str})
		
		with zipfile.ZipFile(mem_zip, "a", zipfile.ZIP_DEFLATED, False) as zf:
			zf.writestr('dataset.json', json.dumps(local_array).encode('ascii'))
		return mem_zip.getvalue()

	def get_metadata(self):
		if self.filtered or self.in_version:
			schema = self.get_schema()
		
			entities = []
			lengths = []
			lm = []
			tags = []
			entities_per_sentence = []
			slices = []

			n_entity = {}
			n_len = {}
			n_lm = {}
			n_tags = {}
			n_slices = {}

			for val in self.status['keys']:
				if type(self.schema[val]) is dict:
					if not len(schema[val]['entities']) in entities_per_sentence:
						entities_per_sentence.append(len(schema[val]['entities']))
					
					if 'slice' in schema[val].keys():
						for sl in schema[val]['slices']:
							if not sl in slices:
								slices.append(sl)
								n_slices[sl] = 1
							else:
								n_slices[sl] += 1

					for cl in schema[val]['entities']:
						if not cl['type'] in entities:
							entities.append(cl['type'])
							n_entity[cl['type']] = 1
						else:
							n_entity[cl['type']] += 1
					
					if not schema[val]['length'] in lengths:
						lengths.append(schema[val]['length'])
						n_len[schema[val]['length']] = 1
					else:
						n_len[schema[val]['length']] += 1
					
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

			return {'entities': entities, 'lengths': lengths, 'lm': lm, 'slices': slices, 'n_slices': n_slices, 'tags': tags, 'n_entity': n_entity, 'n_len': n_len, 'n_lm': n_lm, 'n_tags': n_tags, 'entities_per_sentence': entities_per_sentence}
		else:
			return json.load(self.init.storage.load_file_global(self.meta_path))

	def reset_filters(self):
		self.filtered = False
		return True
	
	def apply_filters(self, filters={}):
		schema = self.get_schema()
		status = {'keys': [], 'lm': [], 'filename': [], 'dp': []}

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
					add_entity =  []
					add_len =  []
					add_name =  []
					add_tag =  []
					add_date =  []
					add_box =  []

					for f in filters:
						for filt in filters[f]:
							if filt == 'num_entities':
								
								min_cl = int(filters[f]['num_entities'][0])
								max_cl = int(filters[f]['num_entities'][1])
								
								if (len(schema[dp]['entities']) <= max_cl) and (len(schema[dp]['entities']) >= min_cl):
									add_num.append(True)
								else:
									add_num.append(False)
							
							if filt == 'entity':
								if filters[f]['entity'] in schema[dp]['entity_types']:
									add_entity.append(True)
								else:
									add_entity.append(False)
									
							if filt == 'length':
								if int(filters[f]['length']) == schema[dp]['length']:
									add_len.append(True)
								else:
									add_len.append(False)

							if filt == 'name':
								if filters[f]['name'] in schema[dp]['text']:
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

							if filt == 'entity_len':
								
								a_min = min(filters[f]['entity_len'][0], filters[f]['entity_len'][1])
								a_max = max(filters[f]['entity_len'][0], filters[f]['entity_len'][1])

								
								if 'entities' in schema[dp]:
									if len(schema[dp]['entities']) == 0:
										add_box.append(False)

									for label in schema[dp]['entities']:
										area = label['end'] - label['start'] + 1
										
										if (area >= a_min) and (area <= a_max):
											add_box.append(True)
										else:
											add_box.append(False)
								else:
									add_box.append(False)
					
					if len(add_entity) == 0:
						add_entity = [True]
					if len(add_len) == 0:
						add_len = [True]
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
						
					add = all([any(add_entity),any(add_len),any(add_name),any(add_tag),any(add_box),any(add_num),any(add_date)])
					if add:
						status['keys'].append(dp)
						status['filename'].append(schema[dp]['filename'])
						status['lm'].append(schema[dp]['lm'])
						status['dp'].append(schema[dp]['entities'])

		self.filtered = True
		self.status = status
		return status

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
		
	def add_datapoint(self, key):
		schema = self.get_schema()
		dp = schema[list(schema.keys())[0]]
		local_array = json.load(self.init.storage.load_file_global(dp['filename']))
		new_datapoint = {'text': key, 'entities': []}
		local_array.append(new_datapoint)
		self.init.storage.add_file_from_binary_global(dp['filename'],io.BytesIO(json.dumps(local_array).encode('ascii')))
		return True

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