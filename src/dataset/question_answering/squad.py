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

class squad_qa_schema(object):
	def __init__(self, init):
		self.init = init
		self.schema = None
		self.metadata = None
		self.schema_path = self.init.prefix_meta + 'schema.json'
		self.meta_path = self.init.prefix_meta + 'squad_qa_data.json'
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
				dataset = json.load(self.init.storage.load_file_global(key))
				local_array = dataset['data']
                
				for el in local_array:
					dp = {}
					dp['filename'] = key
					dp['key'] = el['title']

					dp['paragraphs'] = el['paragraphs']
					dp['questions'] = [] 
					for p in dp['paragraphs']:
						dp['questions'] += p['qas']
					dp['answers'] = [] 
					for q in dp['questions']:
						dp['answers'] += q['answers'] 
					
					dp['lm'] = current['lm'][idx]

					dp['tags'] = []
					dp['slices'] = []
					dp['length'] = 0
					for p in dp['paragraphs']:
						dp['length'] += len(p['context'])
					dp['idx'] = idx_key
					dp['versions'] = [{'key': key, 'version': self.init.get_latest_diff_number(key), 'title': el['title'],'type': 'added', 'source': 'N/A', 'comment': '', 'file': 'raw', 'diff': self.init.prefix_diffs + key + '/' + str(self.init.get_latest_diff_number(key)).zfill(10), 'date': current['lm'][idx]}]
					schema[el['title']] = dp
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

	def get_labels(self, title, version='current'):
		if version == 'current':
			schema = self.get_schema()
			try:
				return schema[title]['paragraphs']
			except:
				for val in schema:
					if title in val:
						return schema[val]['paragraphs']
		else:
			schema = self.get_schema()
			diff = schema[title]['versions'][int(version)-1]['diff']
			dataset = json.load(self.init.storage.load_file_global(diff))
			local_array = dataset['data']
			for el in local_array:
				if el['title'] == schema[title]['key']: 
					return el['paragraphs']
			return []

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
		
		paragraph_lengths = []
		answer_lengths = []
		question_lengths = []
		lm = []
		tags = []
		slices = []

		n_paragraphs = []
		n_answers = []
		n_questions = []
		n_p_len = {}
		n_ans_len = {}
		n_q_len = {}
		n_lm = {}
		n_tags = {}
		n_slices = {}

		for val in schema:
			if type(self.schema[val]) is dict:
				if not len(schema[val]['answers']) in n_answers:
					n_answers.append(len(schema[val]['answers']))
				
				if not len(schema[val]['answers']) in n_paragraphs:
					n_paragraphs.append(len(schema[val]['paragraphs']))

				if 'slice' in schema[val].keys():
					for sl in schema[val]['slices']:
						if not sl in slices:
							slices.append(sl)
							n_slices[sl] = 1
						else:
							n_slices[sl] += 1

				for q in schema[val]['questions']:
					if not len(q['question']) in question_lengths:
						question_lengths.append(len(q['question']))
						n_q_len[len(q['question'])] = 1
					else:
						n_q_len[len(q['question'])] += 1

				for p in schema[val]['paragraphs']:
					if not len(p['context']) in paragraph_lengths:
						paragraph_lengths.append(len(p['context']))
						n_p_len[len(p['context'])] = 1
					else:
						n_p_len[len(p['context'])] += 1

				for a in schema[val]['answers']:
					if not len(a['text']) in answer_lengths:
						answer_lengths.append(len(a['text']))
						n_ans_len[len(a['text'])] = 1
					else:
						n_ans_len[len(a['text'])] += 1
				
				if not len(schema[val]['questions']) in n_questions:
					n_questions.append(len(schema[val]['questions']))
				
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

		metadata = {'answer_lengths': answer_lengths, 'question_lengths' : question_lengths, 'paragraph_lengths' : paragraph_lengths, 
		'lm': lm, 'slices': slices, 'n_slices': n_slices, 'tags': tags, 'n_lm': n_lm, 
		'n_tags': n_tags, 'n_paragraphs': n_paragraphs, 'n_answers': n_answers, 'n_questions': n_questions, 
		'n_ans_len': n_ans_len, 'n_q_len': n_q_len, 'n_p_len': n_p_len}
			
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
				dataset = json.load(self.init.storage.load_file_global(key))
				local_array = dataset['data']
                
				for el in local_array:
					dp = {}
					dp['filename'] = key
					dp['key'] = el['title']

					dp['paragraphs'] = el['paragraphs']
					dp['questions'] = []
					for p in dp['paragraphs']:
						dp['questions'] += p['qas']
					dp['answers'] = [] 
					for q in dp['questions']:
						dp['answers'] += q['answers'] 

					dp['lm'] = self.init.storage.load_file_metadata_global(key)['last_modified']

					dp['tags'] = []
					dp['slices'] = []
					dp['length'] = 0
					for p in dp['paragraphs']:
						dp['length'] += len(p['context'])
					dp['idx'] = idx_key
					dp['versions'] = [{'key': key, 'version': self.init.get_latest_diff_number(key), 'title': el['title'],'type': 'added', 'source': 'N/A', 'comment': '', 'file': 'raw', 'diff': self.init.prefix_diffs + key + '/' + str(self.init.get_latest_diff_number(key)).zfill(10), 'date': self.init.storage.load_file_metadata_global(key)['last_modified']}]
					schema[el['title']] = dp
					idx_key +=1
					idx += 1
			idx += 1

		keys = set(schema.keys())

		for key in modified:
			if '.json' in key:
				idx_key = 0
				dataset = json.load(self.init.storage.load_file_global(key))
				local_array = dataset['data']
				for el in local_array:
					if el['title'] in list(keys):
						if not el['paragraphs'] == schema[el['title']]['paragraphs']:
							dp = {}
							dp['filename'] = key
							dp['key'] = el['title']

							dp['paragraphs'] = el['paragraphs']
							dp['questions'] = []
							for p in dp['paragraphs']:
								dp['questions'] += p['qas']
							dp['answers'] = [] 
							for q in dp['questions']:
								dp['answers'] += q['answers'] 

							dp['lm'] = self.init.storage.load_file_metadata_global(key)['last_modified']
							
							dp['tags'] = []
							dp['slices'] = []
							dp['length'] = 0
							for p in dp['paragraphs']:
								dp['length'] += len(p['context'])
							dp['idx'] = idx_key
							list_ver = schema[el['title']]['versions']
							list_ver.append({'key': key, 'title': el['title'], 'version': self.init.get_latest_diff_number(key),'type': 'modified', 'source': 'N/A', 'comment': '', 'file': 'raw', 'diff': self.init.prefix_diffs + key + '/' + str(self.init.get_latest_diff_number(key)).zfill(10), 'date': self.init.storage.load_file_metadata_global(key)['last_modified']})
							dp['versions'] = list_ver
							dp['idx'] = idx_key
							schema[el['title']] = dp
					else:
						dp = {}
						dp['filename'] = key
						dp['key'] = el['title']

						dp['paragraphs'] = el['paragraphs']
						dp['questions'] = [p['qas'] for p in el['paragraphs']][0]
						dp['answers'] = [q['answers'] for q in dp['questions']][0]

						dp['lm'] = self.init.storage.load_file_metadata_global(key)['last_modified']
						dp['tags'] = []
						dp['slices'] = []
						dp['length'] = 0
						for p in dp['paragraphs']:
							dp['length'] += len(p['context'])
						dp['idx'] = idx_key
						dp['versions'] = [{'key': key, 'version': self.init.get_latest_diff_number(key), 'title': el['title'],'type': 'added', 'source': 'N/A', 'comment': '', 'file': 'raw', 'diff': self.init.prefix_diffs + key + '/' + str(self.init.get_latest_diff_number(key)).zfill(10), 'date': self.init.storage.load_file_metadata_global(key)['last_modified']}]
						schema[el['title']] = dp
						idx += 1
					idx_key +=1

				local_keys = set([el['title'] for el in local_array])
				keys_new = set(schema.keys())
				for dp in list(keys_new):
					if type(schema[dp]) is dict:
						if not schema[dp]['key'] in local_keys:
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
		for val in keys:
			if type(self.schema[val]) is dict:
				if key == val:
					if 'tags' in self.schema[val].keys():
						return self.schema[val]['tags']
		return []

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
		keys = list(self.get_schema().keys())
		for val in keys:
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
		for val in keys:
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
		dataset = json.load(self.init.storage.load_file_global(dp['filename']))
		local_array = dataset['data']
		for idx in range(len(local_array)):
			if local_array[idx]['title'] == key:
				local_array[idx]['paragraphs'] = labels_array
		dataset['data'] = local_array
		self.init.storage.add_file_from_binary_global(dp['filename'],io.BytesIO(json.dumps(dataset).encode('ascii')))
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
				status['dp'].append(schema[dp]['paragraphs'])
		self.status = status
		return status

	def download_files(self):
		if self.status == None:
			self.status = self.read_all_files()

		schema = self.get_schema()
		local_array = []
		mem_zip = io.BytesIO()

		for key in self.status['keys']:
			local_array.append({'title': schema[key]['key'], 'paragraphs': schema[key]['paragraphs']})
		
		dataset = {'version': "stack", 'data': local_array}

		with zipfile.ZipFile(mem_zip, "a", zipfile.ZIP_DEFLATED, False) as zf:
			zf.writestr('dataset.json', json.dumps(dataset).encode('ascii'))
		return mem_zip.getvalue()

	def export_openai(self):
		if self.status == None:
			self.status = self.read_all_files()

		schema = self.get_schema()
		local_array = []
		mem_zip = io.BytesIO()

		for key in self.status['keys']:
			for p in schema[key]['paragraphs']:
				for q in p['qas']:
					if len(q['answers']) > 0:
						for a in q['answers']:
							local_array.append({'prompt': f"{p['context']}\nQuestion: {q['question']}\nAnswer: ", 'completion': a['text']})
					else:
						local_array.append({'prompt': f"{p['context']}\nQuestion: {q['question']}\nAnswer: ", 'completion': 'No appropriate context found'})
		
		with zipfile.ZipFile(mem_zip, "a", zipfile.ZIP_DEFLATED, False) as zf:
			zf.writestr('dataset.json', json.dumps(local_array).encode('ascii'))
		return mem_zip.getvalue()

	def get_metadata(self):
		if (self.filtered or self.in_version) and (self.metadata is None):

			schema = self.get_schema()
		
			paragraph_lengths = []
			answer_lengths = []
			question_lengths = []
			lm = []
			tags = []
			slices = []

			n_paragraphs = []
			n_answers = []
			n_questions = []
			n_p_len = {}
			n_ans_len = {}
			n_q_len = {}
			n_lm = {}
			n_tags = {}
			n_slices = {}

			for val in schema:
				if type(self.schema[val]) is dict:
					if not len(schema[val]['answers']) in n_answers:
						n_answers.append(len(schema[val]['answers']))
					
					if not len(schema[val]['answers']) in n_paragraphs:
						n_paragraphs.append(len(schema[val]['paragraphs']))

					if 'slice' in schema[val].keys():
						for sl in schema[val]['slices']:
							if not sl in slices:
								slices.append(sl)
								n_slices[sl] = 1
							else:
								n_slices[sl] += 1

					for q in schema[val]['questions']:
						if not len(q['question']) in question_lengths:
							question_lengths.append(len(q['question']))
							n_q_len[len(q['question'])] = 1
						else:
							n_q_len[len(q['question'])] += 1

					for p in schema[val]['paragraphs']:
						if not len(p['context']) in paragraph_lengths:
							paragraph_lengths.append(len(p['context']))
							n_p_len[len(p['context'])] = 1
						else:
							n_p_len[len(p['context'])] += 1

					for a in schema[val]['answers']:
						if not len(a['text']) in answer_lengths:
							answer_lengths.append(len(a['text']))
							n_ans_len[len(a['text'])] = 1
						else:
							n_ans_len[len(a['text'])] += 1
					
					if not len(schema[val]['questions']) in n_questions:
						n_questions.append(len(schema[val]['questions']))
					
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

			self.metadata = {'answer_lengths': answer_lengths, 'question_lengths' : question_lengths, 'paragraph_lengths' : paragraph_lengths, 
			'lm': lm, 'slices': slices, 'n_slices': n_slices, 'tags': tags, 'n_lm': n_lm, 
			'n_tags': n_tags, 'n_paragraphs': n_paragraphs, 'n_answers': n_answers, 'n_questions': n_questions, 
			'n_ans_len': n_ans_len, 'n_q_len': n_q_len, 'n_p_len': n_p_len}
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
					add_par =  []
					add_q =  []
					add_ans =  []

					add_l_par =  []
					add_l_q =  []
					add_l_ans =  []

					add_tag =  []
					add_date =  []

					for f in filters:
						for filt in filters[f]:
							
							if filt == 'name':
								if filters[f]['name'] in schema[dp]['tittle']:
									add_name.append(True)
								else:
									add_name.append(False)
							
							if filt == 'p_search':
								size = False
								for p in schema[dp]['paragraphs']:
									if  filters[f][filt] in p['context']:
										size = True

								if size:
									add_par.append(True)
								else:
									add_par.append(False)

							if filt == 'q_search':
								size = False
								for q in schema[dp]['questions']:
									if  filters[f][filt] in q['question']:
										size = True

								if size:
									add_q.append(True)
								else:
									add_q.append(False)

							if filt == 'a_search':
								size = False
								for a in schema[dp]['answers']:
									if  filters[f][filt] in a['text']:
										size = True
								if size:
									add_ans.append(True)
								else:
									add_ans.append(False)
							
							if filt == 'ans_len':
								min_cl = int(filters[f]['ans_len'][0])
								max_cl = int(filters[f]['ans_len'][1])
								size = False
								for answer in schema[dp]['answers']:
									if (len(answer['text']) <= max_cl) and (len(answer['text']) >= min_cl):
										size = True

								if size:
									add_l_ans.append(True)
								else:
									add_l_ans.append(False)

							if filt == 'q_len':
								min_cl = int(filters[f]['q_len'][0])
								max_cl = int(filters[f]['q_len'][1])
								size = False
								for q in schema[dp]['questions']:
									if (len(q['question']) <= max_cl) and (len(q['question']) >= min_cl):
										size = True

								if size:
									add_l_q.append(True)
								else:
									add_l_q.append(False)
							
							if filt == 'par_len':
								min_cl = int(filters[f]['par_len'][0])
								max_cl = int(filters[f]['par_len'][1])
								size = False
								for p in schema[dp]['paragraphs']:
									if (len(p['context']) <= max_cl) and (len(p['context']) >= min_cl):
										size = True

								if size:
									add_l_par.append(True)
								else:
									add_l_par.append(False)

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

					if len(add_par) == 0:
						add_par = [True]
					if len(add_q) == 0:
						add_q = [True]
					if len(add_ans) == 0:
						add_ans = [True]

					if len(add_l_par) == 0:
						add_l_par = [True]
					if len(add_l_q) == 0:
						add_l_q = [True]
					if len(add_l_ans) == 0:
						add_l_ans = [True]

					if len(add_tag) == 0:
						add_tag = [True]
					if len(add_date) == 0:
						add_date = [True]
						
					add = all([any(add_name), any(add_par),any(add_q),any(add_ans),any(add_l_par),any(add_l_q),any(add_l_ans),any(add_tag),any(add_date)])
					if add:
						status['keys'].append(dp)
						status['filename'].append(schema[dp]['filename'])
						status['lm'].append(schema[dp]['lm'])
						status['dp'].append(schema[dp]['paragraphs'])

		self.filtered = True
		self.status = status
		return status

	def add_datapoint(self, key):
		schema = self.get_schema()
		dp = schema[list(schema.keys())[0]]
		dataset = json.load(self.init.storage.load_file_global(dp['filename']))
		local_array = dataset['data']
		new_dp = {'title': key, 'paragraphs': [{'context': '', 'qas': [{'question': '', 'answers': []}]}]}
		local_array.append(new_dp)
		dataset['data'] = local_array
		self.init.storage.add_file_from_binary_global(dp['filename'],io.BytesIO(json.dumps(dataset).encode('ascii')))
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