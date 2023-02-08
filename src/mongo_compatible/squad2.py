import math
import sys
sys.path.append( '../../..' )
from datetime import *
from tzlocal import get_localzone
import cv2
import numpy as np
import time
import zipfile
import os
from datetime import date
import copy
import io
import json
from pathlib import Path
from src.comm.docker_ver import *
path_home = os.getenv('LCP_DKR')+'/' if docker_ver() else str(Path.home())
import hashlib

class squad2(object):
	def __init__(self, client):
		self.client = client
		self.schema = None
		self.metadata = None
		self.status = {}
		self.filtered = False
		self.sliced = False
		self.selected_slices = False
		self.bounding_box_thumbs = True

		self.in_version = False
		self.version_keys = None
		self.selected_version = None
		self.version_schema = ''

	def create_schema_file(self, file):
		# generates the schema file for a squad2 dataset
		dataset = json.load(file)

		# finds the images
		local_array = dataset['data']
		
		for el in local_array:
			for p in el['paragraphs']:
				for q in p['qas']:
					dp = {}
					dp['title'] = el['title']
					dp['context'] = p['context']
					dp['question'] = q['question']
					dp['answers'] = q['answers']
					
					hash = hashlib.md5((dp['title']+dp['context']+dp['question']).encode('utf-8')).hexdigest()
					dp['key'] = hash
					
					if len(q['answers']) > 0:
						dp['labeled'] = True
					else:
						dp['labeled'] = False

					dp['comments'] = []
					dp['slices'] = []
					dp['metadata'] = {}
					
					now = datetime.now()
					dp['lm'] = now.strftime("%d/%m/%Y %H:%M:%S")
					dp['versions'] = [{'key': hash, 'version': 0, 'type': 'added', 'diff': dp.copy(), 'date': dp['lm']}]
					print(dp)
					self.client.find_one_and_replace({'key': dp['key']}, dp, upsert=True)
					
		# stores dp
		self.schema = self.client.find({})

		return True

	def get_labels(self, key, version='current'):
		if version == 'current':
			for dp in self.client.find({'key': key}):
				return dp
		else:
			for dp in self.client.find({'key': key}):
				return dp['versions'][int(version)-1]['diff']
		return {}
			
	def get_schema(self):
		if self.schema == None:
			try:
				self.schema = self.client.find({})
			except:
				self.create_schema_file()
		return self.schema

	def compute_meta_data(self):

		# different metadata values
		paragraph_lengths = []
		answer_lengths = []
		question_lengths = []
		lm = []
		tags = []
		slices = []

		n_answers = []
		n_p_len = {}
		n_ans_len = {}
		n_q_len = {}
		n_lm = {}
		n_tags = {}
		n_slices = {}

		for dp in self.client.find({}):
			if not len(dp['answers']) in n_answers:
				n_answers.append(len(dp['answers']))

			for sl in dp['slices']:
				if not sl in slices:
					slices.append(sl)
					n_slices[sl] = 1
				else:
					n_slices[sl] += 1

			if not len(dp['question']) in question_lengths:
				question_lengths.append(len(dp['question']))
				n_q_len[len(dp['question'])] = 1
			else:
				n_q_len[len(dp['question'])] += 1

			if not len(dp['context']) in paragraph_lengths:
				paragraph_lengths.append(len(dp['context']))
				n_p_len[len(dp['context'])] = 1
			else:
				n_p_len[len(dp['context'])] += 1
			for a in dp['answers']:
				if not len(a['text']) in answer_lengths:
					answer_lengths.append(len(a['text']))
					n_ans_len[len(a['text'])] = 1
				else:
					n_ans_len[len(a['text'])] += 1
			
			if not dp['lm'] in lm:
				lm.append(dp['lm'])
				n_lm[dp['lm']] = 1
			else:
				n_lm[dp['lm']] += 1

			for tag in dp['tags']:
					if not tag in tags:
						tags.append(tag)
						n_tags[tag] = 1
					else:
						n_tags[tag] += 1

		metadata = {'answer_lengths': answer_lengths, 'question_lengths' : question_lengths, 'paragraph_lengths' : paragraph_lengths, 
		'lm': lm, 'slices': slices, 'n_answers': n_answers, 'n_slices': n_slices, 'tags': tags, 'n_lm': n_lm, 'n_tags': n_tags, 'n_ans_len': n_ans_len, 'n_q_len': n_q_len, 'n_p_len': n_p_len}
			
		self.metadata = metadata

		return True

	def get_metadata_tags(self, key):
		for k in self.client.find({'key': key}):
			return k['metadata']
		return []

	def add_metadata_tags(self, key, tag):
		for k in self.client.find({'key': key}):
			dp = k
			dp['metadata'][tag['key']] = tag['val']
			self.client.find_one_and_replace({'key': key}, dp)

		return True

	def remove_metadata_tags(self, key, tag):
		for k in self.client.find({'key': key}):
			dp = k
			if tag['key'] in dp['metadata'].keys():
				if tag['val'] in dp['metadata'][tag['key']]:
					dp['metadata'][tag['key']].remove(tag['val'])
			self.client.find_one_and_replace({'key': key}, dp)

		return True

	def get_comments(self, key):
		for k in self.client.find({'key': key}):
			return k['comments']
		return []

	def add_comment(self, key, comment):
		for dp in self.client.find({'key': key}):
			dp['comments'].append(comment)
			self.client.find_one_and_replace({'key': key}, dp)

		return self.compute_meta_data()

	def add_many_comment(self, keys, comment):
		for key in keys:
			for dp in self.client.find({'key': key}):
				dp['comments'].append(comment)
				self.client.find_one_and_replace({'key': key}, dp)

		return self.compute_meta_data()

	def remove_comment(self, key, comment):
		for dp in self.client.find({'key': key}):
			dp['comments'].remove(comment)
			self.client.find_one_and_replace({'key': key}, dp)

		return self.compute_meta_data()

	def many_remove_all_comments(self, keys):
		for key in keys:
			for dp in self.client.find({'key': key}):
				dp['comments'] = []
				self.client.find_one_and_replace({'key': key}, dp)

		return self.compute_meta_data()

	def set_labels(self, key, labels_array):
		
		for k in self.client.find({'key': key}):
			dp = {}
			dp['title'] = labels_array['title']
			dp['context'] = labels_array['context']
			dp['question'] = labels_array['question']
			dp['answers'] = labels_array['answers']
			dp['comments'] = k['comments']
			dp['slices'] = k['slices']
			dp['metadata'] = k['metadata']
			if len(labels_array['answers']) > 0:
				dp['labeled'] = True
			else:
				dp['labeled'] = False
			now = datetime.now()
			dp['lm'] = now.strftime("%d/%m/%Y %H:%M:%S")
			hash = hashlib.md5((dp['title']+dp['context']+dp['question']).encode('utf-8')).hexdigest()
			dp['key'] = hash
			vers = k['versions']
			vers.append({'key': hash, 'version': len(vers), 'type': 'modified', 'diff': dp.copy(), 'date': dp['lm']})
			dp['versions'] = vers
			self.client.find_one_and_replace({'key': key}, dp)
		self.read_all_files()
		return True

	def read_all_files(self):
		# queries the json
		status = {'keys': [], 'lm': [], 'filename': [], 'dp': []}
		import time
		t0 = time.time()
		list_ = list(self.client.find({}))
		print(f"time variable {time.time() - t0}")
		t0 = time.time()
		for dp in list_:
			dp.pop('_id')
			status['keys'].append(dp['key'])
			status['filename'].append(dp['title'])
			status['lm'].append(dp['lm'])
			status['dp'].append(dp)
		self.status = status
		print(f"time loop {time.time() - t0}")
		return status

	def download_files(self):
		if self.status == None:
			self.status = self.read_all_files()

		local_array = []
		mem_zip = io.BytesIO()

		titles = {}

		for dp in self.status['dp']:
			if dp['title'] in titles:
				titles[dp['title']].append(dp)
			else:
				titles[dp['title']] = [dp]

		paragraphs = {}
		for title in titles:
			paragraphs[title] = {}
			for dp in titles[title]:
				if dp['context'] in paragraphs[title]:
					paragraphs[title][dp['context']].append(dp)
				else:
					paragraphs[title][dp['context']] = [dp]

		questions = {}
		for title in paragraphs:
			for paragraph in paragraphs[title]:
				questions[title][paragraph] = {}
				for dp in paragraphs[title]:
					if paragraphs[title][dp]['question'] in questions[title][paragraphs]:
						questions[title][paragraphs].append(paragraphs[title][dp])
					else:
						questions[title][paragraphs].append(paragraphs[title][dp])
		
		local_array = []
		for t in questions:
			local_array.append({'title': t, 'paragraphs': []})
			for p in questions:
				arr = {'context': p, 'qas': []}
				for q in questions[p]:
					que = {'question': questions[p]['question'], 'answers': []}
					for a in questions[p]['answers']:
						que['answers'].append(a)
					arr['qas'].append(que)
				local_array[-1]['paragraphs'] = arr
		dataset = {'version': "exported_from_stack", 'data': local_array}

		with zipfile.ZipFile(mem_zip, "a", zipfile.ZIP_DEFLATED, False) as zf:
			zf.writestr('dataset.json', json.dumps(dataset).encode('ascii'))
		return mem_zip.getvalue()

	def export_openai(self):
		if self.status == None:
			self.status = self.read_all_files()

		local_array = []
		mem_zip = io.BytesIO()

		for dp in self.status['dp']:
			if len(dp['answers']) > 0:
				for a in dp['answers']:
					local_array.append({'prompt': f"{dp['paragraph']}\nQuestion: {dp['question']}\nAnswer: ", 'completion': a['text']})
			else:
				local_array.append({'prompt': f"{dp['paragraph']}\nQuestion: {dp['question']}\nAnswer: ", 'completion': 'No appropriate context found'})
		
		with zipfile.ZipFile(mem_zip, "a", zipfile.ZIP_DEFLATED, False) as zf:
			zf.writestr('dataset.json', json.dumps(local_array).encode('ascii'))
		return mem_zip.getvalue()

	def get_metadata(self):
		if (self.filtered or self.in_version) and (self.metadata is None):
			paragraph_lengths = []
			answer_lengths = []
			question_lengths = []
			lm = []
			tags = []
			slices = []

			n_answers = []
			n_p_len = {}
			n_ans_len = {}
			n_q_len = {}
			n_lm = {}
			n_tags = {}
			n_slices = {}

			for dp in self.status['dp']:
				if not len(dp['answers']) in n_answers:
					n_answers.append(len(dp['answers']))

				for sl in dp['slices']:
					if not sl in slices:
						slices.append(sl)
						n_slices[sl] = 1
					else:
						n_slices[sl] += 1

				if not len(dp['question']) in question_lengths:
					question_lengths.append(len(dp['question']))
					n_q_len[len(dp['question'])] = 1
				else:
					n_q_len[len(dp['question'])] += 1

				if not len(dp['context']) in paragraph_lengths:
					paragraph_lengths.append(len(dp['context']))
					n_p_len[len(dp['context'])] = 1
				else:
					n_p_len[len(dp['context'])] += 1
				for a in dp['answers']:
					if not len(a['text']) in answer_lengths:
						answer_lengths.append(len(a['text']))
						n_ans_len[len(a['text'])] = 1
					else:
						n_ans_len[len(a['text'])] += 1
				
				if not dp['lm'] in lm:
					lm.append(dp['lm'])
					n_lm[dp['lm']] = 1
				else:
					n_lm[dp['lm']] += 1

				for tag in dp['tags']:
						if not tag in tags:
							tags.append(tag)
							n_tags[tag] = 1
						else:
							n_tags[tag] += 1

			self.metadata = {'answer_lengths': answer_lengths, 'question_lengths' : question_lengths, 'paragraph_lengths' : paragraph_lengths, 
			'lm': lm, 'n_answers': n_answers, 'slices': slices, 'n_slices': n_slices, 'tags': tags, 'n_lm': n_lm, 'n_tags': n_tags, 'n_ans_len': n_ans_len, 'n_q_len': n_q_len, 'n_p_len': n_p_len}
		else:
			if self.metadata is None:
				self.compute_meta_data()
		return self.metadata

	def reset_filters(self):
		print('reset filters')
		self.filtered = False
		self.compute_meta_data()
		return True
	
	def apply_filters(self, filters={}):
		schema = self.get_schema()
		status = {'keys': [], 'lm': [], 'filename': [], 'dp': []}

		if len(filters) == 0:
			self.filtered = False
			return self.status

		self.metadata = None
		for dp in schema:
			
			if self.sliced:
				if 'slices' in dp.keys():
					pre_add = any([sl in dp['slices'] for sl in self.selected_slices])
				else:
					pre_add = False
			else:
				pre_add = True

			if 'labeler' in filters.keys():
				if 'labeler' in dp['metadata'].keys():
					if filters['labeler'] in dp['metadata']['labeler']:
						pre_add = True
					else:
						pre_add = False

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
							if filters[f]['name'] in dp['title']:
								add_name.append(True)
							else:
								add_name.append(False)
						
						if filt == 'p_search':
							if  filters[f][filt] in dp['paragraph']:
								add_par.append(True)
							else:
								add_par.append(False)

						if filt == 'q_search':
							if  filters[f][filt] in dp['question']:
								add_q.append(True)
							else:
								add_q.append(False)

						if filt == 'a_search':
							size = False
							for a in dp['answers']:
								if  filters[f][filt] in a['text']:
									size = True
							if size:
								add_ans.append(True)
							else:
								add_ans.append(False)

						if filt == 'n_ans':
							min_cl = int(filters[f]['n_ans'][0])
							max_cl = int(filters[f]['n_ans'][1])
							if len(dp['answers']) <= max_cl and len(dp['answers']) >= min_cl:
								add_ans.append(True)
							else:
								add_ans.append(False)
						
						if filt == 'ans_len':
							min_cl = int(filters[f]['ans_len'][0])
							max_cl = int(filters[f]['ans_len'][1])
							size = False
							for answer in dp['answers']:
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
							if (len(dp['question']) <= max_cl) and (len(dp['question']) >= min_cl):
								add_l_q.append(True)
							else:
								add_l_q.append(False)
						
						if filt == 'par_len':
							min_cl = int(filters[f]['par_len'][0])
							max_cl = int(filters[f]['par_len'][1])
							size = False
							if (len(dp['paragraph']) <= max_cl) and (len(dp['paragraph']) >= min_cl):
								add_l_par.append(True)
							else:
								add_l_par.append(False)

						if filt == 'tag':
							if 'tags' in dp:
								if filters[f]['tag'] in dp['tags']:
									add_tag.append(True)
								else:
									add_tag.append(False)
							else:
								add_tag.append(False)
						
						if filt == 'date':
							d_min = datetime.strptime(filters[f]['date'][0], '%Y/%m-%d').date()
							d_max = datetime.strptime(filters[f]['date'][1], '%Y/%m-%d').date()
							date = datetime.strptime(dp['lm'], '%m/%d/%Y, %H:%M:%S').astimezone(get_localzone()).date()

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
					dp.pop('_id')
					status['keys'].append(dp['key'])
					status['filename'].append(dp['title'])
					status['lm'].append(dp['lm'])
					status['dp'].append(dp)

		self.filtered = True
		self.status = status
		return status

	def add_datapoint(self, title):
		dp = {}
		dp['title'] = title
		dp['context'] = ''
		dp['question'] = ''
		dp['answers'] = ''
		
		hash = hashlib.md5((dp['title']+dp['context']+dp['question']).encode('utf-8')).hexdigest()
		dp['key'] = hash
		
		dp['comments'] = []
		dp['slices'] = []
		dp['metadata'] = {}
		
		now = datetime.now()
		dp['lm'] = now.strftime("%d/%m/%Y %H:%M:%S")
		dp['versions'] = [{'key': hash, 'version': 0, 'type': 'added', 'diff': dp.copy(), 'date': dp['lm']}]
		self.client.find_one_and_replace({'key': dp['key']}, dp, upsert=True)
		return True

	def remove_datapoint(self, key):
		self.client.find_one_and_delete({'key': key})
		return True
	
	def add_slice(self, slice_name=''):
		for dp in self.status['dp']:
			if not slice_name in dp['slices']:
				dp['slices'].append(slice_name)
			else:
				dp['slices'] = [slice_name]
			self.client.find_one_and_replace({'key': dp['key']}, dp, upsert=True)

		return self.compute_meta_data()

	def remove_slice(self, slice_name=''):
		for dp in self.status['dp']:
			if slice_name in dp['slices']:
				dp['slices'].remove(slice_name)
			self.client.find_one_and_replace({'key': dp['key']}, dp, upsert=True)
			
		return self.compute_meta_data()

	def get_slices(self):
		schema = self.get_schema()
		n_slices = {}
		slices = []
		for dp in schema:
			for sl in dp['slices']:
				if not sl in slices:
					slices.append(sl)
					n_slices[sl] = 1
				else:
					n_slices[sl] += 1
		return n_slices

	def get_slices_key(self, key):
		for dp in self.client.find({'key': key}):
			return dp['slices']
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

	def assing_labelers(self, labelers = []):
		unlabeled_list = []
		for dp in self.client.find({'labeled': False}):
			unlabeled_list.append(dp)
		n_per_labeler = math.floor(len(unlabeled_list)/len(labelers))

		idx = 1
		n_l = 0
		for dp in unlabeled_list:
			dp['labeled'] = True
			dp['metadata']['labeler'] = labelers[n_l]
			self.client.find_one_and_replace({'key': dp['key']}, dp, upsert=True)
			if idx == n_per_labeler:
				idx == 1
				n_l += 1
			else:
				idx+=1