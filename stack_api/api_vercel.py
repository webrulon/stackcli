
import sys
sys.path.append( '..' )
import json
import os
import hashlib

from supabase import create_client, Client
from fastapi import FastAPI, Header, File, UploadFile, Response, Body, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import StreamingResponse
from pathlib import Path
from pydantic import BaseModel
import pymongo

from src.mongo_compatible.squad2 import squad2

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"], 
)

def check_if_user(user_id, sessions, Authorization):
	# reads the toke and hashes it
	token = Authorization.split(" ")[1]
	hashed_token = hashlib.sha256((token).encode('utf-8')).hexdigest()

	if hashed_token in sessions.keys():
		return sessions
	else:
		supabase: Client = create_client(url, key)
		user = supabase.auth.set_session(token, '')
		db = client['_DB_Organizations']
		for dp in db.find({'members': { '$in': [user_id] } }):
			org_name = dp['name']
			admin = (user_id in dp['admins'])
		db = client[org_name]

		user_array = {'client': client, 'user': user, 'admin': admin, 'user_id': user_id, 'db': db, 'dset': None, 'schema': None}
		sessions[hashed_token] = user_array
		return sessions

class Rate(BaseModel):
    label: str

    @classmethod
    def __get_validators__(cls):
        yield cls.validate_to_json

    @classmethod
    def validate_to_json(cls, value):
        if isinstance(value, str):
            return cls(**json.loads(value))
        return value

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_ANON_KEY")
pwd: str = os.environ.get("MONGODB_PWD")

# client = pymongo.MongoClient(f"mongodb+srv://baceituno:{pwd}@cluster0.fesgwqt.mongodb.net/?retryWrites=true&w=majority")

client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client['stack']
col_dsets = db['list_of_datasets']
dataset = squad2(db['coco'])

session_array = {}

# frontend entry-points
@app.post("/create_organization")
async def create_organization(user_id: str, organization: str, Authorization: str = Header(None)):
	try:
		assert(organization != '')
		session_array = check_if_user(user_id, session_array, Authorization)
		db = client['_DB_Organizations']
		org = {'name': organization, 'members': [user_id] , 'admins': [user_id], 'members': [user_id]}
		not_found = True
		for _ in db.find({'name': organization}):
			not_found = False
		if not_found:
			db.insert_one(org)	

		return {'success': True}
	except:
		raise HTTPException(status_code=400, detail="Invalid token")

@app.post("/add_to_organization")
async def add_to_organization(user_id: str, organization: str, admin: bool = False, Authorization: str = Header(None)):
	try:
		session_array = check_if_user(user_id, session_array, Authorization)
		
		db = client["stack"]['organizations']
		for dp in db.find({'name': organization }):
			dp['members'].append(user_id)
			if admin:
				dp['admins'].append(user_id)
			db.find_one_and_replace({'name': organization}, dp)
		return {'success': True}
	except:
		raise HTTPException(status_code=400, detail="Invalid token")

@app.post("/login")
async def login(user_id: str, Authorization: str = Header(None)):
	try:
		session_array = check_if_user(user_id, session_array, Authorization)
		return {'success': True}
	except:
		raise HTTPException(status_code=400, detail="Invalid token")

@app.post("/logout")
async def logout(Authorization: str = Header(None)):
	try:
		token = Authorization.split(" ")[1]
		hashed_token = hashlib.sha256((token).encode('utf-8')).hexdigest()
		del session_array[hashed_token]
	except:
		raise HTTPException(status_code=400, detail="Invalid token")

@app.post("/connect")
async def connect(dataset_id: str, Authorization: str = Header(None)):
	try:
		token = hashlib.sha256((Authorization.split(" ")[1]).encode('utf-8')).hexdigest()
		db = session_array[token]['db']
		col_dsets = db['list_of_datasets']
		
		for dp in col_dsets.find({'dataset_id': dataset_id}):
			schema = dp['schema']

			if schema == 'squad2':
				schema_class = squad2(db[dataset_id])
			else:
				raise HTTPException(status_code=400, detail="invalid schema")

			if not session_array[token]['admin']:
				schema_class.apply_filters({'labeler': session_array[token]['user_id']})

			session_array[token]['dset'] = schema_class
		return {'success': True}
	except:
		raise HTTPException(status_code=400, detail="Invalid token")

@app.post("/add_dataset")
async def add_dataset(dataset_id: str, user_id: str, schema: str, Authorization: str = Header(None)):
	try:
		token = hashlib.sha256((Authorization.split(" ")[1]).encode('utf-8')).hexdigest()
		db = session_array[token]['db']
		col_dsets = db['list_of_datasets']
		
		db = client["stack"]['organizations']
		for dp in db.find({'members': { '$in': [user_id] } }):
			assert(user_id in dp['admins'])

		dataset = {
			'dataset_id': dataset_id,
			'schema': schema,
			'hierarchy': {}
		}

		col_dsets.find_one_and_replace({'dataset_id': dataset_id},dataset)

		if schema == 'squad2':
			schema_class = squad2(db[dataset_id])

		session_array[token]['dset'] = schema_class
		return {'success': True}
	except:
		raise HTTPException(status_code=400, detail="Invalid token or Permissions")

@app.post("/add_file_to_dataset")
async def add_file_to_dataset(file: UploadFile = File(...), Authorization: str = Header(None)):
	try:
		# token = hashlib.sha256((Authorization.split(" ")[1]).encode('utf-8')).hexdigest()
		# session_array[token]['dset'].create_schema_file(file.file)
		dataset.create_schema_file(file.file)
		return {'success': True}
	except:
		raise HTTPException(status_code=400, detail="Invalid token or Permissions")

@app.post("/get_datapoints")
async def get_datapoints(location: dict, Authorization: str = Header(None)):
	# try:
		# token = hashlib.sha256((Authorization.split(" ")[1]).encode('utf-8')).hexdigest()
		# if not session_array[token]['dset'].filtered:
		# 	current = session_array[token]['dset'].read_all_files()
		# else:
		# 	current = session_array[token]['dset'].get_status()

	if dataset.filtered:
		current = dataset.get_status()
	else:
		current = dataset.read_all_files()
	page = location['page']
	length = location['length']
	idx_i = int(page)*int(length)
	idx_f = (int(page)+1)*int(length)

	res = {'keys': [], 'dp': [], 'lm': [], 'len': len(current['keys'])}

	res['keys'] = current['keys'][idx_i:idx_f]
	res['dp'] = current['dp'][idx_i:idx_f]
	res['lm'] = current['lm'][idx_i:idx_f]
	return dict(res)
	# except:
	# 	raise HTTPException(status_code=400, detail="Invalid token or Permissions")

app.post("/get_labeler_datapoints")
async def get_labeler_datapoints(data: dict):
	# try:
		# token = hashlib.sha256((Authorization.split(" ")[1]).encode('utf-8')).hexdigest()
		# if not session_array[token]['dset'].filtered:
		# 	current = session_array[token]['dset'].read_all_files()
		# else:
		# 	current = session_array[token]['dset'].get_status()

	dataset.apply_filters({'labeler': data['user_id']})
	current = dataset.get_status()
	page = data['page']
	length = data['length']
	idx_i = int(page)*int(length)
	idx_f = (int(page)+1)*int(length)

	res = {'keys': [], 'dp': [], 'lm': [], 'len': len(current['keys'])}

	res['keys'] = current['keys'][idx_i:idx_f]
	res['dp'] = current['dp'][idx_i:idx_f]
	res['lm'] = current['lm'][idx_i:idx_f]
	return dict(res)
	# except:
	# 	raise HTTPException(status_code=400, detail="Invalid token or Permissions")

@app.post("/get_labels")
async def get_labels(data: dict, Authorization: str = Header(None)):
	try:
		key = data['key']
		if 'version' in data.keys():
			version = data['version']
		else:
			version = 'current'

		token = hashlib.sha256((Authorization.split(" ")[1]).encode('utf-8')).hexdigest()
		dp = session_array[token]['dset'].get_labels(key, version)
		dp.pop('_id')
		return dp
	except:
		raise HTTPException(status_code=400, detail="Invalid token or Permissions")

@app.post("/set_labels")
async def set_labels(labels: dict):
	# try:
		# token = hashlib.sha256((Authorization.split(" ")[1]).encode('utf-8')).hexdigest()
		# return session_array[token]['dset'].set_labels(labels['key'], labels['array'])
	print(labels)
	return dataset.set_labels(labels['key'], labels['array'])
	# except:
	# 	raise HTTPException(status_code=400, detail="Invalid token or Permissions")

@app.post("/assign_labelers")
async def assign_labelers(labelers: dict):
	# try:
		# token = hashlib.sha256((Authorization.split(" ")[1]).encode('utf-8')).hexdigest()
		# if session_array[token]['admin']:
		# 	session_array[token]['dset'].assign_labelers(labelers)
	dataset.assing_labelers(labelers)

	# except:
		# raise HTTPException(status_code=400, detail="Invalid token or Permissions")

@app.post("/add_datapoint")
async def add_datapoint(data: dict, Authorization: str = Header(None)):
	try:
		key = data['key']
		token = hashlib.sha256((Authorization.split(" ")[1]).encode('utf-8')).hexdigest()
		if session_array[token]['admin']:
			session_array[token]['dset'].add_datapoint(key)
		return {'success': True}
	except:
		raise HTTPException(status_code=400, detail="Invalid token or Permissions")

@app.post("/remove_datapoint")
async def remove_datapoint(data: dict, Authorization: str = Header(None)):
	try:
		key = data['key']
		token = hashlib.sha256((Authorization.split(" ")[1]).encode('utf-8')).hexdigest()
		if session_array[token]['admin']:
			session_array[token]['dset'].remove_datapoint(key)
		return {'success': True}
	except:
		raise HTTPException(status_code=400, detail="Invalid token or Permissions")