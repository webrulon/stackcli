
import sys
sys.path.append( '..' )
import stack_api.api_core as api_core
import json
from fastapi import FastAPI, Header, File, UploadFile, Response, Body, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import StreamingResponse
from pathlib import Path
from pydantic import BaseModel
from src.mongo_compatible.squad2 import squad2
import pymongo

import os
from supabase import create_client, Client

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"], 
)

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
key: str = os.environ.get("SUPABASE_KEY")

client = pymongo.MongoClient("mongodb://localhost:27017/")	

session_array = {}

# frontend entry-points
@app.post("/create_organization")
async def create_organization(username: str, organization: str, Authorization: str = Header(None)):
	try:
		token = Authorization.split(" ")[1]
		supabase: Client = create_client(url, key)
		supabase.auth.set_auth(token)
	
		db = client["stack"]['organizations']
		org = {'name': organization, 'members': [username] , 'admins': [username], 'members': [username]}
		not_found = True
		for _ in db.find({'name': organization}):
			not_found = False
		if not_found:
			db.insert_one(org)	

		return {'success': True}
	except:
		raise HTTPException(status_code=400, detail="Invalid token")

@app.post("/add_to_organization")
async def add_to_organization(username: str, organization: str, admin: bool = False, Authorization: str = Header(None)):
	try:
		# token = Authorization.split(" ")[1]
		# supabase: Client = create_client(url, key)
		# supabase.auth.set_auth(token)
		
		db = client["stack"]['organizations']
		for dp in db.find({'name': organization }):
			dp['members'].append(username)
			if admin:
				dp['admins'].append(username)
			db.find_one_and_replace({'name': organization}, dp)
		return {'success': True}
	except:
		raise HTTPException(status_code=400, detail="Invalid token")

@app.post("/login")
async def login(username: str, Authorization: str = Header(None)):
	try:
		token = Authorization.split(" ")[1]
		supabase: Client = create_client(url, key)
		user = supabase.auth.set_auth(token)
		db = client["stack"]['organizations']
		for dp in db.find({'members': { '$in': [username] } }):
			org_name = dp['name']
			admin = (username in dp['admins'])
		db = client[org_name]

		user_array = {'client': client, 'user': user, 'admin': admin, 'username': username, 'db': db, 'dset': None, 'schema': None}
		session_array[token] = user_array
		return {'success': True}
	except:
		raise HTTPException(status_code=400, detail="Invalid token")

@app.post("/logout")
async def logout(Authorization: str = Header(None)):
	try:
		token = Authorization.split(" ")[1]
		del session_array[token]
	except:
		raise HTTPException(status_code=400, detail="Invalid token")

@app.post("/connect")
async def connect(dataset_id: str, Authorization: str = Header(None)):
	try:
		token = Authorization.split(" ")[1]
		db = session_array[token]['db']
		col_dsets = db['list_of_datasets']
		
		for dp in col_dsets.find({'dataset_id': dataset_id}):
			schema = dp['schema']

			if schema == 'squad2':
				schema_class = squad2(db[dataset_id])
			else:
				raise HTTPException(status_code=400, detail="invalid schema")
						
			if not session_array[token]['admin']:
				schema_class.apply_filters({'labeler': session_array[token]['username']})

			session_array[token]['dset'] = schema_class
		return {'success': True}
	except:
		raise HTTPException(status_code=400, detail="Invalid token")

@app.post("/add_dataset")
async def add_dataset(dataset_id: str, username: str, schema: str, Authorization: str = Header(None)):
	try:
		token = Authorization.split(" ")[1]

		db = session_array[token]['db']
		col_dsets = db['list_of_datasets']
		
		db = client["stack"]['organizations']
		for dp in db.find({'members': { '$in': [username] } }):
			assert(username in dp['admins'])

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
		token = Authorization.split(" ")[1]
		session_array[token]['dset'].create_schema_file(file.file)
		return {'success': True}
	except:
		raise HTTPException(status_code=400, detail="Invalid token or Permissions")

@app.get("/get_datapoints")
async def get_datapoints(page, length, Authorization: str = Header(None)):
	try:
		token = 'bac'
		if not session_array[token]['dset'].filtered:
			current = session_array[token]['dset'].read_all_files()
		else:
			current = session_array[token]['dset'].get_status()

		idx_i = int(page)*int(length)
		idx_f = (int(page)+1)*int(length)

		res = {'keys': [], 'dp': [], 'lm': [], 'len': len(current['keys'])}

		res['keys'] = current['keys'][idx_i:idx_f]
		res['dp'] = current['dp'][idx_i:idx_f]
		res['lm'] = current['lm'][idx_i:idx_f]

		return dict(res)
	except:
		raise HTTPException(status_code=400, detail="Invalid token or Permissions")

@app.get("/get_labels")
async def get_labels(key, version = 'current', Authorization: str = Header(None)):
	try:
		token = Authorization.split(" ")[1]
		dp = session_array[token]['dset'].get_labels(key, version)
		dp.pop('_id')
		return dp
	except:
		raise HTTPException(status_code=400, detail="Invalid token or Permissions")

@app.post("/set_labels")
async def set_labels(labels: dict, Authorization: str = Header(None)):
	try:
		token = Authorization.split(" ")[1]
		return session_array[token]['dset'].set_labels(labels['key'], labels['array'])
	except:
		raise HTTPException(status_code=400, detail="Invalid token or Permissions")

@app.post("/assign_labelers")
async def assign_labelers(labelers: list, Authorization: str = Header(None)):
	try:
		token = Authorization.split("")[1]
		if session_array[token]['admin']:
			session_array[token]['dset'].assign_labelers(labelers)
	except:
		raise HTTPException(status_code=400, detail="Invalid token or Permissions")

@app.post("/add_datapoint")
async def add_datapoint(key: str, Authorization: str = Header(None)):
	try:
		token = Authorization.split("")[1]
		if session_array[token]['admin']:
			session_array[token]['dset'].add_datapoint(key)
		return {'success': True}
	except:
		raise HTTPException(status_code=400, detail="Invalid token or Permissions")

@app.post("/remove_datapoint")
async def remove_datapoint(key: str, Authorization: str = Header(None)):
	try:
		token = Authorization.split("")[1]
		if session_array[token]['admin']:
			session_array[token]['dset'].remove_datapoint(key)
		return {'success': True}
	except:
		raise HTTPException(status_code=400, detail="Invalid token or Permissions")