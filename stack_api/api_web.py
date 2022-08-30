import api_core
from fastapi import FastAPI, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
import pickle
from api_core import API

# API Definition
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"], 
)

# checks if local files are installed
try:
    api_core = API()
    initialized = api_core.start_check()
    assert(initialized)
except:
    try:
        import os
        os.remove(str(Path.home())+'/config.stack')
    except:
        print('no config file')
    api_core = API()
    initilized = api_core.init()
    
    if initilized:
        api_core.start_check()

# End-points

@app.get("/init")
async def init(uri='', name='My Dataset'):
    try:
        api_core.init(uri)
        api_core.connect_post_api(name)
        api_core.start_check()
        return {'success': True}
    except:
        return {'success': False}

@app.get("/disconnect")
async def disconnect_api(uri=''):
    try: 
        return {'success': api_core.disconnectDataset(uri)}
    except:
        return {'success': False}

@app.get("/get_datasets")
async def get_datasets_api():
    try:
        return api_core.get_datasets()
    except:
        return {}

@app.get("/uri")
async def uri_api():
    try:
        return api_core.getURI()
    except:
        return {}

@app.get("/history")
async def history_api():
    try:
        return api_core.history()
    except:
        return {}

@app.post("/add_file/")
async def add_file_api(file: UploadFile = File(description="A file read as UploadFile")):
    try:
        api_core.upload_file_binary(file.filename, file.file)
        return {'success': True}
    except:
        return {'success': False}

@app.get("/commits_version")
async def commits_version_api(version=1,l=5, page=0):
    try:
        return api_core.commits_version(version, l, page)
    except:
        return {}

@app.get("/key_versions")
async def key_versions_api(key='',l=5, page=0):
    try:
        return api_core.key_versions(key, l, page)
    except:
        return {}

@app.get("/last_n_commits")
async def last_n_commits_api(n=5):
    try:
        return api_core.lastNcommits(n)
    except:
        return {}

@app.get("/last_commits_from_hist_api")
async def last_commits_from_hist_api(n=1):
    try:
        return api_core.getHistoryCommits(n)
    except:
        return {}

@app.get("/status")
async def status_api():
    try:
        return api_core.status()
    except:
        return {}

@app.get("/commit_req")
async def commit_api(comment=''):
    return {'success': api_core.commit(comment)}

@app.get("/get_commit_metadata")
async def get_commit_meta_api(commit):
    try:
        return api_core.loadCommitMetadata(commit)
    except:
        return {}

@app.get("/pull_api")
async def pull_file_api(file):
    try:
        return {'file': api_core.storage.pull_file(file)}
    except:
        return {'file': ''}

@app.get("/pull_metadata")
async def pull_metadata_api(file):
    try:
        return api_core.storage.loadFileMetadata(file)
    except:
        return {}

@app.get("/remove_key")
async def remove_key_api(key):
    try:
        api_core.remove(key)
        return {'sucess': True}
    except:
        return {'sucess': False}

@app.get("/remove_commit")
async def remove_commit(version):
    try:
        api_core.remove_commit(version)
        return {'sucess': True}
    except:
        return {'sucess': False}

@app.get("/full_remove_key")
async def full_remove_key_api(key):
    try:
        api_core.remove_full(key)
        return {'sucess': True}
    except:
        return {'sucess': False}

@app.get("/remove_key_version")
async def remove_key_diff_api(key, version=-1):
    try:
        api_core.remove_key_diff(key, version)
        return {'sucess': True}
    except:
        return {'sucess': False}

@app.get("/revert_key_version")
async def revert_key_version_api(key, version=-1):
    try:
        api_core.revert_file(key, version)
        api_core.commit('reverted file ' + key)
        return {'sucess': True}
    except:
        return {'sucess': False}


@app.get("/revert")
async def revert_api(version=0):
    return {'success': api_core.revert(version)}