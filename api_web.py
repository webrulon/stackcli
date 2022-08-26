import api_core

from fastapi import FastAPI
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
except:
    import os
    os.remove(str(Path.home())+'/config.stack')
    api_core = API()
    api_core.init()

# End-points

@app.get("/init")
async def init(uri):
    return {'success': api_core.init(uri)}

@app.get("/connect")
async def connect(dataset):
    return {'success': api_core.connectDataset(dataset)}

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

@app.get("/commits_version")
async def commits_version_api(version=2,l=5, page=0):
    try:
        return api_core.commits_version(version, l, page)
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

@app.post("/commit")
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
        return api_core.storage.pull_file(file)
    except:
        return {}

@app.get("/pull_metadata")
async def pull_metadata_api(file):
    try:
        return api_core.storage.loadFileMetadata(file)
    except:
        return {}

@app.get("/diff")
async def diff_api(v2,v1):
    return ''

@app.get("/diff_file")
async def diff_file_api(file,v2,v1):
    return ''

@app.get("/revert")
async def revert_api(version):
    return {'success': api_core.revert(version)}

@app.get("/revert_file")
async def revert_file_api(file,version):
    return {'success': api_core.revert(file, version)}