import api_core as api_core
from fastapi import FastAPI, File, UploadFile, Response, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.responses import StreamingResponse
from pathlib import Path

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
    api = api_core.API()
    initialized = api.start_check()
    assert(initialized)
except:
    try:
        import os
        os.remove(str(Path.home())+'/config.stack')
    except:
        print('no config file')
    api = api_core.API()
    initilized = api.init()
    
    if initilized:
        api.start_check()

# End-points
@app.get("/init")
async def init(uri='', name='My Dataset'):
    try:
        api.init(uri)
        api.connect_post_api(name)
        api.start_check()
        return {'success': True}
    except:
        return {'success': False}

@app.post("/init_web/")
async def init_web(data: dict):
    try:
        api.init(data['uri'])
        api.connect_post_web(data['name'], data)
        api.start_check()
        json_compatible_item_data = jsonable_encoder(False)
        return JSONResponse(content=json_compatible_item_data)
    except:
        return {'success': False}

@app.post("/init_gskey/")
async def init_gskey(file: UploadFile = File(description="A file read as UploadFile")):
    try:
        api.set_gs_key(file.file)
        return {'success': True}
    except:
        return {'success': False}

@app.get("/directories")
async def directories():
    import os
    from pathlib import Path
    print(str(Path.home()))
    print(str(os.path.abspath('.')))
    return {'success': True}

@app.get("/disconnect")
async def disconnect_api(uri=''):
    try: 
        return {'success': api.disconnectDataset(uri)}
    except:
        return {'success': False}

@app.get("/get_datasets")
async def get_datasets_api():
    try:
        return api.get_datasets()
    except:
        return {}

@app.get("/uri")
async def uri_api():
    try:
        return api.getURI()
    except:
        return {}

@app.get("/history")
async def history_api():
    try:
        return api.history()
    except:
        return {}

@app.post("/add_file/")
async def add_file_api(file: UploadFile = File(description="A file read as UploadFile")):
    try:
        api.upload_file_binary(file.filename, file.file)
        api.commit('')
        return {'success': True}
    except:
        return {'success': False}

@app.get("/commits_version")
async def commits_version_api(version=1,l=5, page=0):
    try:
        return api.commits_version(version, l, page)
    except:
        return {}

@app.get("/key_versions")
async def key_versions_api(key='',l=5, page=0):
    try:
        return api.key_versions(key, l, page)
    except:
        return {}

@app.get("/last_n_commits")
async def last_n_commits_api(n=5):
    try:
        return api.lastNcommits(n)
    except:
        return {}

@app.get("/last_commits_from_hist_api")
async def last_commits_from_hist_api(n=1):
    try:
        return api.getHistoryCommits(n)
    except:
        return {}

@app.get("/status")
async def status_api():
    try:
        return api.status()
    except:
        return {}

@app.get("/commit_req")
async def commit_api(comment=''):
    return {'success': api.commit(comment)}

@app.get("/get_commit_metadata")
async def get_commit_meta_api(commit):
    try:
        return api.loadCommitMetadata(commit)
    except:
        return {}

@app.get("/pull_file_api")
async def pull_file_api(file):
    try:
        return StreamingResponse(api.load_file_binary(file), media_type="image/png")
    except:
        return Response(content='',filename='failure')

@app.get("/remove_key")
async def remove_key_api(key):
    try:
        api.remove(key)
        api.commit('')
        return {'sucess': True}
    except:
        return {'sucess': False}

@app.get("/remove_commit")
async def remove_commit(version):
    try:
        api.remove_commit(version)
        return {'sucess': True}
    except:
        return {'sucess': False}

@app.get("/full_remove_key")
async def full_remove_key_api(key):
    try:
        api.remove_full(key)
        api.commit('')
        return {'sucess': True}
    except:
        return {'sucess': False}

@app.get("/remove_key_version")
async def remove_key_diff_api(key, version=-1):
    try:
        api.remove_key_diff(key, version)
        return {'sucess': True}
    except:
        return {'sucess': False}

@app.get("/revert_key_version")
async def revert_key_version_api(key, version=-1):
    try:
        api.revert_file(key, version)
        api.commit('reverted file ' + key)
        return {'sucess': True}
    except:
        return {'sucess': False}

@app.get("/revert")
async def revert_api(version=0):
    return {'success': api.revert(version)}