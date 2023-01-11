import sys
sys.path.append( '..' )
import stack_api.api_core as api_core
import json
from fastapi import FastAPI, File, UploadFile, Response, Body
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import StreamingResponse
from pathlib import Path
from pydantic import BaseModel

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


from src.comm.docker_ver import *
import os
path_home = os.getenv('LCP_DKR')+'/' if docker_ver() else str(Path.home())
 
# checks if local files are installed
try:
    api = api_core.API()
    initialized = api.start_check()
    api.set_schema()
    # print('doing an initial commit')
    # api.commit('',False)
except:
    print('no config file')
    api = api_core.API()
    print('initializing')
    initilized = api.start_check()
    print('trying')
    try:
        api.set_schema()
    except:
        pass
    print('start check')
    if initilized:
        api.start_check()

# frontend entry-points
@app.post("/init_web/")
async def init_web(data: dict):
    try:
        assert(not '.stack' in data['uri'])
        
        if path_home in data['uri']:
            data['uri'] = data['uri'].replace(path_home,'')

        api.init(data['uri'])
        api.connect_post_web(data['name'], data, data['schema'])
        api.set_schema()
        api.start_check()
        return {'success': True}
    except:
        return {'success': False}

@app.get("/connect/")
async def connect(uri):
    try:
        import time
        t0 = time.time()
        assert(not '.stack' in uri)
        if path_home in uri:
            uri = uri.replace(path_home,'')
        api.init(uri)
        print(f'time to init {time.time() - t0}s')
        t0 = time.time()
        api.connect_post_api()
        print(f'time to connect post api {time.time() - t0}s')
        t0 = time.time()
        api.set_schema()
        print(f'time to set schema {time.time() - t0}s')
        # api.commit()
        t0 = time.time()
        api.reset_version()
        print(f'time to set version {time.time() - t0}s')
        return {'success': True}
    except:
        return {'success': False}

@app.post("/init_gskey/")
def init_gskey(file: UploadFile = File(description="A file read as UploadFile")):
    try:
        api.set_gs_key(file.file)
        return {'success': True}
    except:
        return {'success': False}

@app.post("/set_branch")
def set_branch_api(data: dict):
    try:
        parent = api.Initializer.storage.dataset
        api.set_hierarchy(child = data['branch_name'])
        api.branch(data['branch_name'], data['branch_type'], data['branch_title'])
        api.set_hierarchy(parent = parent)
        return {'success': True}
    except:
        return {'success': False}

@app.get("/get_current_hierarchy")
def get_current_hierarchy_api():
    # try:
    hierarchy = api.get_hierarchy()
    children = []
    for uri in hierarchy['children']:
        children.append({'uri': uri, 'name': api.get_dataset_name(uri)})
    parent = {'uri': hierarchy['parent'], 'name': api.get_dataset_name(hierarchy['parent'])}
    return {'parent': parent, 'children': children}
    # except:
    #     return {'parent': {'uri': '', 'name': ''}, 'children': []}

@app.get("/get_hierarchy")
def get_hierarchy_api(uri):
    try:
        assert(not '.stack' in uri)
        if path_home in uri:
            uri = uri.replace(path_home,'')
        api.init(uri)
        api.connect_post_api()
        api.set_schema()
        return api.get_hierarchy()
    except:
        return {'parent': '', 'children': []}

@app.get("/get_versions")
def get_versions_api():
    try:
        return api.get_versions()
    except:
        return {}

@app.get("/add_version")
def add_version_api(label = ''):
    # try:
    return {'success': api.add_version(label)}
    # except:
    #     return {'success': False}

@app.get("/reset_version")
def reset_version_api():
    try:
        return {'success': api.reset_version()}
    except:
        return {'success': False}

@app.get("/select_version")
def select_version_api(version):
    # try:
    return {'success': api.select_version(str(version))}
    # except:
    #     return {'success': False}

@app.get("/get_branches")
def get_branches_api():
    try:
        hierarchy = api.get_hierarchy()
        res = []
        for uri in hierarchy['children']:
            res.append({'uri': uri, 'name': api.get_dataset_name(uri)})
        return res
    except:
        return []

@app.get("/set_hierarchy")
def set_hierarchy_api(uri_parent, uri_child):
    try:
        assert((not '.stack' in uri_parent) and (not '.stack' in uri_child))

        uri_parent = uri_parent.replace(path_home,'')
        uri_child = uri_child.replace(path_home,'')
        
        api.init(uri_parent)
        api.connect_post_api()
        api.set_schema()
        api.set_hierarchy(child = uri_child)

        api.init(uri_child)
        api.connect_post_api()
        api.set_schema()
        api.set_hierarchy(parent = uri_parent)

        return {'success': True}
    except:
        return {'success': False}

@app.post("/set_current_hierarchy")
def set_current_hierarchy_api(hierarchy: dict):
    try:
        assert('parent' in hierarchy.keys())
        assert('children' in hierarchy.keys())

        assert(type(hierarchy['parent']) is str)
        assert(type(hierarchy['children']) is list)
        
        return {'success': api.set_current_hierarchy(hierarchy)}
    except:
        return {'success': {'parent': '', 'children': []}}

@app.get("/add_child_to_current")
def add_child_to_current_api(child):
    try:
        child = child.replace(path_home,'')
        api.add_child_to_current(child)
        parent = api.Initializer.storage.dataset
        parent = parent.replace(path_home,'')
        try:
            api.init(child)
            api.connect_post_api()
            api.set_schema()
            api.add_parent_to_current(parent)
        except:
            pass
        api.init(parent)
        api.connect_post_api()
        api.set_schema()
        return {'success': True}
    except:
        return {'success': False}

@app.get("/add_parent_to_current")
def add_parent_to_current_api(parent):
    try:
        parent = parent.replace(path_home,'')
        api.add_parent_to_current(parent)
        child = api.Initializer.storage.dataset
        child = child.replace(path_home,'')
        try:
            api.init(parent)
            api.connect_post_api()
            api.set_schema()
            api.add_child_to_current(child)
        except:
            pass
        api.init(child)
        api.connect_post_api()
        api.set_schema()
        return {'success': True}
    except:
        return {'success': False}

@app.get("/current_remove_child")
def current_remove_child(uri=''):
    try:
        uri = uri.replace(path_home,'')
        api.remove_child(child=uri)
        parent = api.Initializer.storage.dataset
        parent = parent.replace(path_home,'')
        try:
            api.init(uri)
            api.connect_post_api()
            api.set_schema()
            api.remove_parent()
        except:
            pass
        api.init(parent)
        api.connect_post_api()
        api.set_schema()
        return {'success': True}
    except:
        return {'success': False}

@app.post("/merge")
def merge_api(data: dict):
    try: 
        api.merge(father=data['father'], child=data['child'])
        api.commit(f"merged branch {data['child']} to {data['father']}")
        return {'success': True}
    except:
        return {'success': False}

@app.get("/merge_child_to_master")
def merge_child_to_master_api(uri):
    try:
        master = api.Initializer.storage.dataset
        api.merge(father=master, child=uri)
        api.commit(f"merged branch {uri} to {master}")
        return {'success': True}
    except:
        return {'success': False}

@app.get("/merge_current_to_master")
def merge_current_to_master_api():
    try: 
        child = api.Initializer.storage.dataset
        hierarchy = api.get_hierarchy()
        if type(hierarchy['parent']) is str:
            if hierarchy['parent'] == '':
                return {'success': False}
            else:
                api.merge(father=hierarchy['parent'], child=child)
                api.commit(f"merged branch {child} to {hierarchy['parent']}")
                return {'success': True}
    except:
        return {'success': False}

@app.get("/disconnect")
def disconnect_api(uri=''):
    try:
        try:         
            api.init(uri)
            api.connect_post_api()
            api.set_schema()
            hierarchy = api.get_hierarchy()
            if len(hierarchy['children']) > 0:
                for child in hierarchy['children']:
                    api.remove_child(child=child)
                    try:
                        api.init(child)
                        api.connect_post_api()
                        api.set_schema()
                        api.remove_parent()
                    except:
                        pass
                api.init(uri)
                api.connect_post_api()
                api.set_schema()
            if type(hierarchy['parent']) is str:
                if hierarchy['parent'] != '':
                    assert(not '.stack' in hierarchy['parent'])
                    if path_home in hierarchy['parent']:
                        hierarchy['parent'] = hierarchy['parent'].replace(path_home,'')
                    try:
                        api.init(hierarchy['parent'])
                        api.connect_post_api()
                        api.set_schema()
                        api.remove_child(child=uri)
                    except: 
                        pass
        except:
            pass    
        return {'success': api.disconnect_dataset(uri)}
    except:
        return {'success': False}

@app.get("/get_datasets")
def get_datasets_api():
    try:
        return api.get_datasets()
    except:
        return {}

@app.get("/uri")
def uri_api():
    try:
        return api.get_uri()
    except:
        return {}

@app.get("/schema")
def schema_api():
    # try:
    return {'value': api.config['schema']}
    # except:
    #     return {'value': 'files'}

@app.get("/history")
def history_api():
    try:
        return api.history()
    except:
        return {}

@app.get('/set_schema')
def set_schema_api():
    api.set_schema()

@app.get('/reset_schema')
def reset_schema_api():
    api.reset_schema()
    return {'success': True}

@app.get('/add_tag')
def add_tag_api(file, tag):
    api.add_tag(file, tag)
    return {'success': True}

@app.get('/remove_tag')
def remove_tag_api(file, tag):
    api.remove_tag(file, tag)
    return {'success': True}

@app.get('/remove_all_tags')
def remove_all_tags_api(file):
    api.remove_all_tags(file)
    return {'success': True}

@app.post('/selection_add_tag')
def selection_add_tag_api(data: list):
    api.selection_add_tag(data[:-1], data[-1])
    return {'success': True}

@app.post('/selection_remove_all_tags')
def selection_remove_all_tags_api(data: list):
    api.selection_remove_all_tags(data)
    return {'success': True}

@app.get('/get_tags')
def get_tags_api(file):
    tags = api.get_tags(file)
    return tags

@app.post("/add_file/")
def add_file_api(file: UploadFile = File(description="A file read as UploadFile")):
    try:
        api.upload_file_binary(file.filename, file.file)
        api.commit('')
        return {'success': True}
    except:
        return {'success': False}

@app.post("/add_multifiles/")
def add_multifiles_api(files: list[UploadFile]):
    try:
        for file in files:
            api.upload_file_binary(file.filename, file.file)
        api.commit('')
        return {'success': True}
    except:
        return {'success': False}

@app.get("/commits_version")
def commits_version_api(version=1,l=5, page=0):
    try:
        return api.commits_version(version, l, page)
    except:
        return {}

@app.get("/key_versions")
def key_versions_api(key='',l=5, page=0):
    # try:
    return api.key_versions(key, l, page)
    # except:
    #     return {}

@app.get("/label_versions")
def label_versions_api(key='',l=5, page=0):
    try:
        return api.label_versions(key, l, page)
    except:
        return {}

@app.get("/last_n_commits")
def last_n_commits_api(n=5):
    try:
        return api.last_n_commits(n)
    except:
        return {}

@app.get("/last_commits_from_hist_api")
def last_commits_from_hist_api(n=1):
    try:
        return api.getHistoryCommits(n)
    except:
        return {}

@app.get("/status")
def status_api():
    # try:
    return api.status()
    # except:
    #     return {}

@app.get("/schema_metadata")
def schema_metadata_api():
    return api.schema_metadata()

@app.get("/current")
def current_api(page=0,max_pp=12):
    # try:
    full_json = api.status()
    idx_i = int(page)*int(max_pp)
    idx_f = (int(page)+1)*int(max_pp)

    current = {'keys': [], 'lm': [], 'len': len(full_json['keys'])}

    current['keys'] = full_json['keys'][idx_i:idx_f]
    current['lm'] = full_json['lm'][idx_i:idx_f]

    return current
    # except:
    #     return {'keys': {}, 'lm': {}}

@app.post("/set_filter/")
def set_filter_api(data: dict):
    return {'success': api.set_filters(data)}

@app.get("/reset_filters")
def reset_filter_api():
    return {'success': api.reset_filters()}

@app.get("/commit_req")
def commit_api(comment=''):
    return {'success': api.commit(comment)}

@app.get("/get_commit_metadata")
def get_commit_meta_api(commit):
    try:
        return api.load_commit_metadata(commit)
    except:
        return {}

@app.get("/pull_file_api")
def pull_file_api(file, version='current'):
    try:
        return StreamingResponse(api.load_file_binary(file, version), media_type="image/png")
    except:
        return Response(content='')

@app.get("/set_bounding_boxes")
def set_bounding_boxes_api(val):
    try:
        return {'success': api.set_bounding_boxes(val)} 
    except:
        return {'success': False}

@app.get("/get_thumbnail")
def get_thumbnail_api(file):
    try:
        import time
        t0 = time.time()
        thumb = api.load_thumbnail(file)
        print(f'time to thumbnail {time.time() - t0}s')
        return StreamingResponse(thumb, media_type="image/png")
    except:
        return Response(content='')

@app.get("/pull_csv_api")
def pull_csv_api(file, row_p=0, col_p=0, version='current'):
    try:
        data, _, _ = api.load_csv_binary(file, row_p, col_p, version)
        return StreamingResponse(data, media_type="image/png")
    except:
        return Response(content='')

@app.get("/pull_csv_metadata_api")
def pull_csv_metadata_api(file, version='current'):
    try:
        _, nr, nc = api.load_csv_binary(file, 0, 0, version)
        return {'rows': nr-1, 'cols': nc-1}
    except:
        return {'rows': 0, 'cols': 0}

@app.get("/remove_key")
def remove_key_api(key):
    try:
        api.remove(key)
        api.commit('')
        return {'sucess': True}
    except:
        return {'sucess': False}

@app.get("/remove_commit")
def remove_commit(version):
    try:
        api.remove_commit(version)
        api.commit('')
        return {'sucess': True}
    except:
        return {'sucess': False}

@app.get("/full_remove_key")
def full_remove_key_api(key):
    try:
        api.remove_full(key)
        api.commit('')
        return {'sucess': True}
    except:
        return {'sucess': False}

@app.get("/remove_key_version")
def remove_key_diff_api(key, version=-1):
    try:
        api.remove_key_diff(key, version)
        api.commit('')
        return {'sucess': True}
    except:
        return {'sucess': False}

@app.get("/get_csv_diff_metadata")
def get_csv_diff_metadata(key, v1='current', v2='current'):
    try:
        return api.load_csv_diff_metadata(key, v1, v2)
    except:
        return {}

@app.get("/get_csv_diff")
def get_csv_diff(key, v1='current', v2='current'):
    try:
        return api.load_csv_diff(key, v1, v2)
    except:
        return {}

@app.get("/revert_key_version")
def revert_key_version_api(key, version=-1, label='raw'):
    # try:
    api.revert_file(key, version)
    api.commit('reverted file ' + key)
    return {'sucess': True}
    # except:
    #     return {'sucess': False}

@app.get("/revert")
async def revert_api(version=0):
    # try:
    api.revert(version)
    api.commit('reverted dataset to version ' + version)
    return {'success': True}
    # except:
    #     return {'success': False}

@app.get("/get_prev_key")
def get_prev_key_api(key):
    return {'key': api.get_prev_key(key)}

@app.get("/get_next_key")
def get_next_key_api(key):
    return {'key': api.get_next_key(key)}

@app.get("/get_labels")
def get_labels_api(filename, version='current'):
    # try:
    return api.get_labels(filename, version)
    # except:
    #     return {}

@app.post("/set_labels")
def set_labels_api(data: dict):
    # try:
    return api.set_labels(data)
    # except:
    #     return {}

@app.get("/add_slice")
def add_slice(slice_name):
    return {'success': api.add_slice(slice_name=slice_name)}

@app.get("/remove_slice")
def remove_slice(slice_name):
    return {'success': api.remove_slice(slice_name=slice_name)}

@app.get("/reset_slices")
def reset_slices():
    return {'success': api.reset_slices()}

@app.post("/select_slice")
def select_slice(slices: list):
    return {'success': api.select_slices(slices=slices)}

@app.get("/get_slices")
def get_slices():
    return api.get_slices()

@app.get("/get_readme")
def get_readme():
    try:
        return Response(content=api.get_readme())
    except:
        return Response(content='')

# stacklib entry-points

@app.get("/init_experiment")
def init_experiment(data: dict):
    api.server_init_experiment(uri=data['uri'],project=data['project'])
    return {'success': True}

@app.post("/add_log")
def add_log(data: dict):
    api.server_add_log(data)
    return {'success': True}

@app.post("/get_models")
def get_models():
    return api.server_get_models()

@app.post("/add_model")
def add_model(data: Rate = Body(...), file: UploadFile = File(...)):
    api.server_upload_model(model=file.file, label=data.label)
    return {'success': True}

@app.get("/add_prediction")
def add_prediction(data: dict):
    return StreamingResponse(api.server_add_prediction(data))

@app.get("/get_model")
def get_model(data: dict):
    return StreamingResponse(api.server_get_model(data['label']))

@app.get("/logout_experiment")
def logout_experiment():
    api.server_logout_experiment()
    return {'success': True}

@app.get("/get_projects")
def get_projects():
    try:
        return api.get_projects()
    except:
        return []

@app.get("/get_logs_list")
def get_logs_list(project):
    return api.get_logs(project)

@app.get("/get_logs_experiment")
def get_logs_experiment(log):
    return api.get_logs_experiment(log)

@app.get("/get_predictions_list")
def get_predictions_list(prediction):
    return api.get_predictions_list(prediction)

@app.get("/get_prediction")
def get_prediction(prediction):
    return api.get_prediction(prediction)