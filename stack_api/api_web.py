import sys
sys.path.append( '..' )
import stack_api.api_core as api_core
from fastapi import FastAPI, File, UploadFile, Response
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import StreamingResponse
from pathlib import Path

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"], 
)

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
    # try:
    assert(not '.stack' in uri)
    if path_home in uri:
        uri = uri.replace(path_home,'')
    api.init(uri)
    api.connect_post_api()
    api.set_schema()
    api.commit()
    return {'success': True}
    # except:
    #     return {'success': False}

@app.post("/init_gskey/")
async def init_gskey(file: UploadFile = File(description="A file read as UploadFile")):
    try:
        api.set_gs_key(file.file)
        return {'success': True}
    except:
        return {'success': False}

@app.post("/set_branch")
async def set_branch_api(data: dict):
    try:
        parent = api.Initializer.storage.dataset
        api.set_hierarchy(child = data['branch_name'])
        api.branch(data['branch_name'], data['branch_type'])
        api.set_hierarchy(parent = parent)
        return {'success': True}
    except:
        return {'success': False}

@app.get("/get_current_hierarchy")
async def get_current_hierarchy_api():
    # try:
    print(api.get_hierarchy())
    return api.get_hierarchy()
    # except:
    #     return {'parents': '', 'children': []}

@app.get("/get_hierarchy")
async def get_hierarchy_api(uri):
    try:
        assert(not '.stack' in uri)
        if path_home in uri:
            uri = uri.replace(path_home,'')
        api.init(uri)
        api.connect_post_api()
        api.set_schema()
        return api.get_hierarchy()
    except:
        return {'parents': '', 'children': []}

@app.get("/get_branches")
async def get_branches_api():
    try:
        hierarchy = api.get_hierarchy()
        return hierarchy['children']
    except:
        return []

@app.get("/set_hierarchy")
async def set_hierarchy_api(uri_parent, uri_child):
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
async def set_current_hierarchy_api(hierarchy: dict):
    try:
        assert('parent' in hierarchy.keys())
        assert('children' in hierarchy.keys())

        assert(type(hierarchy['parent']) is str)
        assert(type(hierarchy['children']) is list)
        
        return {'success': api.set_current_hierarchy(hierarchy)}
    except:
        return {'success': {'parent': '', 'children': []}}

@app.get("/add_child_to_current")
async def add_child_to_current_api(child):
    # try:
    print(api.get_hierarchy())
    child = child.replace(path_home,'')
    api.add_child_to_current(child)
    parent = api.Initializer.storage.dataset
    parent = parent.replace(path_home,'')
    print(api.get_hierarchy())
    # try:
    api.init(child)
    api.connect_post_api()
    api.set_schema()
    api.add_parent_to_current(parent)
    # except:
    #     pass
    api.init(parent)
    api.connect_post_api()
    api.set_schema()
    return {'success': True}
    # except:
    #     return {'success': {'parent': '', 'children': []}}

@app.get("/add_parent_to_current")
async def add_parent_to_current_api(parent):
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
        return {'success': {'parent': '', 'children': []}}

@app.get("/current_remove_child")
async def current_remove_child(uri=''):
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
        return {'success': {'parent': '', 'children': []}}

@app.post("/merge")
async def merge_api(data: dict):
    try: 
        api.merge(father=data['father'], child=data['child'])
        api.commit(f"merged branch {data['child']} to {data['father']}")
        return {'success': True}
    except:
        return {'success': False}

@app.get("/merge_child_to_master")
async def merge_child_to_master_api(uri):
    # try:
    print(uri)
    master = api.Initializer.storage.dataset
    api.merge(father=master, child=uri)
    api.commit(f"merged branch {uri} to {master}")
    return {'success': True}
    # except:
    #     return {'success': False}

@app.get("/merge_current_to_master")
async def merge_current_to_master_api():
    # try: 
    child = api.Initializer.storage.dataset
    hierarchy = api.get_hierarchy()
    if type(hierarchy['parent']) is str:
        if hierarchy['parent'] == '':
            return {'success': False}
        else:
            api.merge(father=hierarchy['parent'], child=child)
            api.commit(f"merged branch {child} to {hierarchy['parent']}")
            return {'success': True}
    # except:
    #     return {'success': False}

@app.get("/disconnect")
async def disconnect_api(uri=''):
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
        return {'success': api.disconnect_dataset(uri)}
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
        return api.get_uri()
    except:
        return {}

@app.get("/schema")
async def schema_api():
    # try:
    return {'value': api.config['schema']}
    # except:
    #     return {'value': 'files'}

@app.get("/history")
async def history_api():
    try:
        return api.history()
    except:
        return {}

@app.get('/set_schema')
async def set_schema_api():
    api.set_schema()

@app.get('/reset_schema')
async def reset_schema_api():
    api.reset_schema()
    return {'success': True}

@app.get('/add_tag')
async def add_tag_api(file, tag):
    api.add_tag(file, tag)
    return {'success': True}

@app.get('/remove_tag')
async def remove_tag_api(file, tag):
    api.remove_tag(file, tag)
    return {'success': True}

@app.get('/remove_all_tags')
async def remove_all_tags_api(file):
    api.remove_all_tags(file)
    return {'success': True}

@app.post('/selection_add_tag')
async def selection_add_tag_api(data: list):
    api.selection_add_tag(data[:-1], data[-1])
    return {'success': True}

@app.post('/selection_remove_all_tags')
async def selection_remove_all_tags_api(data: list):
    api.selection_remove_all_tags(data)
    return {'success': True}

@app.get('/get_tags')
def get_tags_api(file):
    tags = api.get_tags(file)
    return tags

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

@app.get("/label_versions")
async def label_versions_api(key='',l=5, page=0):
    try:
        return api.label_versions(key, l, page)
    except:
        return {}

@app.get("/last_n_commits")
async def last_n_commits_api(n=5):
    try:
        return api.last_n_commits(n)
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

@app.get("/schema_metadata")
async def schema_metadata_api():
    return api.schema_metadata()

@app.get("/current")
async def current_api(page=0,max_pp=12):
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
async def set_filter_api(data: dict):
    return {'success': api.set_filters(data)}

@app.get("/reset_filters")
async def reset_filter_api():
    return {'success': api.reset_filters()}

@app.get("/commit_req")
async def commit_api(comment=''):
    return {'success': api.commit(comment)}

@app.get("/get_commit_metadata")
async def get_commit_meta_api(commit):
    try:
        return api.load_commit_metadata(commit)
    except:
        return {}

@app.get("/pull_file_api")
async def pull_file_api(file, version='current'):
    try:
        return StreamingResponse(api.load_file_binary(file, version), media_type="image/png")
    except:
        return Response(content='')

@app.get("/set_bounding_boxes")
async def set_bounding_boxes_api(val):
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
async def pull_csv_api(file, row_p=0, col_p=0, version='current'):
    try:
        data, _, _ = api.load_csv_binary(file, row_p, col_p, version)
        return StreamingResponse(data, media_type="image/png")
    except:
        return Response(content='')

@app.get("/pull_csv_metadata_api")
async def pull_csv_metadata_api(file, version='current'):
    try:
        _, nr, nc = api.load_csv_binary(file, 0, 0, version)
        return {'rows': nr-1, 'cols': nc-1}
    except:
        return {'rows': 0, 'cols': 0}

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
        api.commit('')
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
        api.commit('')
        return {'sucess': True}
    except:
        return {'sucess': False}

@app.get("/get_csv_diff_metadata")
async def get_csv_diff_metadata(key, v1='current', v2='current'):
    try:
        return api.load_csv_diff_metadata(key, v1, v2)
    except:
        return {}

@app.get("/get_csv_diff")
async def get_csv_diff(key, v1='current', v2='current'):
    try:
        return api.load_csv_diff(key, v1, v2)
    except:
        return {}

@app.get("/revert_key_version")
async def revert_key_version_api(key, version=-1, label='raw'):
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

@app.get("/get_labels")
async def get_labels_api(filename, version='current'):
    try:
        return api.get_labels(filename, version)
    except:
        return {}

@app.post("/set_labels")
async def set_labels_api(data: dict):
    try:
        return api.set_labels(data)
    except:
        return {}

@app.get("/add_slice")
async def add_slice(slice_name):
    return {'success': api.add_slice(slice_name=slice_name)}

@app.get("/remove_slice")
async def remove_slice(slice_name):
    return {'success': api.remove_slice(slice_name=slice_name)}

@app.get("/reset_slices")
async def reset_slices():
    return {'success': api.reset_slices()}

@app.post("/select_slice")
async def select_slice(slices: list):
    return {'success': api.select_slices(slices=slices)}

@app.get("/get_slices")
async def get_slices():
    return api.get_slices()

@app.get("/get_readme")
async def get_readme():
    return Response(content=api.get_readme())

# stacklib entry-points

@app.get("/init_experiment")
async def init_experiment(data: dict):
    api.server_init_experiment(uri=data['uri'],project=data['project'])
    return {'success': True}

@app.post("/add_log")
async def add_log(data: dict):
    api.server_add_log(data)
    return {'success': True}

@app.post("/get_models")
async def get_models():
    return api.server_get_models()

@app.post("/add_model")
async def add_model(data: UploadFile):
    api.server_upload_model(model=data.file, label=data.json['label'])
    return {'success': True}

@app.post("/get_model")
async def get_model(data: dict):
    return StreamingResponse(api.server_get_model(data['label']))

@app.post("/logout_experiment")
async def logout_experiment(project):
    api.server_logout_experiment(project)
    return {'success': True}