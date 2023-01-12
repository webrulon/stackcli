import sys
sys.path.append( '..' )
import stack_api.api_core as api_core
from src.comm.docker_ver import *
import typer
from pathlib import Path
from src.core.core import *

from src.comm.docker_ver import *
import os
path_home = os.getenv('LCP_DKR')+'/' if docker_ver() else str(Path.home())

app = typer.Typer()

# Checks if local files are installed
try:
    api = api_core.API()
    initialized = api.start_check()
    api.set_schema()
    api.commit('',False)
except:
    print(f'Stack has not been installed yet!')
    print(f'setting-up Stack...')
    api = api_core.API()
    initialized = api.start_check()
    try:
        api.set_schema()
    except:
        pass
    if initialized:
        api.commit('First commit')

### CLI Entry-points ###

@app.command("connect")
def connect_cli(uri: str):
    '''
        Connects the CLI to a remote dataset URI or directory
        Dataset can be located in the local system, an S3 bucket, or Google Cloud
    '''
    print('--------------------------------')
    print('- Connecting remote dataset... -')
    print('--------------------------------')
    
    # try:
    if docker_ver():
        assert(False)

    assert(not '.stack' in uri)

    if uri == '.':
        uri = str(os.path.abspath('.'))
    if uri[0:2] == '~/':
        uri = str(os.path.abspath('~/'))
    if uri[0:3] == 'C:/' or uri[0:3] == 'C:\\':
        uri[0:3] = '/c/'

    uri = uri.replace(path_home,'')

    api.init(uri)
    api.connect_post_cli()
    api.set_schema()
    api.reset_version()
    print('connection succesful')
    return {'success': True}
    # except:
        # print('connection unsuccesful!')

        # return {'success': False}

@app.command("put")
def put_command(file: str, target_subpath: str=''):
    '''
        Uploads a file to the dataset
    '''
    try:
        if len(target_subpath)>1:
            if target_subpath[-1] != '/':
                target_subpath = target_subpath + '/'
        api.upload_file_local_path(file, target_subpath)
        api.commit('')
        return True
    except:
        return Exception

@app.command("get")
def get_command(file):
    '''
        Downloads a file from the dataset
    '''
    try:
        return api.pull(file)
    except:
        return {}

@app.command("delete")
def delete_command(file: str):
    '''
        Deletes a file from the dataset
    '''
    api.remove(file)
    api.commit('', verbose=False)

@app.command("disconnect")
def disconnect_cli(uri: str):
    '''
        Disconnects the dataset from the CLI and removes it from
        the list of datasets tracked (does not delete diffs or history)
    '''
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

@app.command("slice")
def slice_cli(name: str, filter_json: str):
    '''
        Lists a slice of the filtered dataset and adds it to
        the list of slices
    '''
    try:
        filter = json.load(open(filter_json, "rb"))
        api.set_filters(filter)
        api.add_slice(slice_name=name)
        api.reset_filters()
        return {'success': True}
    except:
        return {'success': False}

@app.command("remove_slice")
def remove_slice_cli(name: str):
    '''
        Remove a slice from the list of slices
    '''
    try:
        api.remove_slice(slice_name=name)
        return {'success': True}
    except:
        return {'success': False}

@app.command("branch")
def branch_cli(uri: str, name: str = '', filter_json: str = '', type: str = 'copy'):
    '''
        Creates a branch of the dataset from the CLI and adds it to
        the list of datasets tracked
    '''
    try:
        if filter_json != '':
            filter = json.load(open(filter_json, "rb"))
            api.set_filters(filter)
        if name == '':
            name = uri
        parent = api.Initializer.storage.dataset
        api.set_hierarchy(child = name)
        api.branch(uri, type, name)
        api.set_hierarchy(parent = parent)
        if filter_json != '':
            api.reset_filters()
        return {'success': True}
    except:
        return {'success': False}

@app.command("merge")
def merge_cli(child: str = '', parent: str = ''):
    '''
        Merge a child branch of the dataset from the CLI and adds its changes to
        the a parent branch (us)
    '''
    try: 
        if child == '':
            if parent == '':
                child = api.Initializer.storage.dataset
                hierarchy = api.get_hierarchy()
                if type(hierarchy['parent']) is str:
                    if hierarchy['parent'] == '':
                        return {'success': False}
                    else:
                        api.merge(father=hierarchy['parent'], child=child)
                        api.commit(f"merged branch {child} to {hierarchy['parent']}")
                        return {'success': True}
            else:
                master = api.Initializer.storage.dataset
                api.merge(father=master, child=child)
                api.commit(f"merged branch {child} to {master}")
        else:
            if parent == '':
                hierarchy = api.get_hierarchy()
                if type(hierarchy['parent']) is str:
                    if hierarchy['parent'] == '':
                        return {'success': False}
                    else:
                        parent = hierarchy['parent']
            api.merge(father=parent, child=child)
            api.commit(f"merged branch {child} to {parent}")
        return {'success': True}
    except:
        return {'success': False}

@app.command("checkpoints")
def get_versions_command():
    try:
        versions = api.get_versions()
        for v in reversed(list(versions.keys())):
            if v != 'current_v':
                print(f"-- Version: {v} - Label: '{versions[v]['label']}' - Date: {versions[v]['date']} ")
    except:
        return {}

@app.command("add_checkpoint")
def add_version_command(name: str= ''):
    try:
        return {'success': api.add_version(name)}
    except:
        return {} 

@app.command('comment')
def add_tag_api(file: str, comment: str):
    api.add_tag(file, comment)
    return {'success': True}

@app.command('comments')
def get_tags_api(file: str):
    print(api.get_tags(file))
    return {'success': True}

@app.command('remove_comment')
def remove_tag_api(file: str, comment: str):
    api.remove_tag(file, comment)
    return {'success': True}

@app.command('remove_comments_all')
def remove_tags_api(file: str):
    api.selection_remove_all_tags(file)
    return {'success': True}

@app.command("delete_file_and_diffs")
def full_remove_key_api(file: str):
    '''
        Deletes a file from the dataset along with previous versions
    '''
    try:
        api_core.remove_full(file)
        print(f"Removed: {file}")
        api.commit('')
    except:
        print(f"Unable to remove: {file}")

@app.command("uri")
def uri_api():
    '''
        Gets the URI of the current dataset connected
    '''
    uri = api.config
    print('Current dataset: ' + uri['dataset'])
    print('Current storage: ' + uri['storage'])

@app.command("history")
def history_api():
    '''
        Prints a summary of the commits
    '''
    print_history(api.Initializer)

@app.command("datasets")
def dataset_api():
    '''
        Lists all the remote datasets that can connect to our CLI 
    '''
    api.print_datasets()

@app.command("status")
def status_api():
    '''
        Prints the status of the dataset
    '''
    try:
        status = api.status()
        if len(status['keys']) > 0:
            print('\n-------------------------')
            print('- Status of the dataset -')
            print('-------------------------\n')
            print('List of files in dataset:')
            for i in range(len(status['keys'])):
                print('\t-- '+status['keys'][i] + '\tlast modified: '+str(status['lm'][i]))
        else:
            print('everything is ok!')
            print('please add or commit a file')
    except:
        return Exception

@app.command("sync")
def sync_api(comment: str=''):
    '''
        Synchronizes new changes done in the connected dataset
    '''
    return {'success': api.commit(comment)}

@app.command("diff_versions")
def diff_api(version_a: str, version_b: str, file: str=''):
    '''
        List the changes applied to the dataset between version N and 
        version M (optional: give it a specific file to compare)
    '''
    if int(version_b) > int(version_a):
        tmp = version_a
        version_a = version_b
        version_b = tmp

    print_diff(api.Initializer, version_a, version_b, file)
    return True

@app.command("diff_csv")
def diff_csv(v1: str='current', v2: str='current', file: str=''):
    '''
        Printes the differences between two csv files in the path
    '''
    diff_metadata = api.load_csv_diff_metadata(key, v1, v2)
    print(json.dumps(diff_metadata, indent=4))
    diff = api.load_csv_diff(key, v1, v2)
    print(json.dumps(diff, indent=4))
    return True


@app.command("revert_to_version")
def revert_api(version):
    '''
        reverts the whole dataset to a specific version
    '''
    assert(version != '')
    revert_commit(api.Initializer, int(version))
    api.commit('reverted to version '+ version, verbose=False)
    return True

if __name__ == "__main__":
    app()
