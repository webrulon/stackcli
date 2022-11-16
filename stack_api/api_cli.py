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
    api.commit('',False)
except:
    print(f'Stack has not been installed yet!')
    print(f'setting-up Stack...')
    api = api_core.API()
    initialized = api.start_check()

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
    
    try:
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
        api.start_check()
        print('connection succesful')
        return {'success': True}
    except:
        print('connection unsuccesful!')

        return {'success': False}

@app.command("put")
def add_command(file: str, target_subpath: str=''):
    '''
        Uploads a file to the dataset
    '''
    try:
        if len(target_subpath)>1:
            if target_subpath[-1] != '/':
                target_subpath = target_subpath + '/'
        add(api.Initializer,[file],target_subpath)
        api.commit('')
        return True
    except:
        return Exception

@app.command("get")
def pull_command(file):
    '''
        Downloads a file from the dataset
    '''
    try:
        return api.pull(file)
    except:
        return {}

@app.command("delete")
def remove_command(file: str, target_subpath: str=''):
    '''
        Deletes a file from the dataset
    '''
    if len(target_subpath)>1:
        if target_subpath[-1] != '/':
            target_subpath = target_subpath + '/'
    remove(api.Initializer,[file],target_subpath)
    api.commit('', cmd=False)

@app.command("disconnect")
def disconnect_cli(uri: str):
    '''
        Disconnects the dataset from the CLI and removes it from
        the list of datasets tracked (does not delete diffs or history)
    '''
    try:
        return {'success': api.disconnectDataset(uri)}
    except:
        return {'success': False}

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
    uri = api.getURI()
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
        print_status(api.Initializer)
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
        List the changes applied to the dataset between version_a and 
        version_b (optional: give it a specific file to compare)
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
    api.commit('reverted to version '+ version, cmd=False)
    return True

if __name__ == "__main__":
    app()
