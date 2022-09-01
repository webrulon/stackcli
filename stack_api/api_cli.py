import stack_api.api_core as api_core

import typer
from pathlib import Path

from src.core.core import *

app = typer.Typer()

# Checks if local files are installed
try:
    api = api_core.API()
    initialized = api.start_check()
    assert(initialized)
    print('everything is setup alright!')
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

### CLI End-points ###

@app.command("init")
def init(uri: str):
    return {'success': api.init(uri)}

@app.command("add")
def add_command(path: str, subpath: str=''):
    if len(subpath)>1:
        if subpath[-1] != '/':
            subpath = subpath + '/'
    add(api.Initializer,[path],subpath)
    return True


@app.command("remove")
def remove_command(key: str, subpath: str=''):
    if len(subpath)>1:
        if subpath[-1] != '/':
            subpath = subpath + '/'
    remove(api.Initializer,[key],subpath)
    return True

# End-points
@app.command("connect")
def connect_cli(uri: str):
    try:
        api.init(uri)
        api.connect_post_cli()
        api.start_check()
        return {'success': True}
    except:
        return {'success': False}

@app.command("disconnect")
def disconnect_cli(uri: str):
    try: 
        return {'success': api.disconnectDataset(uri)}
    except:
        return {'success': False}

@app.command("remove_full")
def full_remove_key_api(key: str):
    try:
        api_core.remove_full(key)
        print(f"Removed: {key}")
    except:
        print(f"Unable to remove: {key}")

@app.command("uri")
def uri_api():
    uri = api.getURI()
    print(uri)

@app.command("history")
def history_api():
	printHistory(api.Initializer)

@app.command("datasets")
def dataset_api():
    api.print_datasets()


@app.command("status")
def status_api():
    try:
        printStatus(api.Initializer)
    except:
        return Exception

@app.command("commit")
def commit_api(comment: str=''):
    # TODO: do we need to print something?
    return {'success': api.commit(comment)}

@app.command("pull")
def pull_file_api(file):
    try:
        return api.pull(file)
    except:
        return {}


@app.command("diff")
def diff_api(v1: str, v0: str, file: str=''):
    printDiff(api.Initializer, v1, v0, file)
    return True


@app.command("revert")
def revert_api(version):
    assert(version != '')
    revertCommit(api.Initializer, int(version))
    return True


if __name__ == "__main__":
    app()
