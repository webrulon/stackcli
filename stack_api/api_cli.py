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
def init(uri):
    return {'success': api.init(uri)}


@app.command("add")
def add(path, subpath=''):
    if len(subpath)>1:
        if subpath[-1] != '/':
            subpath = subpath + '/'
    add(api.Initializer,[path],subpath)
    return True


@app.command("remove")
def remove(key, subpath=''):
    if len(subpath)>1:
        if subpath[-1] != '/':
            subpath = subpath + '/'
    remove(api.Initializer,[key],subpath)
    return True


@app.command("/full_remove")
def full_remove_key_api(key):
    try:
        api_core.remove_full(key)
        print(f"Removed: {key}")
    except:
        print(f"Unable to remove: {key}")


@app.command("connect")
def connect(dataset):
    return {'success': api.connectDataset(dataset)}


@app.command("uri")
def uri_api():
    uri = api.getURI()
    print(uri)


@app.command("history")
def history_api():
	printHistory(api.Initializer)


@app.command("status")
def status_api():
    try:
        status = api.status()
        print(status)
    except:
        return Exception


@app.command("commit")
def commit_api(comment=''):
    # TODO: do we need to print something?
    return {'success': api.commit(comment)}

@app.command("pull")
def pull_file_api(file):
    try:
        return api.storage.pull_file(file)
    except:
        return {}


@app.command("diff")
def diff_api(v1, v0, file=''):
    printDiff(api.Initializer, v1, v0, file)
    return True


@app.command("revert")
def revert_api(version=0):
    assert(version != '')
    revertCommit(api.Initializer, int(version))
    return True


if __name__ == "__main__":
    app()
