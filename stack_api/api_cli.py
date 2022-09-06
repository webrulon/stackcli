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
        os.remove(path_home+'/.config_stack')
    except:
        print(f'Stack has not been installed yet!')
        print(f'setting-up Stack...')
    api = api_core.API()
    initialized = api.init()

    if initialized:
        api.start_check()

### CLI End-points ###

@app.command("init")
def init(uri: str):
    return {'success': api.init(uri)}

@app.command("add")
def add_command(path: str, subpath: str=''):
        try:
            if len(subpath)>1:
                if subpath[-1] != '/':
                    subpath = subpath + '/'
            add(api.Initializer,[path],subpath)
            api.commit('')
            return True
        except:
            return Exception


@app.command("remove")
def remove_command(key: str, subpath: str=''):
    if len(subpath)>1:
        if subpath[-1] != '/':
            subpath = subpath + '/'
    remove(api.Initializer,[key],subpath)
    api.commit('')

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
        api.commit('')
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

@app.command("sync")
def sync_api(comment: str=''):
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


@app.command("diff_csv")
def diff_csv(csv1: str, csv2: str):
    from csv_diff import load_csv, compare
    diff = compare(load_csv(open(csv1)), load_csv(open(csv2)))
    print(diff)
    return True


@app.command("revert")
def revert_api(version):
    assert(version != '')
    revertCommit(api.Initializer, int(version))
    return True


if __name__ == "__main__":
    app()
