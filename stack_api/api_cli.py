import stack_api.api_core as api_core

import typer
from pathlib import Path

app = typer.Typer()

# Checks if local files are installed
try:
    api = api_core.API()
except:
    import os
    os.remove(str(Path.home()) + '/config.stack')
    api = api_core.API()
    api.init()

### CLI End-points ###


@app.command("init")
def init(uri):
    return {'success': api.init(uri)}


@app.command("connect")
def connect(dataset):
    return {'success': api.connectDataset(dataset)}


@app.command("uri")
def uri_api():
    try:
        return api.getURI()
    except:
        return {}


@app.command("history")
def history_api():
    try:
        return api.history()
    except:
        return {}


@app.command("last_n_commits")
def last_n_commits_api(n=5):
    try:
        return api.lastNcommits(n)
    except:
        return {}


@app.command("last_commits_from_hist_api")
def last_commits_from_hist_api(n=1):
    try:
        return api.getHistoryCommits(n)
    except:
        return {}


@app.command("status")
def status_api():
    try:
        return api.status()
    except:
        return {}


@app.command("commit")
def commit_api(comment=''):
    return {'success': api.commit(comment)}


@app.command("get_commit_metadata")
def get_commit_meta_api(commit):
    try:
        return api.loadCommitMetadata(commit)
    except:
        return {}


@app.command("pull_api")
def pull_file_api(file):
    try:
        return api.storage.pull_file(file)
    except:
        return {}


@app.command("pull_metadata")
def pull_metadata_api(file):
    try:
        return api.storage.loadFileMetadata(file)
    except:
        return {}


@app.command("diff")
def diff_api(v2, v1):
    return ''


@app.command("diff_file")
def diff_file_api(file, v2, v1):
    return ''


@app.command("revert")
def revert_api(version):
    return {'success': api.revert(version)}


@app.command("revert_file")
def revert_file_api(file, version):
    return {'success': api.revert(file, version)}


if __name__ == "__main__":
    app()
