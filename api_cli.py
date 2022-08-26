import api_core

import typer

from pathlib import Path
from typing import Optional

app = typer.Typer()

@app.command()
def add_cloud(url: str = typer.Option(..., prompt=True)):
    print(f"Adding cloud storage at: {url}")

@app.command()
def list(url: str = typer.Option(..., prompt=True)):
    # Read yaml

    # Display url
    print(f"Listing cloud storage at: {url}")

    # Call Boto3

    # Display S3 bucket

@app.command()
def init(name: str = typer.Option(..., prompt=True),
         password: str = typer.Option(..., prompt=True,
                                      confirmation_prompt=True,
                                      hide_input=True)):
    print(f"Hello {name}. Welcome to Stack!")
    # TODO: store username and password

@app.command("create")
def cli_create_user(username: str):
    print(f"Creating user: {username}")

@app.command("delete")
def cli_delete_user(username: str):
    print(f"Deleting user: {username}")

# @app.callback(invoke_without_command=True)
# def main(name: str, lastname: str = "", formal: bool = False):
#     """
#     Say hi to NAME, optionally with a --lastname.
#
#     If --formal is used, say hi very formally.
#     """
#     print("Initializing database")
#     if formal:
#         print(f"Good day Ms. {name} {lastname}.")
#     else:
#         print(f"Hello {name} {lastname}")

@app.command(
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True}
)
def main2(ctx: typer.Context):
    for extra_arg in ctx.args:
        print(f"Got extra arg: {extra_arg}")

    if ctx.invoked_subcommand is None:
        # Is Only  called once at the beginning
        print("Initializing database")

def main3(config: Optional[Path] = typer.Option(None)):
    if config is None:
        print("No config file")
        raise typer.Abort()
    if config.is_file():
        text = config.read_text()
        print(f"Config file contents: {text}")
    elif config.is_dir():
        print("Config is a directory, will use all its config files")
    elif not config.exists():
        print("The config doesn't exist")

def main4(
    config: Path = typer.Option(
        ...,
        exists=True,
        file_okay=True,
        dir_okay=False,
        writable=False,
        readable=True,
        resolve_path=True,
    )
):
    text = config.read_text()
    print(f"Config file contents: {text}")


# checks if local files are installed
try:
    api_core = API()
except:
    import os
    os.remove(str(Path.home())+'/config.stack')
    api_core = API()
    api_core.init()

# End-points


@app.command("init")
async def init(uri):
    return {'success': api_core.init(uri)}

@app.command("connect")
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


if __name__ == "__main__":
    app()

