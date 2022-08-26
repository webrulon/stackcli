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

if __name__ == "__main__":
    app()
