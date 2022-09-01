# Stack

### Install Stack

1. Open your favorite terminal: `Terminal`
2. Install stackcli: `pip install stackcli` (ideally in a virtualenv)

### Build a test dataset

1. Make a directory: `mkdir test_dataset`
2. Go to the directory: `cd test_dataset`
3. Download an image of Einstein: `curl -o einstein.jpg https://upload.wikimedia.org/wikipedia/en/8/86/Einstein_tongue.jpg`


### Try Stack's Command Line Tool (CLI)

1. Init stack in the current directory: ```stackcli init . ``` (note the dot at the end)
2. Add a file to track: `stackcli add einstein.jpg`
3. Commit your changes: `stackcli commit`
4. Check status: `stackcli status`
5. See history of changes: `stackcli history`
6. Remove the file: `stackcli remove einstein.jpg`
7. Revert your changes: `stackcli revert 1`
8. You should see Einstein in your directory again!


# Stack Dev (Ignore if you are just a user)

### Features to implement

### Publish to TestPy

https://typer.tiangolo.com/tutorial/package/

1. Install package: `poetry install'
2. Try CLI: find it first `which stackcli`
3. Create a wheel package: `poetry build'
4. Test wheel: `pip install --user dist'
5. Try the wheel: `stackcli`
6. Publish it to TestPy: `poetry publish --build'
7. Install from TestPyPI:
  1. `pip uninstall stack-cli`
  1. `pip install -i https://test.pypi.org/simple/ stack-cli`
