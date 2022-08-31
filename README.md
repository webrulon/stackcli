# Stack


### Features to implement

#### For stack status

- [] Even  if there are no changes,  it would be great that  `stack status` returns something.

### Publish to TestPy

https://typer.tiangolo.com/tutorial/package/

1. Install package: `poetry install'
2. Try CLI: find it first `which stackcli`
3. Create a wheel package: `poetry build'
4. Test wheel: `pip install --user /home/tonirv/Code/stackcli/dist'
5. Try the wheel: `stackcli`
6. Publish it to TestPy: `poetry publish --build'
7. Install from TestPyPI:
  1. `pip uninstall stack-cli`
  1. `pip install -i https://test.pypi.org/simple/ stack-cli`

