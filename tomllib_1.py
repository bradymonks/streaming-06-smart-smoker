import tomli

toml_str = """
python-version = "3.11.0"
python-implementation = "CPython"
"""

data = tomli.loads(toml_str)