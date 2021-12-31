# BLAB Data Lake (using Google Drive)


## Installation:

- Install
  [Python 3.10](https://www.python.org/downloads/release/python-3100/)
  or newer.

- Install [Poetry](https://python-poetry.org/):
```shell
curl -sSL https://install.python-poetry.org | python3 - --preview
```

- Install the dependencies using Poetry: 
```shell
poetry install
```

- **(Optional - not necessary in production)**
  To install additional dependencies for development,
  documentation generation and testing, add the arguments
  `--with dev,doc,test` to the command in the last step.


- Obtain a service account for Google Drive on
  [Google Cloud Platform](https://console.cloud.google.com/)
  and download the private key file (JSON).

- Copy the file
  `blab-data-lake-settings-template.cfg` to
  `blab-data-lake-settings.cfg` and fill in
  the fields (see [documentation](README_CONFIG.md)).

## Execution:

- Run `poetry run ./blab_data_lake.py` with one of the available commands
  (`sync`, `cleanup`, `serve`) and its arguments.
  Add `-h` to display usage help.
