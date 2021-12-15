# BLAB Data Lake (using Google Drive)


## Installation:

- Install Python 3.10 and Pipenv.

- Create the Pipenv environment and install the Python dependencies
  by running `pipenv install` (add ` --dev` if the development-only
  dependencies are needed).

- Obtain a service account for Google Drive on Google Cloud Platform
  and download the private key file (JSON).

- Copy the file
  `blab-data-lake-settings-template.cfg` to
  `blab-data-lake-settings.cfg` and fill in
  the fields (see [documentation](README_CONFIG.md)).

## Execution:

- Run `pipenv run ./blab_data_lake.py` with one of the available commands
  (`sync`, `cleanup`, `serve`) and its arguments.
  Add `-h` to display usage help.
