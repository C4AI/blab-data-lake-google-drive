### `GoogleDrive` section:

- `ServiceAccountKeyFileName`:
    name of a JSON file that contains the service account
    credentials
- `SharedDriveId`:
    id of the shared drive (shown on the web interface on the URL
    after `drive.google.com/drive/folders/`)
- `SubTreeRootId`:
    id of the subtree root (shown on the web interface on the
    URL after `drive.google.com/drive/folders/`) -
    leave empty to use the entire drive
- `Retries`: number of times to retry in case
    the requests fail (0 by default; see argument `num_retries` on
    [Google API Client Library documentation](https://googleapis.github.io/google-api-python-client/docs/epy/googleapiclient.http.HttpRequest-class.html#execute)).
- `PageSize` (optional, defaults to 100): maximum number of
    files and folders retrieved per request


### `Database` section:

- `Dialect` and `Driver`: see
    [SQLAlchemy documentation](https://docs.sqlalchemy.org/en/14/core/engines.html).
    The corresponding server and the chosen driver **must** be installed.
    Examples:

    | Dialect      | Driver                                                                             |
    |--------------|------------------------------------------------------------------------------------|
    | `SQLite`     | `pysqlite`, `aiosqlite`, `pysqlcipher`                                             |
    | `MySQL`      | `MySQLdb`, `PyMySQL`, `MySQLConnector`, `asyncmy`, `aiomysql`, `CyMySQL`, `PyODBC` |
    | `PostgreSQL` | `psycopg2`, `pg8000`, `asyncpg`, `psycopg2cffi`                                    |
    | `Oracle`     | `cx_Oracle`                                                                        |
    | `MSSQL`      | `PyODBC`, `pymssql`                                                                |

- `Username` and `Password`: database credentials.
- `Host` and `Port`: database address.
- `Database`: database name.
- `Query`: dialect-specific values (empty by default).

### `Local` section:

- `RootPath`: full path to the local directory where files will be saved.
  The directory must exist.
- `DeletionDelay`: during a clean-up execution, only delete files that were
    marked for deletion (by a sync execution) at least this number of seconds
    ago.

### `LakeServer` section:

- `Host` and `Port`: host and port to which the server will bind.
