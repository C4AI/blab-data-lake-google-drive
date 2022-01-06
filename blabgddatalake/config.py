"""Contains classes representing configurations."""

from collections.abc import Mapping
from configparser import ConfigParser
from dataclasses import dataclass, field, fields
from re import sub as re_sub
from types import GenericAlias
from typing import Any, Sequence, TypeVar

from .formats import ExportFormat

T = TypeVar('T', bound='AutoConvertFromStringDataClass')


@dataclass
class AutoConvertFromStringDataClass:
    """Performs basic type conversion automatically.

    It is possible to pass strings to the constructor, and
    they will be converted into ``int``, ``float``, etc.

    If special types are required, sub-classes can
    override ``__post_init__``.
    """

    def __post_init__(self) -> None:
        for f in fields(self):
            value = getattr(self, f.name)
            if isinstance(f.type, GenericAlias):
                f.type = f.type.__origin__  # type: ignore[unreachable]
            if not isinstance(value, f.type):
                try:
                    converted = f.type(value)
                except (ValueError, TypeError) as e:
                    raise TypeError(
                        'Field {} must be {}; "{}" is invalid'.format(
                            f.name, f.type.__name__, value)) from e
                else:
                    setattr(self, f.name, converted)

    @staticmethod
    def __to_snake_case(s: str) -> str:
        # AbcDefGhi -> abc_def_ghi
        return re_sub('[A-Z]', lambda c: '_' + c.group().lower(), s).strip('_')

    @classmethod
    def from_mapping(cls: type[T], m: Mapping[str, Any]) -> T:
        """Create an instance using attributes from a mapping.

        Keys are converted to snake case.

        Args:
            m: mapping to get the attributes from

        Returns:
            AutoConvertFromStringDataClass: an instance \
                with its attributes filled
        """  # noqa: DAR203
        # noinspection PyArgumentList
        return cls(**{cls.__to_snake_case(k): v for k, v in m.items()})


@dataclass
class GoogleDriveConfig(AutoConvertFromStringDataClass):
    """Represents configuration related to Google Drive."""

    service_account_key_file_name: str
    """Name of a JSON file that contains the service account credentials"""

    shared_drive_id: str | None = None
    """The id of the shared drive

    On the web interface, it is part of the URL after
    ``drive.google.com/drive/folders/``."""

    sub_tree_root_id: str | None = None
    """The id of the subtree root.

    It can be a (not necessarily direct) subdirectory of
    the shared drive.

    On the web interface, it is part of the URL after
    ``drive.google.com/drive/folders/``.
    """

    retries: int = 0
    """Number of times to retry in case the requests fail

    See see argument `num_retries` on
    `Google API Client Library documentation <https://googleapis.github.io/\
    google-api-python-client/docs/epy/googleapiclient.http.HttpRequest-class\
    .html#execute>`_.
    """

    page_size: int = 100
    """Maximum number of files and folders retrieved per request"""

    google_workspace_export_formats: dict[str, list[ExportFormat]] = \
        field(default_factory=dict)
    """Formats to export Google Workspace files"""

    @staticmethod
    def parse_gw_extensions(formats: str) -> dict[str, list[ExportFormat]]:
        """Parse a list of extensions per file type.

        Each line must have a file type, a colon and a list of comma-separated
        extensions. Spaces are ignored.

        Args:
            formats: the string to parse

        Returns:
            A dictionary mapping each file type to a list of extensions
        """
        lines = re_sub('[^a-z,:\\.\n]', '', formats).strip().split('\n')
        type_formats = list(
            map(lambda l: l.split(':', 1),
                filter(lambda l: l.count(':') == 1, lines)))
        return {
            t: list(
                map(lambda ext: ExportFormat.from_extension(ext),
                    filter(lambda f: f, fmt.split(','))))
            for t, fmt in type_formats
        }

    def __post_init__(self) -> None:
        f = self.google_workspace_export_formats
        if isinstance(f, str):  # type: ignore[unreachable]
            self.google_workspace_export_formats = (  # type: ignore
                self.parse_gw_extensions(f))
        super().__post_init__()


@dataclass
class DatabaseConfig(AutoConvertFromStringDataClass):
    """Represents configuration related to the local database."""

    dialect: str
    """Name of the database dialect

    The server **must** be installed.
    See `list of dialects and drivers <https://docs\
        .sqlalchemy.org/en/14/core/engines.html>`_.

    .. list-table:: Examples:
        :widths: 30 70
        :header-rows: 1

        * - Dialect
          - Driver
        * - `SQLite`
          - `pysqlite`, `aiosqlite`, `pysqlcipher`
        * - `MySQL`
          - `MySQLdb`, `PyMySQL`, `MySQLConnector`, `asyncmy`, `aiomysql`, \
                `CyMySQL`, `PyODBC`
        * - `PostgreSQL`
          - `psycopg2`, `pg8000`, `asyncpg`, `psycopg2cffi`
        * - `Oracle`
          - `cx_Oracle`
        * - `MSSQL`
          - `PyODBC`, `pymssql`
    """

    driver: str
    """Database driver name

    It **must** be installed. See the attribute ``dialect`` above.
    """

    username: str | None = None
    """Database username"""

    password: str | None = None
    """Database password"""

    host: str | None = None
    """Database host"""

    port: int | None = None
    """Database port"""

    database: str | None = None
    """Database name"""

    query: str = ''
    """Dialect-specific values"""


@dataclass
class LocalConfig(AutoConvertFromStringDataClass):
    """Represents configuration related to the local files."""

    root_path: str
    """Full path to the local directory where files will be saved

    It must be an existing directory."""

    deletion_delay: int
    """Delay to delete files (in seconds)

    During a clean-up execution, only delete files that were
    marked for deletion (by a sync execution) at least this number of
    seconds ago.
    """


@dataclass
class LakeServerConfig(AutoConvertFromStringDataClass):
    """Represents configuration related to the lake HTTP server."""

    host: str
    """Host to listen on

    Usually, it is `127.0.0.1` to allow only local connections and
    `0.0.0.0` to accept connections from any IP.
    """

    port: int
    """Port to listen on"""


@dataclass
class Config:
    """Represents a complete configuration."""

    google_drive: GoogleDriveConfig
    """Google Drive settings"""

    database: DatabaseConfig
    """Database settings"""

    local: LocalConfig
    """Local storage settings"""

    lake_server: LakeServerConfig
    """Lake server settings"""

    @classmethod
    def read_settings(cls, fn: str) -> "Config":
        """Read settings from a configuration file.

        Args:
            fn: name of the configuration file

        Returns:
            parsed configuration
        """
        cp = ConfigParser()
        cp.optionxform = str  # type: ignore # do not convert to lower-case
        cp.read(fn)
        return cls(
            GoogleDriveConfig.from_mapping(cp['GoogleDrive']),
            DatabaseConfig.from_mapping(cp['Database']),
            LocalConfig.from_mapping(cp['Local']),
            LakeServerConfig.from_mapping(cp['LakeServer']),
        )


__all__: Sequence[str] = [
    c.__name__ for c in [
        Config,
        DatabaseConfig,
        GoogleDriveConfig,
        LakeServerConfig,
        LocalConfig,
    ]
]
