from collections.abc import MutableMapping
from configparser import ConfigParser
from dataclasses import dataclass, field, fields
from re import sub as re_sub
from types import GenericAlias
from typing import TypeVar, Type

from .formats import ExportFormat


T = TypeVar('T', bound='AutoConvertFromStringDataClass')


@dataclass
class AutoConvertFromStringDataClass:

    def __post_init__(self):
        for f in fields(self):
            value = getattr(self, f.name)
            if isinstance(f.type, GenericAlias):
                f.type = f.type.__origin__
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
        return re_sub('[A-Z]', lambda c: '_' + c.group().lower(), s).strip('_')

    @classmethod
    def from_mapping(cls: Type[T], m: MutableMapping) -> T:
        return cls(**{cls.__to_snake_case(k): v for k, v in m.items()})


@dataclass
class GoogleDriveConfig(AutoConvertFromStringDataClass):

    service_account_key_file_name: str
    shared_drive_id: str | None = None
    sub_tree_root_id: str | None = None
    retries: int = 0
    page_size: int = 100
    google_workspace_export_formats: dict[str, list[ExportFormat]] = \
        field(default_factory=dict)

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
        type_formats = list(map(lambda l: l.split(':', 1),
                                filter(lambda l: l.count(':') == 1, lines)))
        return {
            type: list(map(lambda ext: ExportFormat.from_extension(ext),
                           filter(lambda f: f, format.split(','))))
            for type, format in type_formats
        }

    def __post_init__(self):
        f = self.google_workspace_export_formats
        if isinstance(f, str):
            self.google_workspace_export_formats = self.parse_gw_extensions(f)
        super().__post_init__()


@dataclass
class DatabaseConfig(AutoConvertFromStringDataClass):
    dialect: str
    driver: str
    username: str | None = None
    password: str | None = None
    host: str | None = None
    port: int | None = None
    database: str | None = None
    query: str = ''


@dataclass
class LocalConfig(AutoConvertFromStringDataClass):

    root_path: str
    deletion_delay: int


@dataclass
class LakeServerConfig(AutoConvertFromStringDataClass):
    host: str
    port: int


@dataclass
class Config:

    google_drive: GoogleDriveConfig
    database: DatabaseConfig
    local: LocalConfig
    lake_server: LakeServerConfig

    @classmethod
    def read_settings(cls, fn: str) -> 'Config':
        """Read settings from a configuration file.

        Args:
            fn: name of the configuration file

        Returns:
            parsed configuration
        """
        cp = ConfigParser()
        setattr(cp, 'optionxform', str)  # do not convert to lower-case
        cp.read(fn)
        return cls(
            GoogleDriveConfig.from_mapping(cp['GoogleDrive']),
            DatabaseConfig.from_mapping(cp['Database']),
            LocalConfig.from_mapping(cp['Local']),
            LakeServerConfig.from_mapping(cp['LakeServer']),
        )
