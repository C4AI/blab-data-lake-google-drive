import unittest
from test import BaseTest
from test.gdmock import GDHttpMock, GDServiceMock
from typing import Any, Callable, TypeVar, cast

from httplib2 import Http
from overrides import overrides
from pyfakefs.fake_filesystem_unittest import Patcher

from blabgddatalake.config import (Config, DatabaseConfig, GoogleDriveConfig,
                                   LakeServerConfig, LocalConfig)

FunT = TypeVar('FunT', bound=Callable[..., Any])


def fakefs(func: FunT) -> FunT:

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        with Patcher() as p:
            p.fs.add_real_file('test/drive.v3.json')
            func(*args, **kwargs)

    return cast(FunT, wrapper)


class SyncTest(BaseTest):

    @overrides
    def setUp(self) -> None:
        super().setUp()
        self.discovery_http = cast(Http,
                                   GDHttpMock(state=self.all_files_by_id))
        self.gd_config = Config(
            GoogleDriveConfig('not-used.json', '_dummy_shared_drive',
                              self.all_files['root'].id),
            DatabaseConfig('sqlite', 'pysqlite'),
            LocalConfig('/pyfakefs-virtual-fs', 60),
            LakeServerConfig('127.0.0.1', 8080))

    @fakefs
    def test_initial(self) -> None:
        """Conversion from the API-provided data to the classes in `remote`."""
        gdservice = GDServiceMock(self.gd_config.google_drive,
                                  self.discovery_http)
        self.check_equal_tree(self.all_files_by_id, gdservice.get_tree())


if __name__ == '__main__':
    unittest.main()
