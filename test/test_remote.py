import unittest
from dataclasses import replace
from test import BaseTest
from test.gdmock import GDHttpMock, GDServiceMock
from typing import cast

from httplib2 import Http
from overrides import overrides

from blabgddatalake.config import (Config, DatabaseConfig, GoogleDriveConfig,
                                   LakeServerConfig, LocalConfig)


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

    def test_initial(self) -> None:
        """Conversion from the API-provided data to the classes in `remote`."""
        gdservice = GDServiceMock(self.gd_config.google_drive,
                                  self.discovery_http)
        self.check_equal_tree(self.all_files_by_id, gdservice.get_tree())

    def test_small_pages(self) -> None:
        """Conversion from the API-provided data to the classes in `remote`."""
        for i in [1, 2, 5]:
            with self.subTest(page_size=i):
                gdservice = GDServiceMock(
                    replace(self.gd_config.google_drive, page_size=i),
                    self.discovery_http)
                self.check_equal_tree(self.all_files_by_id,
                                      gdservice.get_tree())


if __name__ == '__main__':
    unittest.main()
