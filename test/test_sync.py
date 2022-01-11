import unittest
from test import BaseTest, fakefs
from test.gdmock import GDServiceMock

from overrides import overrides

from blabgddatalake.local.localdb import LocalStorageDatabase
from blabgddatalake.sync import GoogleDriveSync


class SyncTest(BaseTest):

    @overrides
    def setUp(self) -> None:
        super().setUp()
        self.gdservice = GDServiceMock(self.config.google_drive, self.http)
        self.db = LocalStorageDatabase(self.config.database)
        self.sync = GoogleDriveSync(self.config, self.gdservice, self.db)

    @fakefs
    def test_none(self) -> None:
        """Non-initialised database."""
        with self.db.new_session() as session:
            self.assertIsNone(self.db.get_tree(session))

    @fakefs
    def test_empty(self) -> None:
        """Empty drive."""
        self.all_files = {'root': self.all_files['root']}
        self.http.state = self.all_files_by_id
        self.sync.sync()
        with self.db.new_session() as session:
            tree = self.db.get_tree(session)
        self.check_equal_tree(self.all_files_by_id, tree)


if __name__ == '__main__':
    unittest.main()
