import unittest
from datetime import datetime, timedelta
from test import BaseTest, fakefs
from test.gdmock import (GDFileMock, GDGoogleWorkspaceFileMock,
                         GDRegularFileMock, GDServiceMock, fake_md5,
                         fake_revision_id, timestamp)
from typing import Iterable, cast

from overrides import overrides

from blabgddatalake.formats import ExportFormat
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
        """Check that non-initialised database has no tree."""
        with self.db.new_session() as session:
            self.assertIsNone(self.db.get_tree(session))

    @fakefs
    def test_empty(self) -> None:
        """Sync from a (mock) drive that contains only an empty folder."""
        self.all_files = {'root': self.all_files['root']}
        self.http.state = self.all_files_by_id
        self.sync.sync()
        with self.db.new_session() as session:
            self.check_equal_tree(
                self.all_files_by_id, self.db.get_tree(session),
                self.config.google_drive.google_workspace_export_formats)

    @classmethod
    def dl_exp(
        cls, all_files: Iterable[GDFileMock],
        export_formats: dict[str, list[ExportFormat]]
    ) -> tuple[dict[str, int], dict[str, int]]:
        dl = {f.id: 1 for f in all_files if isinstance(f, GDRegularFileMock)}

        def actual_export_formats(f: GDGoogleWorkspaceFileMock) -> set[str]:
            return set(f.exportLinks.keys()) & set(
                map(lambda fmt: fmt.mime_type,
                    export_formats.get(f.mimeType.rsplit('.', 1)[-1], [])))

        exp = {
            f.id: 1
            for f in all_files if isinstance(f, GDGoogleWorkspaceFileMock)
            and f.capabilities.canDownload and actual_export_formats(f)
        }
        return dl, exp

    @fakefs
    def test_first(self) -> None:
        """Sync from a (mock) drive for the first time."""
        self.sync.sync()
        with self.db.new_session() as session:
            self.check_equal_tree(
                self.all_files_by_id, self.db.get_tree(session),
                self.config.google_drive.google_workspace_export_formats)
        dl, exp = self.dl_exp(
            self.all_files.values(),
            self.config.google_drive.google_workspace_export_formats)
        self.assertDictEqual(dl, self.gdservice.download_count)
        self.assertDictEqual(exp, self.gdservice.export_count)

    @fakefs
    def test_changed_regular_file_contents(self) -> None:
        """Sync after changes in the contents of a regular file."""
        self.sync.sync()
        f = cast(GDRegularFileMock, self.all_files['tx'])
        old_md5 = f.md5Checksum
        old_hrid = f.headRevisionId
        old_modt = f.modifiedTime
        old_modb = f.lastModifyingUser.displayName
        f.md5Checksum = fake_md5()
        f.headRevisionId = fake_revision_id()
        f.modifiedTime = timestamp(datetime.utcnow() + timedelta(seconds=15))
        f.size = str(int(f.size) * 5)
        f.lastModifyingUser.displayName = 'Another Name'
        self.sync.sync()
        with self.db.new_session() as session:
            self.check_equal_tree(
                self.all_files_by_id, self.db.get_tree(session),
                self.config.google_drive.google_workspace_export_formats)
            obs = self.db.get_obsolete_file_revisions(session)
            self.assertEqual(1, len(obs))
            self.assertEqual(f.id, obs[0].file_id)
            self.assertEqual(old_md5, obs[0].md5_checksum)
            self.assertEqual(old_hrid, obs[0].revision_id)
            self.assertEqual(old_modt, timestamp(obs[0].modified_time))
            self.assertEqual(old_modb, obs[0].modified_by)
        dl, exp = self.dl_exp(
            self.all_files.values(),
            self.config.google_drive.google_workspace_export_formats)
        dl[f.id] = 2
        self.assertDictEqual(dl, self.gdservice.download_count)
        self.assertDictEqual(exp, self.gdservice.export_count)

    @fakefs
    def test_changed_regular_file_metadata(self) -> None:
        """Sync after changes in the metadata of a regular file."""
        self.sync.sync()
        f = cast(GDRegularFileMock, self.all_files['tx'])
        f.modifiedTime = timestamp(datetime.utcnow() + timedelta(minutes=20))
        f.name = 'new-file-name!'
        self.sync.sync()
        with self.db.new_session() as session:
            self.check_equal_tree(
                self.all_files_by_id, self.db.get_tree(session),
                self.config.google_drive.google_workspace_export_formats)
            obs = self.db.get_obsolete_file_revisions(session)
            self.assertEqual(0, len(obs))
        dl, exp = self.dl_exp(
            self.all_files.values(),
            self.config.google_drive.google_workspace_export_formats)
        self.assertDictEqual(dl, self.gdservice.download_count)
        self.assertDictEqual(exp, self.gdservice.export_count)

    @fakefs
    def test_changed_directory_metadata(self) -> None:
        """Sync after changes in the metadata of a directory."""
        self.sync.sync()
        f = cast(GDRegularFileMock, self.all_files['d1'])
        f.modifiedTime = timestamp(datetime.utcnow() + timedelta(minutes=40))
        f.name = 'new-dir-name!'
        self.sync.sync()
        with self.db.new_session() as session:
            self.check_equal_tree(
                self.all_files_by_id, self.db.get_tree(session),
                self.config.google_drive.google_workspace_export_formats)
            obs = self.db.get_obsolete_file_revisions(session)
            self.assertEqual(0, len(obs))
        dl, exp = self.dl_exp(
            self.all_files.values(),
            self.config.google_drive.google_workspace_export_formats)
        self.assertDictEqual(dl, self.gdservice.download_count)
        self.assertDictEqual(exp, self.gdservice.export_count)


if __name__ == '__main__':
    unittest.main()
