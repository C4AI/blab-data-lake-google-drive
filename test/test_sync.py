import unittest
from test.gdmock import (GDDirectoryMock, GDFileMock, GDGoogleDocsFileMock,
                         GDGoogleDrawingsFileMock, GDGoogleJamboardFileMock,
                         GDGoogleSheetsFileMock, GDGoogleSlidesFileMock,
                         GDGoogleWorkspaceFileMock, GDHttpMock,
                         GDRegularFileMock, GDServiceMock)
from typing import Any, Callable, TypeVar, cast

from dateutil import parser as timestamp_parser
from httplib2 import Http
from overrides import overrides
from pyfakefs.fake_filesystem_unittest import Patcher

from blabgddatalake.config import (Config, DatabaseConfig, GoogleDriveConfig,
                                   LakeServerConfig, LocalConfig)
from blabgddatalake.local.file import LocalDirectory, LocalFile
from blabgddatalake.local.gwfile import LocalGoogleWorkspaceFile
from blabgddatalake.local.regularfile import LocalRegularFile
from blabgddatalake.remote.file import RemoteDirectory, RemoteFile
from blabgddatalake.remote.gwfile import RemoteGoogleWorkspaceFile
from blabgddatalake.remote.regularfile import RemoteRegularFile

FunT = TypeVar('FunT', bound=Callable[..., Any])


def fakefs(func: FunT) -> FunT:

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        with Patcher() as p:
            p.fs.add_real_file('test/drive.v3.json')
            func(*args, **kwargs)

    return cast(FunT, wrapper)


# noinspection PyMissingOrEmptyDocstring
class SyncTest(unittest.TestCase):

    @overrides
    def setUp(self) -> None:
        all_files = {
            'root': GDDirectoryMock('root-dir'),
            'd1': GDDirectoryMock('d1-dir'),
            'd2': GDDirectoryMock('d2-dir'),
            'd3': GDDirectoryMock('d3-dir'),
            'd1-1': GDDirectoryMock('d1-1-dir'),
            'd1-2': GDDirectoryMock('d1-2-dir'),
            'tx': GDRegularFileMock('plain_text.txt', mimeType='text/plain'),
            'do': GDGoogleDocsFileMock('a-document'),
            'sh': GDGoogleSheetsFileMock('a-spreadsheet'),
            'pr': GDGoogleSlidesFileMock('a-presentation'),
            'dr': GDGoogleDrawingsFileMock('a-drawing'),
            'jb': GDGoogleJamboardFileMock('a-jam'),
            'bi': GDRegularFileMock('a-binary'),
        }
        all_files['root'].parents = ['_dummy_parent_outside_lake_______']
        for d in ['d1', 'd2', 'd3']:
            all_files[d].parents = [all_files['root'].id]
        for d in ['d1-1', 'd1-2']:
            all_files[d].parents = [all_files['d1'].id]
        for f in ['tx', 'do']:
            all_files[f].parents = [all_files['d1-1'].id]
        for f in ['sh', 'pr']:
            all_files[f].parents = [all_files['d1-2'].id]
        all_files['dr'].parents = [all_files['d2'].id]
        all_files['jb'].parents = [all_files['d1'].id]
        all_files['bi'].parents = [all_files['root'].id]
        self.all_files = all_files
        self.discovery_http = cast(Http,
                                   GDHttpMock(state=self.all_files_by_id))
        self.gd_config = Config(
            GoogleDriveConfig('not-used.json', '_dummy_shared_drive',
                              all_files['root'].id),
            DatabaseConfig('sqlite', 'pysqlite'),
            LocalConfig('/pyfakefs-virtual-fs', 60),
            LakeServerConfig('127.0.0.1', 8080))

    @property
    def all_files_by_id(self) -> dict[str, GDFileMock]:
        return {f.id: f for f in self.all_files.values()}

    def check_equal_file(self, fm: GDFileMock,
                         f: RemoteFile | LocalFile) -> None:
        self.assertEqual(fm.id, f.id)
        self.assertEqual(fm.name, f.name)
        self.assertEqual(fm.mimeType, f.mime_type)
        self.assertEqual(timestamp_parser.parse(fm.createdTime),
                         f.created_time)
        self.assertEqual(timestamp_parser.parse(fm.modifiedTime),
                         f.modified_time)
        self.assertEqual(fm.lastModifyingUser.displayName, f.modified_by)
        self.assertEqual(fm.webViewLink, f.web_url)
        self.assertEqual(fm.iconLink, f.icon_url)
        if isinstance(fm, GDRegularFileMock):
            self.assertIsInstance(f, (RemoteRegularFile, LocalRegularFile))
            assert isinstance(  # to avoid mypy warning
                f, RemoteRegularFile | LocalRegularFile)
            self.assertEqual(fm.md5Checksum, f.md5_checksum)
            self.assertEqual(fm.capabilities.canDownload, f.can_download)
            self.assertEqual(fm.headRevisionId, f.head_revision_id)
            self.assertEqual(int(fm.size), f.size)
        elif isinstance(fm, GDGoogleWorkspaceFileMock):
            self.assertIsInstance(
                f, (RemoteGoogleWorkspaceFile, LocalGoogleWorkspaceFile))
            assert isinstance(  # to avoid mypy warning
                f, RemoteGoogleWorkspaceFile | LocalGoogleWorkspaceFile)
            self.assertEqual(fm.capabilities.canDownload, f.can_export)
            self.assertSetEqual(
                set(fm.exportLinks.keys()),
                set(map(lambda fmt: fmt.mime_type, f.export_formats)))
        if not (isinstance(f, RemoteDirectory | LocalDirectory) and f.is_root):
            self.assertListEqual(fm.parents or [],
                                 [f.parent_id] if f.parent_id else [])

    def check_equal_tree(self, expected: dict[str, GDFileMock],
                         actual: RemoteDirectory) -> None:
        already_checked: set[str] = set()
        pending: list[RemoteFile] = [actual]
        while pending:
            rf = pending.pop()
            if rf.id in already_checked:
                self.fail(f'Duplicate file id {rf.id}')
            try:
                fm = expected[rf.id]
            except KeyError:
                self.fail(f'Unexpected file id {rf.id}')
            else:
                self.check_equal_file(fm, rf)
                already_checked.add(rf.id)
                if isinstance(rf, RemoteDirectory):
                    for child in rf.children:
                        pending.append(child)
        self.assertSetEqual(set(expected.keys()), already_checked)

    @fakefs
    def test_rf(self) -> None:
        """Conversion from the API-provided data to the classes in `remote`."""
        gdservice = GDServiceMock(self.gd_config.google_drive,
                                  self.discovery_http)
        self.check_equal_tree(self.all_files_by_id, gdservice.get_tree())


if __name__ == '__main__':
    unittest.main()