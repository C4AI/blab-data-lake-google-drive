#!/usr/bin/env python3

from dateutil import parser as timestamp_parser
from dataclasses import dataclass, field
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.discovery import build, Resource
from structlog import getLogger
from typing import Optional


_logger = getLogger(__name__)


_DEFAULT_PAGE_SIZE = 100
_FILE_FIELDS = ', '.join(['id', 'name', 'parents', 'kind', 'mimeType',
                         'webViewLink', 'md5Checksum', 'size', 'createdTime',
                          'modifiedTime', 'lastModifyingUser',
                          'headRevisionId', 'iconLink',
                          ])


@dataclass
class RemoteFile:
    name: str
    id: str
    mime_type: str
    created_time: datetime
    modified_time: datetime
    modified_by: str
    web_url: str
    icon_url: str
    parent: Optional['RemoteDirectory']

    def print_tree(self, pfx: list[bool] | None = None) -> None:
        if pfx is None:
            pfx = []
        for i, p in enumerate(pfx[:-1]):
            print(' ┃ ' if p else '   ', end=' ')
        if pfx:
            print(' ┠─' if pfx[-1] else ' ┖─', end=' ')
        print(self.name)

    def download(self, service: Resource, file_name: str) -> bool:
        log = _logger.bind(id=self.id, name=self.name, local_name=file_name)
        log.info('downloading file')
        with open(file_name, 'wb') as fd:
            request = service.files().get_media(
                fileId=self.id,
            )
            downloader = MediaIoBaseDownload(fd, request)
            completed = False
            while not completed:
                status, completed = downloader.next_chunk()
        return True


@dataclass
class RemoteDirectory(RemoteFile):
    children: list[RemoteFile] = field(default_factory=list)
    is_root: bool = False

    def _fill_children(self, service: Resource,
                       gd_config: dict[str, str]) -> None:
        q_items = ['not trashed']
        if self.id:
            q_items.append(f"'{self.id}' in parents")
        q = ' and '.join(q_items)
        shared_drive_id = gd_config.get('SharedDriveId', None)
        params = dict(
            supportsAllDrives=bool(shared_drive_id),
            includeItemsFromAllDrives=bool(shared_drive_id),
            driveId=shared_drive_id or None,
            corpora='drive' if shared_drive_id else 'user',
            pageSize=int(gd_config.get('PageSize', None) or
                         _DEFAULT_PAGE_SIZE),
            fields=f'nextPageToken, files({_FILE_FIELDS})',
            orderBy='folder, name',
            q=q
        )
        page_token = None
        children = []
        page = 0
        log = _logger.bind(id=self.id)
        while page_token is not None or page == 0:
            request = service.files().list(
                pageToken=page_token,
                **params
            )
            log.debug('requesting directory', page=page)
            results = request.execute()
            children += results['files']
            page_token = results.get('nextPageToken', None)
            page += 1
        for f in children:
            metadata = [f['name'], f['id'], f['mimeType'],
                        timestamp_parser.parse(f['createdTime']),
                        timestamp_parser.parse(f['modifiedTime']),
                        f['lastModifyingUser']['displayName'],
                        f['webViewLink'], f['iconLink'],
                        self,
                        ]
            node: RemoteFile
            if f['mimeType'] == 'application/vnd.google-apps.folder':
                node = RemoteDirectory(*metadata)
                node._fill_children(service, gd_config)
            else:
                file_metadata = [
                    int(s) if (s := f.get('size', None)) is not None else None,
                    f.get('md5Checksum', None),
                    f.get('headRevisionId', None),
                ]
                node = RemoteRegularFile(*metadata, *file_metadata)
            self.children.append(node)

    @classmethod
    def get_tree(cls,
                 service: Resource, gd_config: dict[str, str]
                 ) -> 'RemoteDirectory':
        this_id = gd_config.get('SubTreeRootId', None) or \
            gd_config.get('SharedDriveId', None)
        shared_drive_id = gd_config.get('SharedDriveId', None)
        if this_id:
            request = service.files().get(
                fileId=this_id,
                supportsAllDrives=bool(shared_drive_id),
                fields=_FILE_FIELDS,
            )
            _logger.debug('requesting root directory', id=this_id)
            f = request.execute()
            metadata = [f['name'], f['id'], f['mimeType'],
                        timestamp_parser.parse(f['createdTime']),
                        timestamp_parser.parse(f['modifiedTime']),
                        f['lastModifyingUser']['displayName'],
                        f['webViewLink'], f['iconLink'],
                        None,
                        ]
        else:
            metadata = ['', None, None, None, None, None, None]
        root = RemoteDirectory(*metadata, is_root=True)  # type: ignore
        root._fill_children(service, gd_config)
        return root

    def flatten(self) -> dict[str, RemoteFile]:
        d: dict[str, RemoteFile] = {self.id: self}
        for c in self.children:
            d.update(c.flatten() if isinstance(c, RemoteDirectory)
                     else {c.id: c})
        return d

    def print_tree(self, pfx: list[bool] | None = None) -> None:
        super().print_tree(pfx)
        if pfx is None:
            pfx = []
        for child in self.children[:-1]:
            child.print_tree(pfx + [True])
        if self.children:
            self.children[-1].print_tree(pfx + [False])


@dataclass
class RemoteRegularFile(RemoteFile):
    size: int
    md5_checksum: str
    head_revision_id: str

    @property
    def local_name(self) -> str:
        return self.id + \
            '_' + (self.head_revision_id or '') + \
            '_' + (self.md5_checksum or '')

    @property
    def is_google_workspace_file(self) -> bool:
        return self.md5_checksum.startswith('application/vnd.google-apps')


class GoogleDriveService:
    """A class that wraps Google Drive API to obtain the directory tree.

    This class provides a method that obtains the directory tree
    from a Google Drive directory or shared drive.
    """

    def __init__(self, gd_config: dict[str, str]):
        """Create an instance using the configuration given at `gd_config`.

        For a description of the expected keys and values of `gd_config`,
        see :download:`the documentation <../README_CONFIG.md>`.
        """
        self.gd_config = gd_config
        self.service = self.__get_service()

    def __get_service(self) -> Resource:
        scopes = ['https://www.googleapis.com/auth/drive']
        cred = service_account.Credentials.from_service_account_file(
            self.gd_config['ServiceAccountKeyFileName'], scopes=scopes)
        s = build('drive', 'v3', credentials=cred, cache_discovery=False)
        return s

    def get_tree(self) -> RemoteDirectory:
        """
        Fetch and return the directory tree from Google Drive.

        The returned `RemoteDirectory` object is the root directory defined
        by the ``SubTreeRootId`` field of `gd_config`. There is no depth limit.
        """
        return RemoteDirectory.get_tree(self.service, self.gd_config)

    def download_file(self, file: RemoteRegularFile, output_file: str) -> None:
        """Download `file` and save it as a local file at `output_file`."""
        file.download(self.service, output_file)
