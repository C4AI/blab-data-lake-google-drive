#!/usr/bin/env python3

from datetime import datetime
from dataclasses import dataclass, field
from dateutil import parser as timestamp_parser
from typing import Optional, List, Dict
import configparser
import logging

from googleapiclient.discovery import build, Resource
from google.oauth2 import service_account

from .cachedb import MetadataCacheDatabase


logging.basicConfig(level=logging.INFO)

DEFAULT_PAGE_SIZE = 100
FILE_FIELDS = ', '.join(['id', 'name', 'parents', 'kind', 'mimeType',
                         'webViewLink', 'md5Checksum', 'size', 'createdTime',
                         'modifiedTime', 'lastModifyingUser'])


@dataclass
class LakeFile:
    name: str
    id: str
    created_time: datetime
    modified_time: datetime
    modified_by: str
    web_url: str
    parent: Optional['LakeDirectory']


@dataclass
class LakeDirectory(LakeFile):
    children: List[LakeFile] = field(default_factory=list)
    is_root: bool = False

    def _fill_children(self,
                       service: Resource,
                       gd_config: Dict[str, str]
                       ) -> Optional['LakeDirectory']:
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
            pageSize=int(gd_config.get('PageSize', None)
                         or DEFAULT_PAGE_SIZE),
            fields=f'nextPageToken, files({FILE_FIELDS})',
            orderBy='folder, name',
            q=q
        )
        page_token = ''
        children = []
        while page_token is not None:
            results = service.files().list(
                pageToken=page_token or None,
                **params
            ).execute()
            children += results['files']
            page_token = results.get('nextPageToken', None)
        for f in children:
            metadata = [f['name'], f['id'],
                        timestamp_parser.parse(f['createdTime']),
                        timestamp_parser.parse(f['modifiedTime']),
                        f['lastModifyingUser']['displayName'],
                        f['webViewLink'],
                        f['parents'][0] if len(f['parents']) >= 1 else None,
                        ]
            if f['mimeType'] == 'application/vnd.google-apps.folder':
                node = LakeDirectory(*metadata)
                node._fill_children(service, gd_config)
            else:
                file_metadata = [
                    f.get('mimeType', None),
                    f.get('size', None),
                    f.get('md5Checksum', None),
                ]
                node = LakeRegularFile(*metadata, *file_metadata)
            self.children.append(node)

    @classmethod
    def get_tree(cls,
                 service: Resource, gd_config: Dict[str, str]
                 ) -> 'LakeDirectory':
        this_id = gd_config.get('SubTreeRootId', None) or \
                  gd_config.get('SharedDriveId', None)
        shared_drive_id = gd_config.get('SharedDriveId', None)
        if this_id:
            f = service.files().get(
                fileId=this_id,
                supportsAllDrives=bool(shared_drive_id),
                fields=FILE_FIELDS,
            ).execute()
            metadata = [f['name'], f['id'],
                        timestamp_parser.parse(f['createdTime']),
                        timestamp_parser.parse(f['modifiedTime']),
                        f['lastModifyingUser']['displayName'],
                        f['webViewLink'],
                        f['parents'][0] if len(
                            f.get('parents', [])) >= 1 else None
                        ]
        else:
            metadata = ['', None, None, None, None, None, None]
        root = LakeDirectory(*metadata, is_root=True) # type: ignore
        root._fill_children(service, gd_config)
        return root


@dataclass
class LakeRegularFile(LakeFile):
    mime_type: str
    size: int
    md5Checksum: int


def read_settings(fn: str = 'blab-dataimporter-googledrive-settings.cfg') \
                                                -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.optionxform = str  # type: ignore  # do not convert to lower-case
    config.read(fn)
    return config


def get_service(gd_config: Dict[str, str]) -> Resource:
    scopes = ['https://www.googleapis.com/auth/drive']
    credentials = service_account.Credentials.from_service_account_file(
        gd_config['ServiceAccountKeyFileName'], scopes=scopes)
    return build('drive', 'v3', credentials=credentials, cache_discovery=False)


def print_tree(f: LakeFile, pfx: Optional[List[bool]] = None) -> None:
    if pfx is None:
        pfx = []
    for i, p in enumerate(pfx[:-1]):
        print(' ┃ ' if p else '   ', end=' ')
    if pfx:
        print(' ┠─' if pfx[-1] else ' ┖─', end=' ')
    print(f.name)
    if isinstance(f, LakeDirectory):
        for child in f.children[:-1]:
            print_tree(child, pfx + [True])
        if f.children:
            print_tree(f.children[-1], pfx + [False])


config = read_settings()

db = MetadataCacheDatabase(dict(config['Database']))


# gd_config = dict(config['GoogleDrive'])
# logging.info('Fetching files')
# service = get_service(gd_config)
# tree = LakeDirectory.get_tree(service, gd_config)
# print_tree(tree)
