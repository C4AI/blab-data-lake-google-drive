"""Contains a class that represents a directory stored on Google Drive."""
from __future__ import annotations

from dataclasses import dataclass, field
from structlog import getLogger
from typing import Any
from dateutil import parser as timestamp_parser
from overrides import overrides

from blabgddatalake.config import GoogleDriveConfig
from blabgddatalake.remote.file import RemoteFile
import blabgddatalake.remote.gd as gd
import blabgddatalake.remote.regularfile as regularfile

_logger = getLogger(__name__)

_FILE_FIELDS = ', '.join(['id', 'name', 'parents', 'kind', 'mimeType',
                          'webViewLink', 'md5Checksum', 'size', 'createdTime',
                          'modifiedTime', 'lastModifyingUser',
                          'headRevisionId', 'iconLink', 'capabilities'
                          ])


@dataclass
class RemoteDirectory(RemoteFile):
    """Represents a directory stored on Google Drive."""

    children: list[RemoteFile] = field(default_factory=list)
    """Subdirectories and regular files in this directory"""

    is_root: bool = False
    """Whether this directory is the root specified in the settings
        (not necessarily the root on Google Drive)"""

    @classmethod
    def from_dict(cls, metadata: dict[str, Any],
                  parent: RemoteDirectory | None = None) -> RemoteDirectory:
        """Create an instance from a dictionary with data from Google Drive.

        Documentation is available
        `here <https://developers.google.com/drive/api/v3/reference/files>`_.

        Args:
            metadata: a dictionary with file metadata
            parent: the parent directory, if this is not the root

        Returns:
            an instance with the metadata obtained from ``f``

        """
        return RemoteDirectory(  # type: ignore[call-arg]
            metadata['name'], metadata['id'], metadata['mimeType'],
            timestamp_parser.parse(metadata['createdTime']),
            timestamp_parser.parse(metadata['modifiedTime']),
            metadata['lastModifyingUser']['displayName'],
            metadata['webViewLink'], metadata['iconLink'], parent
        )

    def _fill_children(self, gdservice: gd.GoogleDriveService,
                       gd_config: GoogleDriveConfig) -> None:
        service = gdservice.service
        q_items = ['not trashed']
        if self.id:
            q_items.append(f"'{self.id}' in parents")
        q = ' and '.join(q_items)
        shared_drive_id = gd_config.shared_drive_id
        params = dict(
            supportsAllDrives=bool(shared_drive_id),
            includeItemsFromAllDrives=bool(shared_drive_id),
            driveId=shared_drive_id or None,
            corpora='drive' if shared_drive_id else 'user',
            pageSize=int(gd_config.page_size),
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
            results = request.execute(num_retries=gdservice.num_retries)
            children += results['files']
            page_token = results.get('nextPageToken', None)
            page += 1
        for f in children:
            node: RemoteFile
            if f['mimeType'] == 'application/vnd.google-apps.folder':
                rd = RemoteDirectory.from_dict(f, self)
                rd._fill_children(gdservice, gd_config)
                node = rd
            else:
                rrf = regularfile.RemoteRegularFile.from_dict(f)
                rrf.export_extensions = list(map(
                    lambda fmt: str(fmt.extension),
                    gdservice.export_formats().get(f['mimeType'], [])))
                node = rrf
            self.children.append(node)

    @classmethod
    def get_tree(cls, gdservice: gd.GoogleDriveService,
                 gd_config: GoogleDriveConfig) -> RemoteDirectory:
        """Fetch and return the directory tree from Google Drive.

        There is no depth limit.

        Args:
            gdservice: Google Drive service
            gd_config: configuration parameters

        Raises:
            ValueError: sub-tree root id and shared drive id are both
                undefined

        Returns:
            an object representing the root directory defined
            by the ``SubTreeRootId`` field of `gd_config`.
        """
        service = gdservice.service
        this_id = gd_config.sub_tree_root_id or gd_config.shared_drive_id
        shared_drive_id = gd_config.shared_drive_id
        if not this_id:
            raise ValueError('root id cannot be empty or None')
        request = service.files().get(
            fileId=this_id,
            supportsAllDrives=bool(shared_drive_id),
            fields=_FILE_FIELDS,
        )
        _logger.debug('requesting root directory', id=this_id)
        f = request.execute(num_retries=gdservice.num_retries)
        root = RemoteDirectory.from_dict(f)
        root.is_root = True
        root._fill_children(gdservice, gd_config)
        return root

    def flatten(self) -> dict[str, RemoteFile]:
        """Convert the tree to a flat dictionary.

        Returns:
            a flat dictionary where files are mapped by their ids
        """
        d: dict[str, RemoteFile] = {self.id: self}
        for c in self.children:
            d.update(c.flatten() if isinstance(c, RemoteDirectory)
                     else {c.id: c})
        return d

    @overrides
    def print_tree(self, _pfx: list[bool] | None = None) -> None:
        super().print_tree(_pfx)
        if _pfx is None:
            _pfx = []
        for child in self.children[:-1]:
            child.print_tree(_pfx + [True])
        if self.children:
            self.children[-1].print_tree(_pfx + [False])
