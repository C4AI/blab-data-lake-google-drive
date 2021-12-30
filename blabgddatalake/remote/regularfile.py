"""Contains a class that represents a regular file stored on Google Drive."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import md5
from pathlib import Path
from structlog import getLogger
from typing import Any

from dateutil import parser as timestamp_parser
from googleapiclient.http import MediaIoBaseDownload

import blabgddatalake.remote.gd as gd
import blabgddatalake.remote.directory as directory
from blabgddatalake.remote.file import RemoteFile

_logger = getLogger(__name__)


@dataclass
class RemoteRegularFile(RemoteFile):
    """Represents a regular file stored on Google Drive.

    Does not include Google Workspace files.
    """

    size: int
    """File size in bytes"""

    md5_checksum: str
    """File hash"""

    head_revision_id: str
    """Current version id (generated by Google Drive)"""

    can_download: bool
    """Whether the file can be downloaded"""

    @property
    def local_name(self) -> str:
        """Local file name (without path).

        Returns:
            Local file name
        """
        return (self.id +
                '_' + (self.head_revision_id or '') +
                '_' + (self.md5_checksum or ''))

    @classmethod
    def _md5(cls, fn: str) -> str:
        md5_hash = md5()
        with open(fn, 'rb') as fd:
            for chunk_4k in iter(lambda: fd.read(4096), b''):
                md5_hash.update(chunk_4k)
        return md5_hash.hexdigest()

    def download(self, gdservice: gd.GoogleDriveService, file_name: str,
                 skip_if_size_matches: bool = True,
                 also_check_md5: bool = False) -> bool | None:
        """Download the file.

        Args:
            gdservice: Google Drive service
            file_name: local file name to store the contents
            skip_if_size_matches: do not download if file already exists
                and its size matches the expected value
            also_check_md5: in addition to the size, also check file hash
                and only skip the download if it matches

        Returns:
            ``True`` if the download completed successfully,
            ``False`` if some error occurred and
            ``None`` if download was skipped because the file already existed
        """
        service = gdservice.service
        log = _logger.bind(id=self.id, name=self.name, local_name=file_name)

        if skip_if_size_matches:
            p = Path(file_name)
            if p.is_file() and p.stat().st_size == self.size:
                if not also_check_md5:
                    log.info('skipping download, size matches', size=self.size)
                    return None
                if self._md5(file_name) == self.md5_checksum:
                    log.info('skipping download, size and hash match',
                             size=self.size, md5_checksum=self.md5_checksum)
                    return None
        log.info('downloading file')
        with open(file_name, 'wb') as fd:
            request = service.files().get_media(
                fileId=self.id,
            )
            downloader = MediaIoBaseDownload(fd, request)
            completed = False
            while not completed:
                status, completed = downloader.next_chunk(
                    num_retries=gdservice.num_retries)
        return True

    @classmethod
    def from_dict(cls, metadata: dict[str, Any],
                  parent: directory.RemoteDirectory | None = None
                  ) -> RemoteRegularFile:
        """Create an instance from a dictionary with data from Google Drive.

        Documentation is available
        `here <https://developers.google.com/drive/api/v3/reference/files>`_.

        Args:
            metadata: a dictionary with file metadata
            parent: the parent directory, if this is not the root

        Returns:
            an instance with the metadata obtained from ``f``

        """
        return RemoteRegularFile(  # type:ignore[call-arg]
            metadata['name'], metadata['id'], metadata['mimeType'],
            timestamp_parser.parse(metadata['createdTime']),  # type:ignore
            timestamp_parser.parse(metadata['modifiedTime']),
            metadata['lastModifyingUser']['displayName'],
            metadata['webViewLink'], metadata['iconLink'], parent,
            int(s) if (s := metadata.get('size', None)) is not None else 0,
            metadata.get('md5Checksum', None),
            metadata.get('headRevisionId', None),
            metadata.get('capabilities', {}).get('canDownload', False),
        )
