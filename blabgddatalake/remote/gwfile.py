"""Contains a class that represents a Google Workspace file on Google Drive."""

from __future__ import annotations

from dataclasses import dataclass
from structlog import getLogger
from typing import Any

from dateutil import parser as timestamp_parser
from googleapiclient.http import MediaIoBaseDownload

from blabgddatalake.formats import ExportFormat
import blabgddatalake.remote.gd as gd
import blabgddatalake.remote.directory as directory
from blabgddatalake.remote.file import RemoteFile

_logger = getLogger(__name__)


@dataclass
class RemoteGoogleWorkspaceFile(RemoteFile):
    """Represents a Google Workspace file stored on Google Drive."""

    can_export: bool
    """Whether the file can be exported"""

    export_extensions: list[str] | None = None
    """Formats to which the file can be exported"""

    @property
    def local_name(self) -> str:
        """Local file name (without path and extension).

        Returns:
            Local file name
        """
        return (self.id + '_' +
                self.modified_time.strftime('%Y%m%d_%H%M%S%f'))

    def export(self, gdservice: gd.GoogleDriveService,
               formats: list[ExportFormat],
               file_name_without_extension: str) -> bool | None:
        """Download exported versions of the file.

        Args:
            gdservice: Google Drive service
            formats: list of formats
            file_name_without_extension: local file name without extension
                to store the contents

        Returns:
            ``True`` if the download completed successfully,
            ``False`` if some error occurred and
            ``None`` if download was skipped because the file already existed
        """
        service = gdservice.service
        log = _logger.bind(id=self.id, name=self.name,
                           local_name_without_ext=file_name_without_extension)

        log.info('downloading exported file')
        for fmt in formats:
            file_name = file_name_without_extension + '.' + fmt.extension
            with open(file_name, 'wb') as fd:
                request = service.files().export(
                    fileId=self.id,
                    mimeType=fmt.mime_type,
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
                  ) -> RemoteGoogleWorkspaceFile:
        """Create an instance from a dictionary with data from Google Drive.

        Documentation is available
        `here <https://developers.google.com/drive/api/v3/reference/files>`_.

        Args:
            metadata: a dictionary with file metadata
            parent: the parent directory, if this is not the root

        Returns:
            an instance with the metadata obtained from ``f``

        """
        return RemoteGoogleWorkspaceFile(  # type: ignore[call-arg]
            metadata['name'], metadata['id'], metadata['mimeType'],
            timestamp_parser.parse(metadata['createdTime']),
            timestamp_parser.parse(metadata['modifiedTime']),
            metadata['lastModifyingUser']['displayName'],
            metadata['webViewLink'], metadata['iconLink'], parent,
            metadata.get('capabilities', {}).get('canDownload', False),
        )
