"""Contains a class that represents a Google Workspace file on Google Drive."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

from dateutil import parser as timestamp_parser
from structlog import getLogger

import blabgddatalake.remote.file as file
from blabgddatalake.formats import ExportFormat
from blabgddatalake.remote.file import RemoteFile

_logger = getLogger(__name__)


@dataclass
class RemoteGoogleWorkspaceFile(RemoteFile):
    """Represents a Google Workspace file stored on Google Drive."""

    can_export: bool
    """Whether the file can be exported"""

    export_formats: list[ExportFormat]
    """Formats to which the file can be exported"""

    @property
    def local_name(self) -> str:
        """Local file name (without path and extension).

        Returns:
            Local file name
        """
        return self.id + '_' + self.modified_time.strftime('%Y%m%d_%H%M%S%f')

    @classmethod
    def from_dict(
        cls,
        metadata: dict[str, Any],
        parent: file.RemoteDirectory | None = None
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
        formats = list(
            map(ExportFormat.from_mime_type,
                metadata.get('exportLinks', {}).keys()))
        return RemoteGoogleWorkspaceFile(
            metadata['name'],
            metadata['id'],
            metadata['mimeType'],
            timestamp_parser.parse(metadata['createdTime']),
            timestamp_parser.parse(metadata['modifiedTime']),
            metadata['lastModifyingUser']['displayName'],
            metadata['webViewLink'],
            metadata['iconLink'],
            parent,
            metadata.get('capabilities', {}).get('canDownload', False),
            formats,
        )


__all__: Sequence[str] = [c.__name__ for c in [
    RemoteGoogleWorkspaceFile,
]]
