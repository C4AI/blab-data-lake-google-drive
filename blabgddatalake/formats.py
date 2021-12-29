"""Contains information about file formats exported from Google Workspace."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import re

_ms_office_pfx = 'application/vnd.openxmlformats-officedocument.'

gw_mime_type_to_extension: dict[str, str] = {
    'application/epub+zip': 'epub',
    'application/pdf': 'pdf',
    'application/rtf': 'rtf',
    'application/vnd.google-apps.script+json': 'json',
    'application/vnd.oasis.opendocument.presentation': 'odp',
    'application/vnd.oasis.opendocument.spreadsheet': 'ods',
    'application/vnd.oasis.opendocument.text': 'odt',
    _ms_office_pfx + 'presentationml.presentation': 'pptx',
    _ms_office_pfx + 'spreadsheetml.sheet': 'xlsx',
    _ms_office_pfx + 'wordprocessingml.document': 'docx',
    'application/x-vnd.oasis.opendocument.spreadsheet': 'ots',
    'application/zip': 'html.zip',
    'image/jpeg': 'jpg',
    'image/png': 'png',
    'image/svg+xml': 'svg',
    'text/csv': 'csv',
    'text/html': 'html',
    'text/plain': 'txt',
    'text/tab-separated-values': 'tsv',
}
"""
Maps MIME types to their extensions.

Note: ``application/zip`` is only available for Google Docs and Google Sheets,
and in both cases the exported ZIP file is a collection of HTML files (see
the complete list on the `official documentation`_);
therefore, the double extension ``html.zip`` is appropriate.

.. _official documentation: <https://developers.google.com/drive/api/v3/\
ref-export-formats>
"""

gw_extension_to_mime_type = {v: k
                             for k, v in gw_mime_type_to_extension.items()}
"""
Maps extensions to their MIME types.
"""


@dataclass(frozen=True)
class ExportFormat:
    """Represents a file format MIME Type and its extension."""

    mime_type: str
    """MIME Type"""

    extension: str
    """Extension without dot"""

    known: bool
    """Whether this extension is known

    If ```False```, then either the MIME type or the extension was
    automatically guessed and may be incorrect
    """

    def __lt__(self, other: Any) -> bool:
        if not isinstance(other, ExportFormat):
            return False
        return self.extension < other.extension

    @classmethod
    def from_extension(cls, extension: str) -> ExportFormat:
        """Get an instance from an extension.

        Args:
            extension: extension without dot

        Returns:
            a file format that has the specified extension
        """
        extension = extension.lstrip('.')
        try:
            mime_type = gw_extension_to_mime_type[extension]
        except KeyError:
            mime_type = 'application/octet-stream'  # generic binary data
            known = False
        else:
            known = True
        return ExportFormat(mime_type, extension, known)

    @classmethod
    def from_mime_type(cls, mime_type: str) -> ExportFormat:
        """Get an instance from a MIME type.

        Args:
            mime_type: MIME type

        Returns:
            a file format that has the specified MIME type
        """
        try:
            extension = gw_mime_type_to_extension[mime_type]
        except KeyError:
            if (search := re.search('[a-z]+$', mime_type)):
                extension = search.group(0)
            else:
                extension = 'bin'  # generic binary data
            known = False
        else:
            known = True
        return ExportFormat(mime_type, extension, known)
