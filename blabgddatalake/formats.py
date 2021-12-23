"""Contains information about file formats exported from Google Workspace."""

from dataclasses import dataclass


@dataclass
class ExportFormat:
    """Represents a file format MIME Type and its extension."""

    mime_type: str
    """MIME Type"""

    extension: str
    """Extension without dot"""


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
the complete list
`here <https://developers.google.com/drive/api/v3/ref-export-formats>`_);
therefore, the double extension ``html.zip`` is appropriate.
"""

gw_extension_to_mime_type = {v: k
                             for k, v in gw_mime_type_to_extension.items()}
"""
Maps extensions to their MIME types.
"""
