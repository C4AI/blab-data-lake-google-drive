"""Wraps Google Drive API client."""

from __future__ import annotations

from functools import lru_cache

from google.oauth2 import service_account
from googleapiclient.discovery import Resource, build
from httplib2 import Http
from structlog import getLogger

from blabgddatalake.config import GoogleDriveConfig
from blabgddatalake.formats import ExportFormat
import blabgddatalake.remote.file as remotef
import blabgddatalake.remote.regularfile as remoterf
import blabgddatalake.remote.gwfile as remotegwf


_logger = getLogger(__name__)

_FILE_FIELDS = ', '.join(['id', 'name', 'parents', 'kind', 'mimeType',
                          'webViewLink', 'md5Checksum', 'size', 'createdTime',
                          'modifiedTime', 'lastModifyingUser',
                          'headRevisionId', 'iconLink', 'capabilities'
                          ])


class GoogleDriveService:
    """A class that wraps Google Drive API consumer to get the directory tree.

    This class provides a method that obtains the directory tree
    from a Google Drive directory or shared drive.
    """

    def __init__(self, gd_config: GoogleDriveConfig,
                 _http: Http | None = None,
                 _service: Resource | None = None):
        """
        Args:
            gd_config: service configuration
            _service: an optional existing :class:`Resource` instance to reuse
                (usually should be `None` except for testing purposes)
            _http: used to make HTTP requests (usually should be `None`
                except for testing purposes)

        For a description of the expected keys and values of `gd_config`,
        see the section ``GoogleDrive`` in
        :download:`the documentation <../README_CONFIG.md>`.

        In most cases, `_service` should be omitted and the attribute
        :attr:`service` will be set to a fresh instance created by the
        constructor.
        """  # noqa:D205,D400
        self.gd_config = gd_config
        """Configuration parameters"""

        self.service: Resource = _service or self.__get_service()
        """Google Drive service."""

    def __get_service(self, _http: Http | None = None) -> Resource:
        scopes = ['https://www.googleapis.com/auth/drive']
        cred = service_account.Credentials.from_service_account_file(
            self.gd_config.service_account_key_file_name, scopes=scopes)
        s = build('drive', 'v3', credentials=cred, cache_discovery=False)
        return s

    def _fill_children(self, rd: remotef.RemoteDirectory) -> None:
        service = self.service
        gd_config = self.gd_config
        q_items = ['not trashed']
        if rd.id:
            q_items.append(f"'{rd.id}' in parents")
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
        log = _logger.bind(id=rd.id)
        while page_token is not None or page == 0:
            request = service.files().list(
                pageToken=page_token,
                **params
            )
            log.debug('requesting directory', page=page)
            results = request.execute(num_retries=self.num_retries)
            children += results['files']
            page_token = results.get('nextPageToken', None)
            page += 1
        for f in children:
            node: remotef.RemoteFile
            if f['mimeType'] == 'application/vnd.google-apps.folder':
                subdir = remotef.RemoteDirectory.from_dict(f, rd)
                self._fill_children(subdir)
                node = subdir
            elif f['mimeType'].startswith('application/vnd.google-apps.'):
                rgwf = remotegwf.RemoteGoogleWorkspaceFile.from_dict(f, rd)
                rgwf.export_extensions = list(map(
                    lambda fmt: str(fmt.extension),
                    self.export_formats().get(f['mimeType'], [])))
                node = rgwf
            else:
                rrf = remoterf.RemoteRegularFile.from_dict(f, rd)
                node = rrf
            rd.children.append(node)

    def get_tree(self) -> remotef.RemoteDirectory:
        """Fetch and return the directory tree from Google Drive.

        There is no depth limit.

        Raises:
            ValueError: \
                if sub-tree root id and shared drive id are
                both undefined

        Returns:
            an object representing the root directory defined
            by the ``SubTreeRootId`` field of `gd_config`.
        """
        gd_config = self.gd_config
        service = self.service
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
        f = request.execute(num_retries=self.num_retries)
        root = remotef.RemoteDirectory.from_dict(f)
        root.is_root = True
        self._fill_children(root)
        return root

    @lru_cache(maxsize=1)
    def export_formats(self) -> dict[str, list[ExportFormat]]:
        """Get the supported formats to export Google Workspace files.

        Returns:
            A dictionary mapping Google Workspace file MIME types to
            the list of formats they can be exported to
        """
        request = self.service.about().get(fields='exportFormats')
        result = request.execute(num_retries=self.num_retries)
        return {k: list(map(
            lambda mt: ExportFormat.from_mime_type(mt), v))
            for k, v in result['exportFormats'].items()}

    def download_file(self, file: remoterf.RemoteRegularFile,
                      output_file: str,
                      skip_if_size_matches: bool = True,
                      also_check_md5: bool = False) -> bool | None:
        """Download a file from Google Drive.

        This method does not apply to Google Workspace files.

        Args:
            file: the file to download
            output_file: local file where the contents will be saved
            skip_if_size_matches: do not download if file already exists
                and its size matches the expected value
            also_check_md5: in addition to the size, also check file hash
                and only skip the download if it matches

        Returns:
            ``True`` if the download completed successfully,
            ``False`` if some error occurred and
            ``None`` if download was skipped because the file already existed
        """
        return file.download(self, output_file,
                             skip_if_size_matches, also_check_md5)

    @property
    def num_retries(self) -> int:
        """Return the number of times to retry the requests when they fail.

        See argument `num_retries` on
        `Google API Client Library documentation`_.

        .. _Google API Client Library documentation: https://googleapis.\
            github.io/google\
            -api-python-client/docs/epy/googleapiclient.http.\
            HttpRequest-class.html#execute

        Returns:
            maximum number of retries
        """
        return self.gd_config.retries

    def export_file(self, file: remotegwf.RemoteGoogleWorkspaceFile,
                    formats: list[ExportFormat],
                    output_file_without_extension: str) -> bool | None:
        """Download a file exported from Google Drive.

        This method only applies to Google Workspace files.

        Args:
            file: the file to download
            formats: list of formats
            output_file_without_extension: local file where the contents
                will be saved

        Returns:
            ``True`` if the download completed successfully,
            ``False`` if some error occurred and
            ``None`` if download was skipped because the file already existed
        """
        return file.export(self, formats, output_file_without_extension)
