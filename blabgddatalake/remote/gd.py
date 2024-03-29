"""Wraps Google Drive API client."""

from __future__ import annotations

from hashlib import md5
from pathlib import Path
from typing import Any, Sequence, cast

from google.auth.exceptions import GoogleAuthError as GAError
from google.oauth2 import service_account
from googleapiclient.discovery import Resource, build
from googleapiclient.errors import Error as GDError
from googleapiclient.http import HttpRequest, MediaIoBaseDownload
from httplib2 import Http
from httplib2.error import HttpLib2Error
from structlog import getLogger

import blabgddatalake.remote.file as remotef
import blabgddatalake.remote.gwfile as remotegwf
import blabgddatalake.remote.regularfile as remoterf
from blabgddatalake.config import GoogleDriveConfig
from blabgddatalake.formats import ExportFormat

_logger = getLogger(__name__)


class GoogleDriveService:
    """A class that wraps Google Drive API consumer to get the directory tree.

    This class provides a method that obtains the directory tree
    from a Google Drive directory or shared drive.
    """

    FILE_FIELDS = ', '.join([
        'id', 'name', 'parents', 'kind', 'mimeType', 'webViewLink',
        'md5Checksum', 'size', 'createdTime', 'modifiedTime',
        'lastModifyingUser', 'headRevisionId', 'iconLink',
        'capabilities/canDownload', 'exportLinks'
    ])

    def __init__(self,
                 gd_config: GoogleDriveConfig,
                 _http: Http | None = None,
                 _service: Resource | None = None):
        """
        Args:
            gd_config: service configuration
            _http: used to make HTTP requests (usually should be `None`
                except for testing purposes)
            _service: an optional existing :class:`Resource` instance to reuse
                (usually should be `None` except for testing purposes)

        In most cases, `_service` should be omitted and the attribute
        :attr:`service` will be set to a fresh instance created by the
        constructor.
        """  # noqa:D205,D400
        self.gd_config: GoogleDriveConfig = gd_config
        """Configuration parameters"""

        self.service: Resource = _service or self.__get_service(_http)
        """Google Drive service."""

    def __get_service(self, _http: Http | None = None) -> Resource:
        scopes = ['https://www.googleapis.com/auth/drive']
        cred = service_account.Credentials.from_service_account_file(
            self.gd_config.service_account_key_file_name, scopes=scopes)
        s = build('drive', 'v3', credentials=cred, cache_discovery=False)
        return s

    def _fetch_children(
            self, rd: remotef.RemoteDirectory) -> list[dict[str, Any]] | None:
        q_items = ['not trashed']
        if rd.id:
            q_items.append(f"'{rd.id}' in parents")
        q = ' and '.join(q_items)
        shared_drive_id = self.gd_config.shared_drive_id
        params = dict(supportsAllDrives=bool(shared_drive_id),
                      includeItemsFromAllDrives=bool(shared_drive_id),
                      driveId=shared_drive_id or None,
                      corpora='drive' if shared_drive_id else 'user',
                      pageSize=int(self.gd_config.page_size),
                      fields=f'nextPageToken, files({self.FILE_FIELDS})',
                      orderBy='folder, name',
                      q=q)
        page_token = None
        children = []
        page = 0
        log = _logger.bind(id=rd.id)
        while page_token is not None or page == 0:
            request = self.service.files().list(pageToken=page_token, **params)
            log.debug('requesting directory', page=page)
            try:
                results = request.execute(num_retries=self.num_retries)
            except (GDError, GAError, HttpLib2Error, TimeoutError):
                log.exception('could not fetch children')
                return None
            children += results['files']
            page_token = results.get('nextPageToken', None)
            page += 1
        return children

    def _fetch_file_metadata(self, file_id: str) -> dict[str, Any] | None:
        request = self.service.files().get(
            fileId=file_id,
            supportsAllDrives=bool(self.gd_config.shared_drive_id),
            fields=self.FILE_FIELDS,
        )
        try:
            return cast(dict[str, Any],
                        request.execute(num_retries=self.num_retries))
        except (GDError, GAError, HttpLib2Error, TimeoutError):
            _logger.exception('cannot fetch file metadata', file_id=file_id)
            return None

    def _dl_media(self, request: HttpRequest, output_file: str) -> bool:
        """Use MediaIoBaseDownload to download a file.

        Args:
            request: HTTP request
            output_file: full path of the local output file

        Returns:
            whether the download completed successfully
        """
        log = _logger.bind(output_file=output_file)
        with open(output_file, 'wb') as fd:
            downloader = MediaIoBaseDownload(fd, request)
            completed = False
            while not completed:
                try:
                    status, completed = downloader.next_chunk(
                        num_retries=self.num_retries)
                except (GDError, GAError, HttpLib2Error, TimeoutError):
                    log.exception('failed to download file')
                    return False
                log = _logger.bind(output_file=output_file,
                                   progress=status.progress())
                log.debug('download progress')

        return True

    def fetch_regular_file_contents(self, file_id: str,
                                    output_file: str) -> bool:
        """Download a regular file from Google Drive.

        This method does not apply to Google Workspace files.

        Note:
            This method is used internally by  :func:`download_file`.

        Args:
            file_id: id of the file to download
            output_file: full path of the local output file

        Returns:
             whether the download completed successfully
        """
        request = self.service.files().get_media(fileId=file_id)
        return self._dl_media(request, output_file)

    def fetch_exported_gw_file_contents(self, file_id: str, output_file: str,
                                        mime_type: str) -> bool:
        """Download an exported Google Workspace file from Google Drive.

        Note:
            This method is used internally by :func:`export_file`.

        Args:
            file_id: id of the file to download
            output_file: full path of the local output file
            mime_type: MIME type

        Returns:
             whether the download completed successfully
        """
        request = self.service.files().export(
            fileId=file_id,
            mimeType=mime_type,
        )
        return self._dl_media(request, output_file)

    def _fill_children(self, rd: remotef.RemoteDirectory) -> bool:
        children = self._fetch_children(rd)
        if children is None:
            return False
        for f in children:
            node: remotef.RemoteFile
            if f['mimeType'] == 'application/vnd.google-apps.folder':
                subdir = remotef.RemoteDirectory.from_dict(f, rd)
                if not self._fill_children(subdir):
                    return False
                node = subdir
            elif f['mimeType'].startswith('application/vnd.google-apps.'):
                rgwf = remotegwf.RemoteGoogleWorkspaceFile.from_dict(f, rd)
                node = rgwf
            else:
                rrf = remoterf.RemoteRegularFile.from_dict(f, rd)
                node = rrf
            rd.children.append(node)
        return True

    def get_tree(self) -> remotef.RemoteDirectory | None:
        """Fetch and return the directory tree from Google Drive.

        There is no depth limit.

        Raises:
            ValueError: \
                if sub-tree root id and shared drive id are
                both undefined

        Returns:
            an object representing the root directory defined
            by the ``SubTreeRootId`` field of `gd_config`,
            or ``None`` if some error occurred
        """
        this_id = (self.gd_config.sub_tree_root_id
                   or self.gd_config.shared_drive_id)
        if not this_id:
            raise ValueError('root id cannot be empty or None')
        _logger.debug('requesting root directory', id=this_id)
        root_data = self._fetch_file_metadata(this_id)
        if root_data is None:
            return None
        root = remotef.RemoteDirectory.from_dict(root_data)
        root.is_root = True
        if not self._fill_children(root):
            return None
        return root

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

    @classmethod
    def _md5(cls, fn: str) -> str:
        md5_hash = md5()
        with open(fn, 'rb') as fd:
            for chunk_4k in iter(lambda: fd.read(4096), b''):
                md5_hash.update(chunk_4k)
        return md5_hash.hexdigest()

    def download_file(self,
                      rf: remoterf.RemoteRegularFile,
                      output_file: str,
                      skip_if_size_matches: bool = True,
                      also_check_md5: bool = False) -> bool | None:
        """Download a file from Google Drive.

        This method does not apply to Google Workspace files.

        Args:
            rf: the file to download
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
        log = _logger.bind(id=rf.id, name=rf.name, local_name=output_file)
        if skip_if_size_matches:
            p = Path(output_file)
            if p.is_file() and p.stat().st_size == rf.size:
                if not also_check_md5:
                    log.info('skipping download, size matches', size=rf.size)
                    return None
                if self._md5(output_file) == rf.md5_checksum:
                    log.info('skipping download, size and hash match',
                             size=rf.size,
                             md5_checksum=rf.md5_checksum)
                    return None
        log.info('downloading file')
        return self.fetch_regular_file_contents(rf.id, output_file)

    def export_file(self, rf: remotegwf.RemoteGoogleWorkspaceFile,
                    formats: list[ExportFormat],
                    output_file_without_ext: str) -> bool:
        """Download a file exported from Google Drive.

        This method only applies to Google Workspace files.

        Args:
            rf: the file to download
            formats: list of formats
            output_file_without_ext: path of the local file where the contents
                will be saved

        Returns:
            ``True`` if the download completed successfully,
            ``False`` if some error occurred
        """
        log = _logger.bind(id=rf.id,
                           name=rf.name,
                           local_name_without_ext=output_file_without_ext)
        log.info('downloading exported file')
        for fmt in formats:
            ok = self.fetch_exported_gw_file_contents(
                rf.id, output_file_without_ext + '.' + fmt.extension,
                fmt.mime_type)
            if not ok:
                return False
        return True


__all__: Sequence[str] = [c.__name__ for c in [
    GoogleDriveService,
]]
