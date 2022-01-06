"""A module that provides methods to sync files from Google Drive."""

from datetime import datetime, timedelta
from functools import cached_property
from os import remove as os_delete_file
from pathlib import Path
from typing import Any, cast

from sqlalchemy.orm import Session
from structlog import getLogger

from .config import Config
from .formats import ExportFormat
from .local.file import LocalDirectory, LocalFile
from .local.gwfile import LocalExportedGWFileVersion, LocalGoogleWorkspaceFile
from .local.localdb import LocalStorageDatabase
from .local.regularfile import LocalFileRevision, LocalRegularFile
from .remote.file import RemoteDirectory, RemoteFile
from .remote.gd import GoogleDriveService as GDService
from .remote.gwfile import RemoteGoogleWorkspaceFile
from .remote.regularfile import RemoteRegularFile

_logger = getLogger(__name__)


class GoogleDriveSync:
    """A class that provides useful methods to sync files from Google Drive."""

    def __init__(self, config: Config, _gdservice: GDService | None = None):
        """
        Args:
            config: configuration parameters
            _gdservice: a :class:`GoogleDriveService`
                instance to use (if omitted,
                a new instance is created based on the configuration
                parameters)
        """  # noqa:D205,D400
        self.config = config
        self.db = LocalStorageDatabase(config.database)
        self.gdservice = _gdservice or GDService(config.google_drive)

    @property
    def _deletion_delay(self) -> int | None:
        return self.config.local.deletion_delay

    @property
    def _root_path(self) -> Path:
        return Path(self.config.local.root_path).resolve()

    def cleanup(self, delay: float | None = None) -> int:
        """Delete local files marked for deletion before a given instant.

        Args:
            delay: only delete files that were marked for deletion at least
                this number of seconds ago (optional, overrides
                ``config['Local']['DeletionDelay']``).

        Returns:
            0 if no errors occurred, 1 otherwise
        """
        until = datetime.now()
        if delay is not None:
            until -= timedelta(seconds=delay)
        elif (d := self._deletion_delay) is not None:
            until -= timedelta(seconds=float(d))
        _logger.debug('will delete files marked for deletion', until=until)
        to_delete: list[tuple[Path, datetime]] = []
        with self.db.new_session() as session:
            for ftd in self.db.get_obsolete_file_revisions(session, until):
                name = self._root_path / ftd.local_name
                to_delete.append((name, ftd.obsolete_since))
                session.delete(ftd)
            for gwftd in self.db.get_obsolete_gw_file_versions(session, until):
                for n in gwftd.local_names.values():
                    name = self._root_path / n
                    to_delete.append((name, gwftd.obsolete_since))
                session.delete(gwftd)
            session.flush()
            for f in self.db.get_obsolete_files(session, until):
                session.delete(f)
            for name, obsolete_since in to_delete:
                log = _logger.bind(name=name,
                                   marked_for_deletion_at=obsolete_since)
                try:
                    os_delete_file(name)
                except FileNotFoundError:
                    log.warn('not deleting file because it no longer exists')
                except Exception:
                    log.warn('could not delete file')
                else:
                    log.info('file deleted')
            session.commit()
        return 0

    @staticmethod
    def _make_fields_equal(old: Any, new: Any, fields: list[str]) -> int:
        changes = 0
        for field in fields:
            new_value = getattr(new, field, None)
            if getattr(old, field, None) != new_value:
                setattr(old, field, new_value)
                changes += 1
        return changes

    @staticmethod
    def _getattrs(obj: Any, attrs: list[str]) -> dict[str, Any]:
        return {k: getattr(obj, k, None) for k in attrs}

    def _local_file_from_remote_file(self, rf: RemoteFile) -> LocalFile:
        fields = [
            'id', 'name', 'mime_type', 'created_time', 'modified_time',
            'modified_by', 'web_url', 'icon_url', 'parent_id'
        ]
        if isinstance(rf, RemoteDirectory):
            return LocalDirectory(**self._getattrs(rf, fields + ['is_root']))
        elif isinstance(rf, RemoteRegularFile):
            return LocalRegularFile(
                **self._getattrs(rf, fields + ['head_revision_id']))
        elif isinstance(rf, RemoteGoogleWorkspaceFile):
            return LocalGoogleWorkspaceFile(**self._getattrs(rf, fields))
        raise RuntimeError  # should not happen

    def _revision_from_remote_file(self, rf: RemoteRegularFile) \
            -> LocalFileRevision:
        fields = [
            'name', 'mime_type', 'can_download', 'size', 'modified_time',
            'modified_by', 'md5_checksum'
        ]
        return LocalFileRevision(file_id=rf.id,
                                 revision_id=rf.head_revision_id,
                                 **self._getattrs(rf, fields))

    @classmethod
    def _gwversion_from_remote_file(
            cls, rf: RemoteGoogleWorkspaceFile) -> LocalExportedGWFileVersion:
        fields = [
            'modified_time', 'modified_by', 'mime_type', 'name', 'can_export'
        ]
        extensions = list(map(lambda f: f.extension, rf.export_formats))
        return LocalExportedGWFileVersion(file_id=rf.id,
                                          extensions=extensions,
                                          **cls._getattrs(rf, fields))

    @cached_property
    def _supported_export_formats(self) -> dict[str, list[ExportFormat]]:
        return self.gdservice.export_formats()

    @cached_property
    def _chosen_export_formats(self) -> dict[str, list[ExportFormat]]:
        formats = self.config.google_drive.google_workspace_export_formats
        return {('application/vnd.google-apps.' + t): fmt
                for t, fmt in formats.items()}

    @cached_property
    def _unsupported_export_formats(self) -> dict[str, set[ExportFormat]]:
        return {
            t: set(chosen) - set(self._supported_export_formats.get(t, []))
            for t, chosen in self._chosen_export_formats.items()
        }

    @cached_property
    def _export_formats(self) -> dict[str, list[ExportFormat]]:
        return {  # formats that are supported AND chosen
            t: sorted(
                set(available) & set(self._chosen_export_formats.get(t, [])))
            for t, available in self._supported_export_formats.items()
        }

    def _contents_changed(self, rf: RemoteFile, lf: LocalFile) -> bool:
        if isinstance(lf, LocalRegularFile):
            rrf = cast(RemoteRegularFile, rf)
            return (rrf.head_revision_id != lf.head_revision_id
                    or rrf.can_download != lf.can_download)
        elif isinstance(lf, LocalGoogleWorkspaceFile):
            rgwf = cast(RemoteGoogleWorkspaceFile, rf)
            return (rf.modified_time > lf.modified_time
                    or rgwf.can_export != lf.can_export or list(
                        map(lambda fmt: fmt.extension,
                            self._export_formats.get(rf.mime_type, []))) !=
                    lf.head_version.extensions)
        else:
            return False

    def _download(self, f: RemoteRegularFile) -> bool | None:
        fn = self._root_path / f.local_name
        return self.gdservice.download_file(f, str(fn))

    def _export(self, f: RemoteGoogleWorkspaceFile,
                formats: list[ExportFormat]) -> bool | None:
        fn = self._root_path / f.local_name
        return self.gdservice.export_file(f, formats, str(fn))

    def _update(self, rf: RemoteFile, lf: LocalFile) -> bool:
        fields = [
            'id', 'name', 'mime_type', 'created_time', 'modified_time',
            'modified_by', 'web_url', 'icon_url', 'parent_id'
        ]
        changed = False
        if isinstance(rf, RemoteDirectory) and \
                isinstance(lf, LocalDirectory):
            fields += ['is_root']
        elif isinstance(rf, RemoteGoogleWorkspaceFile):
            ver_fields = [
                'name', 'mime_type', 'modified_time', 'modified_by',
                'obsolete_since', 'can_export'
            ]
            ver = cast(LocalGoogleWorkspaceFile, lf).head_version
            if self._make_fields_equal(ver, rf, ver_fields) != 0:
                changed = True
            ext = list(
                map(lambda fmt: fmt.extension,
                    self._export_formats.get(rf.mime_type, [])))
            if ver.extensions != ext or []:
                ver.extensions = ext or []
                changed = True
        elif isinstance(rf, RemoteRegularFile):
            fields += ['head_revision_id']
            rev_fields = [
                'name', 'mime_type', 'can_download', 'size', 'modified_time',
                'modified_by', 'md5_checksum', 'obsolete_since'
            ]
            rev = cast(LocalRegularFile, lf).head_revision
            if self._make_fields_equal(rev, rf, rev_fields) != 0:
                changed = True
            if rev.revision_id != rf.head_revision_id:
                rev.revision_id = rf.head_revision_id
                changed = True
            if rev.file_id != rf.id:
                rev.file_id = rf.id
                changed = True
        if self._make_fields_equal(lf, rf, fields) != 0:
            changed = True
        return changed

    def _sync_new_file(self, session: Session,
                       rf: RemoteFile, lf: LocalFile | None = None) \
            -> LocalFile:
        if not lf:
            lf = self._local_file_from_remote_file(rf)
            session.add(lf)
        log = _logger.bind(id=rf.id, name=rf.name, mime_type=rf.mime_type)
        if isinstance(lf, LocalRegularFile):
            rrf = cast(RemoteRegularFile, rf)
            rev = lf.head_revision
            if rev and (rev.revision_id == rrf.head_revision_id):
                # This happens when can_download has changed, e.g. when
                # a file previously could not be downloaded but the
                # permission has changed
                pass
            else:
                session.add(self._revision_from_remote_file(rrf))
                lf.head_revision_id = rrf.head_revision_id
            session.flush()
            session.expire(lf, ['head_revision'])
            if lf.can_download:
                self._download(rrf)
            else:
                log.info('skipping non-downloadable file')
        elif isinstance(lf, LocalGoogleWorkspaceFile):
            rgwf = cast(RemoteGoogleWorkspaceFile, rf)
            ver = lf.head_version
            formats = self._export_formats.get(rf.mime_type, [])
            ext = list(map(lambda ef: ef.extension, formats))
            if ver and (ver.modified_time == rgwf.modified_time):
                # This happens when can_download has changed or when
                # the list of formats has changed
                pass
            else:
                lf.modified_time = rgwf.modified_time
                ver = self._gwversion_from_remote_file(rgwf)
                session.add(ver)
            session.flush()
            session.expire(lf, ['head_version'])
            if lf.can_export and ext:
                self._export(rgwf, formats)
            else:
                log.info('skipping non-exportable file')

        session.flush()
        self._update(rf, lf)
        return lf

    def sync(self) -> int:
        """Sync files from Google Drive.

        Returns:
            0 if no errors occurred, 1 otherwise
        """  # noqa: DAR401
        for ftype, missing in self._unsupported_export_formats.items():
            if missing:
                unsup = set(map(lambda fmt: fmt.extension, missing))
                _logger.warn('Unsupported export format(s) for type',
                             unsupported_formats=unsup,
                             type=ftype)

        with self.db.new_session() as session:

            local_tree = self.db.get_tree(session)
            local_file_by_id: dict[str, LocalFile] = \
                local_tree.flatten() if local_tree else {}

            remote_tree = self.gdservice.get_tree()
            remote_file_by_id = remote_tree.flatten()

            for fid, f in remote_file_by_id.items():
                log = _logger.bind(name=f.name, id=fid, mime_type=f.mime_type)
                if fid not in local_file_by_id:
                    lf = self.db.get_file_by_id(session, fid, True)
                    if lf is not None:
                        log.info('previously deleted file has been recovered')
                        local_file_by_id[fid] = lf
                        lf.obsolete_since = None  # type: ignore
                    else:
                        # file is new
                        self._sync_new_file(session, f)
                        continue
                # file already existed
                lf = local_file_by_id[fid]
                if self._contents_changed(f, lf):
                    # file contents have changed
                    log.info('file has changed')
                    if isinstance(lf, LocalRegularFile):
                        old_rev = lf.head_revision
                        if old_rev.revision_id != \
                                cast(RemoteRegularFile, f).head_revision_id:
                            old_rev.obsolete_since = datetime.now()
                            log.info('old file revision marked for deletion')
                    elif isinstance(lf, LocalGoogleWorkspaceFile):
                        old_ver = lf.head_version
                        if (old_ver.modified_time < cast(
                                RemoteGoogleWorkspaceFile, f).modified_time):
                            old_ver.obsolete_since = datetime.now()
                            log.info('old GW file version marked for deletion')
                    self._sync_new_file(session, f, lf)
                elif self._update(f, lf):
                    # only metadata has changed
                    log.info('file metadata changed, contents are unchanged')
                else:
                    # nothing has changed
                    log.debug('no changes in file')

            t = datetime.now()
            for fid in local_file_by_id.keys() - remote_file_by_id.keys():
                lf = local_file_by_id[fid]
                lf.obsolete_since = t
                if isinstance(lf, LocalRegularFile):
                    lf.head_revision.obsolete_since = t
                elif isinstance(lf, LocalGoogleWorkspaceFile):
                    lf.head_version.obsolete_since = t
                log = _logger.bind(name=lf.name, id=fid)
                log.info('file (deleted on server) marked for deletion')
            session.commit()
        return 0


def sync(config: Config) -> int:
    """Sync files from Google Drive.

    Args:
        config: configuration parameters.

    Returns:
        0 if no errors occurred, 1 otherwise
    """  # noqa: DAR401
    return GoogleDriveSync(config).sync()


def cleanup(config: Config, delay: float | None = None) -> int:
    """Delete local files that were marked for deletion before a given instant.

    Args:
        config: configuration parameters.
        delay: only delete files that were marked for deletion at least
            this number of seconds ago (optional, overrides
            ``config['Local']['DeletionDelay']``).

    Returns:
        0 if no errors occurred, 1 otherwise
    """
    return GoogleDriveSync(config).cleanup(delay)
