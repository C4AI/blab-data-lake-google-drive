"""A module that provides methods to sync files from Google Drive."""

from datetime import datetime, timedelta
from os import remove as os_delete_file
from pathlib import Path
from structlog import getLogger
from typing import Any, cast

from .remote import RemoteDirectory, RemoteRegularFile, \
    GoogleDriveService as GDService
from .local import LocalStorageDatabase, LocalFile, LocalDirectory, \
    LocalRegularFile, LocalGoogleWorkspaceFile, LocalFileRevision


_logger = getLogger(__name__)


def _db_and_gdservice(config: dict) -> tuple[LocalStorageDatabase, GDService]:
    db = LocalStorageDatabase(config['Database'])
    gdservice = GDService(config['GoogleDrive'])
    return db, gdservice


def cleanup(config: dict, delay: float | None = None) -> int:
    """Delete local files that were marked for deletion before a given instant.

    Args:
        config: configuration parameters (see
            :download:`the documentation <../README_CONFIG.md>`).
        delay: only delete files that were marked for deletion at least
            this number of seconds ago (optional, overrides
            ``config['Local']['DeletionDelay']``).

    Returns:
        0 if no errors occurred, 1 otherwise
    """
    until = datetime.now()
    if delay is not None:
        until -= timedelta(seconds=delay)
    elif (d := config['Local'].get('DeletionDelay', None)) is not None:
        until -= timedelta(seconds=float(d))
    _logger.debug('will delete files marked for deletion', until=until)
    db, gdservice = _db_and_gdservice(config)
    with db.new_session() as session:
        for ftd in db.get_obsolete_file_revisions(session, until):
            name = Path(config['Local']['RootPath']).resolve() / ftd.local_name
            log = _logger.bind(
                name=ftd.local_name,
                marked_for_deletion_at=ftd.obsolete_since)
            try:
                os_delete_file(name)
            except FileNotFoundError:
                log.warn('not deleting file because it no longer exists')
            except Exception:
                log.warn('could not delete file')
            else:
                log.info('file deleted')
                session.delete(ftd)
        session.commit()
    return 0


def sync(config: dict) -> int:
    """Sync files from Google Drive.

    Args:
        config: configuration parameters (see
            :download:`the documentation <../README_CONFIG.md>`).

    Returns:
        0 if no errors occurred, 1 otherwise
    """  # noqa: DAR401
    db, gdservice = _db_and_gdservice(config)

    def download(f: RemoteRegularFile) -> bool | None:
        directory = Path(config['Local']['RootPath'])
        fn = directory.resolve() / f.local_name
        return gdservice.download_file(f, str(fn))

    with db.new_session() as session:

        local_tree = db.get_tree(session)
        local_file_by_id: dict[str, LocalFile] = \
            local_tree.flatten() if local_tree else {}

        remote_tree = gdservice.get_tree()
        remote_file_by_id = remote_tree.flatten()

        for id, f in remote_file_by_id.items():
            remote_file_metadata: dict[str, Any] = dict(
                id=f.id,
                name=f.name,
                mime_type=f.mime_type,
                created_time=f.created_time,
                modified_time=f.modified_time,
                modified_by=f.modified_by,
                web_url=f.web_url,
                icon_url=f.icon_url,
                parent_id=p.id if (p := f.parent) else None,
            )
            remote_revision_metadata: dict[str, Any] = {}
            if isinstance(f, RemoteRegularFile):
                if not f.is_google_workspace_file:
                    remote_file_metadata.update(
                        head_revision_id=f.head_revision_id
                    )
                    remote_revision_metadata.update(
                        file_id=f.id,
                        name=f.name,
                        revision_id=f.head_revision_id,
                        modified_time=f.modified_time,
                        modified_by=f.modified_by,
                        mime_type=f.mime_type,
                        size=f.size,
                        md5_checksum=f.md5_checksum,
                        can_download=f.can_download,
                    )
            elif isinstance(f, RemoteDirectory):
                remote_file_metadata.update(is_root=f.is_root)

            if id not in local_file_by_id:
                # file is new
                if isinstance(f, RemoteRegularFile) and \
                        not f.is_google_workspace_file:
                    if f.can_download:
                        download(f)
                    else:
                        _logger.info('skipping non-downloadable file',
                                     id=f.id, name=f.name)
                new_file: LocalFile
                if isinstance(f, RemoteRegularFile):
                    if f.is_google_workspace_file:
                        new_file = LocalGoogleWorkspaceFile(
                            **remote_file_metadata)
                    else:
                        new_file = LocalRegularFile(**remote_file_metadata)
                        new_revision = LocalFileRevision(
                            **remote_revision_metadata
                        )
                        session.add(new_revision)
                elif isinstance(f, RemoteDirectory):
                    new_file = LocalDirectory(**remote_file_metadata)
                else:
                    raise RuntimeError
                session.add(new_file)
            else:
                lf = local_file_by_id[id]
                local_file_metadata: dict[str, Any] = dict(
                    id=lf.id,
                    name=lf.name,
                    mime_type=lf.mime_type,
                    created_time=lf.created_time,
                    modified_time=lf.modified_time,
                    modified_by=lf.modified_by,
                    web_url=lf.web_url,
                    icon_url=f.icon_url,
                    parent_id=par.id if (par := lf.parent) else None,
                )
                local_revision_metadata: dict[str, Any] = {}
                if isinstance(lf, LocalRegularFile):
                    local_file_metadata.update(
                        head_revision_id=lf.head_revision_id
                    )
                    local_revision_metadata.update(
                        md5_checksum=lf.md5_checksum,
                        name=lf.name,
                        size=lf.size,
                        revision_id=lf.head_revision_id,
                        modified_time=lf.modified_time,
                        modified_by=lf.modified_by,
                        mime_type=lf.mime_type,
                        file_id=lf.id,
                        can_download=cast(RemoteRegularFile, f).can_download,
                    )
                elif isinstance(lf, LocalGoogleWorkspaceFile):
                    pass
                elif isinstance(lf, LocalDirectory):
                    local_file_metadata.update(
                        is_root=lf.is_root,
                    )
                else:
                    raise RuntimeError
                log = _logger.bind(name=f.name, id=id)
                if local_file_metadata == remote_file_metadata and \
                        local_revision_metadata == remote_revision_metadata:
                    # file is unchanged
                    log.debug('no changes in file')
                else:
                    # file metadata has been changed
                    log.info('file metadata changed')
                    unique_cols = ('head_revision_id', )
                    if isinstance(f, RemoteRegularFile) and \
                            not f.is_google_workspace_file and \
                            isinstance(lf, LocalRegularFile) and \
                            (remote_file_metadata[k] for k in unique_cols) != \
                            (local_file_metadata[k] for k in unique_cols):
                        # file contents have been changed
                        if f.can_download:
                            download(f)
                        else:
                            _logger.info('skipping non-downloadable file',
                                         id=f.id, name=f.name)
                        session.add(LocalFileRevision(
                            **remote_revision_metadata))
                        lf.head_revision.obsolete_since = datetime.now()
                        log.info('old file marked for deletion')
                    for k, v in remote_file_metadata.items():
                        if (old := local_file_metadata.get(k, None)) != v:
                            log.info('file metadata changed',
                                     field=k, old_value=old, new_value=v)
                            setattr(lf, k, v)
                    if isinstance(lf, RemoteRegularFile):
                        for k, v in remote_revision_metadata.items():
                            if (old := local_revision_metadata.get(k, None)) \
                                    != v:
                                log.info('file revision metadata changed',
                                         field=k, old_value=old, new_value=v)
                                setattr(lf.head_revision, k, v)
        for fid in local_file_by_id.keys() - remote_file_by_id.keys():
            lf = local_file_by_id[fid]
            if isinstance(lf, LocalRegularFile):
                t = datetime.now()
                lf.head_revision.obsolete_since = t
                lf.obsolete_since = t
                log = _logger.bind(name=lf.name, id=fid)
                log.info('file (deleted on server) marked for deletion')
            session.delete(lf)
        session.commit()

    return 0
