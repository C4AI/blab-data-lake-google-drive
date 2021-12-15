import structlog
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from .remote import Lake, RemoteDirectory, RemoteRegularFile
from .local import LocalStorageDatabase, LocalFile, FileToDelete


logger = structlog.getLogger(__name__)


def _db_and_lake(config: dict) -> tuple[LocalStorageDatabase, Lake]:
    db = LocalStorageDatabase(config['Database'])
    lake = Lake(config['GoogleDrive'])
    return db, lake


def cleanup(config: dict, delay: float | None = None) -> int:
    until = datetime.now()
    if delay is not None:
        until -= timedelta(seconds=delay)
    elif (d := config['Local'].get('DeletionDelay', None)) is not None:
        until -= timedelta(seconds=float(d))
    logger.debug('will delete files marked for deletion', until=until)
    db, lake = _db_and_lake(config)
    with db.new_session() as session:
        for ftd in db.get_files_to_delete(session, until):
            name = Path(config['Local']['RootPath']).resolve() / ftd.local_name
            log = logger.bind(
                name=ftd.local_name,
                marked_for_deletion_at=ftd.removedfromindexat)
            try:
                os.remove(name)
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

    db, lake = _db_and_lake(config)

    def download(f: RemoteRegularFile) -> None:
        directory = Path(config['Local']['RootPath'])
        fn = directory.resolve() / f.local_name
        lake.download_file(f, str(fn))

    with db.new_session() as session:

        local_tree = db.get_tree(session)
        local_file_by_id: dict[str, LocalFile] = local_tree.flatten() \
            if local_tree else {}

        remote_tree = lake.get_tree()
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
            if isinstance(f, RemoteRegularFile):
                remote_file_metadata.update(
                    md5_checksum=f.md5_checksum,
                    size=f.size,
                    head_revision_id=f.head_revision_id,
                )
            elif isinstance(f, RemoteDirectory):
                remote_file_metadata.update(is_root=f.is_root)

            if id not in local_file_by_id:
                # file is new
                if isinstance(f, RemoteRegularFile):
                    mt = f.mime_type
                    if not mt.startswith('application/vnd.google-apps'):
                        download(f)
                new_file = LocalFile(**remote_file_metadata)
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
                if not lf.is_directory:
                    local_file_metadata.update(
                        md5_checksum=lf.md5_checksum,
                        size=lf.size,
                        head_revision_id=lf.head_revision_id,
                    )
                else:
                    local_file_metadata.update(
                        is_root=lf.is_root,
                    )
                log = logger.bind(name=f.name, id=id)
                if local_file_metadata == remote_file_metadata:
                    # file is unchanged
                    log.debug('no changes in file')
                else:
                    # file has been changed
                    log.info('file metadata changed')
                    unique_cols = ('md5_checksum', 'head_revision_id')
                    mt = f.mime_type
                    if isinstance(f, RemoteRegularFile) and \
                            not f.is_google_workspace_file and \
                            (remote_file_metadata[k] for k in unique_cols) != \
                            (local_file_metadata[k] for k in unique_cols):
                        download(f)
                        to_delete = FileToDelete(local_name=f.local_name)
                        session.add(to_delete)
                        log.info('old file marked for deletion')
                    for k, v in remote_file_metadata.items():
                        if (old := local_file_metadata.get(k, None)) != v:
                            log.info('file metadata changed',
                                     field=k, old_value=old, new_value=v)
                            setattr(lf, k, v)
        for fid in local_file_by_id.keys() - remote_file_by_id.keys():
            lf = local_file_by_id[fid]
            if not (lf.is_directory or lf.is_google_workspace_file):
                d: dict[str, Any]
                d = dict(local_name=lf.local_name, id=lf.id, name=lf.name,
                         modified_time=lf.modified_time,
                         size=lf.size, head_revision_id=lf.head_revision_id,
                         md5_checksum=lf.md5_checksum, mime_type=lf.mime_type)
                to_delete = FileToDelete(**d)
                session.add(to_delete)
                log = logger.bind(name=lf.name, id=fid)
                log.info('file (deleted on server) marked for deletion')
            session.delete(lf)
        session.commit()

    return 0
