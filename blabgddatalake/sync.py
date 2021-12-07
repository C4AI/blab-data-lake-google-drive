import configparser
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Union

from .remote import Lake, RemoteDirectory, RemoteRegularFile
from .local import LocalStorageDatabase, LocalFile, FileToDelete


logger = logging.getLogger(__package__)


def read_settings(fn: str = 'blab-dataimporter-googledrive-settings.cfg') \
        -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.optionxform = str  # type: ignore  # do not convert to lower-case
    config.read(fn)
    return config


def local_file_name(f: Union[RemoteRegularFile, LocalFile]) -> str:
    return f.id + \
        '_' + (f.head_revision_id or '') + \
        '_' + (f.md5_checksum or '')


def download(f: RemoteRegularFile) -> None:
    directory = Path(config['Local']['RootPath'])
    fn = directory.resolve() / local_file_name(f)
    lake.download_file(f, str(fn))


if __name__ == '__main__':

    config = read_settings()
    db = LocalStorageDatabase(dict(config['Database']))

    lake = Lake(dict(config['GoogleDrive']))

    # clean up
    with db.new_session() as session:
        until = None
        if d := config['Local'].get('DeletionDelay', None):
            until = datetime.now() - timedelta(seconds=int(d))
        for ftd in db.get_files_to_delete(session):
            name = Path(config['Local']['RootPath']).resolve() / ftd.name
            try:
                os.remove(name)
            except FileNotFoundError:
                logger.warn(
                    f'Not deleting “{ftd.name}” because it no longer exists.')
            except Exception:
                logger.warn(
                    f'Could not delete “{ftd.name}”.')
            else:
                logger.info(f'File “{ftd.name}” has been deleted.')
                session.delete(ftd)
        session.commit()

    # sync
    with db.new_session() as session:

        local_tree = db.get_tree(session)
        local_file_by_id: Dict[str, LocalFile] = local_tree.flatten() \
            if local_tree else {}

        remote_tree = lake.get_tree()
        remote_file_by_id = remote_tree.flatten()

        for id, f in remote_file_by_id.items():
            remote_file_metadata: Dict[str, Any] = dict(
                id=f.id,
                name=f.name,
                mime_type=f.mime_type,
                created_time=f.created_time,
                modified_time=f.modified_time,
                modified_by=f.modified_by,
                web_url=f.web_url,
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
                local_file_metadata: Dict[str, Any] = dict(
                    id=lf.id,
                    name=lf.name,
                    mime_type=lf.mime_type,
                    created_time=lf.created_time,
                    modified_time=lf.modified_time,
                    modified_by=lf.modified_by,
                    web_url=lf.web_url,
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
                if local_file_metadata == remote_file_metadata:
                    # file is unchanged
                    logger.debug(
                        f'File (id: {id}, name: “{f.name}”) unchanged')
                else:
                    # file has been changed
                    logger.info(
                        f'File (id: {id}, name: “{lf.name}”) metadata changed')
                    unique_cols = ('md5_checksum', 'head_revision_id')
                    mt = f.mime_type
                    if isinstance(f, RemoteRegularFile) and \
                        not f.is_google_workspace_file and \
                        (remote_file_metadata[k] for k in unique_cols) != \
                            (local_file_metadata[k] for k in unique_cols):
                        fn = local_file_name(f)
                        to_delete = FileToDelete(name=fn)
                        session.add(to_delete)
                        logger.info(f'Local file “{fn}” marked for deletion')
                        download(f)
                    for k, v in remote_file_metadata.items():
                        if (old := local_file_metadata.get(k, None)) != v:
                            if k in unique_cols:
                                contents_changed = True
                            logger.info(
                                f'File (id: {id}, name: “{f.name}”) '
                                + f'changed field “{k}” from “{old}” to “{v}”')
                            setattr(lf, k, v)
                # TODO: delete files that were deleted on the server
        for fid in local_file_by_id.keys() - remote_file_by_id.keys():
            lf = local_file_by_id[fid]
            if not (lf.is_directory or lf.is_google_workspace_file):
                fn = local_file_name(lf)
                to_delete = FileToDelete(name=fn)
                session.add(to_delete)
                logger.info(f'Local file “{fn}” marked for deletion')
            session.delete(lf)
        session.commit()
