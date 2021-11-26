import configparser
import logging
from pathlib import Path
from typing import Dict, Any

from .remote import Lake, RemoteDirectory, RemoteRegularFile
from .local import LocalStorageDatabse, LocalFile


logger = logging.getLogger(__package__)


def read_settings(fn: str = 'blab-dataimporter-googledrive-settings.cfg') \
        -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.optionxform = str  # type: ignore  # do not convert to lower-case
    config.read(fn)
    return config


if __name__ == '__main__':

    config = read_settings()
    db = LocalStorageDatabse(dict(config['Database']))
    local_tree = db.get_tree()
    local_file_by_id: Dict[str, LocalFile] = {}

    lake = Lake(dict(config['GoogleDrive']))
    remote_tree = lake.get_tree()
    remote_file_by_id = remote_tree.flatten()

    with db.new_session() as session:
        # New files
        for id, f in remote_file_by_id.items():
            if id in local_file_by_id:
                continue
            file_metadata: Dict[str, Any] = dict(
                id=f.id,
                name=f.name,
                created_time=f.created_time,
                modified_time=f.modified_time,
                modified_by=f.modified_by,
                web_url=f.web_url,
                parent_id=p.id if (p := f.parent) else None,
            )
            if isinstance(f, RemoteRegularFile):
                file_metadata.update(
                    md5_checksum=f.md5_checksum,
                    size=f.size,
                    mime_type=f.mime_type,
                )
                if not f.mime_type.startswith('application/vnd.google-apps'):
                    directory = Path(config['Local']['RootPath'])
                    fn = directory.resolve() / f.md5_checksum
                    lake.download_file(f, str(fn))
            elif isinstance(f, RemoteDirectory):
                if f.is_root:
                    file_metadata.update(is_root=True)
            new_file = LocalFile(**file_metadata)
            session.add(new_file)
        # TODO: deleted files
        # TODO: existing files - different contents/metadata
        session.commit()
