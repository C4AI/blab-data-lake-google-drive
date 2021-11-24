import configparser
import logging
from pathlib import Path
from typing import Dict

from .gd import Lake, LakeFile, LakeDirectory, LakeRegularFile
from .cachedb import MetadataCacheDatabase, StoredLakeFile


logger = logging.getLogger(__package__)


def read_settings(fn: str = 'blab-dataimporter-googledrive-settings.cfg') \
        -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.optionxform = str  # type: ignore  # do not convert to lower-case
    config.read(fn)
    return config


def flatten_gd(root: LakeFile) -> Dict[str, LakeFile]:
    d: Dict[str, LakeFile] = {}
    pending = [root]
    while pending:
        node = pending.pop()
        d[node.id] = node
        if isinstance(node, LakeDirectory):
            for c in node.children:
                pending.append(c)
    return d


if __name__ == '__main__':

    config = read_settings()
    db = MetadataCacheDatabase(dict(config['Database']))
    local_tree = db.get_tree()
    local_file_by_id: Dict[str, StoredLakeFile] = {}

    lake = Lake(dict(config['GoogleDrive']))
    gd_tree = lake.get_tree()
    gd_file_by_id = flatten_gd(gd_tree)

    with db.new_session() as session:
        # New files
        for id, f in gd_file_by_id.items():
            if id in local_file_by_id:
                continue
            file_metadata = dict(
                id=f.id,
                name=f.name,
                created_time=f.created_time,
                modified_time=f.modified_time,
                modified_by=f.modified_by,
                web_url=f.web_url,
                parent_id=p.id if (p := f.parent) else None,
            )
            if isinstance(f, LakeRegularFile):
                file_metadata.update(
                    md5_checksum=f.md5_checksum,
                    size=f.size,
                    mime_type=f.mime_type,
                )
                if not f.mime_type.startswith('application/vnd.google-apps'):
                    directory = Path(config['Local']['RootPath'])
                    fn = directory.resolve() / f.md5_checksum
                    lake.download_file(f, fn)
            elif isinstance(f, LakeDirectory):
                if f.is_root:
                    file_metadata.update(is_root=True)
            new_file = StoredLakeFile(**file_metadata)
            session.add(new_file)
        # TODO: deleted files
        # TODO: existing files - different contents/metadata
        session.commit()
