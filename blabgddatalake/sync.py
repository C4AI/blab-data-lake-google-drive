"""A module that provides methods to sync files from Google Drive."""

from datetime import datetime, timedelta
from os import remove as os_delete_file
from pathlib import Path
from structlog import getLogger
from typing import Any, cast

import re

from .formats import ExportFormat
from .remote import RemoteDirectory, RemoteRegularFile, RemoteFile, \
    GoogleDriveService as GDService
from .local import LocalStorageDatabase, LocalFile, LocalDirectory, \
    LocalRegularFile, LocalGoogleWorkspaceFile, LocalFileRevision, \
    LocalExportedGWFileVersion


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


def _parse_gw_extensions(formats: str) -> dict[str, list[ExportFormat]]:
    """Parse a list of extensions per file type.

    Each line must have a file type, a colon and a list of comma-separated
    extensions. Spaces are ignored.

    Args:
        formats: the string to parse

    Returns:
        A dictionary mapping each file type to a list of extensions
    """
    return {
        type: list(map(lambda ext: ExportFormat.from_extension(
            ext), filter(lambda f: f, format.split(','))))
        for type, format in
        map(lambda l: l.split(':', 1),
            filter(lambda l: l.count(':') == 1,
                   re.sub('[^a-z,:\\.\n]', '', formats).strip().split('\n')))
    }


def _make_fields_equal(old: Any, new: Any, fields: list[str]) -> int:
    changes = 0
    for field in fields:
        if getattr(old, field, None) != (value := getattr(new, field, None)):
            setattr(old, field, value)
            changes += 1
    return changes


def _getattrs(obj: Any, attrs: list[str]) -> dict[str, Any]:
    return {k: getattr(obj, k, None) for k in attrs}


def _local_file_from_remote_file(rf: RemoteFile) -> LocalFile:
    fields = ['id', 'name', 'mime_type', 'created_time', 'modified_time',
              'modified_by', 'web_url', 'icon_url', 'parent_id']
    if isinstance(rf, RemoteDirectory):
        return LocalDirectory(**_getattrs(rf, fields + ['is_root']))
    elif isinstance(rf, RemoteRegularFile):
        if not rf.is_google_workspace_file:
            return LocalRegularFile(
                **_getattrs(rf, fields + ['head_revision_id']))
        else:
            return LocalGoogleWorkspaceFile(**_getattrs(rf, fields))
    raise RuntimeError  # should not happen


def _revision_from_remote_file(rf: RemoteRegularFile) -> LocalFileRevision:
    fields = ['name', 'mime_type', 'can_download', 'size',
              'modified_time', 'modified_by', 'md5_checksum']
    return LocalFileRevision(file_id=rf.id, revision_id=rf.head_revision_id,
                             **_getattrs(rf, fields))


def _gwversion_from_remote_file(rf: RemoteRegularFile, extensions: list[str]) \
        -> LocalExportedGWFileVersion:
    fields = ['modified_time', 'modified_by', 'mime_type', 'name']
    return LocalExportedGWFileVersion(file_id=rf.id, extensions=extensions,
                                      can_export=rf.can_download,
                                      **_getattrs(rf, fields))


def sync(config: dict) -> int:
    """Sync files from Google Drive.

    Args:
        config: configuration parameters (see
            :download:`the documentation <../README_CONFIG.md>`).

    Returns:
        0 if no errors occurred, 1 otherwise
    """  # noqa: DAR401
    db, gdservice = _db_and_gdservice(config)

    supported_export_extensions = gdservice.export_formats()
    chosen_export_extensions = {
        'application/vnd.google-apps.' + type: ext
        for type, ext in _parse_gw_extensions(
            config['GoogleDrive']
            .get('GoogleWorkspaceExportingFormats', '')).items()
    }
    missing_export_extensions = {
        type: set(chosen) - set(supported_export_extensions.get(type, []))
        for type, chosen in chosen_export_extensions.items()
    }
    for type, missing in missing_export_extensions.items():
        if missing:
            _logger.warn('Unsupported export format(s) for type', type=type,
                         unsupported_formats=set(map(lambda fmt: fmt.extension,
                                                     missing)))
    export_formats = {
        type: sorted(set(available) & set(
            chosen_export_extensions.get(type, [])))
        for type, available in supported_export_extensions.items()
    }

    def _contents_changed(rf: RemoteFile, lf: LocalFile) -> bool:
        if isinstance(lf, LocalRegularFile):
            rfile = cast(RemoteRegularFile, rf)
            return rfile.head_revision_id != lf.head_revision_id or \
                rfile.can_download != lf.can_download
        elif isinstance(lf, LocalGoogleWorkspaceFile):
            rfile = cast(RemoteRegularFile, rf)
            return rf.modified_time > lf.modified_time or \
                rfile.can_download != lf.can_export or \
                list(map(lambda fmt: fmt.extension, export_formats.get(
                    rf.mime_type, []))) != lf.head_version.extensions
        else:
            return False

    def download(f: RemoteRegularFile) -> bool | None:
        directory = Path(config['Local']['RootPath'])
        fn = directory.resolve() / f.local_name
        return gdservice.download_file(f, str(fn))

    def export(f: RemoteRegularFile, formats: list[ExportFormat]) \
            -> bool | None:
        directory = Path(config['Local']['RootPath'])
        fn = directory.resolve() / f.local_name
        return gdservice.export_file(f, formats, str(fn))

    def _update(rf: RemoteFile, lf: LocalFile) -> bool:
        fields = ['id', 'name', 'mime_type', 'created_time', 'modified_time',
                  'modified_by', 'web_url', 'icon_url', 'parent_id']
        changed = False
        if isinstance(rf, RemoteDirectory) and isinstance(lf, LocalDirectory):
            fields += ['is_root']
        elif isinstance(rf, RemoteRegularFile):
            if rf.is_google_workspace_file:
                ver_fields = ['name', 'mime_type',
                              'modified_time', 'modified_by', 'obsolete_since']
                ver = cast(LocalGoogleWorkspaceFile, lf).head_version
                if _make_fields_equal(ver, rf, ver_fields) != 0:
                    changed = True
                ext = list(map(lambda fmt: fmt.extension, export_formats.get(
                    rf.mime_type, [])))
                if ver.extensions != ext or []:
                    ver.extensions = ext or []
                    changed = True
                if ver.can_export != rf.can_download:
                    ver.can_export = rf.can_download
                    changed = True
            else:
                fields += ['head_revision_id']
                rev_fields = ['name', 'mime_type', 'can_download', 'size',
                              'modified_time', 'modified_by', 'md5_checksum',
                              'obsolete_since']
                rev = cast(LocalRegularFile, lf).head_revision
                if _make_fields_equal(rev, rf, rev_fields) != 0:
                    changed = True
                if rev.revision_id != rf.head_revision_id:
                    rev.revision_id = rf.head_revision_id
                    changed = True
                if rev.file_id != rf.id:
                    rev.file_id = rf.id
                    changed = True
        if _make_fields_equal(lf, rf, fields) != 0:
            changed = True
        return changed

    with db.new_session() as session:

        def sync_new_file(rf: RemoteFile, lf: LocalFile | None = None) \
                -> LocalFile:
            if not lf:
                lf = _local_file_from_remote_file(rf)
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
                    session.add(_revision_from_remote_file(rrf))
                    lf.head_revision_id = rrf.head_revision_id
                session.flush()
                session.expire(lf, ['head_revision'])
                if lf.can_download:
                    download(rrf)
                else:
                    log.info('skipping non-downloadable file')
            elif isinstance(lf, LocalGoogleWorkspaceFile):
                rrf = cast(RemoteRegularFile, rf)
                ver = lf.head_version
                formats = export_formats.get(rf.mime_type, [])
                ext = list(map(lambda ef: ef.extension, formats))
                if ver and (ver.modified_time == rrf.modified_time):
                    # This happens when can_download has changed or when
                    # the list of formats has changed
                    pass
                else:
                    lf.modified_time = rrf.modified_time
                    ver = _gwversion_from_remote_file(rrf, ext)
                    session.add(ver)
                session.flush()
                session.expire(lf, ['head_version'])
                if lf.can_export and ext:
                    export(rrf, formats)
                else:
                    log.info('skipping non-exportable file')

            session.flush()
            _update(rf, lf)
            return lf

        local_tree = db.get_tree(session)
        local_file_by_id: dict[str, LocalFile] = \
            local_tree.flatten() if local_tree else {}

        remote_tree = gdservice.get_tree()
        remote_file_by_id = remote_tree.flatten()

        for id, f in remote_file_by_id.items():
            log = _logger.bind(name=f.name, id=id, mime_type=f.mime_type)
            if id not in local_file_by_id:
                if (lf := db.get_file_by_id(session, id, True)) is not None:
                    log.info('previously deleted file has been recovered')
                    local_file_by_id[id] = lf
                    lf.obsolete_since = None  # type: ignore
                else:
                    # file is new
                    sync_new_file(f)
                    continue
            # file already existed
            lf = local_file_by_id[id]
            if _contents_changed(f, lf):
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
                    if old_ver.modified_time < \
                            cast(RemoteRegularFile, f).modified_time:
                        old_ver.obsolete_since = datetime.now()
                        log.info('old GW file version marked for deletion')
                sync_new_file(f, lf)
            elif _update(f, lf):
                # only metadata has changed
                log.info('file metadata changed, contents are unchanged')
            else:
                # nothing has changed
                log.debug('no changes in file')

        for fid in local_file_by_id.keys() - remote_file_by_id.keys():
            lf = local_file_by_id[fid]
            if isinstance(lf, LocalRegularFile):
                t = datetime.now()
                lf.head_revision.obsolete_since = t
                lf.obsolete_since = t
                log = _logger.bind(name=lf.name, id=fid)
                log.info('file (deleted on server) marked for deletion')
        session.commit()
    return 0
