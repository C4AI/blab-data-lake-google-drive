"""A module that runs a simple HTTP server providing file metadata/contents."""

from flask import abort, Flask, jsonify, request, Response, send_file
from pathlib import Path
from structlog import getLogger
from sys import maxsize
from waitress import serve as waitress_serve

from .config import Config
from .formats import ExportFormat
from .local.localdb import LocalStorageDatabase
from .local.gwfile import LocalGoogleWorkspaceFile
from .local.file import LocalFile
from .local.regularfile import LocalRegularFile

_logger = getLogger(__name__)

app = Flask(__name__)


@app.route("/tree", methods=['GET'])
@app.route("/tree/<id>", methods=['GET'])
def tree(root_id: str | None = None) -> Response | None:
    """Return a file tree containing only metadata.

    Query args:
        depth (int): maximum depth.

    Args:
        root_id: if provided, use a specific file as root of a sub-tree.

    Returns:
        the response in JSON
    """
    config: Config = app.config['options']
    db = LocalStorageDatabase(config.database)
    depth = request.args.get('depth', maxsize, type=int)
    with db.new_session() as session:
        local_tree = db.get_tree(session) if root_id is None \
            else db.get_file_by_id(session, root_id)
        if local_tree:
            return jsonify({'tree': local_tree.as_dict(depth, True)})
    abort(404)


@app.route("/download/<id>", methods=['GET'])
@app.route("/download/<id>/<revision_id>", methods=['GET'])
def download(file_id: str, revision_id: str | None = None) -> Response | None:
    """Return the contents of a file.

    This function does not apply to Google Workspace files,
    which can be downloaded by :func:`export` instead.

    Args:
        file_id: file id
        revision_id: id of the file revision (if omitted, get latest version)

    Returns:
        the file contents
    """
    config: Config = app.config['options']
    db = LocalStorageDatabase(config.database)
    log = _logger.bind(id=file_id)
    with db.new_session() as session:
        f: LocalFile | None
        f = db.get_file_by_id(session, file_id)
        log.info('requested file download', found=bool(f))
        if not isinstance(f, LocalRegularFile):
            abort(404)
        if not revision_id:
            revision_id = f.head_revision_id
        try:
            rev = next(r for r in f.revisions if r.revision_id == revision_id)
        except StopIteration:
            abort(404)
        directory = Path(config.local.root_path)
        fn = directory.resolve() / rev.local_name
        log.info('sending file contents', local_name=fn)
        try:
            return send_file(  # type: ignore[no-any-return]
                fn,
                mimetype=f.mime_type,
                download_name=f.name,
                last_modified=f.modified_time,
                as_attachment=True)
        except FileNotFoundError:
            # should not happen
            abort(503)


@app.route("/export/<id>", methods=['GET'])
def export(file_id: str) -> Response | None:
    """Return the exported contents of a file.

    This function only applies to Google Workspace files. Other files
    can be downloaded by :func:`download` instead.

    Query args:
        extension: file extension (**must** be one of the extensions listed
            by :func:`tree` for this file)

    Args:
        file_id: file id

    Returns:
        the file contents
    """
    extension = request.args.get('extension', '', type=str)
    if not extension:
        abort(400)
    config: Config = app.config['options']
    db = LocalStorageDatabase(config.database)
    log = _logger.bind(id=file_id)
    with db.new_session() as session:
        f: LocalFile | None
        f = db.get_file_by_id(session, file_id)
        log.info('requested file export', found=bool(f))
        if not isinstance(f, LocalGoogleWorkspaceFile):
            abort(404)
        ver = f.head_version

        directory = Path(config.local.root_path)
        try:
            fn = directory.resolve() / ver.local_names[extension]
        except KeyError:
            abort(400)  # extension is unavailable
        log.info('sending exported file contents', local_name=fn)
        try:
            mt = ExportFormat.from_extension(extension).mime_type
            return send_file(  # type: ignore[no-any-return]
                fn,
                mimetype=mt,
                download_name=f.name + '.' + extension,
                last_modified=f.modified_time,
                as_attachment=True)
        except FileNotFoundError:
            # should not happen
            abort(503)


def serve(config: Config, port: int | None) -> int:
    """Start server.

    Args:
        config: configuration parameters.
        port: the port to listen on (if provided, overrides the value
            in ``config.lake_server.port``)

    Returns:
        0 if no problems occurred, 1 otherwise
    """
    server_cfg = config.lake_server
    app.config['options'] = config
    app.config['JSON_SORT_KEYS'] = False
    waitress_serve(app, host=server_cfg.host, port=port or server_cfg.port)
    return 0
