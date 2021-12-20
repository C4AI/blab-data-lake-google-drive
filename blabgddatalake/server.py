"""A module that runs a simple HTTP server providing file metadata/contents."""

from flask import abort, Flask, jsonify, request, Response, send_file
from pathlib import Path
from structlog import getLogger
from sys import maxsize
from waitress import serve as waitress_serve

from .local import LocalStorageDatabase, LocalFile, FileToDelete


_logger = getLogger(__name__)


app = Flask(__name__)


@app.route("/tree", methods=['GET'])
@app.route("/tree/<id>", methods=['GET'])
def tree(id: str | None = None) -> Response | None:
    """Return a file tree containing only metadata.

    Query args:
        depth (int): maximum depth.

    Args:
        id: if provided, use a specific file as root of a sub-tree.

    Returns:
        the response in JSON
    """
    config = app.config['options']
    db = LocalStorageDatabase(config['Database'])
    depth = request.args.get('depth', maxsize, type=int)
    with db.new_session() as session:
        local_tree = db.get_tree(session) if id is None \
            else db.get_file_by_id(session, id)
        if local_tree:
            return jsonify({'tree': local_tree.as_dict(depth, True)})
    abort(404)


@app.route("/download/<id>/<head_revision_id>", methods=['GET'])
def file(id: str, head_revision_id: str) -> Response | None:
    """Return the contents of a file.

    Args:
        id: file id
        head_revision_id: id of the current file version

    Returns:
        the file contents
    """
    config = app.config['options']
    db = LocalStorageDatabase(config['Database'])
    log = _logger.bind(id=id)
    with db.new_session() as session:
        f: LocalFile | FileToDelete | None
        f = db.get_file_by_id(session, id)
        log.info('requested file download', found=bool(f))
        if not f:
            f = db.get_file_to_delete(session, id, head_revision_id)
        if not f or isinstance(f, LocalFile) and f.is_directory:
            abort(404)
        directory = Path(config['Local']['RootPath'])
        fn = directory.resolve() / f.local_name
        log.info('sending file contents', local_name=fn)
        try:
            return send_file(fn, mimetype=f.mime_type,
                             download_name=f.name,
                             last_modified=f.modified_time,
                             as_attachment=True)
        except FileNotFoundError:
            # should not happen
            abort(503)


def serve(config: dict, port: int | None) -> int:
    """Start server.

    Args:
        config: configuration parameters (see
            :download:`the documentation <../README_CONFIG.md>`).
        port: the port to listen on (if provided, overrides the value
            in ``config['LakeServer']['Port']``)

    Returns:
        0 if no problems occurred, 1 otherwise
    """
    server_cfg = config['LakeServer']
    if port is None:
        port = int(p) if (p := server_cfg['Port']) is not None else None
    host = server_cfg['Host']
    app.config['options'] = config
    app.config['JSON_SORT_KEYS'] = False
    waitress_serve(app, host=host, port=port)
    return 0
