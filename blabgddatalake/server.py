from .local import LocalStorageDatabase, LocalFile, FileToDelete
from flask import abort, Flask, jsonify, request, Response, send_file
from pathlib import Path
from sys import maxsize

import waitress

import structlog

logger = structlog.getLogger(__name__)


app = Flask(__name__)


@app.route("/tree", methods=['GET'])
def tree() -> Response | None:
    config = app.config['options']
    db = LocalStorageDatabase(config['Database'])
    depth = request.args.get('depth', maxsize, type=int)
    with db.new_session() as session:
        local_tree = db.get_tree(session)
        if local_tree:
            return jsonify(local_tree.as_dict(depth, True))
    abort(404)


@app.route("/download/<id>/<head_revision_id>", methods=['GET'])
def file(id: str, head_revision_id: str) -> Response | None:
    config = app.config['options']
    db = LocalStorageDatabase(config['Database'])
    log = logger.bind(id=id)
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


def serve(config: dict, port: int) -> int:
    if port is None:
        port = int(p) if (p := config['Server']['Port']) is not None else None
    app.config['options'] = config
    app.config['JSON_SORT_KEYS'] = False
    waitress.serve(app, port=port)
    return 0
