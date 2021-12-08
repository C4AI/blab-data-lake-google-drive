from .local import LocalStorageDatabase
from flask import abort, Flask, jsonify, request, send_file
from pathlib import Path

import waitress

import structlog

logger = structlog.getLogger(__name__)


app = Flask(__name__)


@app.route("/tree", methods=['GET'])
def tree() -> str:
    config = app.config['options']
    db = LocalStorageDatabase(config['Database'])
    recursive = request.args.get('recursive', type=int)
    with db.new_session() as session:
        local_tree = db.get_tree(session)
        if local_tree:
            return jsonify(local_tree.as_dict(recursive, True))
    return jsonify({})


@app.route("/file/<id>", methods=['GET'])
def file(id: str) -> str:
    config = app.config['options']
    db = LocalStorageDatabase(config['Database'])
    log = logger.bind(id=id)
    with db.new_session() as session:
        f = db.get_file_by_id(session, id)
        log.info('requested file download', found=bool(f))
        if f:
            if f.is_directory:
                abort(403)
            directory = Path(config['Local']['RootPath'])
            fn = directory.resolve() / f.local_name
            log.info('sending file contents', local_name=fn)
            try:
                return send_file(fn, mimetype=f.mime_type,
                                 download_name=f.name,
                                 last_modified=f.modified_time,
                                 as_attachment=True)
            except FileNotFoundError:
                abort(404)
    abort(404)
    return ''


def serve(config: dict, port: int) -> int:
    if port is None:
        port = int(p) if (p := config['Server']['Port']) is not None else None
    app.config['options'] = config
    app.config['JSON_SORT_KEYS'] = False
    waitress.serve(app, port=port)
    return 0
