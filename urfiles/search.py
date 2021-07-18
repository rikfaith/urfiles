#!/usr/bin/env python3
# search.py -*-python-*-

import urfiles.config

# pylint: disable=unused-import
from urfiles.log import DEBUG, INFO, ERROR, FATAL

class Search():
    def __init__(self, expr, config, debug=False):
        self.expr = expr
        self.config = config
        self.debug = debug

    def re(self):
        try:
            db = urfiles.db.DB(self.config.config)
            conn = db.connect()
        except Exception as exception:
            FATAL(traceback.format_exc())

        path_ids = db.re_path(conn, self.expr)

        result = dict()
        for path, ids in path_ids:
            result[path] = []
            for id in ids:
                metadata = db.lookup_file(conn, id)
                result[path].append(metadata)

        conn.close()
        return result
