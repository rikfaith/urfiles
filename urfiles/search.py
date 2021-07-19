#!/usr/bin/env python3
# search.py -*-python-*-

import traceback
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
        meta = dict()
        for path, ids in path_ids:
            result[path] = []
            for file_id in ids:
                filedata = db.lookup_file(conn, file_id)
                result[path].append(filedata)
                _, md5, _, _ = filedata
                if md5 not in meta:
                    meta[md5] = db.lookup_meta(conn, md5)

        conn.close()
        return result, meta
