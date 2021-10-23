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

        matches = db.re_path(conn, self.expr)

        meta = dict()
        for path, source, size, mtime_ns, md5 in matches:
            if md5 not in meta:
                meta[md5] = db.lookup_meta(conn, md5)

        conn.close()
        return matches, meta
