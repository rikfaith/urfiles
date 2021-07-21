#!/usr/bin/env python3
# load.py -*-python-*-

import os
import re
import time
import urfiles.db

# pylint: disable=unused-import
from urfiles.log import DEBUG, INFO, ERROR, FATAL


class Load():
    def __init__(self, directories, config, debug=False,
                 md5file='md5sum.txt', statfile='stat.txt'):
        self.directories = directories
        self.config = config
        self.debug = debug
        self.md5file = md5file
        self.statfile = statfile
        self.md5 = dict()

    def _load_md5file(self, directory):
        filename = os.path.join(directory, self.md5file)
        if not os.path.isfile(filename):
            ERROR('Cannot find %s', filename)
            return

        try:
            fp = open(filename, 'r')
        except OSError as e:
            ERROR('Cannot open %s: %s', filename, repr(e))
            return

        INFO('Reading %s', filename)
        current_time = time.time()
        count = 0
        for line in fp:
            md5, path = line.split(' ', 1)
            path = path.strip()
            self.md5[path] = md5
            count += 1
            if time.time() - current_time > 1.0:
                INFO('%d lines read', count)
                current_time = time.time()

        INFO('%d lines read', count)

    def _file(self, db, conn, path, size, mtime_ns):
            ids = db.lookup_path(conn, path)
            for file_id in ids:
                record = db.lookup_file(conn, file_id)
                _, _, existing_size, existing_mtime_ns = record
                if existing_size == size and existing_mtime_ns == mtime_ns:
                    return

            # This file has a new size or timestamp, insert it.
            md5 = self.md5[path]
            file_id = db.insert_file(conn, md5, size, mtime_ns)

            if file_id is not None:
                db.insert_path(conn, path, file_id)

    def _load_statfile(self, directory, db, conn):
        filename = os.path.join(directory, self.statfile)
        if not os.path.isfile(filename):
            ERROR('Cannot find %s', filename)
            return

        try:
            fp = open(filename, 'r')
        except OSError as e:
            ERROR('Cannot open %s: %s', filename, repr(e))
            return

        INFO('Reading %s', filename)
        current_time = time.time()
        count = 0
        for line in fp:
            path, attr = re.split(r' r ', line)
            path = path.strip()
            _, size, _, _, timestamp, _, tm, _ = attr.split()
            ns = re.sub(r'^.*\.', '', tm)
            mtime_ns = float(timestamp) * 1e9 + int(ns)

            self._file(db, conn, path, size, mtime_ns)
            count += 1
            if time.time() - current_time > 1.0:
                INFO('%d lines read', count)
                current_time = time.time()

        INFO('%d lines read', count)
        return

    def load(self):
        try:
            db = urfiles.db.DB(self.config.config)
            conn = db.connect()
        except Exception as e:
            FATAL('Cannot connect to database: %s', repr(e))

        for directory in self.directories:
            self._load_md5file(directory)
            self._load_statfile(directory, db, conn)
