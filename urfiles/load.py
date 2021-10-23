#!/usr/bin/env python3
# load.py -*-python-*-

import csv
import io
import os
import re
import time
import urfiles.db

# pylint: disable=unused-import
from urfiles.log import DEBUG, INFO, ERROR, FATAL

class Load():
    def __init__(self, directories, config, source=None, debug=False,
                 md5file='md5sum.txt', statfile='stat.txt'):
        self.directories = directories
        self.config = config
        self.source = source
        self.debug = debug
        self.md5file = md5file
        self.statfile = statfile
        self.md5 = dict()

        # List of all known md5s
        self.known_md5s = set()

        # Lists of data that needs updating in the database
        self.path_data = []
        self.meta_data = []

    def _unescape(self, path):
        result = path.replace(r'\\', '\\').replace(r'\n', '\n')
#        if path != result:
#            INFO('path=%s -> %s', path, result)
        return result

    def _load_md5file(self, directory):
        filename = os.path.join(directory, self.md5file)
        if not os.path.isfile(filename):
            ERROR('Cannot find %s', filename)
            return

        try:
            fp = open(filename, 'r', errors='ignore')
        except OSError as e:
            ERROR('Cannot open %s: %s', filename, repr(e))
            return

        INFO('Reading %s', filename)
        current_time = time.time()
        count = 0
        for line in fp:
            md5, path = line.split(' ', 1)
            path = path.strip()
            if md5[0] == '\\':
                # The GNU version of md5sum (from Coreutils) uses an initial
                # backslash on the line to indicate that the escaping in the
                # filename is different for this line. The patch is here:
                # http://git.savannah.gnu.org/cgit/coreutils.git/commit/\
                #            ?id=646902b30dee04b9454fdcaa8a30fd89fc0514ca
                # and seems to escape backslashes and newlines. We undo those
                # escapes here.
                md5 = md5[1:]
                path = self._unescape(path)
            if re.search('md5sum.txt', path):
                INFO(path)
            self.md5[path] = md5
            count += 1
            if time.time() - current_time > 1.0:
                INFO('%d lines read', count)
                current_time = time.time()

        INFO('%d lines read', count)

    def _file(self, db, conn, path, source, size, mtime_ns):
        # Look up the md5 for this file
        try:
            md5 = self.md5[path]
        except KeyError as e:
            ERROR('Cannot find md5 for path="%s"', path)
            return

        if path not in self.known_paths:
            self.path_data.append([path, source, size, mtime_ns, md5])

        if md5 not in self.known_md5s:
            self.meta_data.append([md5, '{}'])
            self.known_md5s.add(md5)

    def _load_statfile(self, directory, db, conn):
        if self.source is None:
            source = os.path.basename(directory)
        else:
            source = self.source

        INFO('Reading paths for source=%s', source)
        self.known_paths = db.fetch_paths(source)

        filename = os.path.join(directory, self.statfile)
        if not os.path.isfile(filename):
            ERROR('Cannot find %s', filename)
            return

        try:
            fp = open(filename, 'r', errors='ignore')
        except OSError as e:
            ERROR('Cannot open %s: %s', filename, repr(e))
            return

        INFO('Reading %s', filename)

        current_time = time.time()
        count = 0
        for line in fp:
            try:
                # Because we anchor with a number, we won' have the correct
                # mode.
                path, attr = re.split(r' r [0-9]', line)
            except ValueError as e:
                ERROR('Cannot split "%s": %s', line.strip(), repr(e))
                continue

            path = path.strip()
            _, size, _, _, timestamp, _, tm, _ = attr.split()
            ns = re.sub(r'^.*\.', '', tm)
            mtime_ns = int(float(timestamp) * 1e9 + int(ns))

            self._file(db, conn, path, source, size, mtime_ns)
            count += 1
            if time.time() - current_time > 1.0:
                INFO('%d lines read', count)
                current_time = time.time()

        INFO('%d lines read: %d path updates and %d meta updates pending',
             count, len(self.path_data), len(self.meta_data))

    def _update_database(self, db, conn):
        INFO('Preparing data for bulk load')
        path_rows = io.StringIO()
        path_writer = csv.writer(path_rows)
        path_writer.writerows(self.path_data)

        meta_rows = io.StringIO()
        meta_writer = csv.writer(meta_rows)
        meta_writer.writerows(self.meta_data)

        path_rows.seek(0)
        meta_rows.seek(0)
        INFO('Bulk load starting')
        db.bulk_insert(conn, path_rows=path_rows, meta_rows=meta_rows)
        INFO('Bulk load finished')

    def load(self):
        try:
            db = urfiles.db.DB(self.config.config)
            conn = db.connect()
        except Exception as e:
            FATAL('Cannot connect to database: %s', repr(e))

        INFO('Reading all md5s')
        self.known_md5s = db.fetch_md5s()

        for directory in self.directories:
            self.path_data = []
            self.meta_data = []
            INFO('Loading data from %s', directory)
            try:
                self._load_md5file(directory)
            except UnicodeDecodeErro as e:
                FATAL('Cannot parse from %s: %s', directory, repr(e))
            self._load_statfile(directory, db, conn)
            self._update_database(db, conn)
        INFO('Data loaded')

