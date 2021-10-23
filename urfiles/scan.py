#!/usr/bin/env python3
# scan.py -*-python-*-

# We use multiprocessing.Queue, so importing queue only for queue.Empty
import concurrent.futures
import multiprocessing
import os
import queue
import stat
import time
import traceback
import urfiles.config
import urfiles.db
import urfiles.identify

# pylint: disable=unused-import
from urfiles.log import DEBUG, INFO, ERROR, FATAL


class Scan():
    MAX_MESSAGE_TYPE = 9

    def __init__(self, directories, config, source=None, max_workers=3,
                 debug=False):
        self.directories = directories
        self.config = config
        if source is not None:
            self.source = source
        else:
            self.source = ''
        self.max_workers = max_workers
        self.debug = debug

    @staticmethod
    def _log_callback(target, msg_type, debug_info, msg):
        code = 'C>!SRXDIEF'[msg_type] \
            if msg_type <= Scan.MAX_MESSAGE_TYPE else '?'
        if debug_info and len(debug_info) > 0:
            INFO(":%c:%s: %s %s", code, target, debug_info, msg)
        else:
            INFO(":%c:%s: %s", code, target, msg)

    @staticmethod
    def _directory(idx, dirname, workq, resultq):
        if not os.access(dirname, os.X_OK | os.R_OK):
            resultq.put((idx, 'noaccess', dirname))
            return
        with os.scandir(dirname) as entries:
            for entry in entries:
                if entry.name in ['.', '..']:
                    continue
                workq.put(('entry', dirname, entry.name))

    @staticmethod
    def _file(db, conn, statinfo, idx, path, source, workq, resultq):
        md5 = db.lookup_path(conn, path, statinfo.st_size,
                             statinfo.st_mtime_ns)
        if md5 is not None:
            return

        # This file has a new size or timestamp. Get new metadata.
        identify = urfiles.identify.Identify(path)
        md5, metadata = identify.id()

        # If this is not a file or directory (e.g., a socket), skip it.
        if metadata['type'] == 'unknown':
            return
        if md5 == 0:
            ERROR('path=%s metadata=%s', path, metadata)

        db.insert_path(conn, path, source, statinfo.st_size,
                       statinfo.st_mtime_ns, md5)

        # Do we already have metadata for this md5?
        existing_metadata = db.lookup_meta(conn, md5)
        if not existing_metadata:
            db.insert_meta(conn, md5, metadata)
        elif metadata != existing_metadata:
            INFO(repr(metadata))
            ERROR(repr(existing_metadata))

    @staticmethod
    def _worker(config, idx, workq, resultq, source):
        def internal_worker(db, conn, idx, workq, resultq, source):
            working = True
            while True:
                try:
                    command, basename, dirname = workq.get(True, 1)
                except queue.Empty:
                    if working:
                        resultq.put((idx, 'idle', None))
                    working = False
                    time.sleep(1)
                    continue

                if command == 'quit':
                    return

                if command != 'entry':
                    resultq.put((idx, 'error',
                                 'command={} basename={} dirname={}'.format(
                                     command, basename, dirname)))

                working = True

                if basename is not None:
                    fulldirname = os.path.join(basename, dirname)
                else:
                    fulldirname = dirname

                try:
                    statinfo = os.stat(fulldirname)
                except FileNotFoundError as exception:
                    resultq.put((idx, 'notfound',
                                 fulldirname + ': ' + repr(exception)))
                    continue
                except OSError as exception:
                    resultq.put((idx, 'oserror',
                                 fulldirname + ': ' + repr(exception)))
                    continue

                if stat.S_ISDIR(statinfo.st_mode):
                    Scan._directory(idx, fulldirname, workq, resultq)
                else:
                    Scan._file(db, conn, statinfo,
                               idx, fulldirname, source, workq, resultq)

        assert workq
        assert resultq
        resultq.put((idx, 'starting'), True)
        time.sleep(1)
        try:
            db = urfiles.db.DB(config.config)
            conn = db.connect()
            internal_worker(db, conn, idx, workq, resultq, source)
            conn.commit()
            conn.close()
        except Exception as exception:
            resultq.put((idx, 'error', traceback.format_exc()))
        resultq.put((idx, 'stopping'), True)

    @staticmethod
    def _done_callback(idx, future):
        INFO('worker %d: done callback', idx)
        exc = future.exception()
        if exc is not None:
            INFO('worker %d: %s', idx, str(future.exc))
        result = future.result()
        if result is not None:
            INFO('worker %d: %d', idx, str(future.result()))

    def scan(self, callback=_log_callback.__func__):
        # Start the workers
        manager = multiprocessing.Manager()
        workq = manager.Queue()
        resultq = manager.Queue()
        resultq.put((-1, 'test'))
        futures = []
        INFO('Starting {} concurrent worker(s)'.format(self.max_workers))
        message_time = 0
        with concurrent.futures.ProcessPoolExecutor(
                max_workers=self.max_workers) as executor:
            for idx in range(self.max_workers):
                future = executor.submit(self._worker, self.config, idx, workq,
                                         resultq, self.source)
                futures.append(future)
                future.add_done_callback(
                    lambda future, idx=idx: self._done_callback(idx, future))

            INFO('Filling the queue from {} directory'.format(
                len(self.directories)))
            # Fill the queue
            for directory in self.directories:
                if directory[0] == '/':
                    INFO('Adding {}'.format(directory))
                    workq.put(('entry', None, directory))
                else:
                    INFO('Adding {} in {}'.format(directory, os.getcwd()))
                    workq.put(('entry', os.getcwd(), directory))

            # Get results
            results = 0
            working = [True] * self.max_workers
            while True:
                for idx, future in enumerate(futures):
                    if working[idx] and future.done():
                        INFO('worker %d: no longer running', idx)
                        working[idx] = False
                try:
                    result = resultq.get(False)
                    results += 1
                    DEBUG('result=%s', result)
                    if time.time() - message_time > 1.5:
                        message_time = time.time()
                        INFO('workq=%d resultq=%d workers=%d results=%d',
                             workq.qsize(), resultq.qsize(), sum(working),
                             results)
                    if result[1] == 'metadata':
                        INFO('metadata=%s', str(result[2]))
                    if result[1] == 'working':
                        working[result[0]] = True
                        continue
                    if result[1] == 'idle':
                        working[result[0]] = False
                    if result[1] == 'error':
                        INFO('worker %d: %s', result[0], result[2])
                except queue.Empty:
                    INFO('workq=%d resultq=%d workers=%d results=%d',
                         workq.qsize(), resultq.qsize(), sum(working),
                         results)
                    time.sleep(1)
                if resultq.qsize() == 0 and sum(working) == 0:
                    INFO('workq=%d resultq=%d workers=%d results=%d',
                         workq.qsize(), resultq.qsize(), sum(working),
                         results)
                    INFO('exiting: %d results', results)
                    break
            for idx in range(self.max_workers):
                workq.put(('quit', None, None))
