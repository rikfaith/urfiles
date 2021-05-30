#!/usr/bin/env python3
# scan.py -*-python-*-

# We use multiprocessing.Queue, so importing queue only for queue.Empty
import concurrent.futures
import multiprocessing
import os
import queue
import stat
import sys
import time

# pylint: disable=unused-import
from urfiles.log import DEBUG, INFO, ERROR, FATAL, TRACEBACK

class Scan():
    def __init__(self, directories, max_workers=3, debug=False):
        self.directories = directories
        self.max_workers = max_workers
        self.debug = debug

    @staticmethod
    def _log_callback(target, msg_type, debug_info, msg):
        code = 'C>!SRXDIEF'[msg_type] \
            if msg_type <= Pool.MT_MAX_MESSAGE_TYPE else '?'
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
    def _file(idx, filename, workq, resultq):
        resultq.put((idx, 'file', filename))

    @staticmethod
    def _worker(idx, workq, resultq):
        def internal_worker(idx, workq, resultq):
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
                    resultq.put((idx, 'quit', None))
                    return

                if command != 'entry':
                    resultq.put((idx, 'error', command + ':' + dirname))

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
                    Scan._file(idx, fulldirname, workq, resultq)

        assert workq
        assert resultq
        resultq.put((idx, 'starting'), True)
        internal_worker(idx, workq, resultq)
        return
        try:
            internal_worker(idx, workq, resultq)
        except Exception as exception:
            resultq.put((idx, 'error', repr(exception)))


    @staticmethod
    def _done_callback(idx, future):
        exc = future.exception()
        if exc is not None:
            INFO(idx, 'future.result={}'.format(str(future.result())))
        result = future.result()
        if result is not None:
            INFO(idx, 'future.result={}'.format(str(future.result())))

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
                future = executor.submit(self._worker, idx, workq, resultq)
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
            working = [True] * self.max_workers
            while True:
                for idx, future in enumerate(futures):
                    if not future.running():
                        INFO('worker {} is no longer running'.format(idx))
                        working[idx] = False
                        quit()
#                INFO('workq size = %d', workq.qsize())
#                INFO('resultq size = %d', resultq.qsize())
                try:
                    result = resultq.get(False)
                    if time.time() - message_time > 1.5:
                        message_time = time.time()
                        INFO('running w=%d r=%d w=%d', workq.qsize(),
                             resultq.qsize(), sum(working))
                    if result[1] == 'working':
                        working[result[0]] = True
                        continue
                    if result[1] == 'idle':
                        working[result[0]] = False
                except queue.Empty:
                    INFO('sleeping w=%d r=%d w=%d', workq.qsize(),
                         resultq.qsize(), sum(working))
                    time.sleep(1)
                if workq.qsize() == 0 and sum(working) == 0:
                    INFO('exiting')
                    break
            for idx in range(self.max_workers):
                workq.put(('quit', None, None))
