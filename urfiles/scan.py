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
    def __init__(self, directories, max_workers=1, debug=False):
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
        assert workq
        assert resultq
        resultq.put((idx, 'starting'), True)
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
                resultq.put((idx, 'error', None))

            working = True
            resultq.put((idx, 'working', None))

            if basename is not None:
                fulldirname = os.path.join(basename, dirname)
            else:
                fulldirname = dirname

            try:
                statinfo = os.stat(fulldirname)
            except FileNotFoundError:
                continue

            if stat.S_ISDIR(statinfo.st_mode):
                Scan._directory(idx, fulldirname, workq, resultq)
            else:
                Scan._file(idx, fulldirname, workq, resultq)

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
        INFO('starting')
        with concurrent.futures.ProcessPoolExecutor(
                max_workers=self.max_workers) as executor:
            for idx in range(self.max_workers):
                future = executor.submit(self._worker, idx, workq, resultq)
                futures.append(future)
                future.add_done_callback(
                    lambda future, idx=idx: self._done_callback(idx, future))

            INFO('filling')
            # Fill the queue
            for directory in self.directories:
                if directory[0] == '/':
                    workq.put(('entry', None, directory))
                else:
                    workq.put(('entry', os.getcwd(), directory))

            # Get results
            working = [True] * self.max_workers
            while True:
                for idx, future in enumerate(futures):
                    if not future.running():
                        working[idx] = False
#                INFO('workq size = %d', workq.qsize())
#                INFO('resultq size = %d', resultq.qsize())
                try:
                    result = resultq.get(False)
                    INFO('RESULT QUEUE: %s', result)
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
