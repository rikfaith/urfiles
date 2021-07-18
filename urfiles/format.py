#!/usr/bin/env python3
# format.py -*-python-*-

try:
    import humanize
except ImportError as e:
    print('''\
# Cannot import humanize: {}
# Consider: apt-get install python3-humanize'''.format(e))
    raise SystemExit

import time

# pylint: disable=unused-import
from urfiles.log import PDLOG_SET_LEVEL, DEBUG, INFO, ERROR, FATAL

class Format():
    def __init__(self, debug=False):
        self.debug = debug

    def human(self, value):
        return value

    # Data is a dictionary of path -> metadata. Return a pretty string.
    def pretty_print(self, data, full=True):
        result = ''
        if self.debug:
            print(sorted(data.items()))
        for path, metadatas in sorted(data.items()):
            result += path + '\n'
            for metadata in metadatas:
                file_id, md5sum, bytes, mtime_ns, meta = metadata
                result += '    {} ({}) {} {}\n'.format(
                    bytes,
                    humanize.naturalsize(bytes, binary=True),
                    time.strftime('%Y %b %d %H:%M',
                                  time.localtime(mtime_ns // 1e9)),
                    md5sum if md5sum != 0 else '')
        return result
