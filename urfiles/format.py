#!/usr/bin/env python3
# format.py -*-python-*-

try:
    import humanize
except ImportError as e:
    print('''\
# Cannot import humanize: {}
# Consider: apt-get install python3-humanize'''.format(e))
    raise SystemExit from e

import json
import time

# pylint: disable=unused-import
from urfiles.log import PDLOG_SET_LEVEL, DEBUG, INFO, ERROR, FATAL


class Format():
    def __init__(self, debug=False):
        self.debug = debug

    def pretty_print(self, pathdata, metadata, full=False):
        seen = set()
        result = ''
        if self.debug:
            print(sorted(pathdata.items()))
        for path, source, size, mtime_ns, md5 in sorted(pathdata):
            result += path + '\n'
            result += '    {} ({}) {} [{}]'.format(
                size,
                humanize.naturalsize(size, binary=True),
                time.strftime('%Y %b %d %H:%M',
                              time.localtime(mtime_ns // 1e9)),
                source)
            if md5 != 0:
                result += ' {}'.format(md5)
            if metadata and md5 in metadata:
                if full and md5 not in seen:
                    formatted = json.dumps(metadata[md5], indent=4,
                                           sort_keys=False)
                    padded = '    '.join(formatted.splitlines(True))
                    result += '    {}'.format(padded)
                else:
                    if 'format' in metadata[md5]:
                        result += ' {}'.format(metadata[md5]['format'])
                    if 'width' in metadata[md5] and 'height' in metadata[md5]:
                        result += ' {}x{}'.format(metadata[md5]['width'],
                                                  metadata[md5]['height'])
                seen.add(md5)
            result += '\n'
        return result
