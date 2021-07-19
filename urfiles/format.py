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

    def pretty_print(self, data, meta=None, full=False):
        seen = set()
        result = ''
        if self.debug:
            print(sorted(data.items()))
        for path, filedatas in sorted(data.items()):
            result += path + '\n'
            for filedata in filedatas:
                _, md5, size, mtime_ns = filedata
                result += '    {} ({}) {}'.format(
                    size,
                    humanize.naturalsize(size, binary=True),
                    time.strftime('%Y %b %d %H:%M',
                                  time.localtime(mtime_ns // 1e9)))
                if md5 != 0:
                    result += ' {}'.format(md5)
                if meta and md5 in meta:
                    if full and md5 not in seen:
                        formatted = json.dumps(meta[md5], indent=4,
                                               sort_keys=False)
                        padded = '    '.join(formatted.splitlines(True))
                        result += '    {}'.format(padded)
                    else:
                        if 'format' in meta[md5]:
                            result += ' {}'.format(meta[md5]['format'])
                        if 'width' in meta[md5] and 'height' in meta[md5]:
                            result += ' {}x{}'.format(meta[md5]['width'],
                                                      meta[md5]['height'])
                    seen.add(md5)
                result += '\n'
        return result
