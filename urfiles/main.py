#!/usr/bin/env python3
# main.py -*-python-*-

import argparse
import os
import re
import time
import urfiles.config
import urfiles.db
import urfiles.format
import urfiles.identify
import urfiles.scan
import urfiles.search

# pylint: disable=unused-import
from urfiles.log import PDLOG_SET_LEVEL, DEBUG, INFO, ERROR, FATAL

def main():
    parser = argparse.ArgumentParser(description='urfiles')
    # Configuration maintenance
    parser.add_argument('--config', default=None, metavar=('CONFIGFILE'),
                        help='Configuration file')
    parser.add_argument('--show-config', action='store_true', default=False,
                        help='Shwo configuration and exit')

    # Database maintenance
    parser.add_argument('--init', action='store_true', default=False,
                        help='Initialize database')
    parser.add_argument('--drop', action='store_true', default=False,
                        help='Drop database and exit')
    parser.add_argument('--info', action='store_true', default=False,
                        help='Get information about the database')
    parser.add_argument('--dump', nargs=1, type=str, metavar=('TABLE'),
                        help='Dump specified table')

    # Scanning
    parser.add_argument('--scan', default=None, nargs='+', metavar=('DIR'),
                        help='Directory trees to scan')
    parser.add_argument('--debug', action='store_true', default=False,
                        help='Output verbose debugging messages')
    parser.add_argument('--id', default=None, nargs='+', metavar=('FILE'),
                        help='Identify files (does not use database)')
    parser.add_argument('--full', action='store_true', default=False,
                        help='When identifying a file, calculate md5;'
                        ' when searching, print metadata')

    # Searching
    parser.add_argument('--re', default=None, metavar=('RE'),
                        help='Search paths using regular expression')
    args = parser.parse_args()

    if args.debug:
        PDLOG_SET_LEVEL('DEBUG')

    if args.config:
        config = urfiles.config.Config([args.config])
    else:
        config = urfiles.config.Config()

    if args.show_config:
        print('config')
        for section in config.config.sections():
            print('  [{}]'.format(section))
            for key, value in config.config[section].items():
                  print('    {} = {}'.format(key, value))
        return 0

    if args.id:
        for file in args.id:
            fmt = urfiles.format.Format(debug=args.debug)
            statinfo = os.stat(file)
            identify = urfiles.identify.Identify(file, debug=args.debug)
            md5, meta = identify.id(checksum=args.full)
            print(fmt.pretty_print({file: [(-1, md5, statinfo.st_size,
                                            statinfo.st_mtime_ns)]},
                                   {md5: meta},
                                   full=args.full),
                  end='')
        return 0

    db = urfiles.db.DB(config.config)

    if args.drop:
        db.drop()
        return 0

    if args.init:
        db.maybe_create()
        if not db.connect():
            FATAL('Cannot connect to database')
        return 0

    if args.info:
        for line in db.info():
            print(line)
        return 0

    if args.dump:
        rows = db.fetch_rows(args.dump[0])
        for row in rows:
            print(row)
        return 0

    if args.re:
        search = urfiles.search.Search(args.re, config, debug=args.debug)
        result, meta = search.re()
        fmt = urfiles.format.Format(debug=args.debug)
        print(fmt.pretty_print(result, meta, full=args.full), end='')
        return 0

    if args.scan is None:
        parser.print_help()
        return -1

    scan = urfiles.scan.Scan(args.scan, config, debug=args.debug)
    scan.scan()
    return 0
