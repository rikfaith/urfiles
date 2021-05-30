#!/usr/bin/env python3
# main.py -*-python-*-

import argparse
import re

# pylint: disable=unused-import
from urfiles.log import PDLOG_SET_LEVEL, DEBUG, INFO, ERROR, FATAL
import urfiles.scan


def main():
    parser = argparse.ArgumentParser(description='urfiles')
    parser.add_argument('--scan', default=None, nargs='+',
                        help='Directory trees to scan')
    args = parser.parse_args()

    if args.scan is None:
        parser.print_help()
        return -1

    scan = urfiles.scan.Scan(args.scan)
    scan.scan()
    return 0
