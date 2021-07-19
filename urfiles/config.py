#!/usr/bin/env python3
# config.py -*-python-*-

import configparser
import os

# pylint: disable=unused-import
from urfiles.log import PDLOG_SET_LEVEL, DEBUG, INFO, ERROR, FATAL, DECODE


class Config():
    def __init__(self, paths=['/etc/urfiles', '~/.config/urfiles',
                              '~/.urfiles']):
        self.paths = []
        for path in paths:
            self.paths.append(os.path.expanduser(path))
        self.config = configparser.ConfigParser()

        self.config.read_dict(
            {'postgresql': {'host': 'localhost',
                            'database': 'urfiles',
                            'user': 'postgresql'},
             })
        self.config.read(self.paths)

        error = ''
        if not self.config.has_section('postgresql') or \
           'password' not in self.config['postgresql']:
            error += '''
    The configuration file (e.g., ~/.urfiles) must have a section called
    [postgresql] with a "password" key. This password will be used with the
    "host", "database", and "user" keys to access the text database.'''

        if error != '':
            FATAL(error)
