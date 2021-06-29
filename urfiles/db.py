#!/usr/bin/env python3
# db.py -*-python-*-

import os
import json

try:
    import psycopg2
except ImportError as e:
    print('''\
# Cannot import psycopg2: {}
# Consider: apt-get install python3-psycopg2'''.format(e))
    raise SystemExit

from urfiles.log import PDLOG_SET_LEVEL, DEBUG, INFO, ERROR, FATAL, DECODE

class DB():
    def __init__(self, config, section='postgresql'):
        self.config = config
        self.section = section

        if self.section not in self.config:
            FATAL('Configuration file is missing the [%s] section',
                  self.section)

        self.params = dict()
        for key, value in self.config[self.section].items():
            self.params[key] = value

    def _connect(self, params, autocommit=False, use_schema=True):
        try:
            conn = psycopg2.connect(**params)
            conn.autocommit = autocommit
            with conn.cursor() as cur:
                if use_schema:
                    cur.execute('set search_path to urfiles, public')
        except (Exception, psycopg2.DatabaseError) as error:
            DECODE('Cannot connect to database')
            return None
        return conn

    def _execute(self, commands, args=None, conn=None, autocommit=False,
                 use_schema=True, use_database=True, close=None,
                 commit=True):
        DEBUG('autocommit=%s use_schema=%s use_database=%s close=%s commit=%s',
             autocommit, use_schema, use_database, close, commit)
        retcode = True
        if use_database or 'database' not in self.params:
            params = self.params
        else:
            params = self.params.copy()
            del params['database']

        if conn is None:
            conn = self._connect(params, autocommit=autocommit,
                                 use_schema=use_schema)
            if conn is None:
                FATAL('Cannot connect to database.'
                      ' Has --init been used to initialize the database??')

            # If we created a connection, then we'll close that connection,
            # and the cursor will become invalid, so we have to close both in
            # this case.
            if close is None:
                close = True

        cur = conn.cursor()

        for command in commands:
            try:
                cur.execute(command, args)
            except (Exception, psycopg2.DatabaseError) as error:
                DECODE('command=%s args=%s failed', command, args)
                retcode = False
                break

        if commit:
            conn.commit()
        if close == True:
            cur.close()
            cur = None
            conn.close()
            conn = None
        return retcode, conn, cur

    def _maybe_create_tables(self):
        commands = [
            '''create table if not exists hash (
            md5sum text primary key,
            bytes int
            )''',

            '''create table if not exists path (
            path text primary key,
            file_ids int[]
            )''',

            '''create table if not exists file (
            file_id serial primary key,
            md5sum text,
            bytes int,
            mtime_ns bigint,
            metadata json
            )''',
            ]
        return self._execute(commands)[0]

    def _create_database(self):
        commands = [
            "create database {} with encoding='UTF8'".
                format(self.params['database']),

            '''create schema urfiles'''
            ]
        return self._execute(commands, use_database=False, use_schema=False,
                             autocommit=True)[0]

    def maybe_create(self):
        commands = [
            '''select datname from pg_database where datistemplate=false'''
        ]
        retcode, conn, cur = self._execute(commands, use_database=False,
                                           use_schema=False, close=False,
                                           commit=False)
        if not retcode:
            cur.close()
            conn.close()
            return retcode

        exists = False
        for datname in cur:
            DEBUG('datname=%s', datname[0])
            if datname[0] == self.params['database']:
                exists = True
                break
        cur.close()
        conn.close()

        if not exists:
            retcode = self._create_database()
        if not retcode:
            return retcode
        return self._maybe_create_tables()

    def drop(self):
        commands = [
            'drop database if exists {}'.format(self.params['database']),
            'drop schema if exists {}'.format(self.params['database'])
        ]
        retcode, _, _ = self._execute(commands, use_database=False,
                                      autocommit=True)
        if retcode:
            INFO('Database %s dropped', self.params['database'])

    def connect(self):
        # FIXME we should have two different sets of params.
        self.conn = self._connect(self.params)
        return self.conn

    def _get_version(self):
        versions = []
        commands = [
            '''select version();'''
        ]
        retcode, conn, cur = self._execute(commands, close=False)
        if retcode:
            for version in cur:
                versions.append(version[0])
        cur.close()
        conn.close()
        return versions[0]

    def _get_tables(self):
        tables = []
        commands = [
            '''select tablename from pg_catalog.pg_tables where
            schemaname != 'pg_catalog' and
            schemaname != 'information_schema';'''
        ]
        retcode, conn, cur = self._execute(commands, close=False)
        if retcode:
            for table in cur:
                tables.append(table[0])
        cur.close()
        conn.close()
        return tables

    def _get_table_size(self, table):
        sizes = []
        commands = [
            '''select pg_size_pretty(pg_relation_size(%s));'''
        ]
        retcode, conn, cur = self._execute(commands, (table,), close=False)
        if retcode:
            for size in cur:
                sizes.append(size[0])
        cur.close()
        conn.close()
        return sizes[0]

    def _get_table_count(self, table):
        counts = []
        commands = [
            '''select count(*) from %s;'''
        ]
        retcode, conn, cur = self._execute(commands,
                                           (psycopg2.extensions.AsIs(table),),
                                           close=False)
        if retcode:
            for size in cur:
                counts.append(size[0])
        cur.close()
        conn.close()
        return counts[0]

    def _get_table_description(self, table):
        columns = []
        commands = [
            '''select column_name, data_type from information_schema.columns
            where table_name=%s;'''
        ]
        retcode, conn, cur = self._execute(commands, (table,), close=False)
        if retcode:
            for column in cur:
                columns.append(column)
        cur.close()
        conn.close()
        return columns

    def info(self):
        output = []
        output.append('Database')
        output.append('  {}'.format(self._get_version()))

        output.append('Tables')
        tables = self._get_tables()
        for table in tables:
            size = self._get_table_size(table)
            count = self._get_table_count(table)
            output.append('  {:<30s} {:<10s} {:>d} entries'.format(table,
                                                                   size,
                                                                   count))

        for table in tables:
            output.append('Description of {}'.format(table))
            columns = self._get_table_description(table)
            for column in columns:
                output.append('  {:<30s} {}'.format(column[0], column[1]))

        return output

    def connect(self):
        # FIXME we should have two different sets of params.
        self.conn = self._connect(self.params)
        return self.conn

    def fetch_rows(self, table):
        commands = [
            '''select * from %s;'''
            ]

        retcode, conn, curr = self._execute(commands,
                                            (psycopg2.extensions.AsIs(table),),
                                            close=False)
        rows = []
        if retcode:
            for row in curr:
                rows.append(row)
        curr.close()
        conn.close()
        return rows

    def lookup_path(self, conn, path):
        commands = [
            '''select file_ids from path where path=%s;''',
            ]
        retcode, _, curr = self._execute(commands, (path,), conn=conn)

        ids = []
        if retcode:
            result = curr.fetchone()
            if result is not None:
                ids = result[0]
        curr.close()
        return ids

    def lookup_file(self, conn, file_id):
        INFO('file_id=%s', repr(file_id))
        commands = [
            '''select * from file where file_id=%s;''',
            ]
        retcode, _, curr = self._execute(commands, (file_id,), conn=conn)
        metadata = curr.fetchone() if retcode else None
        curr.close()
        return metadata

    def insert_file(self, conn, md5sum, bytes, mtime_ns, metadata):
        commands = [
            '''insert into file(md5sum, bytes, mtime_ns, metadata)
            values(%s,%s,%s,%s) returning file_id;'''
            ]
        retcode, _, curr = self._execute(commands,
                                         (md5sum, bytes, mtime_ns,
                                          json.dumps(metadata),),
                                         conn=conn, commit=False)
        file_id = None
        if retcode:
            result = curr.fetchone()
            if result is not None:
                file_id = result[0]
        curr.close()
        return file_id

    def insert_path(self, conn, path, file_id):
        commands = [
            '''insert into path(path,file_ids) values(%s,%s)
            on conflict(path) do update set file_ids=%s||path.file_ids;'''
            ]
        retcode, _, _ = self._execute(commands, (path, [file_id], file_id),
                                        conn=conn, commit=False)
        return retcode
