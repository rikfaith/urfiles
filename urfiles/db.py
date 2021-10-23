#!/usr/bin/env python3
# db.py -*-python-*-

import json

try:
    import psycopg2
except ImportError as e:
    print('''\
# Cannot import psycopg2: {}
# Consider: apt-get install python3-psycopg2'''.format(e))
    raise SystemExit from e

# pylint: disable=unused-import
from urfiles.log import PDLOG_SET_LEVEL, DEBUG, INFO, ERROR, FATAL, DECODE


class DB():
    def __init__(self, config, section='postgresql'):
        self.config = config
        self.section = section
        self.conn = None

        if self.section not in self.config:
            FATAL('Configuration file is missing the [%s] section',
                  self.section)

        self.params = dict()
        for key, value in self.config[self.section].items():
            self.params[key] = value

    @staticmethod
    def _connect(params, autocommit=False, use_schema=True):
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
        if close is True:
            cur.close()
            cur = None
            conn.close()
            conn = None
        return retcode, conn, cur

    def _maybe_create_tables(self):
        commands = [
            '''create table if not exists path (
            path text,
            source text,
            bytes bigint,
            mtime_ns bigint,
            md5 text,
            primary key(path, source, bytes, mtime_ns)
            )''',

            '''create table if not exists meta (
            md5 text primary key,
            metadata json
            )'''
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

    def fetch_rows(self, table):
        commands = [
            '''select * from %s;'''
        ]

        retcode, conn, cur = self._execute(commands,
                                           (psycopg2.extensions.AsIs(table),),
                                           close=False)
        rows = []
        if retcode:
            for row in cur:
                rows.append(row)
        cur.close()
        conn.close()
        return rows

    def fetch_md5s(self):
        commands = [
            '''select md5 from meta;'''
        ]

        retcode, conn, cur = self._execute(commands, close=False)
        md5s = set()
        if retcode:
            for row in cur:
                md5s.add(row[0])
        cur.close()
        conn.close()
        return md5s

    def fetch_paths(self, source):
        commands = [
            '''select path from path where source=%s;'''
        ]

        retcode, conn, cur = self._execute(commands, (source,), close=False)
        paths = set()
        if retcode:
            for row in cur:
                paths.add(row[0])
        cur.close()
        conn.close()
        return paths

    def insert_path(self, conn, path, source, size, mtime_ns, md5):
        commands = [
            '''insert into path(path,source,bytes,mtime_ns,md5)'''
            ''' values(%s,%s,%s,%s,%s);'''
        ]
        retcode, _, _ = self._execute(commands,
                                      (path, source, size, mtime_ns, md5),
                                      conn=conn, commit=False)
        return retcode

    def lookup_path(self, conn, path, size, mtime_ns):
        commands = [
            '''select md5 from path where'''
            ''' path=%s and bytes=%s and mtime_ns=%s;''',
        ]
        retcode, _, cur = self._execute(commands, (path,size,mtime_ns),
                                        conn=conn)

        md5 = cur.fetchone() if retcode else None
        cur.close()
        return md5

    def re_path(self, conn, re):
        commands = [
            '''select path,source,bytes,mtime_ns,md5'''
            ''' from path where path ~ %s;''',
        ]
        retcode, _, cur = self._execute(commands, (re,), conn=conn)

        paths = []
        if retcode:
            while True:
                result = cur.fetchone()
                if result is None:
                    break
                paths.append(result)
        cur.close()
        return paths

    def insert_meta(self, conn, md5, metadata):
        commands = [
            '''insert into meta(md5, metadata) values(%s,%s);'''
        ]
        retcode, _, _ = self._execute(commands, (md5, json.dumps(metadata),),
                                      conn=conn, commit=False)
        return retcode

    def lookup_meta(self, conn, md5):
        commands = [
            '''select * from meta where md5=%s;''',
        ]
        retcode, _, cur = self._execute(commands, (str(md5),), conn=conn)

        metadata = None
        if retcode:
            row = cur.fetchone()
            if row is not None:
                metadata = row[0]
        cur.close()
        return metadata

    def bulk_insert(self, conn, path_rows=None, meta_rows=None):
        cur = conn.cursor()
        if path_rows:
            cur.copy_expert("copy path from stdin with delimiter ',' csv",
                            path_rows)
        if meta_rows:
            cur.copy_expert("copy meta from stdin with delimiter ',' csv",
                            meta_rows)
        conn.commit()
