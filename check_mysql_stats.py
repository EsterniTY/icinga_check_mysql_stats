#!/usr/bin/python3.9 -u
# -*- coding: utf-8 -*-
import argparse
import sys
import pymysql
import time
import os.path
import tempfile
import hashlib

__version__ = '1.0.0'
counters = ['Connections', 'Queries', 'Questions']
fields = {
    'Connections': 'Connections per second',
    'Queries': 'Queries per second',
    'Questions': 'Questions per second'
}

def read_old_data(file): 
    olddata = {}

    if not os.path.exists(file):
        return {}

    try:
        with open(file, 'r') as f:
            header = f.readline().split('|')
            now = int(time.time())

            if header[0] == __version__ and int(header[1]) < now:
                olddata['__timedelta__'] = now - int(header[1])
                for row in f:
                    clean = row.strip().split(':')
                    olddata[clean[0]] = int(clean[1])
            f.close()
    except IOError as e:
        print('[CRITICAL] Error reading cache data: %s' % e.args[1])
        exit(2)

    return olddata

def read_data(args):
    data = {}

    try:
        db = pymysql.connect(host=args.hostname, user=args.username, password=args.password, db='sys', cursorclass=pymysql.cursors.DictCursor)
        c = db.cursor()

        c.execute("SELECT Variable_name, Variable_value FROM performance_schema.global_status WHERE Variable_name IN ('Connections', 'Queries', 'Questions', 'Uptime', 'Open_files', 'Open_tables', 'Table_locks_waited', 'Threads_connected', 'Threads_running','Threads_connected')")

        for row in c.fetchall():
            data[row['Variable_name']] = row['Variable_value']

        c.close()
    except pymysql.err.OperationalError as e:
        print('[CRITICAL] %s' % e.args[1])
        exit(2)

    return data

def write_data(file, data):
    try:
        with open(file, 'w') as f:
            f.write('%s|%s\n' %(__version__, int(time.time())))

            for key in counters:
                f.write('%s:%s\n' % (key, data[key]))

            f.close()
    except IOError as e:
        print('[CRITICAL] Error writing cache data: %s' % e.args[1])
        exit(2)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('-V', '--version', action='version', version='%(prog)s v' + sys.modules[__name__].__version__)
    parser.add_argument('-H', '--hostname', help='The host address of the MySQL server', required=True)
    parser.add_argument('-u', '--username', help='MySQL user', required=True)
    parser.add_argument('-p', '--password', help='MySQL password')
    args = parser.parse_args()

    suffix = '%s:%s:%s' % (os.getlogin(), args.hostname, args.username)
    tmp_file = '%s/%s.%s.dat' % (tempfile.gettempdir(), os.path.basename(__file__), hashlib.md5(suffix.encode('utf-8')).hexdigest())

    olddata = read_old_data(tmp_file)
    data = read_data(args)
    msgdata = []
    perfdata = []

    if len(data) == 0:
        print('[CRITICAL] No data recieved')
        exit(2)

    write_data(tmp_file, data)

    if len(olddata) == 0:
        print('[UNKNOWN] Collecting data')
        exit(3)
  
    for key in data:
        k = fields[key] if key in fields else key.lower().capitalize().replace('_', ' ')
        if key in counters:
            v = int((int(data[key]) - int(olddata[key])) / olddata['__timedelta__'])
            msgdata.append('\_ [%s] %s = %s' % ('OK', k, v))
            perfdata.append('%s=%s;;;' % (k.lower().replace(' ', '_'), v))
        else:
            if key != 'Uptime':
                msgdata.append('\_ [%s] %s = %s' % ('OK', k, data[key]))

        perfdata.append('%s=%s;;;' % (key.lower().replace(' ', '_'), data[key]))

    if len(perfdata):
        perfdata.append('%s=%s;;;' % ('timedelta', olddata['__timedelta__']))
        d = int(data['Uptime'])
        t = time.gmtime(d);
        if d < 60:
            f = time.strftime('%S', t)
        elif d < 3600:
            f = time.strftime('%M:%S', t)
        elif d < 86400:
            f = time.strftime('%H:%M:%S', t)
        else:
            f = "%s day(s) %s" % (d//86400, time.strftime('%H:%M:%S', t))

        print('[%s] Uptime: %s\n%s|%s' % (
            'OK', 
            f, 
            '\n'.join(msgdata), 
            ' '.join(perfdata))
        )
    else:
        print('\n'.join(msgdata))
