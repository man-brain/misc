#!/usr/bin/python

import subprocess
import time
import json
import os
import sys
import re
import psycopg2
import logging
import datetime

from barman import lockfile

def get_diagnose():
    cmd = 'barman diagnose'
    answer = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE).communicate()[0]
    return json.loads(answer)

def get_list_of_servers(bd):
    return bd['servers'].keys()

def get_last_backup(bd, server):
    backups_info = bd['servers'][server]['backups']
    last = None
    for backup in backups_info.keys():
        if time.strftime('%Y%m%d') in backup \
                and backups_info[backup]['status'] == 'DONE':
            last = backup
    return last

def rsync_pgdata(bd, server, backup):
    barman_home = bd['global']['config']['barman_home']
    from_path = os.path.join(barman_home, server, 'base', backup, 'pgdata/')
    to_path = os.path.join(barman_home, server, 'pgdata')
    cmd = 'rsync -a %s %s' % (from_path, to_path)
    logging.debug(cmd)
    res = subprocess.call(cmd, shell=True)
    if res != 0:
        return None
    return to_path

def link_wals(bd, server, backup, path):
    begin_wal = bd['servers'][server]['backups'][backup]['begin_wal']
    end_wal = bd['servers'][server]['backups'][backup]['end_wal']
    timeline = begin_wal[:8]
    begin_epoch = int(begin_wal[8:-8], 16)
    end_epoch = int(end_wal[8:-8], 16)

    xlogs_dir = os.path.join(path, 'barman_xlogs')
    if not os.path.exists(xlogs_dir):
        os.mkdir(xlogs_dir)
    for i in xrange(begin_epoch, end_epoch+1):
        cmd = 'cp -rl %s %s' % (path.replace('pgdata', 'wals/%s%08X/*' % (timeline, i)), xlogs_dir)
        logging.debug(cmd)
        res = subprocess.call(cmd, shell=True, stderr=sys.stderr, stdout=sys.stdout)
        if res != 0:
            return res
    return 0

def hack_configs(path):
    cmd = """ sed -i %s/conf.d/postgresql.conf -e "/^shared_preload_libraries/s/'.*'/''/" """ % path
    res = subprocess.call(cmd, shell=True, stderr=sys.stderr, stdout=sys.stdout)
    if res != 0:
        return res

    cmd = """ sed -i %s/conf.d/postgresql.conf -e "/^shared_buffers/s/=\ .*$/=\ 4GB/" """ % path
    res = subprocess.call(cmd, shell=True, stderr=sys.stderr, stdout=sys.stdout)
    if res != 0:
        return res

    cmd = """ sed -i %s/conf.d/postgresql.conf -e "/^stats_temp_directory/s/\ =\ .*$/\ =\ \'pg_stat_tmp\'/" """ % path
    res = subprocess.call(cmd, shell=True, stderr=sys.stderr, stdout=sys.stdout)
    if res != 0:
        return res

    escaped = path.replace('/', '\/') + '\/conf.d\/pg_hba.conf'
    cmd = """ sed -i %s/conf.d/postgresql.conf -e "/^hba_file/s/'.*'/'%s'/" """ % (path, escaped)
    res = subprocess.call(cmd, shell=True, stderr=sys.stderr, stdout=sys.stdout)
    if res != 0:
        return res

    cmd = """echo -e "recovery_target = 'immediate'\nrestore_command = 'cp %s/%%f %%p'" >%s/recovery.conf """ % (os.path.join(path, 'barman_xlogs'), path)
    res = subprocess.call(cmd, shell=True, stderr=sys.stderr, stdout=sys.stdout)
    if res != 0:
        return res
    return 0

def start_postgres(bd, server, backup, path):
    version = get_pg_version(bd, server, backup)
    cmd = '/usr/pgsql-%s/bin/pg_ctl start -D %s' % (version, path)
    logging.debug(cmd)
    res = subprocess.call(cmd, shell=True, stderr=sys.stderr, stdout=sys.stdout)
    return res

def get_pg_version(bd, server, backup):
    full_version = bd['servers'][server]['backups'][backup]['version']
    version = '%d.%d' % (full_version/10000, full_version/100 % 100)
    return version

def get_conn_string(bd, server):
    conninfo = bd['servers'][server]['config']['conninfo']
    hostinfo = re.match('host=[0-9a-z_\.-]*', conninfo).group(0)
    conninfo = conninfo.replace(hostinfo, 'host=localhost')
    return conninfo

def check_consistency_of_one_backup(bd, server, backup):
    if backup:
        path = rsync_pgdata(bd, server, backup)
        if path is None:
            logging.error('Could not rsync pgdata for %s. Skipping it.' % server)
            return 1, None
        res = link_wals(bd, server, backup, path)
        if res != 0:
            logging.error('Could not link xlogs for %s. Skipping it.' % server)
            return 2, None
        res = hack_configs(path)
        if res != 0:
            logging.error('Could not hack configs for %s. Skipping it.' % server)
            return 3, None
        res = start_postgres(bd, server, backup, path)
        if res != 0:
            logging.error('Could not start PostgreSQL for %s. Skipping it.' % server)
            return 4, None

        conninfo = get_conn_string(bd, server)
        for i in xrange(1, 360): # 6 hours
            try:
                time.sleep(60)
                conn = psycopg2.connect(conninfo)
                cur = conn.cursor()
                cur.execute('SELECT 42;')
                if cur.fetchone()[0] == 42:
                    logging.info('Backup %s for %s is OK.' % (backup, server))
                    return 0, path
            except Exception as err:
                if 'the database system is starting up' not in err:
                    logging.warning(err)
                continue
        logging.error('PostgreSQL has not reached consistent state for %s after 6 hours.' % server)
        return 5, None
    else:
        logging.error('Seems that last good backup has been done not today. Skipping server %s.' % server)
        return 6, None

def drop_deployed_backup(bd, server, backup, path):
    version = get_pg_version(bd, server, backup)
    cmd = '/usr/pgsql-%s/bin/pg_ctl stop -m immediate -D %s' % (version, path)
    logging.debug(cmd)
    res = subprocess.call(cmd, shell=True, stderr=sys.stderr, stdout=sys.stdout)
    if res != 0:
        return res
    time.sleep(5)
    cmd = 'rm -rf %s' % path
    logging.debug(cmd)
    res = subprocess.call(cmd, shell=True, stderr=sys.stderr, stdout=sys.stdout)
    return res

def init_logging(bd):
    level = getattr(logging, 'DEBUG')
    filename = bd['global']['config']['log_file'].replace('barman.log', 'consistency.log')
    root = logging.getLogger()
    root.setLevel(level)
    _format = logging.Formatter("%(levelname)s\t%(asctime)s\t\t%(message)s")
    _handler = logging.FileHandler(filename)
    _handler.setFormatter(_format)
    _handler.setLevel(level)
    root.handlers = [_handler, ]


if __name__ == '__main__':
    barman_data = get_diagnose()
    init_logging(barman_data)

    with lockfile.LockFile('/tmp/consistency_check.lock') as locked:
       if not locked:
           logging.warning('Another process is checking backups already. Exiting.')
           sys.exit(0)

       status_file_path = '/tmp/check_backup_consistency.status'
       if os.path.exists(status_file_path):
           status_file = open(status_file_path, 'r')
           ts, status, description = status_file.read().rstrip().split(';')
           last = datetime.datetime.fromtimestamp(float(ts))
           current_date = datetime.datetime.today()
           day_start = current_date.combine(current_date.date(), current_date.min.time())
           if last > day_start:
               logging.info('Backups have already been checked today. Not doing anything.')
               sys.exit(0)
           status_file.close()

       problems = []
       for server in get_list_of_servers(barman_data):
           backup = get_last_backup(barman_data, server)
           res, path = check_consistency_of_one_backup(barman_data, server, backup)
           if res != 0:
               problems.append(server)
           else:
               drop_deployed_backup(barman_data, server, backup, path)

       status = 0
       msg = 'All backups are consistent. Good boy!'
       if len(problems) != 0:
           problems.sort()
           status = 1
           msg = 'Clusters with failed backups are %s. Take a look at them.' % ', '.join(problems)

       logging.info(msg)
       status_file = open(status_file_path, 'w')
       status_file.write('%d;%d;%s\n' % (int(time.time()), status, msg))
       status_file.close()
