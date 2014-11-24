#!/bin/env python
import os
import sys
import time
import signal
import sqlite3 as lite
import socket
import datetime
from daemon import Daemon

def get_valid_tasks(tasks, a_dir, pid_min):
    subdirs = os.listdir(a_dir)
    pid_me = os.getpid()
    for name in subdirs:
        if os.path.isdir(os.path.join(a_dir, name)):
            if name.isdigit():
               pid_num = int(name)
               if pid_num > pid_min and pid_num != pid_me:
                 tasks.append(pid_num)

def parse_iostat(line, task_iostat):
    if 'rchar' in line: 
        task_iostat['rchar'] = int(line[6:])
    elif 'wchar' in line:
        task_iostat['wchar'] = int(line[6:])
    elif 'syscr' in line:
        task_iostat['syscr'] = int(line[6:])
    elif 'syscw' in line:
        task_iostat['syscw'] = int(line[6:])
    elif 'read_bytes' in line:
        task_iostat['rbytes'] = int(line[12:])
    elif 'write_bytes' in line[0:11]:
        task_iostat['wbytes'] = int(line[13:])
    else:
        return 0

def log_task_iostat(iter, task, db_conn):
    proc_task_prefix = '/proc/' + str(task) + '/'
    iostat_file = os.path.join(proc_task_prefix + 'io')
    #print 'reading ' + iostat_file
    try:
        proc = os.readlink(proc_task_prefix + 'exe')
    except OSError:
        proc = 'NULL'
        pass
    #print 'cmdline: ' + proc
    task_iostat = {}
    col_idx = -1
    with open(iostat_file, "r") as f:
        for line in f:
            parse_iostat(line, task_iostat)
    f.close()
    rchar = wchar = rbytes = wbytes = syscr = syscw = 0
    rchar = task_iostat['rchar']
    wchar = task_iostat['wchar']
    rbytes = task_iostat['rbytes']
    wbytes = task_iostat['wbytes']
    syscw = task_iostat['syscw']
    syscr = task_iostat['syscr']
    #print 'rchar: ' + str(rchar)
    #print 'wchar: ' + str(wchar)
    #print 'rbytes: ' + str(rbytes)
    #print 'wbytes: ' + str(wbytes)
    #print 'syscr: ' + str(syscr)
    #print 'syscw: ' + str(syscw)
    db_conn.execute('''INSERT INTO iostat VALUES(?, ?, ?, ?, ?, ?, ?, ?)''',
                 (iter, proc, 
                 wchar, rchar, wbytes, rbytes, syscw, syscr))
    db_conn.commit()

def get_topN_tasks(N, tasks, pick_rule):
    sz_limit = 100 * 1024 #100K
    tgt_tasks = []
    pre_sort_tasks = []
    for t in tasks:
        rw_cnt = 0
        with open('/proc/' + str(t) + '/io', "r") as f:
            for line in f:
                if 'read_bytes' in line and (pick_rule == 0 or pick_rule == 2): 
                    rw_cnt += int(line[12:])
                elif 'write_bytes' in line[0:11] and (pick_rule == 1 or pick_rule == 2): 
                    rw_cnt += int(line[13:])
                else:
                    pass
        f.close()
        if rw_cnt > sz_limit:  # qualified for record, TODO: retrieve db, calculate the increase
            pre_sort_tasks.append([rw_cnt, t])
     
    tgt_tasks = sorted(pre_sort_tasks, reverse=True, key=lambda x: (x[0], x[1]))

    if N > len(tgt_tasks):
       return tgt_tasks
    tgt_tasks = tgt_tasks[:N]
    return tgt_tasks
       

# create a table and insert data in
# table fmt:
# round no | exe name | wchar | rchar | syscallr |syscallw | wbytes | rbytes





def main_function():
    db_conn = None
    cur = None
    timestamp_file = './timestamp.txt'
    def signal_handler(sig, frame):
        print('Ctrl+C catched, abort ...')
        if db_conn is not None:
            cur.close()
            db_conn.close()
        sys.exit(0)

    # initialize signal handler
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # create timestamp and pid files
    try:
        os.remove(timestamp_file) 
    except OSError:
        pass
    
    f = open(timestamp_file, 'w+')
    now = datetime.datetime.now()
    f.write(str(now))
    f.close()

    # initialize db
    db_name = 'iostat__' + socket.gethostname() + '.db'
    try:
        os.remove('./' + db_name) 
    except OSError:
        pass

    db_conn = lite.connect(db_name)
    cur = db_conn.cursor()
    cur.execute('''DROP TABLE IF EXISTS iostat''')
    cur.execute('''CREATE TABLE iostat(iter INT, proc VARCHAR(1024), wchar INT, rchar INT, wbytes INT, rbytes INT, callw INT, callr INT)''')

    # mainloop
    iter = 1 #round #1
    while True:
        tasks = []
        get_valid_tasks(tasks, '/proc', pid_min)
        tgt_tasks = get_topN_tasks(top_N, tasks, pick_rule) # pickup top N tasks
        #for t in tgt_tasks:
        #    print str(t[1]) + '\t' + str(t[0]) 


        for task in tgt_tasks:
            log_task_iostat(iter, task[1], db_conn)
        time.sleep(interval) # every N seconds
        iter = iter + 1

class iosampler(Daemon):
    def run(self):
        main_function()

if __name__ == '__main__' :
#    main_function()
    # param handling
    if len(sys.argv) != 5:
        print 'Usage ' + sys.argv[0] + ' pid_min(numeric) r|w|rw top_N(numeric) interval(seconds)'
        sys.exit()

    pid_min = sys.argv[1]
    # print pid_min
    if not pid_min.isdigit():
        print 'Usage ' + sys.argv[0] + ' pid_min(numeric) r|w|rw top_N(numeric) interval(seconds)'
        sys.exit()
    pid_min = int(pid_min)        
    if pid_min < 2800:
        pid_min = 2800

    # read-most/write-most/read-write most
    pick_rule = sys.argv[2]
    # print pick_rule
    rule_ext = None
    if (not pick_rule == "r") and (not pick_rule == "w") and (not pick_rule == "rw"):
        print 'Usage ' + sys.argv[0] + ' pid_min(numeric) r|w|rw top_N(numeric) interval(seconds)'
        sys.exit()
    if pick_rule == "r":
       rule_ext = 'read most'
       pick_rule = 0
    elif pick_rule == "w":
       rule_ext = 'write most'
       pick_rule = 1
    else:
       rule_ext = 'write + read most'
       pick_rule = 2
    
    # top N
    top_N = sys.argv[3]
    # print top_N
    if not top_N.isdigit():
        print 'Usage ' + sys.argv[0] + ' pid_min(numeric) r|w|rw top_N(numeric) interval(seconds)'
        sys.exit()
    top_N = int(top_N)        
    if top_N > 30:
        top_N = 30

    # interval
    interval = sys.argv[4]
    # print interval
    if not interval.isdigit():
        print 'Usage ' + sys.argv[0] + ' pid_min(numeric) r|w|rw top_N(numeric) interval(seconds)'
        sys.exit()
    interval = int(interval)        
    if interval < 5:
        interval = 5

    # prompt current configuration
    print 'I/O workload sample settings:'
    print '(1) top N tasks to pick: ' + str(top_N)
    print '(2) minimal pid number:  ' + str(pid_min)
    print '(3) rule to pick task:   ' + rule_ext
    print '(4) sampling interval:   ' + str(interval) + ' seconds'


    iosampler_inst = iosampler('/var/run/iosampler.pid')
    iosampler_inst.start()
