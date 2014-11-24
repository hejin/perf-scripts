#!/bin/env python
import os
import sys
import time
import signal
import sqlite3 as lite
import socket
import datetime
from daemon import Daemon

top_N     = 0
pid_min   = 0
pick_rule = 0
interval  = 0
all_task_iostat = {}

def get_valid_tasks(a_dir):
    global pid_min
    tasks = []

    try:
        subdirs = os.listdir(a_dir)
        pid_me = os.getpid()
        for name in subdirs:
            if os.path.isdir(os.path.join(a_dir, name)):
                if name.isdigit():
                    pid_num = int(name)
                    if pid_num > pid_min and pid_num != pid_me:
                        tasks.append(pid_num)
    except OSError:
        pass

    return tasks


# get iostat for this task from /proc/xxx/io
def get_task_iostat(now, task):
    proc_task_prefix = '/proc/' + str(task) + '/'
    iostat_file = os.path.join(proc_task_prefix + 'io')
    #print 'reading ' + iostat_file
    try:
        proc = os.readlink(proc_task_prefix + 'exe')
    except OSError:
        proc = 'NULL'
        pass
    task_iostat = {}
    task_iostat['pid'] = task
    task_iostat['exec'] = proc
    task_iostat['time'] = now
    task_iostat['rchar'] = 0
    task_iostat['wchar'] = 0
    task_iostat['read_bytes'] = 0
    task_iostat['write_bytes'] = 0
    task_iostat['syscw'] = 0
    task_iostat['syscr'] = 0
    try:
        with open(iostat_file, "r") as f:
            for line in f:
                name, value = line.split(': ', 2)
                task_iostat[name] = int(value)
        f.close()
    except OSError:
        pass
    except IOError:
        pass

    return task_iostat



# Print process i/o stat in the following format:
# round # | pid | time | wchar | rchar | syscallr |syscallw | wbytes | rbytes | exe name
def show_iostat(iter):
    global all_task_iostat

    tmp = all_task_iostat.items()

    # if we want to sort the results according to your preference, please modify.
    sort_by1 = "wchar" # for possible values, please see below
    sort_by2 = "rchar" # for possible values, please see below
    tmp = sorted(tmp, key=lambda d: (d[1][sort_by1], d[1][sort_by2]))

    print "round    pid   time                       wchar        rchar        syscallr   syscallw   wbytes       rbytes       exe_name"
    for ios in tmp:
        iostat = ios[1]
        pid    = iostat['pid']
        rchar  = iostat['rchar']
        wchar  = iostat['wchar']
        rbytes = iostat['read_bytes']
        wbytes = iostat['write_bytes']
        syscw  = iostat['syscw']
        syscr  = iostat['syscr']
        proc   = iostat['exec']
        now    = iostat['time']
        print ("%-8d %-5d %-26s %-12d %-12d %-10d %-12d %-12d %-10d %s") % \
               (iter, pid, now, wchar, rchar, syscr, syscw, wbytes, rbytes, proc)
        # TODO iostats can be saved to db or anywhere else.
        # TODO we can also produce incremental delta here, or this can be re-processed
        #      later by other tools, e.g. by grep/awk, some iostat of specific process
        #      can be analyzed.


# filter out the top N tasks with the max changed r/w
def get_topN_tasks(now, tasks):
    global top_N
    global pick_rule

    # XXX we can return all tasks here. No need to filter anything out.
    return tasks

    sz_limit = 100 * 1024 #100K
    tgt_tasks = []
    pre_sort_tasks = []
    results = []
    for t in tasks:
        rw_cnt = 0
        task_iostat = get_task_iostat(now, t)
        if (pick_rule == 0 or pick_rule == 2):
            rw_cnt = task_iostat['read_bytes'];
        if (pick_rule == 1 or pick_rule == 2):
            rw_cnt = task_iostat['write_bytes']
        else:
            pass
        if rw_cnt > sz_limit:  # qualified for record, TODO: retrieve db, calculate the increase
            pre_sort_tasks.append([rw_cnt, t])

    tgt_tasks = sorted(pre_sort_tasks, reverse=True, key=lambda x: (x[0], x[1]))

    if top_N < len(tgt_tasks):
        tgt_tasks = tgt_tasks[:top_N]
    for t in tgt_tasks:
        results.append(t[1]);
    return results;


# main loop
def main_function():
    global interval

    # mainloop
    iter = 1 #round #1
    while True:
        now = datetime.datetime.now()
        tasks = get_valid_tasks('/proc')
        tgt_tasks = get_topN_tasks(now, tasks) # pickup top N tasks

        for task in tgt_tasks:
            task_iostat = get_task_iostat(now, task)
            all_task_iostat[task] = task_iostat

        show_iostat(iter)

        iter = iter + 1
        time.sleep(interval) # every N seconds
    # end of while



def print_usage_and_exit():
    print 'Usage ' + sys.argv[0] + ' pid_min(numeric) r|w|rw top_N(numeric) interval(seconds)'
    sys.exit()

def args_parse():
    global top_N
    global pid_min
    global pick_rule
    global interval
    # param handling
    if len(sys.argv) != 5:
        print_usage_and_exit()

    pid_min = sys.argv[1]
    if not pid_min.isdigit():
        print_usage_and_exit()

    pid_min = int(pid_min)
    if pid_min < 2800:
        pid_min = 2800

    # read-most/write-most/read-write most
    pick_rule = sys.argv[2]
    rule_ext = ""
    if (not pick_rule == "r") and (not pick_rule == "w") and (not pick_rule == "rw"):
        print_usage_and_exit()

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
        print_usage_and_exit()

    top_N = int(top_N)
    if top_N > 30:
        top_N = 30

    # interval
    interval = sys.argv[4]
    # print interval
    if not interval.isdigit():
        print_usage_and_exit()

    interval = int(interval)
    if interval < 5:
        interval = 5

    verbose = 0
    if (verbose):
        print 'I/O workload sample settings:'
        print '(1) top N tasks to pick: ' + str(top_N)
        print '(2) minimal pid number:  ' + str(pid_min)
        print '(3) rule to pick task:   ' + rule_ext
        print '(4) sampling interval:   ' + str(interval) + ' seconds'


def signal_handler(sig, frame):
    sys.exit(0)

if __name__ == '__main__' :
    # initialize signal handler
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    args_parse()
    main_function()
