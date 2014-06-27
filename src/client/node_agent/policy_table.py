#!/usr/bin/python

###################################################
# BigDatos Node Agent Policy table implementation
# Copyright (c) DatosCloud, Inc 2014.
###################################################

import collections
import logging 

# import foo

logging.basicConfig(level=logging.DEBUG,
                    format ='[%(levelname)s] (%(threadName)-10s) %(message)s',
                   )  

# The policy_table in not thread-safe
class policy_table:
    def __init__(self):
        logging.debug("In policy_table init..")
        self.table = {}

    def add_manage_policy(self, filename, interval):
        # print "add_manage_policy - 1 - interval=", interval
        if self.table.has_key(filename):
            print "Policy already exists!"
            exit(1)

            # Handle this
            # What about existing entries in the pending and in_progress tables

        else:
            print "Policy table: adding manage policy - filename:", filename, "interval:", interval
            pentry = policy_entry(filename, interval)
            # print "Got a policy entry.."
            self.table[filename] = pentry
            # print "Added policy entry to table"

    def show(self):       

        logging.debug("NC: New - Policy table currently has " + str(len(self.table)) + " entries")
        print "---------------------------------------------------------------"
        count = 0
        for key in self.table:
            pentry = self.table[key]
            print "Entry " + str(count) + ":"
            pentry.show()
            count = count + 1
        print " "
        print " "

    def add_job(self, seq_id, filename, offset, write_size, timestamp):

        print "Policy_table: Adding job for filename:" + filename
        if (not self.table.has_key(filename)):
            print "Error: Policy table does not have policy for filename" + filename
            exit(1)
        
        # print "dd - 1"
        pentry = self.table[filename]
        # print "found policy_entry.."
        pentry.show()

        pentry.add_job(int(offset), int(write_size), int(timestamp))

# The policy entry is not thread safe
class policy_entry:
    def __init__(self, filename, interval):
        # print "In policy_entry init"
        self.filename = filename
        self.interval = interval

        # logging.debug("foo global = " + str(foo.test_val))

        # print "d - 1"
        self.pending_jobs = collections.deque()
        
        # print "d -2"
        self.in_progress_jobs = collections.deque()

        # print "done policy_entry init"

        # foo.test_val = foo.test_val + 1
    def show(self):
        # print "Policy_entry show:"
        # print "Policy_entry start ...."

        # NC: HACK here..
        print "filename:" + self.filename + " interval:" + str(self.interval)
        
        print "pending_jobs queue has ", len(self.pending_jobs), "jobs"
        if (len(self.pending_jobs) != 0):
            for job in self.pending_jobs:                
                job.show()
        print "in_progress_jobs queue has ", len(self.in_progress_jobs), "jobs"    
        if (len(self.in_progress_jobs) != 0):
            for job in self.in_progress_jobs:
                job.show()
        
    def add_job(self, offset, write_size, timestamp):       
        job = work_job(offset, write_size, timestamp)
        self.pending_jobs.appendleft(job)

# The job is not thread safe
class work_job:
    def __init__(self, offset, write_size, timestamp):        
        self.offset = offset
        self.write_size = write_size
        self.timestamp = timestamp
        
    def show(self):
        print "job -- offset:" + str(self.offset) + " write_size: " + str(self.write_size) + " timestamp: " + str(self.timestamp)

