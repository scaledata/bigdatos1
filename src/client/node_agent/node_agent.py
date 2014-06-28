#!/usr/bin/python

###################################################
# BigDatos Node Agent implementation
# Copyright (c) DatosCloud, Inc 2014.
###################################################

# Imports
###############

# Logging/rabbit imports
import sys
import pika
import logging

# Thread related imports
import threading

# Time related imports
import datetime
import time

# BigDatos Constants
import datos_constants

# BigDatos modules
import policy_table

# Swift backend
from swift_backend import swift_backend

# Other imports
import argparse
import collections
import subprocess
import math

# Local includes
import common

# Globals
import g

# Init globals ###################################
# Constants (relatively) ############
g.node_agent_start_time = 0
g.epoch_time = 0
g.channel = 0

g.node_agent_str = "node agent>" # Move
g.app_listener_is_alive = False
g.enable_hack = False

# ip address hardcoded to VM1
g.node_agent_ip = "10.0.0.3"

# Globals shared between threads ######
g.should_shutdown = False
g.mlist_thread = 0
g.g_exp_thread = 0

g.policy_table = policy_table.policy_table()
g.policy_table_lock = threading.Lock()
############# End of global section ##############

# TO DO:
def on_connected(connection):
    # Called when we are fully connected to RabbitMQ
    print "RabbitMQ connected"
    connection.channel(on_channel_open)

# TO DO:
def on_channel_open(new_channel):
    # Called when our channel has opened
    print "channel open"
    g.channel = new_channel
    g.channel.queue_declare(queue=datos_constants.NODE_AGENT_QUEUE_NAME,
                          passive=True, # HACK NC: 
                          durable=True, 
                          exclusive=False, 
                          auto_delete=False, 
                          callback=on_queue_declared)

# TO DO:
def on_queue_declared(frame):
    # Called when RabbitMQ has told us our Queue has been declared, frame is the response from RabbitMQ
    print "queue declared"
    g.channel.basic_consume(handle_delivery, queue=datos_constants.NODE_AGENT_QUEUE_NAME)    

def handle_delivery(ch, method, header, body):
    print "handle_delivery received: ", body
    
    ch.basic_ack(method.delivery_tag)
    # print "acked message"

    if (g.enable_hack):
        print "Hacking the incoming message to be: "

        fake_timestamp = common.get_secs_since_epoch() # Say the write just came in..
        body = "operation:change_log,seqid:0,filename:/test2.bar,offset:0,write_size:100,timestamp:" + str(fake_timestamp)
        print "New message is: " + body

        #print "waiting ....."
        #time.sleep(5)
    else:
        # g.enable_hack = True
        print "Hack is not enabled.."
        
    # Put the msg body into the policy_table and signal the pe_listener to process it
    words = body.split(',')
    filename = 0
    interval = 0    

    # for word in words:
    #    print word
        
    # print "check 1"
    # word[0] Operation
    sections = words[0].split(':')
    # print "sections=", sections
    # print "sections[0]: ", sections[0], " sections[1]:", sections[1]
    
    if (sections[0] != "operation"):
        print "incorrect field - expect operation!"
        exit(1)

    if (sections[1] == "manage"):
        print "Handing manage policy msg"    
        handle_manage_policy_msg(words)
    elif (sections[1] == "change_log"):
        handle_change_log_msg(words)

def handle_manage_policy_msg(words):
    filename = 0
    interval = 0

    sections = words[0].split(':')
 
    if (sections[0] != "operation"):
        print "incorrect field - expect operation!"
        exit(1)

    if (sections[1] != "manage"):
        print "incorrect field - expect manage!"  
        exit(1)
  
    # word[1] Filename
    sections = words[1].split(':')
    if (sections[0] != "filename"):
        print "incorrect field - expect filename!"
        exit(1)
    filename = sections[1]
    
    # word[2] Interval
    sections = words[2].split(':')
    if (sections[0] != "interval"):
        print "incorrect field - expect interval!"
        exit(1)
    interval = int(sections[1])

    print "Asked to manage filename:", filename, " at interval: ", interval, "secs"

    # print "Trying to lock the policy table.."
    g.policy_table_lock.acquire()
    # print "Got policy table lock"
    g.policy_table.add_manage_policy(filename, interval)
    g.policy_table.show()
    g.policy_table_lock.release()
    # print "Released policy table lock"

    print "going to check applistener status - status = " + str(g.app_listener_is_alive)
    # If the app-listener is not alive - launch it..
    if (not g.app_listener_is_alive):
        # Launch Applistener
        print "Applistener is not alive -- going to launch it"
        
        # Real
        child = subprocess.Popen(["python", "../app_listener/thread_main.py", "10.0.0.3", "/home/dev1/hadoop_play/hadoop_store/hdfs/namenode/edit2/current"])
        # child = subprocess.Popen(["python", "./test_listener.py"])

        print "Return code was " + str(child)
        g.app_listener_is_alive = True
    else:
        print "Applistener is already alive.."

def handle_change_log_msg(words):
    print "Going to handle change_log msg.."
    print "in handle_change_log_msg()"

    # Expected format of this message is:
    # operation:change_log,seqid:%d,filename:%s,offset:<bytes>,write_size:<bytes>,timestamp:<time>

    seq_id = 0
    filename = 0
    offset = 0
    write_size = 0
    timestamp = 0
    
    sections = words[0].split(':')
 
    if (sections[0] != "operation"):
        print "incorrect field - expect operation!"
        exit(1)

    if (sections[1] != "change_log"):
        print "incorrect field - expect change_log!"  
        exit(1)
        
    # print "check1"

    # word[1] seqid
    sections = words[1].split(':')
    # print "sections[0]: ", sections[0], " sections[1]:", sections[1]
    if (sections[0] != "seqid"):
        print "incorrect field - expect seqid!"
        exit(1)
    seq_id = int(sections[1])

    # print "check2"
    # word[2] filename
    sections = words[2].split(':')
    # print "sections[0]: ", sections[0], " sections[1]:", sections[1]
    if (sections[0] != "filename"):
        print "incorrect field - expect filename!"
        exit(1)
    filename = sections[1]

    # print "check3"
    # word[3] Offset
    sections = words[3].split(':')
    if (sections[0] != "offset"):
        print "incorrect field - expect offset!"
        exit(1)
    offset = int(sections[1])

    # print "check4"
    # word[4] write_size
    sections = words[4].split(':')
    if (sections[0] != "write_size"):
        print "incorrect field - expect write_size!"
        exit(1)
    write_size = int(sections[1])

    # print "check5"
    # word[5] timestamp
    print "words5 is" + words[5]
    sections = words[5].split(':')
    print "xx"
    if (sections[0] != "timestamp"):
        print "incorrect field - expect timestamp!"
        exit(1)

    # print "sections[0] = " + sections[0] + "sections[1] = " + sections[1]
    timestamp = float(sections[1])
    # print "orig timestamp = " + str(timestamp)
    ntimestamp = math.floor(timestamp)
    # print "new timestamp = " + str(ntimestamp)

    # print "check 6"
    print "Received change_log message: seq_id=" + str(seq_id) + " filename=" + filename + " offset=" + str(offset) + " write_size=" + str(write_size) + " timestamp = " + str(timestamp)

    # print "Trying to lock the policy table.."
    g.policy_table_lock.acquire()
    print "Got lock on the policy table.."

    g.policy_table.show()
    print "going to add job to pt.."
    g.policy_table.add_job(seq_id, filename, offset, write_size, timestamp)
    g.policy_table.show()
    g.policy_table_lock.release()
    # print "Released lock on the policy table.."

def copy_from_hdfs_to_vm2(filename, send_job, new_timestamp_to_send):

    print "copy_from_hdfs_to_vm2() requested to send down job for filename: " + filename
    print "job_info:"
    send_job.show()
    print "Sending with timestamp: " + str(new_timestamp_to_send)

    # hadoop fs -copyToLocal /nc_test/test1.bar ~/temp
    print "Trying to copy filename from hadoop to local temp.."

    # Say Hdfs dir is /data
    hdfs_path = str(filename)
    print "hdfs_dir = " + hdfs_path

    child = subprocess.Popen(["hadoop", "fs", "-copyToLocal", hdfs_path, "/home/dev1/temp"])
    print "In here"

    # Wait for the file to land..
    time.sleep(5)

    # scp test1.bar dev1@10.0.0.11:/home/dev1/store_temp
    print "Trying to transfer test1.bar from local temp to swift"
    
    local_path = "/home/dev1/temp/" + filename
    print local_path

    #child = subprocess.Popen(["scp", local_path, "dev1@10.0.0.11:/home/dev1/store_temp"])
    swift_store = swift_backend('admin', 
                            'indata2d', 
                            'admin', 
                            'http://controller:35357/v2.0')
    
    swift_store.put('demo', filename, local_path)
    
    print "In here"
    
    print "Done sending the file.."

def print_thread_stats(name):
    
    # TO DO: Change to log
    print "Thread: ", name, " is alive"
       
#####################################
# Basic NodeAgent listener thread
# Accepts policies from the PE
# Accepts hdfs change logs notifications
#  from Applistener
#####################################
class main_listener(threading.Thread):
    def __init__(self, threadid, name, counter):
        threading.Thread.__init__(self)
        self.threadID = threadid
        self.name = name
        self.counter = counter
        self.connection = 0

    def run(self):
        print "In run() method of main_listener thread:", self.name

        logging.getLogger('pika').setLevel(logging.CRITICAL)    
        credentials = pika.PlainCredentials('guest', 'guest')
    
        parameters = pika.ConnectionParameters(
               g.node_agent_ip,
               5672, 
               '/',
               credentials)
        self.connection = pika.SelectConnection(parameters, on_connected)

        while (not g.should_shutdown):

            # time.sleep(5)
            print_thread_stats(self.name)
               
            try:
                # Loop so we can communicate with RabbitMQ
                self.connection.ioloop.start()
            except:
                pass
                
        # Gracefully close the connection
        # self.connection.close()
        # Loop until we're fully closed, will stop on its own
        # self.connection.ioloop.start()

        print "main_listener thread:", self.name, " is going to shutdown.. - bye"

#####################################
# NodeAgent xport_process_thread
#####################################
class xport_process_thread(threading.Thread):
    def __init__(self, threadid, name, counter):
        threading.Thread.__init__(self)
        self.threadID = threadid
        self.name = name
        self.counter = counter
        self.connection = 0

    def run(self):
        print "In run() method of export process thread:" + self.name

        while (not g.should_shutdown):

            time.sleep(5)
            print_thread_stats(self.name)
            
            print "export process thread looking through policy table"
            g.policy_table_lock.acquire()
            print "xport thread got policy table lock"
            
            num_policies = g.policy_table.get_num_policies()
            print "Policy table has " + str(num_policies) + " policies"

            current_time = common.get_secs_since_epoch()
            elapsed_time = current_time - g.node_agent_start_time

            print "Elapsed time since start (of Node agent) is " + str(elapsed_time) + " secs"
 
            keys = g.policy_table.get_keys()
            for key in keys:
                pentry = g.policy_table.get_policy(key)
                print "Looking at policy:"
                pentry.show()

                # Find out if we have a new "era" for this policy
                # Note, this does not mean that we have something to send 
                #  - it just means that it is time to see if there were some
                # some changes in the timeframe since we last sent down changes
                # for this file.

                time_since_last_sent_interval = common.get_secs_since_epoch() - pentry.last_timestamp_sent
                print "time_since_last_sent_interval = " + str(time_since_last_sent_interval)
                
                if (time_since_last_sent_interval >= pentry.interval):

                    if (pentry.last_timestamp_sent != 0):
                        new_timestamp_to_send = pentry.last_timestamp_sent + pentry.interval
                        print "Old era for this policy is over: need to send data with timestamp: " + str(new_timestamp_to_send) + " secs"
                    else:
                        new_timestamp_to_send = common.get_secs_since_epoch()
                        print "Old era for this policy is over: need to send data with timestamp: " + str(new_timestamp_to_send) + " secs (first send of file)"

                    # Check to see if there are any jobs that have come in the last era                                                
                    print "At start, (pending jobs q size is): " + str(len(pentry.pending_jobs))        
                    while (len(pentry.pending_jobs) != 0):
                        job = pentry.pending_jobs[0]
                        print "Looking at job:"
                        job.show()
                        
                        if (job.timestamp <= new_timestamp_to_send):
                            print "Found job in the old era -- need to send it down.."
                            
                            pentry.last_timestamp_sent = new_timestamp_to_send
                            print "ready to send job.."
                            time.sleep(5)

                            # Dequeue the job from the left (increasing time order)
                            send_job = pentry.pending_jobs.popleft()
                            # TO DO: Validate we popped the correct job out
                            
                            print "Popped out job (pending jobs q size is): " + str(len(pentry.pending_jobs)) + " -- going to send it"
                            copy_from_hdfs_to_vm2(pentry.filename, send_job, new_timestamp_to_send) 
                            
                else:
                    print "Nothing to do here -- policy is still in its current era.."

            g.policy_table_lock.release()
            print "xport thread released policy table lock"

        print "xport_process_thread thread:" + self.name + " is going to shutdown.. - bye"    

########################
# Node agent startup
# Starts up threads needed
# Allocs resources..
########################
def startup():
    print "Node Agent startup"

    g.should_shutdown = False

    # Global section 2

    # Set the epoch start time and start time of the node agent
    g.epoch_time = datetime.datetime.utcfromtimestamp(0)   
    g.node_agent_start_time = common.get_secs_since_epoch()

    print "Node agent start time is set to: " + str(g.node_agent_start_time) + " secs (since epoch start)"

    print "going to launch PL_thread-1.."
    g.mlist_thread = main_listener(1, "PL_thread-1", 1)   
    g.mlist_thread.start()

    print "going to launch xport_process_thread-1.."
    g.exp_thread = xport_process_thread(1, "EXP_thread-1", 1)   
    g.exp_thread.start()

def main_work():
    print "Node Agent main_work()"
    
    # state = started
    state = 1 # started

    while True:
        sys.stdout.write(g.node_agent_str)
        # logging.debug("foo global = " + str(foo.test_val))
        userline = sys.stdin.readline().rstrip('\n')
        if (userline == 'quit'):
            break

        # Every 20 sec, send a heartbeat (with state, message order) to the Policy engine
        # type=node_agent_heartbeat, seq_id=<num>
        
def shutdown():
    print "Node Agent shutting down.."
    
    # Let all threads know they should finish
    g.should_shutdown = True

    # Need to stop the rabittmq ioloop
    print "Stopping the IOloop in startup()"
    g.mlist_thread.connection.ioloop.stop()
 
    # Wait for all threads to finish
    g.mlist_thread.join()
    g.exp_thread.join()
    
    # Stop the applistener
    if (g.app_listener_is_alive):
        print "App listener was alive -- need to stop it.."

        hb_str = "operation:heartbeat,status:shutdown"
        # ip address hardcoded to VM1
        dest_ip = "10.0.0.3"

        hb_queue_name = datos_constants.APPLISTENER_HEARTBEAT_QUEUE;
        logging.getLogger('pika').setLevel(logging.CRITICAL)
        credentials = pika.PlainCredentials('guest', 'guest')
    
        hb_connection = pika.BlockingConnection(pika.ConnectionParameters(
                dest_ip,
                5672, 
                '/',
                credentials))
        hb_channel = hb_connection.channel()   
        hb_channel.queue_declare(queue=hb_queue_name)
        hb_channel.basic_publish(exchange='',
                              routing_key=hb_queue_name,
                              body=hb_str)
    
        print "Finished send Shutdown(HB) message to Applistener " + hb_str      
        
        # NC: Fix this..
        hb_connection.close()
    else:
        print "App listener not alive.. done"

########################
# Main
########################
print "Welcome to BigDatos NodeAgent version (", datos_constants.VERSION, ")"

startup()
main_work()
shutdown()

print "Goodbye.."


