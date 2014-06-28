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
# import foo

# BigDatos modules
import policy_table

# Swift backend
from swift_backend import swift_backend

# Other imports
import argparse
import collections
import subprocess

# Global section 
node_agent_str = "node agent>"
app_listener_is_alive = False
enable_hack = False

# ip address hardcoded to VM1
node_agent_ip = "10.0.0.3"

# Globals shared between threads
should_shutdown = False
mlist_thread = 0
policy_table = policy_table.policy_table()
policy_table_lock = threading.Lock()

# TO DO:
def on_connected(connection):
    # Called when we are fully connected to RabbitMQ
    print "RabbitMQ connected"
    connection.channel(on_channel_open)

# TO DO:
def on_channel_open(new_channel):
    # Called when our channel has opened
    global channel
    print "channel open"
    channel = new_channel
    channel.queue_declare(queue=datos_constants.NODE_AGENT_QUEUE_NAME,
                          passive=False, 
                          durable=True, 
                          exclusive=False, 
                          auto_delete=False, 
                          callback=on_queue_declared)

# TO DO:
def on_queue_declared(frame):
    global channel # TO DO: How it is ok for this to not be declared in global scope??
    # Called when RabbitMQ has told us our Queue has been declared, frame is the response from RabbitMQ
    print "queue declared"
    channel.basic_consume(handle_delivery, queue=datos_constants.NODE_AGENT_QUEUE_NAME)    

def handle_delivery(ch, method, header, body):
    global enable_hack
    print "handle_delivery received: ", body
    
    ch.basic_ack(method.delivery_tag)
    # print "acked message"

    if (enable_hack):
        print "Hacking the incoming message to be: "
        body = "operation:change_log,seqid:0,filename:test1.bar,offset:0,write_size:100,timestamp:66054"
        print "New message is: " + body
    else:
        # enable_hack = True
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
    # Global defs
    global policy_table
    global policy_table_lock
    global app_listener_is_alive # Move globals to new module or something cleaner

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
    policy_table_lock.acquire()
    # print "Got policy table lock"
    policy_table.add_manage_policy(filename, interval)
    policy_table.show()
    policy_table_lock.release()
    # print "Released policy table lock"

    print "going to check applistener status - status = " + str(app_listener_is_alive)
    # If the app-listener is not alive - launch it..
    if (not app_listener_is_alive):
        # Launch Applistener
        print "Applistener is not alive -- going to launch it"
        # child = subprocess.Popen(["python", "./test_listener.py"], stdout=subprocess.PIPE)
        # child = subprocess.Popen(["python", "./test_listener.py"])

        child = subprocess.Popen(["python", "../app_listener/thread_main.py", "10.0.0.3", "/home/dev1/hadoop_play/hadoop_store/hdfs/namenode/edit2/current"])
        # child = subprocess.call("python ./hacked_listener.py")

        print "Return code was " + str(child)
        app_listener_is_alive = True
    else:
        print "Applistener is already alive.."

def handle_change_log_msg(words):
    print "Going to handle change_log msg.."
    
    # Global defs
    global policy_table
    global policy_table_lock
    global app_listener_is_alive # Move globals to new module or something cleaner

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
    sections = words[5].split(':')
    if (sections[0] != "timestamp"):
        print "incorrect field - expect timestamp!"
        exit(1)
    timestamp = int(sections[1])

    # print "check 6"
    print "Received change_log message: seq_id=" + str(seq_id) + " filename=" + filename + " offset=" + str(offset) + " write_size=" + str(write_size) + " timestamp = " + str(timestamp)

    # print "Trying to lock the policy table.."
    policy_table_lock.acquire()
    # print "Got lock on the policy table.."

    policy_table.show()
    # print "going to add job to pt.."
    policy_table.add_job(seq_id, filename, offset, write_size, timestamp)
    policy_table.show()
    policy_table_lock.release()
    # print "Released lock on the policy table.."

    print "Hack flow .."

    # hadoop fs -copyToLocal /nc_test/test1.bar ~/temp
    print "Trying to copy filename from hadoop to local temp.."

    # Say Hdfs dir is /data
    hdfs_path = str(filename)
    print "hdfs_dir = " + hdfs_path

    child = subprocess.Popen(["hadoop", "fs", "-copyToLocal", hdfs_path, "/home/dev1/temp"])
    print "In here"

    time.sleep(5)

    # scp test1.bar dev1@10.0.0.11:/home/dev1/store_temp
    print "Trying to scp test1.bar from local temp to vm2"
    
    local_path = "/home/dev1/temp/" + filename
    print local_path
    #child = subprocess.Popen(["scp", local_path, "dev1@10.0.0.11:/home/dev1/store_temp"])
    swift_store = swift_backend('admin', 
                            'indata2d', 
                            'admin', 
                            'http://controller:35357/v2.0')
    
    swift_store.put('demo', filename, local_path)
    
    print "In here"

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
        global node_agent_ip
        print "In run() method of main_listener thread:", self.name

        logging.getLogger('pika').setLevel(logging.CRITICAL)    
        credentials = pika.PlainCredentials('guest', 'guest')
    
        parameters = pika.ConnectionParameters(
               node_agent_ip,
               5672, 
               '/',
               credentials)
        self.connection = pika.SelectConnection(parameters, on_connected)

        while (not should_shutdown):

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

########################
# Node agent startup
# Starts up threads needed
# Allocs resources..
########################
def startup():
    print "Node Agent startup"
    
    global should_shutdown
    global mlist_thread

    should_shutdown = False

    # Global section 2

    print "going to launch PL_thread-1.."
    mlist_thread = main_listener(1, "PL_thread-1", 1)   
    mlist_thread.start()

def main_work():
    print "Node Agent main_work()"
    
    # state = started
    state = 1 # started

    while True:
        sys.stdout.write(node_agent_str)
        # logging.debug("foo global = " + str(foo.test_val))
        userline = sys.stdin.readline().rstrip('\n')
        if (userline == 'quit'):
            break

        # Every 20 sec, send a heartbeat (with state, message order) to the Policy engine
        # type=node_agent_heartbeat, seq_id=<num>
        
def shutdown():
    global should_shutdown
    global app_listener_is_alive

    print "Node Agent shutting down.."
    
    # Let all threads know they should finish
    should_shutdown = True

    # Need to stop the rabittmq ioloop
    print "Stopping the IOloop in startup()"
    mlist_thread.connection.ioloop.stop()
 
    # Wait for all threads to finish
    mlist_thread.join()
    
    # Stop the applistener
    if (app_listener_is_alive):
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


