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

# Other imports
import argparse
import collections

# Global section 
version = 0.2
policy_eng_queue_name = "policy_eng_queue"
node_agent_str = "node agent>"

# ip address hardcoded to VM1
node_agent_ip = "10.0.0.3"

# Globals shared between threads
should_shutdown = False
plist_thread = 0
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
                          passive=True, 
                          durable=True, 
                          exclusive=False, 
                          auto_delete=False, 
                          callback=on_queue_declared)

# TO DO:
def on_queue_declared(frame):
    # Called when RabbitMQ has told us our Queue has been declared, frame is the response from RabbitMQ
    print "queue declared"
    channel.basic_consume(handle_delivery, queue=datos_constants.NODE_AGENT_QUEUE_NAME)    

def handle_delivery(ch, method, header, body):
    # Global defs
    global policy_table
    global policy_table_lock

    print "handle_delivery received: ", body
    
    ch.basic_ack(method.delivery_tag)
    # print "acked message"

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
    if (sections[1] != "manage"):
        print "incorrect field - expect manage!"
        exit(1)

    # print "check 2"    
    # word[1] Filename
    sections = words[1].split(':')
    # print "sections[0]: ", sections[0], " sections[1]:", sections[1]
    if (sections[0] != "filename"):
        print "incorrect field - expect filename!"
        exit(1)
    filename = sections[1]
    
    # print "check 3"
    # word[2] Interval
    sections = words[2].split(':')
    # print "sections[0]: ", sections[0], " sections[1]:", sections[1]
    if (sections[0] != "interval"):
        print "incorrect field - expect interval!"
        exit(1)
    interval = int(sections[1])

    # print "check 4"
    print "Asked to manage filename:", filename, " at interval: ", interval, "secs"
    
    # print "Trying to lock the policy table.."
    policy_table_lock.acquire()
    # print "Got policy table lock"
    policy_table.add_manage_policy(filename, interval)
    policy_table.show()
    policy_table_lock.release()
    # print "Released policy table lock"

    # If the app-listener is not alive - launch it..
    
def print_thread_stats(name):
    
    # TO DO: Change to log
    print "Thread: ", name, " is alive"
       
##############################
# Basic Policylistener thread
##############################
class policy_listener(threading.Thread):
    def __init__(self, threadid, name, counter):
        threading.Thread.__init__(self)
        self.threadID = threadid
        self.name = name
        self.counter = counter
        self.connection = 0

    def run(self):
        print "In run() method of policy_listener thread:", self.name

        logging.getLogger('pika').setLevel(logging.CRITICAL)    
        credentials = pika.PlainCredentials('guest', 'guest')
    
        parameters = pika.ConnectionParameters(
               node_agent_ip,
               5672, 
               '/',
               credentials)
        self.connection = pika.SelectConnection(parameters, on_connected)

        while (not should_shutdown):

            time.sleep(5)
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

        print "policy_listener thread:", self.name, " is going to shutdown.. - bye"

########################
# Node agent startup
# Starts up threads needed
# Allocs resources..
########################
def startup():
    print "Node Agent startup"
    
    global should_shutdown
    global plist_thread

    should_shutdown = False

    # Global section 2

    print "going to launch PL_thread-1.."
    plist_thread = policy_listener(1, "PL_thread-1", 1)   
    plist_thread.start()

def main_work():
    print "Node Agent main_work()"
    
    # state = started
    state = 1 # started

    while True:
        sys.stdout.write(node_agent_str)
        userline = sys.stdin.readline().rstrip('\n')
        if (userline == 'quit'):
            break

        # Every 20 sec, send a heartbeat (with state, message order) to the Policy engine
        # type=node_agent_heartbeat, seq_id=<num>
        
def shutdown():
    global should_shutdown

    print "Node Agent shutting down.."
    
    # Let all threads know they should finish
    should_shutdown = True
   
    # Need to stop the rabittmq ioloop
    print "Stopping the IOloop in startup()"
    plist_thread.connection.ioloop.stop()
 
    # Wait for all threads to finish
    plist_thread.join()
    
########################
# Main
########################
print "Welcome to BigDatos NodeAgent version (", version, ")"

startup()
main_work()
shutdown()

print "Goodbye.."


