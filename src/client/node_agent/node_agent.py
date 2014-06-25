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

# Other imports
import argparse

# Global section 
version = 0.2
policy_eng_queue_name = "policy_eng_queue"
 # ip address hardcoded to VM1
node_agent_ip = "10.0.0.3"

# Globals shared between threads
should_shutdown = False
plist_thread = 0

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
    print "handle_delivery received: ", body
    
    ch.basic_ack(method.delivery_tag)
    print "acked message"

    # Put the msg body in a queue and signal the pe_listener to process it
    
    

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
    def run(self):
        print "In run() method of policy_listener thread:", self.name

        logging.getLogger('pika').setLevel(logging.CRITICAL)    
        credentials = pika.PlainCredentials('guest', 'guest')
    
        parameters = pika.ConnectionParameters(
               node_agent_ip,
               5672, 
               '/',
               credentials)
        connection = pika.SelectConnection(parameters, on_connected)

        while (not should_shutdown):

            time.sleep(5)
            print_thread_stats(self.name)
               
            try:
                # Loop so we can communicate with RabbitMQ
                connection.ioloop.start()
            except KeyboardInterrupt:
                # Gracefully close the connection
                connection.close()
                # Loop until we're fully closed, will stop on its own
                connection.ioloop.start()


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
    

########################
# Main
########################
print "Welcome to BigDatos NodeAgent version (", version, ")"

startup()
main_work()

print "Goodbye.."


