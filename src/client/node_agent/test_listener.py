#!/usr/bin/python

# Logging/rabbit imports
import sys
import pika
import logging

# Thread related imports
import threading

# BigDatos Constants
import datos_constants
import datetime
import time

# Globals
should_shutdown = False
log_file = open("listener.log", "a")
hlist_thread = 0

# ip address hardcoded to VM1
node_agent_ip = "10.0.0.3"

def on_connected(hb_connection):
    global log_file

    # Called when we are fully connected to RabbitMQ
    print "RabbitMQ connected"
    now = datetime.datetime.now()
    log_file.write(str(now) + " RabbitMQ connected\n")
    log_file.flush()
    hb_connection.channel(on_channel_open)

# TO DO:
def on_channel_open(new_channel):
    global log_file
    # Called when our channel has opened
    global hb_channel
    print "channel open"

    now = datetime.datetime.now()
    log_file.write(str(now) + " Channel open\n")
    log_file.flush()

    hb_channel = new_channel
    hb_channel.queue_declare(queue=datos_constants.APPLISTENER_HEARTBEAT_QUEUE,
                          passive=True, 
                          durable=True, 
                          exclusive=False, 
                          auto_delete=False, 
                          callback=on_queue_declared)

# Callback function called when this queue is declared
def on_queue_declared(frame):
    # Called when RabbitMQ has told us our Queue has been declared, frame is the response from RabbitMQ
    global hb_channel
    global log_file
    print "queue declared"
    
    now = datetime.datetime.now()
    log_file.write(str(now) + " queue declared\n")
    log_file.flush()

    hb_channel.basic_consume(handle_delivery, queue=datos_constants.APPLISTENER_HEARTBEAT_QUEUE)    

def handle_delivery(ch, method, header, body):
    # Global defs
    global log_file
    global should_shutdown

    now = datetime.datetime.now()    
    log_file.write(str(now) + " Applistener: handle_delivery received: " + body + "\n")
    log_file.flush()

    ch.basic_ack(method.delivery_tag)
    # print "acked message"
    
    words = body.split(',')
    
    # Shutdown should look like this:
    # "operation:heartbeat,status:shutdown"

    sections = words[1].split(':')
    if (sections[1] == "shutdown"):
        should_shutdown = True
        
        now = datetime.datetime.now() 
        log_file.write(str(now) + " Applistener: Got shutdown message ..\n")
        log_file.flush()
    else:
        now = datetime.datetime.now() 
        log_file.write(str(now) + " Applistener: Ignored last message ..\n")
        log_file.flush()

##############################
# Basic heartbeat listener thread
##############################
class hb_listener(threading.Thread):
    def __init__(self, threadid, name, counter):
        threading.Thread.__init__(self)
        self.threadID = threadid
        self.name = name
        self.counter = counter
        self.connection = 0

    def run(self):
        # global should_shutdown
        global node_agent_ip

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

            now = datetime.datetime.now()    
            log_file.write(str(now) + "in hb_listener while loop..")
            log_file.flush()

            # time.sleep(5)
            # print_thread_stats(self.name)
               
            try:
                # Loop so we can communicate with RabbitMQ
                self.connection.ioloop.start()
            except:
                pass
                
        # Gracefully close the connection
        # self.connection.close()
        # Loop until we're fully closed, will stop on its own
        # self.connection.ioloop.start()

        now = datetime.datetime.now()    
        log_file.write(str(now) + " hb_listener thread:" + self.name + " is going to shutdown.. - bye\n")
        log_file.flush()

# Main ---
log_file.write("Welcome to BigDatos Applistener..")
log_file.flush()

hlist_thread = hb_listener(1, "HL_thread-1", 1)   
hlist_thread.start()

now = datetime.datetime.now()
log_file.write(str(now) + " Applistener: Heartbeat listener thread launched..\n")
log_file.flush()

while (not should_shutdown):
    now = datetime.datetime.now()
    log_file.write(str(now) + " Applistener: Says hello..\n")
    log_file.flush()

    time.sleep(1)

now = datetime.datetime.now()
log_file.write(str(now) + " Applistener: Shutting down..\n")
log_file.flush()

hlist_thread.connection.ioloop.stop()
hlist_thread.join()

now = datetime.datetime.now()
log_file.write(str(now) + " Applistener: Goodbye..\n")
log_file.flush()

log_file.close()
