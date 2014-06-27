#This is the main thread which collects all input from worker thread and pushes it to Node Agent
import argparse
from thread_edit_log_worker import edit_log_worker
import Queue
import threading
from construct import *
import os

import calendar
import time

# Logging/rabbit imports
import sys
import pika
import logging

import datetime
import time

# BigDatos common modules
import datos_constants
import debug_logger

# Local modules
import g

# Globals
should_shutdown = False
hlist_thread = 0

ctrl_queue = None
msg_queue = None
worker_thread = None

# channel to node agent
node_agent_connection = None
node_agent_channel = None

# ip address hardcoded to VM1
node_agent_ip = "10.0.0.3"

def parse_input():
    parser = argparse.ArgumentParser(description='Start App Listener')
    parser.add_argument('IPAddr', metavar='IPAddr', type=str, 
                       help='an IP address Hadoop name node binds to')
    parser.add_argument('editLogDir', metavar='EditLogDir', type=str, 
                       help='A directory containing the Edit Log')
    
    args = parser.parse_args()
    
    # print "Name Node IPAddr: " + args.IPAddr + ", edit log directory: " + args.editLogDir
    g.debug_log.log("Name Node IPAddr: " + args.IPAddr + ", edit log directory: " + args.editLogDir)
    
    return args

def startup(edit_log_dir):

    #TODO: start a thread for the editLogDir
    #print "App Listener startup"
    g.debug_log.log("App Listener startup")

    global ctrl_queue
    global msg_queue
    global worker_thread
    
    ctrl_queue = Queue.Queue()
    msg_queue = Queue.Queue()
    
    # print "going to launch edit log worker_thread-1.."
    g.debug_log.log("going to launch edit log worker_thread-1..")
    worker_thread = edit_log_worker(1, "edit_log_thread-1", edit_log_dir, ctrl_queue, msg_queue)   
    worker_thread.start()    

    # connect to the queue to node agent
    credentials = pika.PlainCredentials('guest', 'guest')
    
    global node_agent_connection
    node_agent_connection = pika.BlockingConnection(pika.ConnectionParameters(
               node_agent_ip,
               5672, 
               '/',
               credentials))
    global node_agent_channel
    node_agent_channel = node_agent_connection.channel()
    
    return

def run():
    
    try: 
        
        while True:
            item = msg_queue.get()
            
            my_parser = Struct ("EditLogFirstParser", 
                        UBInt8("opcode"))
    
            my_ret = my_parser.parse(item)
            
            g.debug_log.log("Get the message " + str(my_ret["opcode"]) + "\n")
            
            if my_ret["opcode"] == 0:
                
                my_parser = Struct ("EditLogSecondParser",
                            UBInt8("opcode"), 
                            UBInt32("length"), 
                            Bytes("junk", 16),
                            UBInt16("name_len"),
                            String("name", lambda ctx: ctx.name_len),
                            UBInt64("mtime"))

                my_ret = my_parser.parse(item)
            
                #print "Name " + str(my_ret["name"]) + ", time " + str(my_ret["mtime"]) + ", name len " + str(my_ret["length"])
            
                #print "Name len: " + str(my_ret["name_len"])
            
                my_file_name = my_ret["name"]
                my_file_name = my_file_name[:- 10]
            
                g.debug_log.log("Name " + my_file_name + ", time " + str(my_ret["mtime"]) + ", name len " + str(my_ret["length"]) + "\n")
                
                cur_time = calendar.timegm(time.gmtime())
                
                msg_to_send = "operation:change_log,seqid:1,filename:" + my_file_name + ",offset:0,write_size:0,timestamp:" + str(cur_time)      
    
                node_agent_channel.basic_publish(exchange='',
                                      routing_key=datos_constants.NODE_AGENT_QUEUE_NAME,
                                      body=msg_to_send)
                
                g.debug_log.log(" [x] Sent " + msg_to_send)
                
    
    except KeyboardInterrupt:
        
        ctrl_queue.put("fin")
        
    worker_thread.join()
    
    
    
    node_agent_connection.close()
    

def main():
    g.debug_log = debug_logger.debug_logger("listener.log")
    g.debug_log.log("Applistener (Real): Hello..")
    
    args = parse_input()
    
    # Create worker thread and heartbeat thread
    startup(args.editLogDir)
    
    #node_agent_channel = get_node_agent_channel(args.IPAddr)
    
    # operation:change_log,seqid:seqno,filename:name,offset:<bytes>,write_size:<bytes>,timestamp:<secs>
    
    #TODO: Wait on one work queue
    run()
    
    g.debug_log.log("Applistener (Real): Bye..")
    g.debug_log.close()

    return
    
if __name__ == "__main__":
    main()
