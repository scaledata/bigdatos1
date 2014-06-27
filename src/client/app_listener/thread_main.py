#This is the main thread which collects all input from worker thread and pushes it to Node Agent

import argparse
from thread_edit_log_worker import edit_log_worker
import Queue
import threading

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

    return

def run():
    
    try: 
        
        while True:
            item = msg_queue.get()
            
            # print "Item is " + str(item)
            g.debug_log.log("Item is " + str(item))
    except KeyboardInterrupt:
        
        ctrl_queue.put("fin")
        
    worker_thread.join()
        

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
