#This is the main thread which collects all input from worker thread and pushes it to Node Agent
#from cm_api.api_client import ApiResource
import argparse
from thread_edit_log_worker import edit_log_worker
import Queue
import threading
from construct import *
import os

import calendar
import time

ctrl_queue = None
msg_queue = None
worker_thread = None

log_file = None

sample_bin = None

def parse_input():
    parser = argparse.ArgumentParser(description='Start App Listener')
    parser.add_argument('IPAddr', metavar='IPAddr', type=str, 
                       help='an IP address Hadoop name node binds to')
    parser.add_argument('editLogDir', metavar='EditLogDir', type=str, 
                       help='A directory containing the Edit Log')
    
    args = parser.parse_args()
    
    print "Name Node IPAddr: " + args.IPAddr + ", edit log directory: " + args.editLogDir
    
    return args

def startup(edit_log_dir):

    #TODO: start a thread for the editLogDir
    print "App Listener startup"

    global ctrl_queue
    global msg_queue
    global worker_thread
    global log_file
    global sample_bin
    
    ctrl_queue = Queue.Queue()
    msg_queue = Queue.Queue()
    log_file = os.open("./mylog.log", os.O_RDWR|os.O_CREAT|os.O_TRUNC)
    sample_bin = open("./mybin.log", 'wb')

    print "going to launch edit log worker_thread-1.."
    worker_thread = edit_log_worker(1, "edit_log_thread-1", edit_log_dir, ctrl_queue, msg_queue)   
    worker_thread.start()    

    return

def run():
    
    try: 
        
        while True:
            item = msg_queue.get()
            
            
            my_parser = Struct ("EditLogFirstParser", 
                        UBInt8("opcode"))
    
            my_ret = my_parser.parse(item)
            
            os.write(log_file, "Get the message " + str(my_ret["opcode"]) + "\n")
            
            os.fsync(log_file)
            
            print "Get the message " + str(my_ret["opcode"])
            
            
            if my_ret["opcode"] == 0:
                
                sample_bin.write(item)
                
                sample_bin.close()
                my_parser = Struct ("EditLogSecondParser",
                            UBInt8("opcode"), 
                            UBInt32("length"), 
                            Bytes("junk", 16),
                            UBInt16("name_len"),
                            String("name", lambda ctx: ctx.name_len),
                            UBInt64("mtime"))

                my_ret = my_parser.parse(item)
            
                print "Name " + str(my_ret["name"]) + ", time " + str(my_ret["mtime"]) + ", name len " + str(my_ret["length"])
            
                print "Name len: " + str(my_ret["name_len"])
            
                os.write(log_file, "Name " + str(my_ret["name"]) + ", time " + str(my_ret["mtime"]) + ", name len " + str(my_ret["length"]) + "\n")
            
                os.fsync(log_file)  
                
                my_file_name = my_ret["name"]
                my_file_name = my_file_name[:- 10]
                
                cur_time = calendar.timegm(time.gmtime())
                
                msg_to_send = "operation:change_log,seqid:1,filename:" + my_file_name + ",offset:0,write_size:0,timestamp:" + str(cur_time)      

            
    except KeyboardInterrupt:
        
        ctrl_queue.put("fin")
        
    worker_thread.join()
        

def main():
    
    args = parse_input()
    
    # Create worker thread and heartbeat thread
    startup(args.editLogDir)
    
    #node_agent_channel = get_node_agent_channel(args.IPAddr)
    
    # seqid:seqno,filename:name,offset:<bytes>,write_size:<bytes>,timestamp:<secs>
    
    #TODO: Wait on one work queue
    run()
    
    return
    
if __name__ == "__main__":
    main()
