#TOOD: Need to add Copyright placeholder here
#TODO: Need to figure out how to create PyDev project and compile it from there.
#TODO: Need to figure out how to use the logging infrastructure
    
import pika
import argparse
import logging
from local_pe_backend import local_pe_backend

my_local_store = local_pe_backend()
channel = None
global_queue_name = None

def parse_input():
    parser = argparse.ArgumentParser(description='Start Policy Engine with an IP address')
    parser.add_argument('IPAddr', metavar='IPAddr', type=str, 
                       help='an IP address to listen to for the communication')
    parser.add_argument('queue_name', metavar='QueueName', type=str, 
                       help='an queue name to listen to for the communication')
    
    args = parser.parse_args()
    
    print "IPAddr: " + args.IPAddr + ", queue name: " + args.queue_name
    
    return args



def handle_delivery(ch, method, header, body):
    print " [x] Received %r" % (body,)
    cmdComps = body.split(',')
    
    if len(cmdComps) != 3:
        #TODO: Emit the ERROR entry to Error logging
        print "[ERROR] Failed to get the full command. \n The full command should be operation: type, filename: name, interval: seconds"
        return
    else:
        operationPair = cmdComps[0].split(':')
        namePair = cmdComps[1].split(':')
        intervalPair = cmdComps[2].split(':')

    	if len(intervalPair) != 2 or len(operationPair) != 2 or len(namePair) != 2:
            #TODO: Ack the failure
            print "[ERROR] Failed to get the full command. \n The full command should be operation: type, filename: name, interval: seconds"
            ch.basic_ack(delivery_tag = method.delivery_tag)
            
            return
  
    	else:
            operation = operationPair[1].strip()
            filename = namePair[1].strip()
            interval = intervalPair[1].strip()
            
            if operation == 'manage':
    			#TODO: Store the policy in Swift
                print "manage function"
                
                my_local_store.storePolicy(filename, interval)
                
            elif operation == 'store':
    			# TODO: Store the data, implement it later
                print "Start to store data at interval " + interval
                storeInterval, storeStatus = my_local_store.readPolicy(filename)
                
                if not storeStatus: 
                    # TODO: Raise error
                    print "Cannot find the policy for " + str(filename)
                    ch.basic_ack(delivery_tag = method.delivery_tag)
            
                    return
                
                # TODO: Ack to Node Agent
                
            elif operation == 'retrieve':
    			# TODO: retrieve the data
                print "retrieve function"       
                
                storeInterval, storeStatus = my_local_store.readPolicy(filename)
                
                if not storeStatus: 
                    # TODO: Raise error
                    print "Cannot find the policy for " + str(filename)
                    ch.basic_ack(delivery_tag = method.delivery_tag)
            
                    return
                
                print "Stored interval is " + str(storeInterval)
                
            else: 
    			# TODO: Raise error in error logging
    			print "Non-supported command: " + operation

    ch.basic_ack(delivery_tag = method.delivery_tag)
            
    return


def on_connected(connection):
    """Called when we are fully connected to RabbitMQ"""
    print "RabbitMQ connected"
    connection.channel(on_channel_open)

def on_channel_open(new_channel):
    """Called when our channel has opened"""
    global channel
    print "channel open"
    channel = new_channel
    channel.queue_declare(queue=global_queue_name,
                          passive=True, 
                          durable=True, 
                          exclusive=False, 
                          auto_delete=False, 
                          callback=on_queue_declared)

# Step #4
def on_queue_declared(frame):
    """Called when RabbitMQ has told us our Queue has been declared, frame is the response from RabbitMQ"""
    print "queue declared"
    channel.basic_consume(handle_delivery, queue=global_queue_name)

def main():
    
    args = parse_input()

    logging.getLogger('pika').setLevel(logging.CRITICAL)
    
    credentials = pika.PlainCredentials('guest', 'guest')
    
    global global_queue_name
    global_queue_name = args.queue_name
    
    parameters = pika.ConnectionParameters(
               args.IPAddr,
               5672, 
               '/',
               credentials)
    connection = pika.SelectConnection(parameters, on_connected)
    
    try:
        # Loop so we can communicate with RabbitMQ
        connection.ioloop.start()
    except KeyboardInterrupt:
        # Gracefully close the connection
        connection.close()
        # Loop until we're fully closed, will stop on its own
        connection.ioloop.start()
    
    
if __name__ == "__main__":
    main()
