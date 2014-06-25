#TOOD: Need to add Copyright placeholder here
#TODO: Need to figure out how to create PyDev project and compile it from there.
#TODO: Need to figure out how to use the logging infrastructure
    
import pika
import argparse
import logging
from local_pe_backend import local_pe_backend
import datos_constants as DC

my_local_policy_store = local_pe_backend()
my_local_mapping_store = local_pe_backend('FileRelations.txt')

channel = None
global_policy_engine_queue = DC.POLICY_ENGINE_QUEUE_NAME
global_node_agent_queue = DC.NODE_AGENT_QUEUE_NAME
node_agent_channel = None

def parse_input():
    parser = argparse.ArgumentParser(description='Start Policy Engine with an IP address')
    parser.add_argument('IPAddr', metavar='IPAddr', type=str, 
                       help='an IP address Policy Engine binds to accept messages')
    parser.add_argument('node_agent_ip', metavar='NodeAgentIPAddr', type=str, 
                       help='an IP address Node Agent binds to accept messages')
    
    args = parser.parse_args()
    
    print "Policy Engine IPAddr: " + args.IPAddr + ", node agent: " + args.node_agent_ip
    
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
                
                my_local_policy_store.put(filename, interval)
                
                #TODO: for Hive, need to query the Hive metastore
                my_local_mapping_store.put(filename, filename)
                
                node_agent_channel.basic_publish(exchange='',
                          routing_key=global_node_agent_queue,
                          body=body)
                
            elif operation == 'store':
    			# TODO: Store the data, implement it later
                print "Start to store data at interval " + interval
                storeInterval, storeStatus = my_local_policy_store.get(filename)
                
                if not storeStatus: 
                    # TODO: Raise error
                    print "Cannot find the policy for " + str(filename)
                    ch.basic_ack(delivery_tag = method.delivery_tag)
            
                    return
                
                
                
                
            elif operation == 'retrieve':
    			# TODO: retrieve the data
                print "retrieve function"       
                
                storeInterval, storeStatus = my_local_policy_store.get(filename)
                
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
    channel.queue_declare(queue=global_policy_engine_queue,
                          passive=True, 
                          durable=True, 
                          exclusive=False, 
                          auto_delete=False, 
                          callback=on_queue_declared)

# Step #4
def on_queue_declared(frame):
    """Called when RabbitMQ has told us our Queue has been declared, frame is the response from RabbitMQ"""
    print "queue declared"
    channel.basic_consume(handle_delivery, queue=global_policy_engine_queue)

def main():
    
    args = parse_input()

    logging.getLogger('pika').setLevel(logging.CRITICAL)
    
    credentials = pika.PlainCredentials('guest', 'guest')
    
    #Initialize the channel to node agent
    node_agent_connection = pika.BlockingConnection(pika.ConnectionParameters(
               args.node_agent_ip,
               5672, 
               '/',
               credentials))
    global node_agent_channel
    node_agent_channel = node_agent_connection.channel()
    
    node_agent_channel.queue_declare(queue=global_node_agent_queue)
    
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
        
        node_agent_connection.close()
        
        # Loop until we're fully closed, will stop on its own
        connection.ioloop.start()
    
    
if __name__ == "__main__":
    main()
