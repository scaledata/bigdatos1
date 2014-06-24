#TOOD: Need to add Copyright placeholder here
#TODO: Need to figure out how to create PyDev project and compile it from there.
#TODO: Need to figure out how to use the logging infrastructure
    
import pika
import argparse
import logging


def parseInput():
    parser = argparse.ArgumentParser(description='Start Policy Engine with an IP address')
    parser.add_argument('IPAddr', metavar='IPAddr', type=str, 
                       help='an IP address to listen to for the communication')
    parser.add_argument('queueName', metavar='QueueName', type=str, 
                       help='an queue name to listen to for the communication')
    
    args = parser.parse_args()
    
    print "IPAddr: " + args.IPAddr + ", queue name: " + args.queueName
    
    return args


def callback(ch, method, properties, body):
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
		print "[ERROR] Failed to get the full command. \n The full command should be operation: type, filename: name, interval: seconds"
		return
	else:       
 
		operation = operationPair[1].strip()
		filename = namePair[1].strip()
		interval = intervalPair[1].strip()

		if operation == 'datamanage':
			#TODO: Store the policy in Swift
			print "manage function" 
		elif operation == 'datastore':
			# TODO: Store the data, implement it later
			print "Start to store data at interval " + interval
		elif operation == 'dataretrieve':
			# TODO: retrieve the data
			print "retrieve function"       

			if interval % storeInterval != 0:
			# TODO: Raise error
				print "We do not support retrieval from Point-in-Time other than the scheduled backup time"
				return

		else: 
			# TODO: Raise error in error logging
			print "Non-supported command: " + operation

		return


def main():
    
    args = parseInput()

    logging.getLogger('pika').setLevel(logging.CRITICAL)
    
    credentials = pika.PlainCredentials('guest', 'guest')
    
    connection = pika.BlockingConnection(pika.ConnectionParameters(
               args.IPAddr,
               5672, 
               '/',
               credentials))
    channel = connection.channel()

    channel.queue_declare(queue=args.queueName)
    
    channel.basic_consume(callback,
                      queue=args.queueName,
                      no_ack=False)

    print ' [*] Waiting for messages. To exit press CTRL+C'

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()

    connection.close()
    
    
    
if __name__ == "__main__":
    main()
