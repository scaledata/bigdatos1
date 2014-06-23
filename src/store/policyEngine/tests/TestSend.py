import pika
import argparse
import logging

def parseInput():
    parser = argparse.ArgumentParser(description='Start Policy Engine with an IP address')
    parser.add_argument('IPAddr', metavar='IPAddr', type=str, 
                       help='an IP address of the RabbitMQ Broker')
    parser.add_argument('queueName', metavar='QueueName', type=str, 
                       help='an queue name to subscribe to')
    
    args = parser.parse_args()
    
    print "IPAddr: " + args.IPAddr + ", queue name: " + args.queueName
    
    return args


def main():
    logging.getLogger('pika').setLevel(logging.CRITICAL)
    
    args = parseInput()
     
    credentials = pika.PlainCredentials('guest', 'guest')
    
    connection = pika.BlockingConnection(pika.ConnectionParameters(
               args.IPAddr,
               5672, 
               '/',
               credentials))
    channel = connection.channel()
    
    channel.queue_declare(queue=args.queueName)
    
    channel.basic_publish(exchange='',
                          routing_key=args.queueName,
                          body='Hello World!')
    
    print " [x] Sent 'Hello World!'"
    
    
    connection.close()

if __name__ == "__main__":
    main()