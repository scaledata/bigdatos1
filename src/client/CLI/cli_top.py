#!/usr/bin/python

# Imports
import sys
import pika
import argparse
import logging

###################################################
# BigDatos CLI implementation
# Copyright (c) DatosCloud, Inc 2014.
###################################################

version = 0.2
cli_str = "bigdatos>"

# commands = ["datamanage", "dataretrieve", "help"]

##########################
# Globals
##########################
int_conv = {"sec":1, \
            "min":60, \
            "hour":3600, \
            "day":3600*24, \
            "week":3600*24*7}

# Day - need to define time of day
# Week - need to define day of week + day meta

def list_impl(args):
    print "coming soon"

def dmanage_impl(args):
    # print "in dmanage_impl: ", args

    words = args.split(' ')

    count = 0
    for w in words:
        # print count, ": ", w
        count += 1

    length = len(words)
    # print "len:", length

    if (length != 3 and length != 4):
        dmanage_show_help()
        return

    filename = words[1]
    int1 = words[2]
    
    if (length == 3):
        # Default is mins
        int2 = "min"
    else:
        int2 = words[3]

    # print "filename=", filename, "int1=", int1, "int2=", int2

    # Check the int
    act_int = int(int1)
    # print "Actual int is:", act_int

    # So far so good
    # Convert the units to secs
    valid_unit = False
    for key in int_conv:
        # print "checking:", key
        if (key == int2):
            valid_unit = True
            # print "Found valid unit:", int2
            break

    if (not valid_unit):
        # print "Invalid unit:", int2
        dmanage_show_help()
        return

    # Convert units
    act_int = act_int * int_conv[int2]
    # print "Converted actual int to: ", act_int

    send_dmanage_msg(filename, act_int)

def dmanage_show_help():
    print "datamanage <filename> <interval time> <units (sec|min|hour|day|week)>"
    print "eg: datamanage test.out 30 min"

    # <filename><name></filename><interval><secs></interval>
    # <filename>
    # name
    # </filename>
    #
    # operation:type,filename:name,interval:secs

def dretrieve_impl(args):
    # Remove this..
    args = args
    # print "in dretrieve_impl: ", args

def help_impl(args):
    print "Valid commands are:"
    for key in command_fns:
        print key
    print " "

def send_dmanage_msg(filename, act_int):
    str = "operation:datamanage,filename:%s,interval:%d" % (filename, act_int)
    # print "Going to send rabbitmq:", str
    send_rabbit_msg(str)

def send_dretrieve_msg(filename, time):
    str = "operation:dataretrieve,filename:%s,req_time:%d" % (filename, time)
    # print "Going to send rabbitmq:", str
    send_rabbit_msg(str)

def send_rabbit_msg(str):
    print "in send_rabbit_ms - going to send rabbitmq:", str
    
    # ip address hardcoded to VM2
    dest_ip = "10.0.0.11"

    queue_name = "nc_queue"

    logging.getLogger('pika').setLevel(logging.CRITICAL)
    credentials = pika.PlainCredentials('guest', 'guest')
    
    connection = pika.BlockingConnection(pika.ConnectionParameters(
               dest_ip,
               5672, 
               '/',
               credentials))
    channel = connection.channel()
    
    channel.queue_declare(queue=queue_name)
    
    channel.basic_publish(exchange='',
                          routing_key=queue_name,
                          body=str)
    
    print "Finished sending (to vm2) rabbitmq message: ", str      
    connection.close()

# Define a dict
command_fns = {"datamanage":dmanage_impl, \
               "dataretrieve":dretrieve_impl, \
               "list":list_impl, \
               "help":help_impl}

def process_input(userline):
    valid_command = False

    userline.lstrip()
    # print "after strip:",userline

    if (userline == ""):
        # print "Empty str"
        return

    for key in command_fns:
        # Check if the line start 
        # print "checking:", key
        if (userline.startswith(key)):
            # print "found match"
            valid_command = True

            # Invoke the fn ptr
            command_fns[key](userline)
            break

    if (not valid_command):
        print "Invalid command.."
        help_impl(userline)

########################
# Main
########################
print "Welcome to BigDatos CLI (", version, ")"

buffer = []
while True:
    sys.stdout.write(cli_str)
    userline = sys.stdin.readline().rstrip('\n')
    if (userline == 'quit'):
        break
    else:
        # print "Line was:",userline, "\n"
        process_input(userline)
        
print "Goodbye.."


