#!/usr/bin/python

###########################
# Imports
###########################

# Logging/rabbit imports
import sys
import pika
import logging

# Time related imports
import datetime
import time

# Other imports
import argparse

###################################################
# BigDatos CLI implementation
# Copyright (c) DatosCloud, Inc 2014.
###################################################

version = 0.2

# DAtos SHell
cli_str = "dash>"

# commands = ["manage", "retrieve", "help"]

##########################
# Globals
##########################
int_conv = {"sec":1, \
            "min":60, \
            "hour":3600, \
            "day":3600*24, \
            "week":3600*24*7}

# TO DO:
# Day - need to define time of day
# Week - need to define day of week + day meta

###########################################
# Implementation of the list command
###########################################
def list_impl(args):
    print "coming soon"

###########################################
# Implementation of the manage command
###########################################
def manage_impl(args):
    # print "in manage_impl: ", args

    words = args.split(' ')

    length = len(words)
    # print "len:", length

    if (length != 3 and length != 4):
        manage_show_help()
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
        manage_show_help()
        return

    # Convert units
    act_int = act_int * int_conv[int2]
    # print "Converted actual int to: ", act_int

    send_manage_msg(filename, act_int)

################################################
# Show the help message for the manage command
################################################
def manage_show_help():
    print "manage <filename> <interval time> <units (sec|min|hour|day|week)>"
    print "eg: datamanage test.out 30 min"

    # operation:type,filename:name,interval:secs

###########################################
# Implementation of the retrieve command
###########################################
def retrieve_impl(args):
    # Remove this..
    args = args
    # print "in retrieve_impl: ", args

    words = args.split(' ')

    length = len(words)
    # print "len:", length

    if (length != 3):
        print "retrieve - incorrect no: of arguments"
        retrieve_show_help()
        return

    filename = words[1]
    req_time = words[2]

    print "filename=", filename, "req_time=", req_time

    time_comp = req_time.split(':')
    print "time comp has: ", len(time_comp), " parts"

    for t in time_comp:
        print "t=", t

    if (len(time_comp) != 6):
        print "retrieve - Incorrect time specification: ", time
        retrieve_show_help()
        return

    # TO DO: Handle variable time spec
    # Eg: 06:30 - means 6mins sec today?

    # TO DO: Figure out how to handle timezones.
    req_time_obj = datetime.datetime(int(time_comp[0]), int(time_comp[1]), int(time_comp[2]),
                                     int(time_comp[3]), int(time_comp[4]), int(time_comp[5])) 

    epoch_obj = datetime.datetime.utcfromtimestamp(0)
    delta = req_time_obj - epoch_obj
    
    print "req_time is: ", req_time_obj
    print "epoch start time is: ", epoch_obj
    print "delta = ", delta

    delta_secs = delta.total_seconds()
    print "delta_secs = ", delta_secs

    send_retrieve_msg(filename, delta_secs)

################################################
# Show the help message for the retrieve command
################################################
def retrieve_show_help():
    print "retrieve <filename> <UTC timespec>"
    print "eg: retrieve test.out 2014:06:24:5:30:00"

    # operation:type,filename:name,req_time

def help_impl(args):
    print "Valid commands are:"
    for key in command_fns:
        print key
    print " "

def send_manage_msg(filename, act_int):
    str = "operation:manage,filename:%s,interval:%d" % (filename, act_int)
    # print "Going to send rabbitmq:", str
    send_rabbit_msg(str)

def send_retrieve_msg(filename, time):
    str = "operation:retrieve,filename:%s,req_time:%d" % (filename, time)
    # print "Going to send rabbitmq:", str
    send_rabbit_msg(str)

def send_rabbit_msg(str):
    print "in send_rabbit_ms - going to send rabbitmq:", str
    
    # ip address hardcoded to VM1
    # dest_ip = "10.0.0.3"

    # ip address hardcoded to VM2
    dest_ip = "10.0.0.11"

    queue_name = "main_queue"

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
command_fns = {"manage":manage_impl, \
               "retrieve":retrieve_impl, \
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

def run_interactive():
    buffer = []
    while True:
        sys.stdout.write(cli_str)
        userline = sys.stdin.readline().rstrip('\n')
        if (userline == 'quit'):
            break
        else:
            # print "Line was:",userline, "\n"
            process_input(userline)

def parse_input():
    #parser = argparse.ArgumentParser(description='Datos shell')
    #parser.add_argument('command', metavar='command', type=str, 
    #                   help='Optional arg to run a single command')

    #args = parser.parse_args()

    args = sys.argv
    # for arg in args:
    #    print "arg=", arg
    
    # Temp list where the first element is removed
    # Better way of doing this?
    temp_args = args[1:]
    
    userline = ' '.join(temp_args)
    print "userline=", userline
    
    return userline

def run_single():
    print " in run_single"
    userline = parse_input()

    process_input(userline)
    
########################
# Main
########################
print "Welcome to BigDatos CLI(", version,")"

if (len(sys.argv) > 1):
    run_single()
else:
    run_interactive()

print "Goodbye.."


