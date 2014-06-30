# Time related imports
import datetime
import time

# Globals
#import g
epoch_time = datetime.datetime.utcfromtimestamp(0)   

def get_secs_since_epoch():
    current_time = datetime.datetime.now()
    delta = current_time - epoch_time
    return delta.total_seconds()
