# Time related imports
import datetime
import time

# Globals
import g

def get_secs_since_epoch():
    current_time = datetime.datetime.now()
    delta = current_time - g.epoch_time
    return delta.total_seconds()
