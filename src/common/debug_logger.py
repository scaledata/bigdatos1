# Datoscloud Debug logger
#
##

import sys
import datetime
import time
import logging

class debug_logger():
     def __init__(self, filename):
        logging.debug("In debug_logger init..")
        self.log_file = open(filename, "a")

     def log(self, log_str):
        now = datetime.datetime.now() 
        self.log_file.write(str(now) + log_str + "\n")
        self.log_file.flush()

     def close(self):
        self.log_file.close()
