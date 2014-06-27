#This is the thread which parses the Edit Log entries and sends it to main thread
import Queue
import threading
import time

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit

import os

#from __future__ import with_statement

from errno import EACCES
from os.path import realpath
from sys import argv, exit
from threading import Lock

import os

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

# BigDatos common modules
import datos_constants
import debug_logger

# Local modules
import g 

##############################
# Basic edit_log_worker thread
##############################
class edit_log_worker(threading.Thread):
    def __init__(self, threadid, name, edit_log_dir, ctrl_queue, msg_queue):
        threading.Thread.__init__(self)
        self.threadID = threadid
        self.name = name
        self.ctrl_queue = ctrl_queue
        self.msg_queue = msg_queue
        self.seq = 0
        self.edit_log_dir = edit_log_dir
        
        
    def run(self):        
        # print "In run() method of edit_log_worker thread:", self.name
        g.debug_log.log("In run() method of edit_log_worker thread:" + self.name)

        # Start FUSE
        save_fd = os.open(self.edit_log_dir, os.O_RDONLY)
        mount_point = self.edit_log_dir    
    
        #fuse = FUSE(app_listener_fuse(save_fd, mount_point, self.msg_queue), 
        #            mount_point, foreground=False, nonempty=True)


        while True:
            
            if self.ctrl_queue.empty() == False:
                break
            
            #parse_edit_log()
            
            self.msg_queue.put("seq: " + str(self.seq))
            self.seq = self.seq + 1

            time.sleep(1)
            

        # print "Edit log worker thread:", self.name, " is going to shutdown.. - bye"
        g.debug_log.log("Edit log worker thread:" + self.name + " is going to shutdown.. - bye")
        

class app_listener_fuse(LoggingMixIn, Operations):
    """Collect useful information before passing down operations."""
   
    def __init__(self, save_fd, mount_point, msg_queue = None):     
        self.savefd = save_fd
        self.rwlock = Lock()
     
        self.mount_point = mount_point
        
        self.msg_queue = msg_queue
        
        os.fchdir(self.savefd)
        os.close(self.savefd)
    

    #def __call__(self, op, path, *args):
        #return super(app_listener_fuse, self).__call__(op, path, *args)
    
    
    # Some utility functions
    def is_absolute_path(self, fileName):
        if fileName and fileName[0] != '\0' and fileName[0] == '/':
            return True
        else:
            return false
        
    def get_relative_path(self, path):

        rPath = "." + path;

        return rPath;

           
    def getattr(self, path, fh=None):        
        rpath = self.get_relative_path(path)
                
        # print "At getattr for " + rpath
        g.debug_log.log("At getattr for " + rpath)
        
        try: 
            st = os.lstat(rpath)
            
            # print "After getattr for " + rpath
            g.debug_log.log("After getattr for " + rpath)
            
            ret = dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))
            
            #for key in ('st_atime', 'st_ctime', 'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'):
             #   print key + ":" + str(getattr(st, key))
                
            return ret
        
        except:
            
            raise FuseOSError(ENOENT)
        
    def statfs(self, path):
        rpath = self.get_relative_path(path)
               
        # print "At statfs"
        g.debug_log.log("At statfs")

        stv = os.statvfs(rpath)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))

    def create(self, path, mode):       
        rpath = self.get_relative_path(path)
        
        # print "At create"
        g.debug_log.log("At create")

        ret = os.open(rpath, os.O_WRONLY | os.O_CREAT, mode)
        
        # print "After create, ret: " + str(ret)
        g.debug_log.log("After create, ret: " + str(ret))
        
        return ret


    def access(self, path, mode):
        
        rpath = self.get_relative_path(path)
        
        ret = os.access(rpath, mode)
    
        print "At access " + rpath + ", mode: " + str(mode) + ", ret:" + str(ret)
        g.debug_log.log("At access " + rpath + ", mode: " + str(mode) + ", ret:" + str(ret))
        
        if not ret:
            raise FuseOSError(EACCES)
            
    def mkdir(self, path, mode):
    
        rpath = self.get_relative_path(path)
        
        
        print "At mkdir"
        
        return os.mkdir(rpath, mode)
        
    def mknod(self, path, mode, dev):
        
        rpath = self.get_relative_path(path)

        print "At mknod"

        return os.mknod(rpath, mode, dev)
    
    def open(self, path, flags):


        rpath = self.get_relative_path(path)

        print "At open"

        return os.open(rpath, flags)

        
    
    def readlink(self, path):
        rpath = self.get_relative_path(path)
        
        
        print "At readlink"
        return os.readlink(rpath)
        
    def unlink(self, path):
        
        rpath = self.get_relative_path(path)
        
        
        print "At unlink"
        return os.unlink(rpath)
        
    def utimens(self, path, times=None):

        rpath = self.get_relative_path(path)
        
        
        print "At utimens"
        
        return os.utime(rpath, times)

    def utime(self, path, times=None):

        rpath = self.get_relative_path(path)
        
        
        print "At utime"
        
        return os.utime(rpath, times)
    
    def rmdir(self, path):
        rpath = self.get_relative_path(path)
        
        
        print "At rmdir"
        
        return os.rmdir(rpath)
    
    def symlink(self, target, source):
        
        
        print "At symlink"
        
        rtarget = self.get_relative_path(target)
        
        return os.symlink(source, rtarget)
    
    def rename(self, old, new):

        rold = self.get_relative_path(old)
        rnew = self.get_relative_path(new)
        
        
        print "At rename, rename " + str(rold) + " to " + str(rnew)
        
        if not self.msg_queue is None:
            self.msg_queue.put("operation:rename,old:" + old + ",new:" + new)
        
        return os.rename(rold, rnew)
    
    def link(self, target, source):
        rtarget = self.get_relative_path(target)
        rsource = self.get_relative_path(source)
        
        
        print "At link"
        
        return os.link(rsource, rtarget)
    
    def chmod(self, path, mode):
        rpath = self.get_relative_path(path)

        print "At chmod"

        return os.chmod(rpath, mode)
    
    def chown(self, path, uid, gid):
        rpath = self.get_relative_path(path)

        print "At chown"

        return os.chown(rpath, uid, gid)
            
    listxattr = None
    
    
    
    def flush(self, path, fh):
        
        
        print "At flush"
        
        return os.fsync(fh)

    def fsync(self, path, datasync, fh):
        
        
        print "At fsync"
        
        return os.fsync(fh)


    def truncate(self, path, length, fh=None):
        rpath = self.get_relative_path(path)

        print "At truncate for file " + str(rpath)

        f = os.open(rpath, os.O_RDWR)
        return os.ftruncate(f, length)

    def read(self, path, size, offset, fh):
        
        print "At read"
        
        with self.rwlock:
            os.lseek(fh, offset, 0)
            return os.read(fh, size)

    def readdir(self, path, fh):
        
        print "At readdir"
        
        rpath = self.get_relative_path(path)

        return ['.', '..'] + os.listdir(rpath)
    
    def release(self, path, fh):
        
        print "At release for path " + str(path)
        
        return os.close(fh)
    
    def write(self, path, data, offset, fh):
        rpath = self.get_relative_path(path)

        print "At write"

        with self.rwlock:
            os.lseek(fh, offset, 0)
            print "write at offset " + str(offset) + " for file " + str(rpath)
            return os.write(fh, data)

if __name__ == "__main__":
    if len(argv) != 2:
        print 'usage: %s <mountpoint>' % argv[0]
        exit(1)


    #TODO: find out the fuse_get_context() counterpart in python
    
    save_fd = os.open(argv[1], os.O_RDONLY)
    mount_point = argv[1]    
    
    fuse = FUSE(app_listener_fuse(save_fd, mount_point), argv[1], foreground=True, nonempty=True)


