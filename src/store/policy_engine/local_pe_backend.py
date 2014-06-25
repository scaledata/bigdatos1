import threading
import settings as s

class local_pe_backend(object):
    #TODO: Replace by Metastore's policy engine store, also needs to be discoverable
    
    def __init__(self, dbFilename=None):
        
        if dbFilename is None: 
            self.dbFilename = "Policies.txt"
        else:
            self.dbFilename = dbFilename
        
        self.filelock = threading.Lock()
        
    def storePolicy(self, filename, interval):
        
        self.filelock.acquire()
        try: 
            fileObj = open(self.dbFilename, "a+")
            
            fileObj.write( str(filename) + "\t" + str(interval) + "\n")
            
            fileObj.close()
        except:
            print "Cannot open file " + str(self.dbFilename)
        finally:
            self.filelock.release()
            
        return
    
    def readPolicy(self, filename):
        
        storeInterval = str(s.INVALID_INTERVAL)
        
        try: 
            fileObj = open(self.dbFilename, "r")
            
            for line in fileObj:
            
                lineComps = line.split("\t")
                
                storeFilename = lineComps[0]
                
                if storeFilename == filename: 
                    storeInterval = lineComps[1]
            
            fileObj.close()
        
        except:
            print "Cannot open file " + str(self.dbFilename)

        status = False
        
        if storeInterval != str(s.INVALID_INTERVAL) : 
            status = True
        
        return (storeInterval.strip(), status)

