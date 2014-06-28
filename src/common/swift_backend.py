from swiftclient import client
from swiftclient import Connection, RequestException
from swiftclient import __version__ as client_version
import swiftclient

class swift_backend(object):
    '''
    Swift client to store object to and retrieve object from SWIFT 
    '''
    
    def __init__(self, username, password, tenant_name, authurl):
        self.username = username
        self.password = password
        self.tenant_name = tenant_name
        self.authurl = authurl
        
        self.os_options = {
            'tenant_id': None,
            'tenant_name': tenant_name,
            'service_type': None,
            'endpoint_type': None,
            'auth_token': None,
            'object_storage_url': None,
            'region_name': None,
        }
        
        self.conn = Connection(
                                self.authurl,
                                self.username,
                                self.password,
                                1, 
                                auth_version=2.0, 
                                os_options=self.os_options)
                               
         
    
    def put(self, container_name, filename, path):
        
        #put_headers = {'x-object-meta-mtime': "%f" % getmtime(path)}

        '''resps = self.conn.get_container(container_name)
     
        for resp in resps:
            print "resp is " + str(resp)'''
        
        self.conn.put_object(
                        container_name, filename, open(path, 'rb'))
     
        return
    
    
    def get(self, container_name, filename, output_name):
     
        headers, body = \
                self.conn.get_object(container_name, filename, resp_chunk_size=65536)
                
        fp = open(output_name, 'wb')
        
        for chunk in body:
            fp.write(chunk)
            
        fp.close()
             
        return 

