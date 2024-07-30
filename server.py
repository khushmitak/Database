from collections import namedtuple

from gevent.pool import Pool
from gevent.server import StreamServer

# Errors that can occur while processing requests
class CommandError(Exception): pass
class DisconnectionError(Exception): pass

Error = namedtuple('Error', ('message',))

# a class for parsing incoming requests and outgoing responses
class ProtocolHandler:
    def handle_requests(self, socket_file):
        pass
    
    def write_response(self, socket_file, data):
        pass
    
class Server:
    def __init__(self, host='127.0.0.1', port=31337, max_clients=64):
        self.pool = Pool(max_clients)
        self.server = StreamServer((host, port), self.connection_handler, spawn=self.pool)
        
        self.protocol=ProtocolHandler()
        self.kv= {}
    
    def connection_handler(self, conn, address):
        
        
        
    def get_response(self, data):
        pass
    
    