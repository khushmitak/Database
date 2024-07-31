from gevent import socket
from gevent.pool import Pool
from gevent.server import StreamServer

from collections import namedtuple
from io import BytesIO
from socket import error as socket_error

# Errors that can occur while processing requests
class CommandError(Exception): pass
class DisconnectionError(Exception): pass

Error = namedtuple('Error', ('message',))

# a class for parsing incoming requests and outgoing responses
class ProtocolHandler:
    # Redis protocols that supports different data types
    def __init__(self): {
        '+': self.handle_simple_string,
        '-': self.handle_error,
        ':': self.handle_integer,
        '$': self.handle_string,
        '*': self.handle_array,
        '%': self.handle_dict}  
        
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
    
    # for handling individual client connections
    def connection_handler(self, conn, address):
        socket_file = conn.makefile('rwb')
        
        while True:
            try:
                data = self.protocol.handle_requests(socket_file)
            except DisconnectionError:
                break
            
            try:
                response = self.get_response(data)
            except CommandError as exc:
                response = Error(exc.args[0])
                
            # after handling requests and generating response, send the response to the client
            self.protocol.write_response(socket_file, response)
        
    def get_response(self, data):
        pass
    
    def run(self):
        self.server.serve_forever()
    
    