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

# a class for processing incoming requests and outgoing responses
class ProtocolHandler:
    # Redis protocols that supports different data types
    def __init__(self): 
        self.handlers = {
        '+': self.handle_simple_string,
        '-': self.handle_error,
        ':': self.handle_integer,
        '$': self.handle_string,
        '*': self.handle_array,
        '%': self.handle_dictionary}  
        
    def handle_requests(self, socket_file):
        first_byte = socket_file.read(1)
        if not first_byte: #if first byte is empty, client has disconnected
            raise DisconnectionError()
        
        try:
            return self.handlers[first_byte](socket_file)
        except KeyError: #error if first byte is not a key in the self handlers dictionary
            raise CommandError('Bad Request')
    
    def handle_simple_string(self, socket_file):
        return socket_file.readline().rstrip('\r\n')

    def handle_error(self, socket_file):
        return Error(socket_file.readline().rstrip('\r\n'))

    def handle_integer(self, socket_file):
        return int(socket_file.readline().rstrip('\r\n'))
    
    def handle_string(self, socket_file): #for bulky strings
        length = int(socket_file.readline().rstrip('\r\n'))
        if (length == -1):
            return None
        length += 2
        return socket_file.read(length)[:-2]
        
    def handle_array(self, socket_file):
        number_elements = int(socket_file.readline().rstrip('\r\n'))
        return [self.handle_requests(socket_file) for _ in range(number_elements)]
        
    def handle_dictionary(self, socket_file):
        number_items = int(socket_file.readline().rstrip('\r\n'))
        elements = [self.handle_requests(socket_file) for _ in range(number_items * 2)]
        
        return dict(zip(elements[::2], elements[1::2]))
        
    def write_response(self, socket_file, data):
        buffer = BytesIO()
        self.write(buffer, data)
        buffer.seek(0)
        socket_file.write(buffer.getvalue())
        socket_file.flush()
    
    def write(self, buffer, data):
        if isinstance(data, str):
            data = data.encode('utf-8')
        
        if isinstance(data, bytes):
            buffer.write('$%s\r\n%s' % (len(data), data))
        elif isinstance(data, int):
            buffer.write(':%s\r\n' % data)
        elif isinstance(data, Error):
            buffer.write('-%s\r\n' % Error.message)
        elif isinstance(data, (list, tuple)):
            buffer.write('*%s\r\n' % len(data))
            for item in data:
                self._write(buffer, item)
        elif isinstance(data, dict):
            buffer.write('%%%s\r\n' % len(data))
            for key in data:
                self._write(buffer, key)
                self._write(buffer, data[key])
        elif data is None:
            buffer.write('$-1\r\n')
        else:
            raise CommandError('unrecognized type: %s' % type(data))
            
   
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