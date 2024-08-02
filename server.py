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
        
        self.commands = self.get_commands()
    
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
    
    def get_commands(self):
        return {
            'GET': self.get,
            'SET': self.set,
            'DELETE': self.delete,
            'FLUSH': self.flush,
            'MGET': self.mget,
            'MSET': self.mset}
    
    # a method for processing incoming requests and outgoing responses
    def get_response(self, data):
        if not isinstance(data, list):
            try:
                data = data.split()
            except:
                raise CommandError('Request must be a simple string or a list')
        
        if not data:
            raise CommandError('Missing command')
        
        command = data[0].upper()
        if command not in self.commands:
            raise CommandError('Unrecognized command: %s' % command)

        return self.commands[command](*data[1:])
    
    def get(self, key):
        return self.kv.get(key)
    
    def set(self, key, value):
        self.kv[key] = value
        return 1
    
    def delete(self, key):
        if key in self.kv:
            del self.kv[key]
            return 1
        return 0
    
    def flush(self):
        kvlength = len(self.kv)
        self.kv.clear()
        return kvlength
    
    def mget(self, *keys):
        return [self.kv.get(key) for key in keys]
    
    def mset (self, *items):
        data = zip(items[::2], items[1::2])
        for key, value in data:
            self._kv[key] = value
        return len(data)
        
    def run(self):
        self.server.serve_forever()
        
class Client:
    def __init__(self, host='127.0.0.1', port='31337'):
        self.protocol = ProtocolHandler()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((host, port))
        self.fh = self.socket.makefile('rwb')

    def execute(self, *args):
        self.protocol.write_response(self.fh, args)
        response = self.protocol.handle_requests(self.fh)
        if isinstance(response, Error):
            raise CommandError(response.message)
        return response
    
    def get(self, key):
        return self.execute('GET', key)
    
    def set(self, key, value):
        return self.execute('SET', key, value)
    
    def delete(self, key):
        return self.execute('DELETE', key)

    def flush(self):
        return self.execute('FLUSH')

    def mget(self, *keys):
        return self.execute('MGET', *keys)

    def mset(self, *items):
        return self.execute('MSET', *items)