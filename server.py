from collections import namedtuple

# Two types of errors that can occur while processing requests
class CommandError(Exception): pass
class DisconnectionError(Exception): pass

Error = namedtuple('Error', ('message',))

# a class for parsing incoming requests and outgoing responses
class ProtocolHandler(object):
    def handle_requests(self, socket_file):
        pass
    
    def write_response(self, socket_file, data):
        pass
    
