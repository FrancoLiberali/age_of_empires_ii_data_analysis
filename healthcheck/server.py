from logger.logger import Logger
from socketserver import TCPServer
import socketserver
import socket

RECV=b'PING!'
SEND=b'PONG!'
PORT=9999
HOST=''

logger=Logger()

class PingHandler(socketserver.BaseRequestHandler):

    def handle(self):
        data=b''
        try:            
            while len(data) < len(RECV):
                data += self.request.recv(len(RECV) - len(data))
        except socket.errror:
            return
        if data == RECV:
            try:
                self.request.sendall(SEND)
            except socket.error:
                pass


def run():
    with TCPServer((HOST, PORT), PingHandler) as server:
        logger.info(f"Health Check Server started")
        server.serve_forever()

    