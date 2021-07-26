from logger.logger import Logger
from socketserver import TCPServer
import socketserver

RECV=b'PING!'
SEND=b'PONG!'
PORT=9999
HOST=''

logger=Logger()

class PingHandler(socketserver.BaseRequestHandler):

    def handle(self):
        self.data = self.request.recv(5)
        if self.data == RECV:
            self.request.sendall(SEND)


def run():
    with TCPServer((HOST, PORT), PingHandler) as server:
        logger.info(f"Health Check Server started")
        server.serve_forever()

    