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
        try:
            self.data = self.request.recv(len(RECV))
        except socket.errror as err:
            logger.info("PingHandler: Socket error receiving ping request: {}".format(err.strerror))
            return
        if self.data == RECV:
            try:
                self.request.sendall(SEND)
            except socket.error as err:
                logger.info("PingHandler: Socket error responding ping request: {}".format(err.strerror))


def run():
    with TCPServer((HOST, PORT), PingHandler) as server:
        logger.info(f"Health Check Server started")
        server.serve_forever()

    