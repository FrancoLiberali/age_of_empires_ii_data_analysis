import multiprocessing
import socket
import socketserver
from socketserver import TCPServer

from logger.logger import Logger

RECV=b'PING!'
SEND=b'PONG!'

PORT = 9999
HOST = ''

logger = Logger()

class PingHandler(socketserver.BaseRequestHandler):
    def handle(self):
        data=b''
        try:
            while len(data) < len(RECV):
                data += self.request.recv(len(RECV) - len(data))
                # logger.debug("Server Receive: data={} len(data)={}".format(data, len(data)))
        except socket.error as err:
            # logger.debug("PingHandler: Socket error receiving ping request: {}".format(err.strerror))
            return
        if data == RECV:
            # logger.debug("Received PING!")
            try:
                self.request.sendall(SEND)
                # logger.debug("Server Sent Ping Response")
            except socket.error as err:
                pass


def run():
    with TCPServer((HOST, PORT), PingHandler) as server:
        logger.info(f"Health Check Server started")
        server.serve_forever()

def start_in_new_process():
    ping_server = multiprocessing.Process(target=run)
    ping_server.start()
    return ping_server
