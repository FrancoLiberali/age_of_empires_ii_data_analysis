import socket

from healthcheck.constants import SEND, RECV
from logger.logger import Logger

PORT = 9999
logger = Logger()

def ping(host):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, PORT))
        sock.sendall(SEND)
        logger.debug("Client Sent Request Ping")
    except socket.error:
        return False
    try:
        data=b''
        while len(data) < len(RECV):
            data += sock.recv(len(RECV) - len(data))
            logger.debug("Client Receive: data={} len(data)={}".format(data, len(data)))
    except socket.error:
        return False
    if data != RECV:
        logger.debug("Ping: received {}. Returned false".format(data))
        return False
    return True
