import socket

from logger.logger import Logger

SEND=b'PING!'
RECV=b'PONG!'

PORT = 9999
logger = Logger()

def ping(host):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, PORT))
        sock.sendall(SEND)
        logger.debug(f"Client Sent Request Ping to {host}")
    except socket.error:
        return False
    try:
        data=b''
        while len(data) < len(RECV):
            data += sock.recv(len(RECV) - len(data))
            logger.debug(f"Client Receive from {host}: data={data} len(data)={len(data)}")
    except socket.error:
        return False
    if data != RECV:
        logger.debug(f"Ping: received {data} from {host}. Returned false")
        return False
    return True
