from logger.logger import Logger
import socket

PORT = 9999
RECV=b'PING!'
SEND=b'PONG!'
logger=Logger()

def ping(host):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((host, PORT))
        sock.sendall(SEND)        
    except socket.error:
        return False
    data=b''
    while len(data) < len(RECV):
        try:
            data += sock.recv(len(RECV) - len(data))
        except socket.error:
            return False
    if data != RECV:
        return False
    return True