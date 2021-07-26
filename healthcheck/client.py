from logger.logger import Logger
import socket

PORT = 9999
SEND=b'PING!'
RECV=b'PONG!'
logger=Logger()

def ping(host):    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, PORT))
        sock.sendall(SEND)    
    except socket.error:
        return False    
    try:
        data=b''
        while len(data) < len(RECV):
            data += sock.recv(len(RECV) - len(data))
    except socket.error:
        return False
    if data != RECV:
        return False
    return True