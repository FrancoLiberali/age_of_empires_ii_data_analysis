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
        sock.sendall(RECV)        
    except socket.error:
        logger.info(f"Sent ping request to {host} and did not respond")
        return False
    data = sock.recv(5)
    logger.info(f"Sent ping request to {host} and got response")
    return True