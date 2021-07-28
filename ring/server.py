import json
import ring
import socket
import struct

from logger.logger import Logger

HOST = ''
PORT = 9998
ELECTION = 'ELECTION'
COORDINATOR = 'COORDINATOR'
logger = Logger()

class RingElectionServer:
    def __init__(self, hostname, nodes, out_queue):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((HOST, PORT))
        self.server_socket.listen()
        self.hostname = hostname
        self.nodes = nodes
        self.participating = False
        self.leader = None
        self.out_queue = out_queue

    def declare_as_leader(self):
        self.participating=False
        self.leader = self.hostname
        self.out_queue.put(self.hostname)

    def participate(self):
        self.out_queue.put(None)
        self.leader = None
        self.participating = True

    def handle_election_request(self, request):
        # the election went all the way round and the host continues being the leader -> declare leader
        if request['leader'] == self.hostname:
            self.participating = False
            ring.client.send_coordinator_message(self.hostname, self.hostname, self.nodes)
            return
        # incoming voted leader is greater than host -> forward incoming leader
        if (request['leader'] > self.hostname):
            if ring.client.send_election_message(request['leader'], self.hostname, self.nodes):
                if not self.participating:
                    self.participate()
            else:
                self.declare_as_leader()
            return
        # incoming voted leader is smaller than host -> forward host as leader  
        if (request['leader'] < self.hostname) and not self.participating:
            if ring.client.send_election_message(self.hostname, self.hostname, self.nodes):
                self.participate()
            else:
                self.declare_as_leader()
        else:
            logger.debug(f"Discarded election sent by {request['sender']} proposing {request['leader']}")

    def handle_coordinator_request(self, request):
        self.out_queue.put(request['leader'])
        self.leader = request['leader']
        self.participating = False
        if request['leader'] != self.hostname:
            ring.client.send_coordinator_message(request['leader'], self.hostname,self.nodes)

    def recv_all(self, sock, size):
        buf=b''
        while len(buf)<size:
            buf += sock.recv(size-len(buf))
        return buf

    def handle_client(self, sock):
        data_size = struct.unpack('!i', self.recv_all(sock, 4))[0]
        data = self.recv_all(sock, data_size)
        request = json.loads(data) 
        if request['type'] == ELECTION:
            self.handle_election_request(request)
        if request['type'] == COORDINATOR:
            self.handle_coordinator_request(request)

    def accept_new_connection(self):
        c, addr = self.server_socket.accept()
        return c

    def run(self):
         while True:
            client_sock = self.accept_new_connection()
            self.handle_client(client_sock)

def run(hostname, nodes, out_queue):
    server = RingElectionServer(hostname, nodes, out_queue)
    server.run()
