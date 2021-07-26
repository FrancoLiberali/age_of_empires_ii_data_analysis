from logger.logger import Logger
import socket
import json
import struct

PORT=9998
logger=Logger()

def send_neighbour(host_id, nodes, message):
    neighbour_node_pos = nodes.index(host_id) + 1
    while neighbour_node_pos % len(nodes) != nodes.index(host_id):
        neighbour_node = nodes[neighbour_node_pos % len(nodes)]
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((neighbour_node, PORT))
            msg = json.dumps(message)
            sock.sendall(struct.pack('!i'+ str(len(msg)) +'s', len(msg), msg.encode('utf-8')))
            logger.info(f"{message['type']} message sent to {neighbour_node} with {message['leader']} as proposed leader")
            return True
        except socket.error as err:
            logger.info(f"Socket Error: {err.strerror} connecting to {neighbour_node}")
            neighbour_node_pos += 1
                       
    return False


def send_election_message(voted_id, host_id, nodes):
    message = {
        'type': 'ELECTION',
        'leader': voted_id,
        'sender': host_id,
    }
    return send_neighbour(host_id, nodes, message)
    
def send_coordinator_message(leader_id, host_id, nodes):
    message = {
        'type': 'COORDINATOR',
        'leader': leader_id,
        'sender': host_id,
    }
    return send_neighbour(host_id, nodes, message)
    

def start_election(host_id, nodes):
    return send_election_message(host_id, host_id, nodes)