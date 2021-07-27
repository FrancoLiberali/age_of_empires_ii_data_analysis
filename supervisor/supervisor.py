from config.envvars import get_config_param
import multiprocessing
import healthcheck.server
import healthcheck.client
import ring.server
import ring.client
import queue
import time
import subprocess
from logger.logger import Logger


TASK_INTERVAL=4
logger = Logger()

class Supervisor:
    def __init__(self, id, supervisors, nodes):
        self.id = id
        self.supervisors = supervisors
        self.nodes = nodes
        self.leader = None
        self.leader_queue = multiprocessing.Queue()
        

    def wait_for_leader(self):
        while self.leader == None:            
            self.leader = self.leader_queue.get()
            if self.leader != None:
                if self.leader == self.id:
                    logger.info(f"I am the new leader")
                else:
                    logger.info(f"New leader elected: {self.leader}")
            else:
                logger.debug("Leader = {}".format(self.leader))
    
    def start_election(self):
        logger.info(f"Starting election...")
        if ring.client.start_election(self.id, self.supervisors):
            self.leader = None
            self.wait_for_leader()
        else:
            self.leader = self.id

    def start_node(self, node):
        result = subprocess.run(['docker', 'start', node], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logger.info('Starting {}...'.format(node))
        logger.debug('Starting {}. Result={}. Output={}. Error={}'.format(node, result.returncode, result.stdout, result.stderr))

    def node_is_down(self, node):
        if not healthcheck.client.ping(node):
            logger.info("{} is down. Starting it...".format(node))
            return True
        return False

    def do_leader_tasks(self):
        logger.debug("Doing leader tasks")
        logger.debug("Checking if all nodes are alive...")
        for node in self.nodes:
            if self.node_is_down(node):
                self.start_node(node)
        for supervisor in self.supervisors:
            if supervisor != self.id and self.node_is_down(node):
                self.start_node(node)

    def do_non_leader_tasks(self):
        logger.debug("Doing non leader tasks")
        logger.debug("Checking if leader supervisor is alive...")
        if not healthcheck.client.ping(self.leader):
            logger.info("Leader supervisor {} not responding".format(self.leader))
            self.leader = None
            self.start_election()

    def check_if_leader_changed(self):
        try:
            self.leader = self.leader_queue.get(block=False)
            self.wait_for_leader()
        except queue.Empty:
            logger.debug("Leader did not change")

    def run(self):
        ping_server = multiprocessing.Process(target=healthcheck.server.run)        
        election_server = multiprocessing.Process(target=ring.server.run, args=(self.id, self.supervisors, self.leader_queue))
        ping_server.start()
        election_server.start()
        time.sleep(2)
        self.start_election()
        while True:
            if self.leader == self.id:
                self.do_leader_tasks()
            elif self.leader != None:
                self.do_non_leader_tasks()
            self.check_if_leader_changed()
            time.sleep(TASK_INTERVAL)


def main():
    hostname=get_config_param("SUPERVISOR_NAME", logger)
    supervisors=get_config_param("SUPERVISORS", logger).split(',')
    nodes=get_config_param("NODES", logger).split(',')
    supervisor = Supervisor(hostname, supervisors, nodes)
    supervisor.run()  

if __name__ == '__main__':
    main()
