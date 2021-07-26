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


TASK_INTERVAL=3
logger = Logger()

class Supervisor:
    def __init__(self, id, supervisors, nodes):
        self.id = id
        self.supervisors = supervisors
        self.nodes = nodes
        self.leader = None
        self.leader_queue = multiprocessing.Queue()
        

    def start_election(self):
        logger.info(f"Starting election")
        if ring.client.start_election(self.id, self.supervisors):
            self.leader = None
            while self.leader == None:
                self.leader = self.leader_queue.get()
                if self.leader != None:
                    logger.info(f"New leader elected: {self.leader}")
        else:
            self.leader = self.id

    def start_node(self, node):
        result = subprocess.run(['docker', 'start', node], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logger.info('Command executed. Result={}. Output={}. Error={}'.format(result.returncode, result.stdout, result.stderr))

    def do_leader_tasks(self):
        logger.info(f"Doing leader tasks")
        for node in self.nodes:
            if not healthcheck.client.ping(node):
                logger.info("Starting {}".format(node))
                self.start_node(node)

    def do_non_leader_tasks(self):
        logger.info(f"Doing non leader tasks")
        if not healthcheck.client.ping(self.leader):
            logger.info(f"Leader supervisor not responding")
            self.leader = None
            self.start_election()

    def check_if_leader_changed(self):
        try:
            self.leader = self.leader_queue.get(block=False)
            while self.leader == None:
                self.leader = self.leader_queue.get()
            logger.info(f"New leader: {self.leader}")
        except queue.Empty:
            pass

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
