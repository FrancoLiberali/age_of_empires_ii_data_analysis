from config.envvars import NODES_KEY, get_config_param
from logger.logger import Logger
import random
import subprocess
import time

logger = Logger()

TIME_BETWEEN_STOPS = 2

def main():
    nodes = get_config_param(NODES_KEY, logger).split(',')
    while True:
        node_to_stop = random.choice(nodes)
        result = subprocess.run(['docker', 'stop', node_to_stop],
                                check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logger.info(f'Command executed. Result={result.returncode}. Output={result.stdout}. Error={result.stderr}')
        time.sleep(TIME_BETWEEN_STOPS)

if __name__ == '__main__':
    main()
