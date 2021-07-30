import os

from communications.file import JsonFile, OneLineFile
from config.envvars import INPUT_QUEUE_NAME_KEY, get_config_param
from communications.rabbitmq_interface import LastHashStrategy, QueueInterface, RabbitMQConnection
import healthcheck.server
from logger.logger import Logger

logger = Logger()

ONLY_STATE = 1
STATE_AND_NEW_STATE = 2

STORAGE_DIR = "/data/"
DATA_STORAGE_DIR = STORAGE_DIR + "data_dict/"
DATA_FILE_NAME = "data_dict.json"

MASTER_SENTINEL_FILE_NAME = "master_sentinel.txt"
STATE_FILE_NAME = "state.txt"

STATE_RECEIVING = "STATE_RECEIVING"
STATE_CALCULATING = "STATE_CALCULATING"

def write_to_new_data(times_used_by_civ, actual_hash):
    files_list = os.listdir(DATA_STORAGE_DIR)
    if len(files_list) <= ONLY_STATE:
        write_dict(actual_hash, times_used_by_civ)


def rename_new_data_to_data(actual_hash):
    os.rename(DATA_STORAGE_DIR + actual_hash,
              DATA_STORAGE_DIR + DATA_FILE_NAME)


def check_if_new_state_file_is_present(input_queue):
    files_list = os.listdir(DATA_STORAGE_DIR)
    if len(files_list) == STATE_AND_NEW_STATE:
        new_state_file_name = filter(
            lambda file_name: file_name != DATA_FILE_NAME,
            files_list
        )
        if new_state_file_name in input_queue.last_hash.values():
            rename_new_data_to_data(new_state_file_name)

def get_data_dict():
    data_dict_file = JsonFile(
        DATA_STORAGE_DIR,
        DATA_FILE_NAME
    )
    data_dict = data_dict_file.content
    data_dict_file.close()
    return data_dict

def write_dict(file_name, dict):
    dict_file = JsonFile(
        DATA_STORAGE_DIR,
        file_name,
        read_content=False
    )
    dict_file.write(dict)
    dict_file.close()

def clear_data_dict():
    write_dict(DATA_FILE_NAME, {})

def calculate_stage(calculate_function, data_dict, output_queue):
    if len(data_dict.keys()):
        list_of_columns = calculate_function(logger, data_dict)
        logger.info("Sending results to output")
        output_queue.send_list_of_columns(list_of_columns)
        clear_data_dict()
    write_state(STATE_RECEIVING)


def on_sentinel_callback(_, __):
    write_state(STATE_CALCULATING)


def get_state():
    state_file = OneLineFile(
        STORAGE_DIR,
        STATE_FILE_NAME
    )
    state = state_file.content or STATE_RECEIVING
    state_file.close()
    return state


def write_state(state):
    state_file = OneLineFile(
        STORAGE_DIR,
        STATE_FILE_NAME,
        read_content=False
    )
    state_file.write(state)
    state_file.close()

def main_civ_calculator(
        output_queue_name,
        get_receive_data_function,
        calculate_function):
    healthcheck.server.start_in_new_process()
    connection = RabbitMQConnection()
    input_queue = QueueInterface(
        connection,
        get_config_param(INPUT_QUEUE_NAME_KEY, logger),
        last_hash_strategy=LastHashStrategy.LAST_HASH_PER_REDUCER_ID
    )
    output_queue = QueueInterface(
        connection, output_queue_name)

    os.makedirs(os.path.dirname(DATA_STORAGE_DIR), exist_ok=True)
    check_if_new_state_file_is_present(input_queue)
    data_dict = get_data_dict()
    
    initial_state = get_state()
    logger.debug(f'Initial state: {initial_state}')
    logger.debug(f'Initial data size: {len(data_dict.keys())}')

    if initial_state == STATE_CALCULATING:
        # allow same messages again if the next dataset have same data
        input_queue.set_last_hash("")
        calculate_stage(calculate_function, data_dict, output_queue)
        data_dict = {}

    while True:
        logger.info('Starting to receive data')
        input_queue.consume(
            get_receive_data_function(
                data_dict
            ),
            between_hash_and_ack_callback=rename_new_data_to_data,
            on_sentinel_callback=on_sentinel_callback
        )
        # allow same messages again if the next dataset have same data
        input_queue.set_last_hash("")
        calculate_stage(calculate_function, data_dict, output_queue)
        data_dict = {}