import os

from communications.file import JsonFile, OneLineFile
from config.envvars import REDUCERS_AMOUNT_KEY, REDUCERS_QUEUE_PREFIX_KEY, ROWS_CHUNK_SIZE_KEY, get_config_param
from communications.constants import SENTINEL_KEY
from communications.rabbitmq_interface import ExchangeInterface, QueueInterface, RabbitMQConnection, SENTINEL_MESSAGE, SENTINEL_MESSAGE_WITH_REDUCER_ID_SEPARATOR, split_columns_into_list
import healthcheck.server
from logger.logger import Logger
from master_reducers_arq.partition_function import PartitionFunction

logger = Logger()
ROWS_CHUNK_SIZE = get_config_param(ROWS_CHUNK_SIZE_KEY, logger)

MASTER_ID = "master"

STATE_STORAGE_DIR = "/data/"

SENTINELS_RECEIVED_FILE_NAME = "sentinels.json"
SENTINEL_RECEIVED = 1

STATE_FILE_NAME = "state.txt"
STATE_CONFIGURING_QUEUES = "STATE_CONFIGURING_QUEUES"
STATE_DISPACHING = "STATE_DISPACHING"
STATE_RECEIVING_SENTINELS = "STATE_RECEIVING_SENTINELS"

def send_sentinel_to_reducers(output_exchage):
    logger.info("Sending sentinel to reducers for alerting them than no more data will be sended.")
    output_exchage.send_sentinel(SENTINEL_KEY)

def send_dict_by_key(output_exchange, dict_by_key, tag_to_send=None, check_chunk_size=True):
    for key, rows in list(dict_by_key.items()):
        if len(rows) > ROWS_CHUNK_SIZE or (len(rows) > 0 and not check_chunk_size):
            if tag_to_send:
                rows = [tag_to_send] + rows
            output_exchange.send_list_as_rows(rows, key)
            dict_by_key.pop(key)
            del rows

def add_to_dict_by_key(output_exchange,
                       partition_function,
                       dict_by_key,
                       received_rows,
                       match_id_index,
                       tag_to_send=None):
    for row_string in received_rows:
        key = partition_function.get_key(
            split_columns_into_list(row_string)[match_id_index]
        )
        rows_list = dict_by_key.get(key, [])
        rows_list.append(row_string)
        dict_by_key[key] = rows_list

    # TODO sacar el check_chunk_size=False para volver a poner la optimizacion de chunks si queda tiempo
    send_dict_by_key(output_exchange, dict_by_key, tag_to_send=tag_to_send, check_chunk_size=False)


def get_receive_sentinel_function(sentinels_objetive):
    # python function currying
    sentinels_received_file = JsonFile(
        STATE_STORAGE_DIR, SENTINELS_RECEIVED_FILE_NAME
    )
    reducers_map = sentinels_received_file.content
    logger.debug(f"Initial reducers sentinels received: {len(reducers_map.keys())}")
    def on_sentinel_callback(reducer_id, _):
        if reducers_map.get(reducer_id, None) is None:
            reducers_map[reducer_id] = SENTINEL_RECEIVED
            sentinels_received_file.write(reducers_map)
        sentinel_received_amount = len(reducers_map.keys())
        logger.info(
            f"Recived sentinel from reducer {reducer_id}. Sentinels received: {sentinel_received_amount} / {sentinels_objetive}")
        if sentinel_received_amount == sentinels_objetive:
            return QueueInterface.STOP_CONSUMING
        return QueueInterface.NO_STOP_CONSUMING
    return on_sentinel_callback


def receive_a_sentinel_per_reducer(barrier_queue, reducers_amount):
    barrier_queue.consume(
        None,
        on_sentinel_callback=get_receive_sentinel_function(
            reducers_amount
        )
    )


def subscribe_reducers_queues_to_keys(connection, reducers_input_exchange, partition_function, reducers_amount):
    posibles_keys = partition_function.get_posibles_keys()
    logger.info(f"Starting to subscribe reducer's queues to keys: {posibles_keys}")

    reducers_queue_prefix = get_config_param(REDUCERS_QUEUE_PREFIX_KEY, logger)
    reducers_queues = []
    for i in range(1, reducers_amount + 1):
        reducers_queues.append(
            QueueInterface(connection, f"{reducers_queue_prefix}{i}")
        )

    for index, key in enumerate(posibles_keys):
        queue_to_subscribe = reducers_queues[index % reducers_amount]
        queue_to_subscribe.bind(reducers_input_exchange, key)

    for queue in reducers_queues:
        queue.bind(reducers_input_exchange, SENTINEL_KEY)
    logger.info("All reducers queues subscribed to keys")


def send_sentinel_and_go_to_dispaching(state_file, reducers_output_queue, entry_queue):
    logger.info("Sending sentinel to next stage to notify all data sended")
    send_sentinel_with_master_id_and_last_hash(
        reducers_output_queue, entry_queue
    )
    state_file.write(STATE_DISPACHING)

def receive_sentinels_stage(state_file, barrier_queue, reducers_output_queue, entry_queue, reducers_amount):
    logger.info(
        "Waiting for a sentinel per reducer that notifies they finished.")
    receive_a_sentinel_per_reducer(barrier_queue, reducers_amount)
    send_sentinel_and_go_to_dispaching(
        state_file, reducers_output_queue, entry_queue)

def delete_sentinels_received():
    try:
        os.remove(STATE_STORAGE_DIR + SENTINELS_RECEIVED_FILE_NAME)
    except FileNotFoundError:
        pass

def send_sentinel_with_master_id_and_last_hash(reducers_output_queue, entry_queue):
    reducers_output_queue.send_string(
        f"{MASTER_ID}{SENTINEL_MESSAGE_WITH_REDUCER_ID_SEPARATOR}{entry_queue.last_hash}{SENTINEL_MESSAGE_WITH_REDUCER_ID_SEPARATOR}{SENTINEL_MESSAGE}"
    )

def main_master(
        barrier_queue_name,
        reducers_output_queue_name,
        output_exchange_name,
        subscribe_to_entries_function,
        receive_and_dispach_function
    ):
    healthcheck.server.start_in_new_process()
    connection = RabbitMQConnection()

    entry_queue = subscribe_to_entries_function(connection)

    barrier_queue = QueueInterface(connection, barrier_queue_name)
    reducers_output_queue = QueueInterface(
        connection, reducers_output_queue_name)

    reducers_amount = get_config_param(REDUCERS_AMOUNT_KEY, logger)
    partition_function = PartitionFunction(reducers_amount)

    output_exchage = ExchangeInterface.newDirect(
        connection, output_exchange_name)

    state_file = OneLineFile(
        STATE_STORAGE_DIR, STATE_FILE_NAME
    )
    state = state_file.content or STATE_CONFIGURING_QUEUES
    logger.info(f"Initial state: {state}")
    if state == STATE_CONFIGURING_QUEUES:
        subscribe_reducers_queues_to_keys(
            connection,
            output_exchage,
            partition_function,
            reducers_amount
        )
        state_file.write(STATE_DISPACHING)
        logger.info("Finished reducers queues configuration")
    elif state == STATE_RECEIVING_SENTINELS:
        logger.debug("Init at receiving sentinels")
        entry_queue.set_last_hash(SENTINEL_MESSAGE)
        sentinels_received_file = JsonFile(
            STATE_STORAGE_DIR, SENTINELS_RECEIVED_FILE_NAME
        )
        sentinel_received_amount = len(sentinels_received_file.content.keys())
        sentinels_received_file.close()
        if sentinel_received_amount == reducers_amount:
            send_sentinel_and_go_to_dispaching()
        else:
            receive_sentinels_stage(
                state_file, barrier_queue, reducers_output_queue,
                entry_queue, reducers_amount
            )
    while True:
        delete_sentinels_received()
        receive_and_dispach_function(
            entry_queue,
            output_exchage,
            partition_function,
            state_file
        )
        receive_sentinels_stage(
            state_file, barrier_queue, reducers_output_queue,
            entry_queue, reducers_amount
        )
