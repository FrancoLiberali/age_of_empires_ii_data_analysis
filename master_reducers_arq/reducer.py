from more_itertools import first_true

from communications.constants import FROM_CLIENT_PLAYER_TOKEN_INDEX
from config.envvars import INPUT_QUEUE_NAME_KEY, REDUCER_ID_KEY, get_config_param
from communications.rabbitmq_interface import QueueInterface, RabbitMQConnection, SENTINEL_MESSAGE, SENTINEL_MESSAGE_WITH_REDUCER_ID_SEPARATOR
from logger.logger import Logger


def player_already_exists_in_list(player_columns, players_list):
    return first_true(
        players_list,
        pred=lambda player: player[FROM_CLIENT_PLAYER_TOKEN_INDEX] == player_columns[FROM_CLIENT_PLAYER_TOKEN_INDEX]
    ) is not None

def get_send_sentinel_to_master_function(logger, barrier_queue):
    def on_sentinel_callback(_, __):
        logger.info("Sending sentinel to master to notify finished")
        barrier_queue.send_string(
            f"{get_config_param(REDUCER_ID_KEY, logger)}{SENTINEL_MESSAGE_WITH_REDUCER_ID_SEPARATOR}{SENTINEL_MESSAGE}"
        )
    return on_sentinel_callback

def main_reducer(
        barrier_queue_name,
        output_queue_name,
        receive_and_reduce_function
    ):
    logger = Logger()
    connection = RabbitMQConnection()

    barrier_queue = QueueInterface(connection, barrier_queue_name)
    input_queue = QueueInterface(
        connection,
        get_config_param(INPUT_QUEUE_NAME_KEY, logger)
    )
    output_queue = QueueInterface(connection, output_queue_name)

    receive_and_reduce_function(
        input_queue,
        output_queue,
        get_send_sentinel_to_master_function(logger, barrier_queue)
    )
    connection.close()
