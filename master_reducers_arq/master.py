from config.envvars import REDUCERS_AMOUNT_KEY, REDUCERS_QUEUE_PREFIX_KEY, get_config_param
from communications.constants import SENTINEL_KEY
from communications.rabbitmq_interface import ExchangeInterface, QueueInterface, RabbitMQConnection, SENTINEL_MESSAGE, SENTINEL_MESSAGE_WITH_REDUCER_ID_SEPARATOR
from logger.logger import Logger
from master_reducers_arq.partition_function import PartitionFunction

logger = Logger()


def get_receive_sentinel_function(sentinel_received_amount, sentinels_objetive):
    # python function currying
    def receive_sentinel(reducer_id):
        sentinel_received_amount[0] += 1
        logger.info(
            f"Recived sentinel from reducer {reducer_id}. Sentinels received: {sentinel_received_amount[0]} / {sentinels_objetive}")
        if sentinel_received_amount[0] == sentinels_objetive:
            return QueueInterface.STOP_CONSUMING
        return QueueInterface.NO_STOP_CONSUMING
    return receive_sentinel


def receive_a_sentinel_per_reducer(barrier_queue, reducers_amount):
    sentinel_received_amount = [0]  # using a list to pass by reference
    barrier_queue.consume(
        None,
        on_sentinel_callback=get_receive_sentinel_function(
            sentinel_received_amount,
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


def main_master(
        barrier_queue_name,
        reducers_output_queue_name,
        output_exchange_name,
        subscribe_to_entries_function,
        receive_and_dispach_function,
        send_last_sentinel_function
    ):
    connection = RabbitMQConnection()

    entry_queue = subscribe_to_entries_function(connection)

    barrier_queue = QueueInterface(connection, barrier_queue_name)
    reducers_output_queue = QueueInterface(
        connection, reducers_output_queue_name)

    reducers_amount = get_config_param(REDUCERS_AMOUNT_KEY, logger)
    partition_function = PartitionFunction(reducers_amount)

    output_exchage = ExchangeInterface.newDirect(
        connection, output_exchange_name)
    subscribe_reducers_queues_to_keys(
        connection,
        output_exchage,
        partition_function,
        reducers_amount
    )

    receive_and_dispach_function(
        entry_queue,
        output_exchage,
        partition_function
    )

    logger.info("Sending sentinel to reducers for alerting them than no more data will be sended.")
    output_exchage.send_sentinel(SENTINEL_KEY)
    logger.info("Waiting for a sentinel per reducer that notifies they finished.")
    receive_a_sentinel_per_reducer(barrier_queue, reducers_amount)

    logger.info("Sending sentinel to next stage to notify all data sended")
    send_last_sentinel_function(reducers_output_queue, entry_queue)

    connection.close()


def send_normal_sentinel(reducers_output_queue, _):
    reducers_output_queue.send_sentinel()


MASTER_ID = "master"

def send_sentinel_with_master_id_and_last_hash(reducers_output_queue, entry_queue):
    reducers_output_queue.send_string(
        f"{MASTER_ID}{SENTINEL_MESSAGE_WITH_REDUCER_ID_SEPARATOR}{entry_queue.last_hash}{SENTINEL_MESSAGE_WITH_REDUCER_ID_SEPARATOR}{SENTINEL_MESSAGE}"
    )
