from config.envvars import REDUCERS_AMOUNT_KEY, get_config_param
from communications.constants import SENTINEL_KEY
from communications.rabbitmq_interface import ExchangeInterface, QueueInterface, RabbitMQConnection
from logger.logger import Logger
from master_reducers_arq.partition_function import PartitionFunction

logger = Logger()


def get_receive_sentinel_function(sentinel_received_amount, sentinels_objetive):
    # python function currying
    def receive_sentinel():
        sentinel_received_amount[0] += 1
        logger.info(
            f"Sentinels from reducers received: {sentinel_received_amount[0]} / {sentinels_objetive}")
        if sentinel_received_amount[0] == sentinels_objetive:
            return QueueInterface.STOP_CONSUMING
        return QueueInterface.NO_STOP_CONSUMING
    return receive_sentinel


def receive_a_sentinel_per_reducer(barrier_queue, reducers_amount):
    sentinel_received_amount = [0]  # using a list to pass by reference
    barrier_queue.consume(
        None,
        get_receive_sentinel_function(
            sentinel_received_amount,
            reducers_amount
        )
    )


def send_keys_to_reducers(keys_queue, partition_function, reducers_amount):
    posibles_keys = partition_function.get_posibles_keys()
    logger.info(f"Starting to send keys to reducers: {posibles_keys}")
    for key in posibles_keys:
        # as it is round robin, all reducers will get equitative keys amount
        keys_queue.send_string(key)
    logger.info("All keys sended, sending sentinels to notify reducers that no more keys are going to be sended")
    for _ in range(0, reducers_amount):
        keys_queue.send_sentinel()


def main_master(
        keys_queue_name,
        barrier_queue_name,
        reducers_output_queue_name,
        output_exchange_name,
        subscribe_to_entries_function,
        receive_and_dispach_function
    ):
    connection = RabbitMQConnection()

    entry_queue = subscribe_to_entries_function(connection)

    keys_queue = QueueInterface(connection, keys_queue_name)
    barrier_queue = QueueInterface(connection, barrier_queue_name)
    reducers_output_queue = QueueInterface(
        connection, reducers_output_queue_name)

    reducers_amount = get_config_param(REDUCERS_AMOUNT_KEY, logger)
    partition_function = PartitionFunction(reducers_amount)

    send_keys_to_reducers(keys_queue, partition_function,
                          reducers_amount)
    logger.info("Waiting for a sentinel per reducer that notifies they are subscribed to the corresponding keys")
    receive_a_sentinel_per_reducer(barrier_queue, reducers_amount)

    output_exchage = ExchangeInterface.newDirect(
        connection, output_exchange_name)
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
    reducers_output_queue.send_sentinel()

    connection.close()
