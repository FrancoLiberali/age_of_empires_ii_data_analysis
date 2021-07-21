from config.envvars import INPUT_QUEUE_NAME_KEY, get_config_param
from communications.rabbitmq_interface import QueueInterface, RabbitMQConnection
from logger.logger import Logger


def main_reducer(
        barrier_queue_name,
        output_queue_name,
        receive_and_reduce_function,
        send_result_to_next_stage_function=None
    ):
    logger = Logger()
    connection = RabbitMQConnection()

    barrier_queue = QueueInterface(connection, barrier_queue_name)
    input_queue = QueueInterface(
        connection,
        get_config_param(INPUT_QUEUE_NAME_KEY, logger)
    )
    output_queue = QueueInterface(connection, output_queue_name)

    result = receive_and_reduce_function(input_queue, output_queue)
    if send_result_to_next_stage_function is not None:
        send_result_to_next_stage_function(output_queue, result)

    logger.info("Sending sentinel to master to notify finished")
    barrier_queue.send_sentinel()
    connection.close()
