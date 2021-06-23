from communications.constants import SENTINEL_KEY
from communications.rabbitmq_interface import ExchangeInterface, QueueInterface, RabbitMQConnection

def get_set_keys_function(keys):
    # python function currying
    def set_keys(queue, received_string, _):
        keys.append(received_string)
    return set_keys


def receive_keys(keys_queue):
    keys = []
    print('Waiting for keys assignement')
    keys_queue.consume(
        get_set_keys_function(keys)
    )
    print(f'Assigned keys are: {keys}')
    return keys


def subscribe_to_keys(connection, keys, input_exchange_name):
    print(f"Subscribing to keys")
    input_exchange = ExchangeInterface.newDirect(
        connection, input_exchange_name)
    input_queue = QueueInterface.newPrivate(connection)

    for key in keys + [SENTINEL_KEY]:
        input_queue.bind(input_exchange, key)
    print(f"Finished subscribing to keys")
    return input_queue


def main_reducer(
        keys_queue_name,
        barrier_queue_name,
        input_exchange_name,
        output_queue_name,
        receive_and_reduce_function,
        send_result_to_next_stage_function=None
    ):
    connection = RabbitMQConnection()

    keys_queue = QueueInterface(connection, keys_queue_name)
    barrier_queue = QueueInterface(connection, barrier_queue_name)
    output_queue = QueueInterface(connection, output_queue_name)

    keys = receive_keys(keys_queue)
    input_queue = subscribe_to_keys(
        connection, keys, input_exchange_name)
    print("Sending sentinel to master to notify ready to receive data")
    barrier_queue.send_sentinel()

    result = receive_and_reduce_function(input_queue, output_queue, keys)
    if send_result_to_next_stage_function is not None:
        send_result_to_next_stage_function(output_queue, result, keys)

    print("Sending sentinel to master to notify finished")
    barrier_queue.send_sentinel()
    connection.close()
