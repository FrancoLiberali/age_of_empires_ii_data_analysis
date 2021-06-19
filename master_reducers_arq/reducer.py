import pika

from communications.constants import SENTINEL_KEY, \
    STRING_ENCODING, \
    RABBITMQ_HOST, \
    SENTINEL_MESSAGE
from communications.rabbitmq_interface import send_sentinel_to_queue

def get_set_keys_function(keys):
    # python function currying
    def set_keys(channel, method, properties, body):
        chunk_string = body.decode(STRING_ENCODING)
        if chunk_string == SENTINEL_MESSAGE:
            print("Sentinel message received, stoping receiving keys")
            channel.stop_consuming()
        else:
            keys.append(chunk_string)
        channel.basic_ack(delivery_tag=method.delivery_tag)
    return set_keys


def receive_keys(channel, keys_queue_name):
    channel.queue_declare(queue=keys_queue_name)
    keys = []
    channel.basic_consume(
        queue=keys_queue_name,
        on_message_callback=get_set_keys_function(keys)
    )
    print('Waiting for keys assignement')
    channel.start_consuming()
    print(f'Assigned keys are: {keys}')
    return keys


def subscribe_to_keys(channel, keys, input_exchange_name):
    print(f"Subscribing to keys")
    channel.exchange_declare(
        exchange=input_exchange_name,
        exchange_type='direct')

    result = channel.queue_declare(queue='')
    private_queue_name = result.method.queue
    for key in keys + [SENTINEL_KEY]:
        channel.queue_bind(
            exchange=input_exchange_name,
            queue=private_queue_name,
            routing_key=key)
    print(f"Finished subscribing to keys")
    return private_queue_name


def send_sentinel_to_master(channel, barrier_queue_name):
    channel.queue_declare(queue=barrier_queue_name)
    send_sentinel_to_queue(channel, barrier_queue_name)

def main_reducer(
        keys_queue_name,
        barrier_queue_name,
        input_exchange_name,
        receive_and_reduce_function,
        send_result_to_next_stage_function=None
    ):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    keys = receive_keys(channel, keys_queue_name)
    private_queue_name = subscribe_to_keys(channel, keys, input_exchange_name)
    print("Sending sentinel to master to notify ready to receive data")
    send_sentinel_to_master(channel, barrier_queue_name)

    result = receive_and_reduce_function(channel, private_queue_name, keys)
    if send_result_to_next_stage_function is not None:
        send_result_to_next_stage_function(channel, result, keys)

    print("Sending sentinel to master to notify finished")
    send_sentinel_to_master(channel, barrier_queue_name)
    connection.close()
