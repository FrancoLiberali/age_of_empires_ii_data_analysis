import pika
import os

from communications.constants import STRING_ENCODING, \
    RABBITMQ_HOST, \
    SENTINEL_MESSAGE, \
    SENTINEL_KEY
from communications.rabbitmq_interface import send_sentinel_to_queue, send_string_to_queue
from partition_function.partition_function import PartitionFunction


def get_receive_sentinel_function(sentinel_received_amount, sentinels_objetive):
    # python function currying
    def receive_sentinel(channel, method, properties, body):
        if body.decode(STRING_ENCODING) == SENTINEL_MESSAGE:
            sentinel_received_amount[0] += 1
            print(
                f"Sentinels from reducers received: {sentinel_received_amount[0]} / {sentinels_objetive}")
            if sentinel_received_amount[0] == sentinels_objetive:
                channel.stop_consuming()
        channel.basic_ack(delivery_tag=method.delivery_tag)
    return receive_sentinel

def receive_a_sentinel_per_reducer(channel, reducers_amount, barrier_queue_name):
    channel.queue_declare(queue=barrier_queue_name)
    sentinel_received_amount = [0]  # using a list to pass by reference
    channel.basic_consume(
        queue=barrier_queue_name,
        on_message_callback=get_receive_sentinel_function(
            sentinel_received_amount,
            reducers_amount),
    )
    channel.start_consuming()

def send_keys_to_reducers(channel, partition_function, reducers_amount, keys_queue_name):
    channel.queue_declare(queue=keys_queue_name)

    posibles_keys = partition_function.get_posibles_keys()
    print(f"Starting to send keys to reducers: {posibles_keys}")
    for key in posibles_keys:
        # as it is round robin, all reducers will get equitative keys amount
        send_string_to_queue(
            channel,
            keys_queue_name,
            key
        )
    print("All keys sended, sending sentinels to notify reducers that no more keys are going to be sended")
    for _ in range(0, reducers_amount):
        send_sentinel_to_queue(
            channel, keys_queue_name)


# TODO unificar con el client
def send_sentinel_to_reducers(channel, output_exchange_name):
    channel.basic_publish(exchange=output_exchange_name,
                          routing_key=SENTINEL_KEY,
                          body=SENTINEL_MESSAGE.encode(STRING_ENCODING))


def main_master(
        keys_queue_name,
        barrier_queue_name,
        reducers_output_queue_name,
        output_exchange_name,
        subscribe_to_entries_function,
        receive_and_dispach_function
    ):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    entry_queue_name = subscribe_to_entries_function(channel)

    # TODO usar codigo unificado cuando esté
    reducers_amount = int(os.environ["REDUCERS_AMOUNT"])
    partition_function = PartitionFunction(reducers_amount)

    send_keys_to_reducers(channel, partition_function,
                          reducers_amount, keys_queue_name)
    print("Waiting for a sentinel per reducer that notifies they are subscribed to the corresponding keys")
    receive_a_sentinel_per_reducer(
        channel, reducers_amount, barrier_queue_name)

    channel.exchange_declare(
        exchange=output_exchange_name,
        exchange_type='direct')
    receive_and_dispach_function(
        channel, entry_queue_name, partition_function)

    print("Sending sentinel to reducers for alerting them than no more data will be sended.")
    send_sentinel_to_reducers(channel, output_exchange_name)
    print("Waiting for a sentinel per reducer that notifies they finished.")
    receive_a_sentinel_per_reducer(
        channel, reducers_amount, barrier_queue_name)

    print("Sending sentinel to next stage to notify all data sended")
    send_sentinel_to_queue(channel, reducers_output_queue_name)

    connection.close()