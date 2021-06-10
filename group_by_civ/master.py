import pika
import os

from communications.constants import FROM_CLIENT_PLAYER_MATCH_INDEX, \
    GROUP_BY_CIV_MASTER_TO_REDUCERS_EXCHANGE_NAME, \
    GROUP_BY_CIV_MASTER_TO_REDUCERS_QUEUE_NAME, \
    GROUP_BY_CIV_REDUCERS_BARRIER_QUEUE_NAME, \
    GROUP_BY_CIV_REDUCERS_TO_WINNER_RATE_CALCULATOR_QUEUE_NAME, \
    JOIN_REDUCERS_TO_GROUP_BY_CIV_MASTER_QUEUE_NAME, \
    STRING_COLUMN_SEPARATOR, \
    STRING_ENCODING, \
    RABBITMQ_HOST, \
    SENTINEL_MESSAGE, \
    STRING_LINE_SEPARATOR, \
    SENTINEL_KEY
from communications.rabbitmq_interface import send_sentinel_to_queue, send_string_to_queue
from partition_function.partition_function import PartitionFunction

PLAYERS_INPUT_QUEUE_NAME = JOIN_REDUCERS_TO_GROUP_BY_CIV_MASTER_QUEUE_NAME # TODO envvar
OUTPUT_EXCHANGE_NAME = GROUP_BY_CIV_MASTER_TO_REDUCERS_EXCHANGE_NAME  # TODO envvar
KEYS_QUEUE_NAME = GROUP_BY_CIV_MASTER_TO_REDUCERS_QUEUE_NAME  # TODO envvar
BARRIER_QUEUE_NAME = GROUP_BY_CIV_REDUCERS_BARRIER_QUEUE_NAME  # TODO envvar
REDUCERS_OUTPUT_QUEUE_NAME = GROUP_BY_CIV_REDUCERS_TO_WINNER_RATE_CALCULATOR_QUEUE_NAME  # TODO envvar

def get_receive_sentinel_function(sentinel_received_amount, sentinels_objetive):
    # python function currying
    def receive_sentinel(channel, method, properties, body):
        if body.decode(STRING_ENCODING) == SENTINEL_MESSAGE:
            sentinel_received_amount[0] += 1
            print(
                f"Sentinels from group by match reducers received: {sentinel_received_amount[0]} / {sentinels_objetive}")
            if sentinel_received_amount[0] == sentinels_objetive:
                channel.stop_consuming()
        channel.basic_ack(delivery_tag=method.delivery_tag)
    return receive_sentinel


def receive_a_sentinel_per_reducer(channel, reducers_amount):
    channel.queue_declare(queue=BARRIER_QUEUE_NAME)
    sentinel_received_amount = [0]  # using a list to pass by reference
    channel.basic_consume(
        queue=BARRIER_QUEUE_NAME,
        on_message_callback=get_receive_sentinel_function(
            sentinel_received_amount,
            reducers_amount),
    )
    channel.start_consuming()


PLAYERS_CHUNK_SIZE = 100  # TODO envvar, es muy importante


def send_players_by_key(channel, players_by_key, check_chunk_size=True):
    for key, players in list(players_by_key.items()):
        if len(players) > PLAYERS_CHUNK_SIZE or not check_chunk_size:
            players_string = STRING_LINE_SEPARATOR.join(players)
            channel.basic_publish(exchange=OUTPUT_EXCHANGE_NAME,
                                  routing_key=key,
                                  body=players_string.encode(STRING_ENCODING))
            players_by_key.pop(key)
            del players


def add_to_players_by_key(channel, partition_function, players_by_key, received_players):
    for player_string in received_players:
        key = partition_function.get_key(
            player_string.split(STRING_COLUMN_SEPARATOR)[FROM_CLIENT_PLAYER_MATCH_INDEX]
        )
        rows_list = players_by_key.get(key, [])
        rows_list.append(player_string)
        players_by_key[key] = rows_list

    send_players_by_key(channel, players_by_key)

# TODO unificar con el client


def send_sentinel_to_reducers(channel):
    channel.basic_publish(exchange=OUTPUT_EXCHANGE_NAME,
                          routing_key=SENTINEL_KEY,
                          body=SENTINEL_MESSAGE.encode(STRING_ENCODING))


def get_dispach_to_reducers_function(players_by_key, partition_function):
    def dispach_to_reducers(channel, method, properties, body):
        chunk_string = body.decode(STRING_ENCODING)
        if chunk_string == SENTINEL_MESSAGE:
            print(
                "Sentinel message received, stoping receive players and dispach it to reducers. Sending sentinel to reducers for alerting them than no more players will come.")
            channel.stop_consuming()
            # send the remaining players
            send_players_by_key(channel, players_by_key, False)
            send_sentinel_to_reducers(channel)
        else:
            received_players = [
                player_string for player_string in chunk_string.split(STRING_LINE_SEPARATOR)]
            add_to_players_by_key(
                channel,
                partition_function,
                players_by_key,
                received_players
            )
        channel.basic_ack(delivery_tag=method.delivery_tag)
    return dispach_to_reducers


def receive_and_dispach_players(channel, partition_function, reducers_amount):
    channel.queue_declare(queue=PLAYERS_INPUT_QUEUE_NAME)

    channel.exchange_declare(
        exchange=OUTPUT_EXCHANGE_NAME,
        exchange_type='direct')

    players_by_key = {}
    channel.basic_consume(
        queue=PLAYERS_INPUT_QUEUE_NAME,
        on_message_callback=get_dispach_to_reducers_function(
            players_by_key, partition_function),
    )
    print("Starting to receive players from client and dispach it to reducers by key")
    channel.start_consuming()

    print("Waiting for a sentinel per reducer that notifies they finished grouping")
    receive_a_sentinel_per_reducer(channel, reducers_amount)


def send_keys_to_reducers(channel, partition_function, reducers_amount):
    channel.queue_declare(queue=KEYS_QUEUE_NAME)

    posibles_keys = partition_function.get_posibles_keys()
    print(f"Starting to send keys to reducers: {posibles_keys}")
    for key in posibles_keys:
        # as it is round robin, all reducers will get equitative keys amount
        send_string_to_queue(
            channel,
            KEYS_QUEUE_NAME,
            key
        )
    print("All keys sended, sending sentinels to notify reducers that no more keys are going to be sended")
    for _ in range(0, reducers_amount):
        send_sentinel_to_queue(
            channel, KEYS_QUEUE_NAME)

    print("Waiting for a sentinel per reducer that notifies they are subscribed to the corresponding keys")
    receive_a_sentinel_per_reducer(channel, reducers_amount)


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    # TODO usar codigo unificado cuando esté
    reducers_amount = int(os.environ["REDUCERS_AMOUNT"])
    partition_function = PartitionFunction(reducers_amount)

    send_keys_to_reducers(channel, partition_function, reducers_amount)

    receive_and_dispach_players(channel, partition_function, reducers_amount)

    print("Sending sentinel to client to notify all civs sended")
    channel.queue_declare(
        queue=REDUCERS_OUTPUT_QUEUE_NAME)
    send_sentinel_to_queue(channel, REDUCERS_OUTPUT_QUEUE_NAME)

    connection.close()


if __name__ == '__main__':
    main()
